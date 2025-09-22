use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::{Request as AxRequest, State};
use axum::http::HeaderValue;
use axum::response::Response;
use serde_json::json;
use tracing::info;
use uuid::Uuid;

use crate::app_state::AppState;
use crate::caches::{IP_LOG, IpAccessEntry, prune_ip_bucket};

// Request logging middleware with safe body handling and header redaction.
pub(crate) async fn log_requests_mw(
    State(state): State<AppState>,
    mut req: AxRequest,
    next: axum::middleware::Next,
) -> Response {
    if !state.log_requests {
        return next.run(req).await;
    }

    let req_id = Uuid::new_v4().to_string()[..12].to_string();
    let method = req.method().clone();
    let uri = req.uri().clone();
    let connect_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0);
    let headers = req.headers().clone();
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    // snapshot headers (all or minimal)
    let mut hdr_map = serde_json::Map::new();
    if state.log_headers_mode_all {
        for (k, v) in headers.iter() {
            let val = v.to_str().unwrap_or("");
            hdr_map.insert(
                k.to_string(),
                json!(redact_header(k.as_str(), val, state.log_redact)),
            );
        }
    } else {
        let minimal = [
            "user-agent",
            "content-type",
            "range",
            "content-length",
            "accept",
            "referer",
            "origin",
        ];
        for &k in &minimal {
            if let Some(v) = headers.get(k) {
                hdr_map.insert(
                    k.to_string(),
                    json!(redact_header(k, v.to_str().unwrap_or(""), state.log_redact)),
                );
            } else {
                hdr_map.insert(k.to_string(), json!("-"));
            }
        }
    }

    // Optionally log JSON body, without consuming it for downstream handlers.
    // Read the full body into memory, log a truncated snippet, and restore it.
    let mut body_snippet: Option<String> = None;
    let should_log_body = state.log_body_all
        || (state.log_json_body && ct.to_ascii_lowercase().contains("application/json"));
    if should_log_body {
        // Only read body when Content-Length exists and is within safe bounds.
        let cl_opt = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok());
        let hard_skip_threshold = state.log_body_max.saturating_mul(4);
        match cl_opt {
            None => {
                // Unknown length (chunked or missing): skip reading to avoid unbounded memory.
                body_snippet = Some("<skipped unknown content-length>".to_string());
            }
            Some(cl) if cl > hard_skip_threshold => {
                body_snippet = Some(format!("<skipped large body: content-length={cl}>"));
            }
            Some(_) => {
                let (parts, body) = req.into_parts();
                // Read full body (bounded by CL) and restore; log truncated snippet only.
                match axum::body::to_bytes(body, usize::MAX).await {
                    Ok(bytes) => {
                        let slice_len = std::cmp::min(bytes.len(), state.log_body_max);
                        if slice_len > 0 {
                            let s = String::from_utf8_lossy(&bytes[..slice_len]).to_string();
                            if !s.is_empty() {
                                body_snippet = Some(s);
                            }
                        }
                        req = AxRequest::from_parts(parts, Body::from(bytes));
                    }
                    Err(_) => {
                        req = AxRequest::from_parts(parts, Body::empty());
                    }
                }
            }
        }
    }

    info!(
        target: "fakehub",
        "[{}] HTTP {} {}",
        req_id,
        method,
        uri,
    );
    info!(target: "fakehub", "[{}] Headers: {}", req_id, serde_json::to_string(&hdr_map).unwrap_or_default());
    if let Some(ref s) = body_snippet {
        info!(target: "fakehub", "[{}] Body[<= {}]: {}", req_id, state.log_body_max, s);
    }

    let started = std::time::Instant::now();
    let mut resp = next.run(req).await;
    let dur_ms = started.elapsed().as_millis();
    let status = resp.status();
    // attach X-Request-ID before logging to avoid borrow conflicts
    let _ = resp.headers_mut().insert(
        "X-Request-ID",
        HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("-")),
    );

    // Re-read after mutation
    let resp_ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    let resp_len = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");

    info!(
        target: "fakehub",
        "[{}] Response {} -> {} ({} ms) ct={} len={}",
        req_id,
        method,
        status.as_u16(),
        dur_ms,
        resp_ct,
        resp_len
    );
    if state.log_resp_headers {
        let mut hdrs = serde_json::Map::new();
        for (k, v) in resp.headers().iter() {
            let val = v.to_str().unwrap_or("");
            hdrs.insert(
                k.to_string(),
                json!(redact_header(k.as_str(), val, state.log_redact)),
            );
        }
        info!(target: "fakehub", "[{}] Response headers: {}", req_id, serde_json::to_string(&hdrs).unwrap_or_default());
    }

    if let Some(ip_key) = extract_client_ip(&headers, connect_ip) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let path = uri
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| uri.path().to_string());
        let retention_ms_u64 = state.ip_log_retention_secs.saturating_mul(1000);
        let retention_ms = std::cmp::min(retention_ms_u64, i64::MAX as u64) as i64;
        let per_ip_cap = state.ip_log_per_ip_cap;
        let mut map = IP_LOG.write().await;
        let bucket = map.entry(ip_key).or_insert_with(VecDeque::new);
        prune_ip_bucket(bucket, now_ms, retention_ms);
        if bucket.len() >= per_ip_cap {
            while bucket.len() >= per_ip_cap {
                bucket.pop_front();
            }
        }
        bucket.push_back(IpAccessEntry {
            at_ms: now_ms,
            method: method.to_string(),
            path,
            status: status.as_u16(),
        });
    }

    resp
}

fn redact_header(key: &str, val: &str, redact: bool) -> String {
    if !redact {
        return val.to_string();
    }
    let k = key.to_ascii_lowercase();
    if [
        "authorization",
        "cookie",
        "set-cookie",
        "proxy-authorization",
        "x-api-key",
        "x-hf-token",
    ]
    .contains(&k.as_str())
    {
        "***".to_string()
    } else {
        val.to_string()
    }
}

fn extract_client_ip(
    headers: &axum::http::HeaderMap,
    connect: Option<SocketAddr>,
) -> Option<String> {
    if let Some(val) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        for part in val.split(',') {
            let trimmed = part.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    if let Some(val) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
        let trimmed = val.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    connect.map(|addr| addr.ip().to_string())
}
