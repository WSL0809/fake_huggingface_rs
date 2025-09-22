use std::cmp;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use serde_json::json;

use crate::app_state::AppState;
use crate::caches::{IP_LOG, IpAccessEntry, prune_ip_bucket};

#[derive(Deserialize)]
pub struct IpLogQuery {
    pub ip: String,
    pub mins: Option<u64>,
    pub limit: Option<usize>,
}

pub async fn get_ip_log(
    State(state): State<AppState>,
    Query(params): Query<IpLogQuery>,
) -> impl IntoResponse {
    let IpLogQuery { ip, mins, limit } = params;
    let ip = ip.trim().to_string();
    if ip.is_empty() {
        return crate::http_error(StatusCode::BAD_REQUEST, "ip required");
    }

    let req_window_secs = mins
        .and_then(|m| m.checked_mul(60))
        .unwrap_or(state.ip_log_retention_secs);
    let window_secs = req_window_secs.min(state.ip_log_retention_secs).max(60);
    let window_ms_u64 = window_secs.saturating_mul(1000);
    let window_ms = cmp::min(window_ms_u64, i64::MAX as u64) as i64;

    let limit = limit
        .unwrap_or(state.ip_log_per_ip_cap)
        .min(state.ip_log_per_ip_cap)
        .max(1);

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let cutoff = now_ms.saturating_sub(window_ms);

    let mut returned: Vec<IpAccessEntry> = Vec::new();
    let mut total = 0usize;

    {
        let mut map = IP_LOG.write().await;
        if let Some(bucket) = map.get_mut(ip.as_str()) {
            let retention_ms_u64 = state.ip_log_retention_secs.saturating_mul(1000);
            let retention_ms = cmp::min(retention_ms_u64, i64::MAX as u64) as i64;
            prune_ip_bucket(bucket, now_ms, retention_ms);
            total = bucket.len();
            let mut filtered: Vec<IpAccessEntry> = bucket
                .iter()
                .filter(|entry| entry.at_ms >= cutoff)
                .cloned()
                .collect();
            if filtered.len() > limit {
                let start = filtered.len().saturating_sub(limit);
                filtered = filtered[start..].to_vec();
            }
            returned = filtered;
            if bucket.is_empty() {
                map.remove(ip.as_str());
            }
        }
    }

    let entries_json: Vec<_> = returned
        .into_iter()
        .map(|entry| {
            json!({
                "at_ms": entry.at_ms,
                "method": entry.method,
                "path": entry.path,
                "status": entry.status,
            })
        })
        .collect();

    Json(json!({
        "ip": ip,
        "window_secs": window_secs,
        "returned": entries_json.len(),
        "total": total,
        "entries": entries_json,
    }))
    .into_response()
}
