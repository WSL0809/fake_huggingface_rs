use std::collections::HashSet;
use std::env;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::extract::{Path as AxPath, Request as AxRequest, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};
use uuid::Uuid;

// Use mimalloc as the global allocator for the server binary
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod app_state;
mod caches;
mod utils;

use app_state::AppState;
use caches::{
    PATHS_INFO_CACHE, PathsInfoEntry, SHA256_CACHE, SIBLINGS_CACHE, Sha256Entry, SiblingsEntry,
};
use utils::fs_walk::{collect_paths_info, list_siblings_except_sidecar};
use utils::headers::{file_headers_common, set_content_range};
use utils::paths::{file_size, is_sidecar_path, normalize_join_abs, secure_join};
use utils::sidecar::{etag_from_sidecar, get_sidecar_map};
use utils::repo_json::{build_repo_json, RepoJsonFlavor, RepoKind};

const CHUNK_SIZE: usize = 262_144; // 256 KiB per read chunk

#[tokio::main]
async fn main() {
    init_tracing();

    let root = env::var("FAKE_HUB_ROOT").unwrap_or_else(|_| "fake_hub".to_string());
    let root_abs = dunce::canonicalize(&root).unwrap_or_else(|_| PathBuf::from(&root));

    let state = AppState {
        root: Arc::new(root_abs.clone()),
        log_requests: !matches!(
            env::var("LOG_REQUESTS").as_deref(),
            Ok("0") | Ok("false") | Ok("False")
        ),
        log_body_max: env::var("LOG_BODY_MAX")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(4096),
        log_headers_mode_all: matches!(env::var("LOG_HEADERS").as_deref(), Ok("all")),
        log_resp_headers: !matches!(
            env::var("LOG_RESP_HEADERS").as_deref(),
            Ok("0") | Ok("false") | Ok("False")
        ),
        log_redact: !matches!(
            env::var("LOG_REDACT").as_deref(),
            Ok("0") | Ok("false") | Ok("False")
        ),
        log_body_all: !matches!(
            env::var("LOG_BODY_ALL").as_deref(),
            Ok("0") | Ok("false") | Ok("False")
        ),
        log_json_body: !matches!(
            env::var("LOG_JSON_BODY").as_deref(),
            Ok("0") | Ok("false") | Ok("False")
        ),
        cache_ttl: Duration::from_millis(
            env::var("CACHE_TTL_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(2_000),
        ),
        paths_info_cache_cap: env::var("PATHS_INFO_CACHE_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(512),
        siblings_cache_cap: env::var("SIBLINGS_CACHE_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(256),
        sha256_cache_cap: env::var("SHA256_CACHE_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1024),
    };

    println!("[fake-hub] FAKE_HUB_ROOT = {}", root_abs.display());

    // Build router
    let app = Router::new()
        // Datasets catch-all under /api/datasets
        .route(
            "/api/datasets/{*rest}",
            get(get_dataset_catchall_get).post(get_dataset_paths_info_post),
        )
        // Models catch-all under /api/models
        .route(
            "/api/models/{*rest}",
            get(get_model_catchall_get).post(get_model_paths_info_post),
        )
        // Resolve route fallback: GET and HEAD
        .route("/{*rest}", get(resolve_catchall).head(resolve_catchall))
        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(state, log_requests_mw));

    // Bind server
    let host = "0.0.0.0";
    let port: u16 = 8000;
    let listener = tokio::net::TcpListener::bind((host, port))
        .await
        .expect("bind server");
    // Print accessible URLs: bound addr + loopback + best-effort LAN IP
    let bound = listener.local_addr().ok();
    let loopback_url = format!("http://127.0.0.1:{port}");
    let lan_ip = local_ipv4_guess();
    match (bound, lan_ip) {
        (Some(b), Some(ip)) => println!(
            "[fake-hub] Listening on http://{} (local: {}, lan: http://{}:{})",
            b,
            loopback_url,
            ip,
            port
        ),
        (Some(b), None) => println!(
            "[fake-hub] Listening on http://{} (local: {})",
            b,
            loopback_url
        ),
        (None, Some(ip)) => println!(
            "[fake-hub] Listening (lan: http://{}:{}, local: {})",
            ip,
            port,
            loopback_url
        ),
        _ => println!("[fake-hub] Listening on {host}:{port}"),
    }
    axum::serve(listener, app).await.expect("server run");
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = fmt::layer().with_target(false).with_level(true);
    let subscriber = Registry::default().with(env_filter).with(fmt_layer);
    tracing::subscriber::set_global_default(subscriber).ok();
}

// Best-effort LAN IPv4 detection without extra crates.
// Uses UDP connect trick; no packets are sent until write, but OS selects an egress interface.
fn local_ipv4_guess() -> Option<std::net::Ipv4Addr> {
    use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
    // Fall back chain to popular public resolvers to improve chances, but we only need routing decision.
    let candidates = [
        SocketAddr::from((std::net::IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 80)),
        SocketAddr::from((std::net::IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 80)),
    ];
    for dest in candidates {
        if let Ok(s) = UdpSocket::bind("0.0.0.0:0") {
            if s.connect(dest).is_ok() {
                if let Ok(local) = s.local_addr() {
                    if let std::net::IpAddr::V4(v4) = local.ip() {
                        if !v4.is_loopback() && !v4.is_unspecified() {
                            return Some(v4);
                        }
                    }
                }
            }
        }
    }
    None
}

// ============ Middleware (request logging) ==========
async fn log_requests_mw(
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
        // Skip reading huge bodies based on Content-Length to avoid unbounded memory.
        let cl_opt = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok());
        let hard_skip_threshold = state.log_body_max.saturating_mul(4);
        if matches!(cl_opt, Some(cl) if cl > hard_skip_threshold) {
            body_snippet = Some(format!(
                "<skipped large body: content-length={}>",
                cl_opt.unwrap()
            ));
        } else {
            let (parts, body) = req.into_parts();
            // Read full body to preserve downstream semantics, but log truncated snippet only.
            match axum::body::to_bytes(body, usize::MAX).await {
                Ok(bytes) => {
                    let slice_len = std::cmp::min(bytes.len(), state.log_body_max);
                    if slice_len > 0 {
                        let s = String::from_utf8_lossy(&bytes[..slice_len]).to_string();
                        body_snippet = if !s.is_empty() { Some(s) } else { None };
                    }
                    req = AxRequest::from_parts(parts, Body::from(bytes));
                }
                Err(_) => {
                    req = AxRequest::from_parts(parts, Body::empty());
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

// ============ Models ============

async fn get_model_catchall_get(
    State(state): State<AppState>,
    AxPath(rest): AxPath<String>,
) -> impl IntoResponse {
    // rest can be "{repo_id}" or "{repo_id}/revision/{revision}"
    let parts: Vec<&str> = rest.split('/').collect();
    // Support tree listing: /api/models/{repo_id}/tree/{revision}
    if parts.len() >= 3 && parts[parts.len() - 2] == "tree" {
        let _revision = parts.last().unwrap_or(&"");
        let repo_id = parts[..parts.len() - 2].join("/");
        let Some(repo_path) = secure_join(&state.root, &repo_id) else {
            return http_not_found("Repository not found");
        };
        if !repo_path.is_dir() {
            return http_not_found("Repository not found");
        }
        match utils::fs_walk::collect_paths_info(&repo_path, None).await {
            Ok(vals) => return Json(vals).into_response(),
            Err(e) => return e,
        }
    }
    if parts.len() >= 3 && parts[parts.len() - 2] == "revision" {
        let revision = parts.last().unwrap_or(&"");
        let repo_id = parts[..parts.len() - 2].join("/");
        match build_model_response(&state, &repo_id, Some(revision)).await {
            Ok(val) => Json(val).into_response(),
            Err(e) => e,
        }
    } else {
        let repo_id = rest;
        match build_model_response(&state, &repo_id, None).await {
            Ok(val) => Json(val).into_response(),
            Err(e) => e,
        }
    }
}

async fn get_model_paths_info_post(
    State(state): State<AppState>,
    AxPath(rest): AxPath<String>,
    req: AxRequest,
) -> impl IntoResponse {
    // expect "{repo_id}/paths-info/{revision}"
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() >= 3 && parts[parts.len() - 2] == "paths-info" {
        let _revision = parts.last().unwrap_or(&"");
        let repo_id = parts[..parts.len() - 2].join("/");
        let Some(repo_path) = secure_join(&state.root, &repo_id) else {
            return http_not_found("Repository not found");
        };
        if !repo_path.is_dir() {
            return http_not_found("Repository not found");
        }
        match paths_info_response(&state, &repo_path, req).await {
            Ok(vals) => Json(vals).into_response(),
            Err(e) => e,
        }
    } else {
        http_not_found("Not Found")
    }
}

async fn build_model_response(
    state: &AppState,
    repo_id: &str,
    revision: Option<&str>,
) -> Result<Value, Response> {
    let Some(repo_path) = secure_join(&state.root, repo_id) else {
        return Err(http_not_found("Repository not found"));
    };
    if !repo_path.is_dir() {
        return Err(http_not_found("Repository not found"));
    }
    // repo_path is canonical from secure_join; avoid redundant canonicalize
    let cache_key = format!("model:{}", repo_path.display());
    let now = Instant::now();
    // Try cache
    if let Some(hit) = {
        let cache = SIBLINGS_CACHE.read().await;
        cache.inner.get(&cache_key).cloned()
    } {
        if now.duration_since(hit.at) < state.cache_ttl {
            let val = build_repo_json(
                RepoKind::Model,
                repo_id,
                revision,
                &hit.siblings,
                hit.total,
                RepoJsonFlavor::Rich,
            );
            return Ok(val);
        }
    }

    // Miss or expired: compute
    let (siblings, total_size): (Vec<Value>, u64) =
        list_siblings_except_sidecar(&repo_path).await.unwrap_or_default();
    // Insert to cache (bounded)
    {
        let mut cache = SIBLINGS_CACHE.write().await;
        if cache.inner.len() >= state.siblings_cache_cap {
            while let Some((old_k, old_at)) = cache.evict_q.pop_front() {
                if let Some(entry) = cache.inner.get(&old_k) {
                    if entry.at == old_at {
                        cache.inner.remove(&old_k);
                        break;
                    }
                }
            }
        }
        cache.evict_q.push_back((cache_key.clone(), now));
        cache.inner.insert(
            cache_key,
            SiblingsEntry { siblings: siblings.clone(), total: total_size, at: now },
        );
    }

    let val = build_repo_json(
        RepoKind::Model,
        repo_id,
        revision,
        &siblings,
        total_size,
        RepoJsonFlavor::Minimal,
    );
    Ok(val)
}

// ============ Datasets ============
async fn get_dataset_catchall_get(
    State(state): State<AppState>,
    AxPath(rest): AxPath<String>,
) -> impl IntoResponse {
    // rest can be "{repo_id}" or "{repo_id}/revision/{revision}"
    let parts: Vec<&str> = rest.split('/').collect();
    // Support tree listing: /api/datasets/{repo_id}/tree/{revision}
    if parts.len() >= 3 && parts[parts.len() - 2] == "tree" {
        let _revision = parts.last().unwrap_or(&"");
        let repo_id = parts[..parts.len() - 2].join("/");
        let ds_base = state.root.join("datasets");
        let Some(ds_path) = secure_join(&ds_base, &repo_id) else {
            return http_not_found("Dataset not found");
        };
        if !ds_path.is_dir() {
            return http_not_found("Dataset not found");
        }
        match utils::fs_walk::collect_paths_info(&ds_path, None).await {
            Ok(vals) => return Json(vals).into_response(),
            Err(e) => return e,
        }
    }
    if parts.len() >= 3 && parts[parts.len() - 2] == "revision" {
        let revision = parts.last().unwrap_or(&"");
        let repo_id = parts[..parts.len() - 2].join("/");
        match build_dataset_response(&state, &repo_id, Some(revision)).await {
            Ok(val) => Json(val).into_response(),
            Err(e) => e,
        }
    } else {
        let repo_id = rest;
        match build_dataset_response(&state, &repo_id, None).await {
            Ok(val) => Json(val).into_response(),
            Err(e) => e,
        }
    }
}

async fn get_dataset_paths_info_post(
    State(state): State<AppState>,
    AxPath(rest): AxPath<String>,
    req: AxRequest,
) -> impl IntoResponse {
    // expect "{repo_id}/paths-info/{revision}"
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() >= 3 && parts[parts.len() - 2] == "paths-info" {
        let _revision = parts.last().unwrap_or(&"");
        let repo_id = parts[..parts.len() - 2].join("/");
        let ds_base = state.root.join("datasets");
        let Some(ds_path) = secure_join(&ds_base, &repo_id) else {
            return http_not_found("Dataset not found");
        };
        if !ds_path.is_dir() {
            return http_not_found("Dataset not found");
        }
        match paths_info_response(&state, &ds_path, req).await {
            Ok(vals) => Json(vals).into_response(),
            Err(e) => e,
        }
    } else {
        http_not_found("Not Found")
    }
}

async fn build_dataset_response(
    state: &AppState,
    repo_id: &str,
    revision: Option<&str>,
) -> Result<Value, Response> {
    let ds_base = state.root.join("datasets");
    let Some(ds_path) = secure_join(&ds_base, repo_id) else {
        return Err(http_not_found("Dataset not found"));
    };
    if !ds_path.is_dir() {
        return Err(http_not_found("Dataset not found"));
    }
    // ds_path is canonical from secure_join; avoid redundant canonicalize
    let cache_key = format!("dataset:{}", ds_path.display());
    let now = Instant::now();
    if let Some(hit) = {
        let cache = SIBLINGS_CACHE.read().await;
        cache.inner.get(&cache_key).cloned()
    } {
        if now.duration_since(hit.at) < state.cache_ttl {
            let val = build_repo_json(
                RepoKind::Dataset,
                repo_id,
                revision,
                &hit.siblings,
                hit.total,
                RepoJsonFlavor::Minimal,
            );
            return Ok(val);
        }
    }

    let (siblings, total_size): (Vec<Value>, u64) =
        list_siblings_except_sidecar(&ds_path).await.unwrap_or_default();
    {
        let mut cache = SIBLINGS_CACHE.write().await;
        if cache.inner.len() >= state.siblings_cache_cap {
            while let Some((old_k, old_at)) = cache.evict_q.pop_front() {
                if let Some(entry) = cache.inner.get(&old_k) {
                    if entry.at == old_at {
                        cache.inner.remove(&old_k);
                        break;
                    }
                }
            }
        }
        cache.evict_q.push_back((cache_key.clone(), now));
        cache.inner.insert(
            cache_key,
            SiblingsEntry { siblings: siblings.clone(), total: total_size, at: now },
        );
    }

    let val = build_repo_json(
        RepoKind::Dataset,
        repo_id,
        revision,
        &siblings,
        total_size,
        RepoJsonFlavor::Rich,
    );
    Ok(val)
}

// ============ paths-info (shared) ============

#[derive(Debug, Deserialize)]
struct PathsInfoBody {
    #[serde(default)]
    paths: Option<Vec<String>>,
    #[serde(default)]
    expand: Option<bool>,
}

async fn paths_info_response(
    state: &AppState,
    base_dir: &Path,
    req: AxRequest,
) -> Result<Vec<Value>, Response> {
    // parse JSON body if any
    let (_parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_else(|_| Bytes::new());
    let mut paths: Vec<String> = Vec::new();
    let mut expand = true;
    if !body_bytes.is_empty() {
        if let Ok(body) = serde_json::from_slice::<PathsInfoBody>(&body_bytes) {
            if let Some(p) = body.paths {
                paths = p.into_iter().filter(|s| !s.is_empty()).collect();
            }
            if let Some(e) = body.expand {
                expand = e;
            }
        }
    }

    // Build cache key; base_dir comes from secure_join and is already canonical
    let base_abs = base_dir.to_path_buf();
    let sidecar = base_abs.join(".paths-info.json");
    let (sc_mtime, sc_size) = sidecar
        .metadata()
        .ok()
        .and_then(|m| {
            m.modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| (d.as_secs(), m.len()))
        })
        .unwrap_or((0, 0));
    let mut paths_sorted = paths.clone();
    paths_sorted.sort();
    paths_sorted.dedup();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    expand.hash(&mut hasher);
    for p in &paths_sorted {
        p.hash(&mut hasher);
    }
    let req_sig = hasher.finish();
    let cache_key = format!(
        "{}|{}|{}|{}",
        base_abs.display(),
        sc_mtime,
        sc_size,
        req_sig
    );
    // Try cache
    if let Some(hit) = {
        let cache = PATHS_INFO_CACHE.read().await;
        cache.inner.get(&cache_key).cloned()
    } {
        if Instant::now().duration_since(hit.at) < state.cache_ttl {
            return Ok(hit.items);
        }
    }

    let mut results: Vec<Value> = Vec::new();

    // Fast path: common case of a single file path; avoid re-walking and avoid cloning sidecar map multiple times.
    if paths.len() == 1 {
        let trimmed = paths[0].trim();
        if !(trimmed.is_empty() || trimmed == "/" || trimmed == ".") {
            let norm_rel = trimmed.trim_start_matches('/');
            let abs_target = normalize_join_abs(&base_abs, norm_rel);
            if (abs_target.starts_with(&base_abs) || abs_target == base_abs) && abs_target.is_file() {
                let sc_map = get_sidecar_map(&base_abs).await.unwrap_or_default();
                let rel_norm = norm_rel.replace('\\', "/");
                if let Some(sc) = sc_map.get(&rel_norm) {
                    let sidecar_size = sc
                        .get("size")
                        .and_then(|v| v.as_i64())
                        .or_else(|| sc.get("lfs").and_then(|v| v.get("size")).and_then(|v| v.as_i64()));
                    let size_i64 = match sidecar_size { Some(s) if s >= 0 => s, _ => file_size(&abs_target).unwrap_or(0) as i64 };
                    let mut rec = serde_json::Map::new();
                    rec.insert("path".to_string(), json!(rel_norm));
                    rec.insert("type".to_string(), json!("file"));
                    rec.insert("size".to_string(), json!(size_i64));
                    if let Some(oid) = sc.get("oid").and_then(|v| v.as_str()) { rec.insert("oid".to_string(), json!(oid)); }
                    if let Some(lfs) = sc.get("lfs").and_then(|v| v.as_object()) {
                        let mut ldict = serde_json::Map::new();
                        if let Some(loid) = lfs.get("oid").and_then(|v| v.as_str()) { ldict.insert("oid".to_string(), json!(loid)); }
                        let lfs_size = lfs.get("size").and_then(|v| v.as_i64()).unwrap_or(size_i64);
                        ldict.insert("size".to_string(), json!(lfs_size));
                        rec.insert("lfs".to_string(), Value::Object(ldict));
                    }
                    results.push(Value::Object(rec));
                } else {
                    let size = file_size(&abs_target).unwrap_or(0);
                    results.push(json!({ "path": rel_norm, "type": "file", "size": (size as i64) }));
                }
                // continue into de-dup + cache insert below
            }
        }
    }
    // Prepare sidecar_map once for multi-path cases
    let sc_map_for_multi = if !paths.is_empty() { get_sidecar_map(&base_abs).await.ok() } else { None };

    if paths.is_empty() {
        results = collect_paths_info(&base_abs, None).await?;
    } else {
        for p in paths {
            let trimmed = p.trim();
            if trimmed.is_empty() || trimmed == "/" || trimmed == "." {
                if expand {
                    results.extend(collect_paths_info(&base_abs, None).await?);
                } else {
                    results.push(json!({"path": "", "type": "directory"}));
                }
                continue;
            }
            if expand {
                // If it's a file, avoid full walk and use sidecar fast path
                let norm_rel = trimmed.trim_start_matches('/');
                let abs_target = normalize_join_abs(&base_abs, norm_rel);
                if (abs_target.starts_with(&base_abs) || abs_target == base_abs) && abs_target.is_file() {
                    if let Some(sc_map) = sc_map_for_multi.as_ref() {
                        let rel_norm = norm_rel.replace('\\', "/");
                        if let Some(sc) = sc_map.get(&rel_norm) {
                            let sidecar_size = sc.get("size").and_then(|v| v.as_i64()).or_else(|| sc.get("lfs").and_then(|v| v.get("size")).and_then(|v| v.as_i64()));
                            let size_i64 = match sidecar_size { Some(s) if s >= 0 => s, _ => file_size(&abs_target).unwrap_or(0) as i64 };
                            let mut rec = serde_json::Map::new();
                            rec.insert("path".to_string(), json!(rel_norm));
                            rec.insert("type".to_string(), json!("file"));
                            rec.insert("size".to_string(), json!(size_i64));
                            if let Some(oid) = sc.get("oid").and_then(|v| v.as_str()) { rec.insert("oid".to_string(), json!(oid)); }
                            if let Some(lfs) = sc.get("lfs").and_then(|v| v.as_object()) {
                                let mut ldict = serde_json::Map::new();
                                if let Some(loid) = lfs.get("oid").and_then(|v| v.as_str()) { ldict.insert("oid".to_string(), json!(loid)); }
                                let lfs_size = lfs.get("size").and_then(|v| v.as_i64()).unwrap_or(size_i64);
                                ldict.insert("size".to_string(), json!(lfs_size));
                                rec.insert("lfs".to_string(), Value::Object(ldict));
                            }
                            results.push(Value::Object(rec));
                            continue;
                        }
                    }
                }
                // Directory or no sidecar: fall back to walk
                results.extend(collect_paths_info(&base_abs, Some(trimmed)).await?);
            } else {
                let norm_rel = trimmed.trim_start_matches('/');
                let abs_target = normalize_join_abs(&base_abs, norm_rel);
                if abs_target.starts_with(&base_abs) || abs_target == base_abs {
                    if abs_target.is_dir() {
                        results.push(json!({"path": norm_rel.replace('\\', "/"), "type": "directory"}));
                    } else if abs_target.is_file() {
                        if let Some(sc_map) = sc_map_for_multi.as_ref() {
                            let rel_norm = norm_rel.replace('\\', "/");
                            if let Some(sc) = sc_map.get(&rel_norm) {
                                let sidecar_size = sc.get("size").and_then(|v| v.as_i64()).or_else(|| sc.get("lfs").and_then(|v| v.get("size")).and_then(|v| v.as_i64()));
                                let size_i64 = match sidecar_size { Some(s) if s >= 0 => s, _ => file_size(&abs_target).unwrap_or(0) as i64 };
                                let mut rec = serde_json::Map::new();
                                rec.insert("path".to_string(), json!(rel_norm));
                                rec.insert("type".to_string(), json!("file"));
                                rec.insert("size".to_string(), json!(size_i64));
                                if let Some(oid) = sc.get("oid").and_then(|v| v.as_str()) { rec.insert("oid".to_string(), json!(oid)); }
                                if let Some(lfs) = sc.get("lfs").and_then(|v| v.as_object()) {
                                    let mut ldict = serde_json::Map::new();
                                    if let Some(loid) = lfs.get("oid").and_then(|v| v.as_str()) { ldict.insert("oid".to_string(), json!(loid)); }
                                    let lfs_size = lfs.get("size").and_then(|v| v.as_i64()).unwrap_or(size_i64);
                                    ldict.insert("size".to_string(), json!(lfs_size));
                                    rec.insert("lfs".to_string(), Value::Object(ldict));
                                }
                                results.push(Value::Object(rec));
                                continue;
                            }
                        }
                        // Fallback: single file info without sidecar
                        let size = file_size(&abs_target).unwrap_or(0);
                        results.push(json!({"path": norm_rel.replace('\\', "/"), "type": "file", "size": (size as i64)}));
                    }
                }
            }
        }
    }
    // de-dup by (path,type)
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut unique: Vec<Value> = Vec::new();
    for it in results.into_iter() {
        let path = it["path"].as_str().unwrap_or("").to_string();
        let typ = it["type"].as_str().unwrap_or("").to_string();
        if seen.insert((path.clone(), typ.clone())) {
            unique.push(it);
        }
    }
    let unique_clone = unique.clone();
    {
        let mut cache = PATHS_INFO_CACHE.write().await;
        let now_i = Instant::now();
        // Evict in O(1) amortized using insertion queue
        if cache.inner.len() >= state.paths_info_cache_cap {
            while let Some((old_k, old_at)) = cache.evict_q.pop_front() {
                if let Some(entry) = cache.inner.get(&old_k) {
                    if entry.at == old_at {
                        cache.inner.remove(&old_k);
                        break;
                    }
                }
            }
        }
        cache.evict_q.push_back((cache_key.clone(), now_i));
        cache.inner.insert(
            cache_key,
            PathsInfoEntry { items: unique_clone, at: now_i },
        );
    }
    Ok(unique)
}

// (old POST sha256 removed; GET-only implemented in catchall)

// collect_paths_info moved to utils::fs_walk

// Compute sha256 with TTL cache keyed by (path, mtime, size)
async fn sha256_file_cached(state: &AppState, p: &Path) -> io::Result<String> {
    let md = p.metadata()?;
    let size = md.len();
    let mtime = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // p is canonical at call sites; avoid redundant canonicalize for cache key
    let key = (p.to_path_buf(), mtime, size);
    if let Some(hit) = {
        let cache = SHA256_CACHE.read().await;
        cache.inner.get(&key).cloned()
    } {
        if Instant::now().duration_since(hit.at) < state.cache_ttl {
            return Ok(hit.sum);
        }
    }
    let mut file = tokio::fs::File::open(p).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let sum = hex::encode(hasher.finalize());
    {
        let mut cache = SHA256_CACHE.write().await;
        if cache.inner.len() >= state.sha256_cache_cap {
            while let Some((old_k, old_at)) = cache.evict_q.pop_front() {
                if let Some(entry) = cache.inner.get(&old_k) {
                    if entry.at == old_at { cache.inner.remove(&old_k); break; }
                }
            }
        }
        let now_i = Instant::now();
        cache.evict_q.push_back((key.clone(), now_i));
        cache.inner.insert(key, Sha256Entry { sum: sum.clone(), at: now_i });
    }
    Ok(sum)
}

// ============ Resolve (GET/HEAD) ============
async fn resolve_catchall(
    State(state): State<AppState>,
    AxPath(rest): AxPath<String>,
    req: AxRequest,
) -> impl IntoResponse {
    // Two patterns supported:
    // - /{repo_id}/resolve/{revision}/{filename...} (GET|HEAD)
    // - /{repo_id}/sha256/{revision}/{filename...} (GET only)
    let path = if rest.starts_with('/') {
        rest.clone()
    } else {
        format!("/{rest}")
    };

    // First, handle /sha256/
    if let Some(idx) = path.rfind("/sha256/") {
        let left = &path[1..idx];
        let right = &path[(idx + "/sha256/".len())..];
        let mut right_parts = right.splitn(2, '/');
        let _revision = right_parts.next().unwrap_or("");
        let filename = right_parts.next().unwrap_or("");
        if left.is_empty() || filename.is_empty() {
            return http_not_found("Not Found");
        }
        if req.method() == Method::HEAD {
            return http_error(StatusCode::METHOD_NOT_ALLOWED, "Use GET for sha256");
        }
        if is_sidecar_path(filename) {
            return http_not_found("File not found");
        }
        let rel = format!("{}/{}", left.trim_start_matches('/'), filename);
        let Some(filepath) = secure_join(&state.root, &rel) else {
            return http_not_found("File not found");
        };
        if !filepath.is_file() {
            return http_not_found("File not found");
        }
        match sha256_file_cached(&state, &filepath).await {
            Ok(sum) => {
                let body = json!({ "sha256": sum });
                return (StatusCode::OK, Json(body)).into_response();
            }
            Err(_) => return http_error(StatusCode::INTERNAL_SERVER_ERROR, "Hash compute failed"),
        }
    }

    // Otherwise, treat as /resolve/
    // Expect pattern: /{repo_id}/resolve/{revision}/{filename...}
    // We'll find the last occurrence of "/resolve/" and split.
    let needle = "/resolve/";
    let Some(idx) = path.rfind(needle) else {
        return http_not_found("Not Found");
    };
    let left = &path[1..idx]; // skip leading '/'
    let right = &path[(idx + needle.len())..];
    // right = {revision}/{filename...}
    let mut right_parts = right.splitn(2, '/');
    let revision = right_parts.next().unwrap_or("");
    let filename = right_parts.next().unwrap_or("");
    if left.is_empty() || revision.is_empty() || filename.is_empty() {
        return http_not_found("Not Found");
    }

    // .paths-info.json cannot be served as file
    if is_sidecar_path(filename) {
        return http_not_found("File not found");
    }

    let rel = format!("{}/{}", left.trim_start_matches('/'), filename);
    let Some(filepath) = secure_join(&state.root, &rel) else {
        return http_not_found("File not found");
    };
    if !filepath.is_file() {
        return http_not_found("File not found");
    }

    if req.method() == Method::HEAD {
        return head_file(&state, left, revision, filename, &filepath).await;
    }
    // GET with Range
    let range_header = req
        .headers()
        .get("range")
        .or_else(|| req.headers().get("Range"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if let Some(rh) = range_header {
        let total = match fs::metadata(&filepath).await { Ok(m) => m.len(), Err(_) => 0 };
        match parse_range(&rh, total) {
            RangeParse::Invalid => {
                // ignore range, return full file
                return full_file_response(&state, left, revision, filename, &filepath).await;
            }
            RangeParse::Unsatisfiable => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "Content-Range",
                    HeaderValue::from_str(&format!("bytes */{total}")).unwrap(),
                );
                headers.insert("Accept-Ranges", HeaderValue::from_static("bytes"));
                return (StatusCode::RANGE_NOT_SATISFIABLE, headers).into_response();
            }
            RangeParse::Ok(start, end) => {
                let length = end - start + 1;
                let stream = stream! {
                    let mut f = match fs::File::open(&filepath).await { Ok(f) => f, Err(e) => { error!("open file: {}", e); yield Err(io::Error::other("open failed")); return; } };
                    if let Err(e) = f.seek(std::io::SeekFrom::Start(start)).await { error!("seek: {}", e); yield Err(io::Error::other("seek failed")); return; }
                    let mut remaining = length as usize;
                    let mut buf = vec![0u8; CHUNK_SIZE];
                    while remaining > 0 {
                        let cap = std::cmp::min(buf.len(), remaining);
                        match f.read(&mut buf[..cap]).await {
                            Ok(0) => break,
                            Ok(n) => {
                                yield Ok::<Bytes, io::Error>(Bytes::copy_from_slice(&buf[..n]));
                                remaining -= n;
                            }
                            Err(e) => { error!("read: {}", e); break; }
                        }
                    }
                };
                let mut headers = file_headers_common(revision, length);
                set_content_range(&mut headers, start, end, total);
                let body = Body::from_stream(stream);
                return Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .body(body)
                    .map(|mut r| {
                        *r.headers_mut() = headers;
                        r
                    })
                    .unwrap()
                    .into_response();
            }
        }
    }

    full_file_response(&state, left, revision, filename, &filepath).await
}

async fn full_file_response(
    _state: &AppState,
    _repo_id: &str,
    revision: &str,
    filename: &str,
    path: &Path,
) -> Response {
    // Read entire file into body stream using tokio_util::io::ReaderStream if desired.
    // For simplicity and parity, we use a streaming reader.
    let file = match fs::File::open(path).await {
        Ok(f) => f,
        Err(_) => return http_not_found("File not found"),
    };
    let size = file.metadata().await.ok().map(|m| m.len()).unwrap_or(0);
    let stream = tokio_util::io::ReaderStream::with_capacity(file, CHUNK_SIZE);
    let mut headers = file_headers_common(revision, size);
    // Best-effort ETag for GET: add if available in sidecar; otherwise omit (do not break userspace)
    // Derive repo_root by stripping the filename components from the absolute filepath to avoid extra joins/canonicalize.
    {
        let mut repo_root = path.to_path_buf();
        let depth = filename.split('/').count();
        for _ in 0..depth {
            if let Some(parent) = repo_root.parent() {
                repo_root = parent.to_path_buf();
            }
        }
        if let Ok(sc_map) = get_sidecar_map(&repo_root).await {
            let rel_path = filename.replace('\\', "/");
            if let Some((etag, is_lfs)) = etag_from_sidecar(&sc_map, &rel_path, size) {
                let quoted = format!("\"{etag}\"");
                headers.insert(
                    "ETag",
                    HeaderValue::from_str(&quoted)
                        .unwrap_or(HeaderValue::from_static("\"-\"")),
                );
                if is_lfs {
                    headers.insert(
                        "x-lfs-size",
                        HeaderValue::from_str(&size.to_string()).unwrap(),
                    );
                }
            }
        }
    }
    let body = Body::from_stream(stream);
    Response::builder()
        .status(StatusCode::OK)
        .body(body)
        .map(|mut r| {
            for (k, v) in headers.iter() {
                r.headers_mut().insert(k, v.clone());
            }
            r
        })
        .unwrap()
}

async fn head_file(
    _state: &AppState,
    repo_id: &str,
    revision: &str,
    filename: &str,
    filepath: &Path,
) -> Response {
    let size = match fs::metadata(filepath).await { Ok(m) => m.len(), Err(_) => 0 };
    let mut headers = file_headers_common(revision, size);

    // ETag strictly from sidecar; otherwise 500
    // Derive repo_root by walking up from the filepath to avoid extra canonicalize.
    let mut repo_root = filepath.to_path_buf();
    let depth = filename.split('/').count();
    for _ in 0..depth {
        if let Some(parent) = repo_root.parent() { repo_root = parent.to_path_buf(); }
    }
    let rel_path = filename.replace('\\', "/");
    let sc_map = get_sidecar_map(&repo_root).await.unwrap_or_default();
    let etag_pair = etag_from_sidecar(&sc_map, &rel_path, size);
    if etag_pair.is_none() {
        error!("ETag missing for {}@{}:{}", repo_id, revision, rel_path);
        return http_error(StatusCode::INTERNAL_SERVER_ERROR, "ETag not available");
    }
    let (etag, is_lfs) = etag_pair.unwrap();
    let quoted = format!("\"{etag}\"");
    headers.insert(
        "ETag",
        HeaderValue::from_str(&quoted).unwrap_or(HeaderValue::from_static("\"-\"")),
    );
    if is_lfs {
        headers.insert(
            "x-lfs-size",
            HeaderValue::from_str(&size.to_string()).unwrap(),
        );
    }
    (StatusCode::OK, headers).into_response()
}

enum RangeParse {
    Invalid,
    Unsatisfiable,
    Ok(u64, u64),
}

fn parse_range(h: &str, total: u64) -> RangeParse {
    let s = h.trim();
    let mut it = s.splitn(2, '=');
    let unit = it.next().unwrap_or("");
    let rest = it.next().unwrap_or("");
    if !unit.eq_ignore_ascii_case("bytes") {
        return RangeParse::Invalid;
    }
    let first = rest.split(',').next().unwrap_or("").trim();
    if !first.contains('-') {
        return RangeParse::Invalid;
    }
    let mut ab = first.splitn(2, '-');
    let a = ab.next().unwrap_or("");
    let b = ab.next().unwrap_or("");
    if a.is_empty() {
        // suffix: bytes=-N
        let Ok(n) = b.parse::<u64>() else {
            return RangeParse::Invalid;
        };
        if n == 0 {
            return RangeParse::Invalid;
        }
        let start = total.saturating_sub(n);
        let end = if total > 0 { total - 1 } else { 0 };
        RangeParse::Ok(start, end)
    } else {
        let Ok(start) = a.parse::<u64>() else {
            return RangeParse::Invalid;
        };
        let mut end = if b.is_empty() {
            total.saturating_sub(1)
        } else {
            match b.parse::<u64>() {
                Ok(v) => v,
                Err(_) => return RangeParse::Invalid,
            }
        };
        if start >= total {
            return RangeParse::Unsatisfiable;
        }
        if end >= total {
            end = total.saturating_sub(1);
        }
        if end < start {
            return RangeParse::Unsatisfiable;
        }
        RangeParse::Ok(start, end)
    }
}

// ============ Helpers ============
fn http_not_found(msg: &str) -> Response {
    let body = json!({"detail": msg});
    (StatusCode::NOT_FOUND, Json(body)).into_response()
}

fn http_error(status: StatusCode, msg: &str) -> Response {
    let body = json!({"detail": msg});
    (status, Json(body)).into_response()
}

// helpers moved to utils::{paths,sidecar,fs_walk}

#[cfg(test)]
mod tests {
    use super::parse_range;
    use super::*;
    use axum::routing::get;
    use axum::Router;
    use std::sync::Arc;
    use tower::util::ServiceExt;

    #[test]
    fn parse_range_happy_paths() {
        use super::RangeParse;
        assert!(matches!(parse_range("bytes=0-0", 10), RangeParse::Ok(0, 0)));
        assert!(matches!(parse_range("bytes=5-9", 10), RangeParse::Ok(5, 9)));
        assert!(matches!(parse_range("bytes=5-", 10), RangeParse::Ok(5, 9)));
        assert!(matches!(parse_range("bytes=-3", 10), RangeParse::Ok(7, 9)));
    }

    #[test]
    fn parse_range_bad_cases() {
        use super::RangeParse;
        assert!(matches!(parse_range("bits=0-1", 10), RangeParse::Invalid));
        assert!(matches!(parse_range("bytes=10-10", 10), RangeParse::Unsatisfiable));
        assert!(matches!(parse_range("bytes=0-1000", 100), RangeParse::Ok(0, 99)));
    }

    #[tokio::test]
    async fn router_head_get_with_etag() {
        // Arrange a tiny repo under fake_hub/tests_repo_etag
        let root = dunce::canonicalize("fake_hub").unwrap_or_else(|_| std::path::PathBuf::from("fake_hub"));
        let repo_id = "tests_repo_etag";
        let repo_dir = root.join(repo_id);
        tokio::fs::create_dir_all(&repo_dir).await.unwrap();
        let file_path = repo_dir.join("x.bin");
        tokio::fs::write(&file_path, b"hello").await.unwrap();
        let size = file_path.metadata().unwrap().len();
        let sidecar = repo_dir.join(".paths-info.json");
        let sc = serde_json::json!({
            "entries": [{
                "path": "x.bin", "type": "file", "size": size as i64,
                "lfs": {"oid": "sha256:1234", "size": size as i64}
            }]
        });
        tokio::fs::write(&sidecar, serde_json::to_vec(&sc).unwrap()).await.unwrap();

        // Build router with only resolve route
        let state = AppState {
            root: Arc::new(root.clone()),
            log_requests: false,
            log_body_max: 1024,
            log_headers_mode_all: false,
            log_resp_headers: false,
            log_redact: true,
            log_body_all: false,
            cache_ttl: std::time::Duration::from_millis(2000),
            paths_info_cache_cap: 64,
            siblings_cache_cap: 64,
            sha256_cache_cap: 64,
        };
        let app = Router::new()
            .route("/{*rest}", get(resolve_catchall).head(resolve_catchall))
            .with_state(state);

        // HEAD should return ETag from sidecar (1234)
        let uri = format!("/{repo_id}/resolve/main/x.bin");
        let resp = app
            .clone()
            .oneshot(axum::http::Request::builder().method("HEAD").uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let etag = resp.headers().get("ETag").unwrap().to_str().unwrap();
        assert_eq!(etag, "\"1234\"");
        assert!(resp.headers().get("Accept-Ranges").is_some());

        // GET with range
        let req = axum::http::Request::builder()
            .method("GET")
            .uri(&uri)
            .header("Range", "bytes=0-1")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        let cr = resp.headers().get("Content-Range").unwrap().to_str().unwrap();
        assert!(cr.starts_with("bytes 0-1/"));
        assert!(resp.headers().get("Accept-Ranges").is_some());
    }
}
