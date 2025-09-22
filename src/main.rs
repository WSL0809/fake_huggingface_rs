use std::collections::HashSet;
use std::env;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::Request as AxRequest;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use time::{UtcOffset, macros::format_description};
use tracing::info;
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod app_state;
mod caches;
mod middleware;
mod resolve;
mod routes_admin;
mod routes_blake3;
mod routes_datasets;
mod routes_models;
mod utils;

use app_state::AppState;
use caches::{PATHS_INFO_CACHE, PathsInfoEntry};
// Only import what is used to avoid warnings
use utils::sidecar::get_sidecar_map;

pub(crate) const CHUNK_SIZE: usize = 262_144; // 256 KiB per read chunk

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
        ip_log_retention_secs: {
            let secs = env::var("IP_LOG_RETENTION_SECS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(1800);
            secs.max(60)
        },
        ip_log_per_ip_cap: {
            let cap = env::var("IP_LOG_PER_IP_CAP")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(200);
            cap.max(1)
        },
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

    // Startup log (respect LOG_REDACT)
    if state.log_redact {
        info!(target: "fakehub", "[fake-hub] FAKE_HUB_ROOT configured (redacted)");
    } else {
        info!(target: "fakehub", "[fake-hub] FAKE_HUB_ROOT = {}", root_abs.display());
    }

    // Build router
    let mut router = Router::new()
        .route("/api/blake3/{*repo}", get(routes_blake3::get_repo_blake3))
        // Datasets catch-all under /api/datasets
        .route(
            "/api/datasets/{*rest}",
            get(routes_datasets::get_dataset_catchall_get)
                .post(routes_datasets::get_dataset_paths_info_post),
        )
        // Models catch-all under /api/models
        .route(
            "/api/models/{*rest}",
            get(routes_models::get_model_catchall_get)
                .post(routes_models::get_model_paths_info_post),
        )
        // Resolve route fallback: GET and HEAD
        .route(
            "/{*rest}",
            get(resolve::resolve_catchall).head(resolve::resolve_catchall),
        );

    router = router.route("/admin/ip-log", get(routes_admin::get_ip_log));

    let state_for_layer = state.clone();
    let app = router
        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(
            state_for_layer,
            middleware::log_requests_mw,
        ));

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
        (Some(b), Some(ip)) => info!(target: "fakehub",
            "[fake-hub] Listening on http://{} (local: {}, lan: http://{}:{})",
            b, loopback_url, ip, port
        ),
        (Some(b), None) => info!(target: "fakehub",
            "[fake-hub] Listening on http://{} (local: {})",
            b, loopback_url
        ),
        (None, Some(ip)) => info!(target: "fakehub",
            "[fake-hub] Listening (lan: http://{}:{}, local: {})",
            ip, port, loopback_url
        ),
        _ => info!(target: "fakehub", "[fake-hub] Listening on {host}:{port}"),
    }
    let make_service = app.into_make_service_with_connect_info::<SocketAddr>();

    axum::serve(listener, make_service)
        .await
        .expect("server run");
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Format timestamp as local time: "YYYY-MM-DD HH:MM:SS"
    let offset = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
    let ts_format = format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    let timer = OffsetTime::new(offset, ts_format);
    let fmt_layer = fmt::layer()
        .with_target(false)
        .with_level(true)
        .with_timer(timer);
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

#[derive(Debug, Deserialize)]
struct PathsInfoBody {
    #[serde(default)]
    paths: Option<Vec<String>>,
    #[serde(default)]
    expand: Option<bool>,
}

pub(crate) async fn paths_info_response(
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
            // LRU refresh on hit
            let fresh = Instant::now();
            let mut cachew = PATHS_INFO_CACHE.write().await;
            let cloned_items = if let Some(entry) = cachew.inner.get_mut(&cache_key) {
                entry.at = fresh;
                Some(entry.items.clone())
            } else {
                None
            };
            cachew.evict_q.push_back((cache_key.clone(), fresh));
            if let Some(items) = cloned_items {
                return Ok(items);
            }
            return Ok(hit.items);
        }
    }

    let mut results: Vec<Value> = Vec::new();
    let sc_map = get_sidecar_map(&base_abs).await.unwrap_or_default();
    if paths.is_empty() {
        if expand {
            if let Some(vals) = utils::fs_walk::collect_paths_info_from_sidecar(&base_abs).await {
                results = vals;
            } else {
                return Err(http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Sidecar missing or incomplete",
                ));
            }
        } else {
            results.push(json!({"path": "", "type": "directory"}));
        }
    } else {
        for p in paths {
            let trimmed = p.trim();
            if trimmed.is_empty() || trimmed == "/" || trimmed == "." {
                if expand {
                    if let Some(vals) =
                        utils::fs_walk::collect_paths_info_from_sidecar(&base_abs).await
                    {
                        results.extend(vals);
                    } else {
                        return Err(http_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Sidecar missing or incomplete",
                        ));
                    }
                } else {
                    results.push(json!({"path": "", "type": "directory"}));
                }
                continue;
            }
            let norm_rel = trimmed.trim_start_matches('/');
            let rel_norm = norm_rel.replace('\\', "/");
            if expand {
                if let Some(sc) = sc_map.get(&rel_norm) {
                    let Some(size_i64) = sc.get("size").and_then(|v| v.as_i64()).or_else(|| {
                        sc.get("lfs")
                            .and_then(|v| v.get("size"))
                            .and_then(|v| v.as_i64())
                    }) else {
                        return Err(http_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Sidecar missing size",
                        ));
                    };
                    let mut rec = serde_json::Map::new();
                    rec.insert("path".to_string(), json!(rel_norm));
                    rec.insert("type".to_string(), json!("file"));
                    rec.insert("size".to_string(), json!(size_i64));
                    if let Some(oid) = sc.get("oid").and_then(|v| v.as_str()) {
                        rec.insert("oid".to_string(), json!(oid));
                    }
                    if let Some(lfs) = sc.get("lfs").and_then(|v| v.as_object()) {
                        let mut ldict = serde_json::Map::new();
                        if let Some(loid) = lfs.get("oid").and_then(|v| v.as_str()) {
                            ldict.insert("oid".to_string(), json!(loid));
                        }
                        let lfs_size = lfs.get("size").and_then(|v| v.as_i64()).unwrap_or(size_i64);
                        ldict.insert("size".to_string(), json!(lfs_size));
                        rec.insert("lfs".to_string(), Value::Object(ldict));
                    }
                    results.push(Value::Object(rec));
                } else {
                    results.push(json!({"path": rel_norm.clone(), "type": "directory"}));
                    let prefix = if rel_norm.is_empty() {
                        String::new()
                    } else {
                        format!("{}/", rel_norm)
                    };
                    for (k, v) in sc_map.iter() {
                        if prefix.is_empty() || k.starts_with(&prefix) {
                            let Some(size_i64) =
                                v.get("size").and_then(|x| x.as_i64()).or_else(|| {
                                    v.get("lfs")
                                        .and_then(|x| x.get("size"))
                                        .and_then(|x| x.as_i64())
                                })
                            else {
                                return Err(http_error(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "Sidecar missing size",
                                ));
                            };
                            let mut rec = serde_json::Map::new();
                            rec.insert("path".to_string(), json!(k));
                            rec.insert("type".to_string(), json!("file"));
                            rec.insert("size".to_string(), json!(size_i64));
                            if let Some(oid) = v.get("oid").and_then(|x| x.as_str()) {
                                rec.insert("oid".to_string(), json!(oid));
                            }
                            if let Some(lfs) = v.get("lfs").and_then(|x| x.as_object()) {
                                let mut ldict = serde_json::Map::new();
                                if let Some(loid) = lfs.get("oid").and_then(|x| x.as_str()) {
                                    ldict.insert("oid".to_string(), json!(loid));
                                }
                                let lfs_size =
                                    lfs.get("size").and_then(|x| x.as_i64()).unwrap_or(size_i64);
                                ldict.insert("size".to_string(), json!(lfs_size));
                                rec.insert("lfs".to_string(), Value::Object(ldict));
                            }
                            results.push(Value::Object(rec));
                        }
                    }
                }
            } else {
                if let Some(sc) = sc_map.get(&rel_norm) {
                    let Some(size_i64) = sc.get("size").and_then(|v| v.as_i64()).or_else(|| {
                        sc.get("lfs")
                            .and_then(|v| v.get("size"))
                            .and_then(|v| v.as_i64())
                    }) else {
                        return Err(http_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Sidecar missing size",
                        ));
                    };
                    let mut rec = serde_json::Map::new();
                    rec.insert("path".to_string(), json!(rel_norm));
                    rec.insert("type".to_string(), json!("file"));
                    rec.insert("size".to_string(), json!(size_i64));
                    if let Some(oid) = sc.get("oid").and_then(|v| v.as_str()) {
                        rec.insert("oid".to_string(), json!(oid));
                    }
                    if let Some(lfs) = sc.get("lfs").and_then(|v| v.as_object()) {
                        let mut ldict = serde_json::Map::new();
                        if let Some(loid) = lfs.get("oid").and_then(|v| v.as_str()) {
                            ldict.insert("oid".to_string(), json!(loid));
                        }
                        let lfs_size = lfs.get("size").and_then(|v| v.as_i64()).unwrap_or(size_i64);
                        ldict.insert("size".to_string(), json!(lfs_size));
                        rec.insert("lfs".to_string(), Value::Object(ldict));
                    }
                    results.push(Value::Object(rec));
                } else {
                    results.push(json!({"path": rel_norm, "type": "directory"}));
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
            PathsInfoEntry {
                items: unique_clone,
                at: now_i,
            },
        );
    }
    Ok(unique)
}

// ============ Helpers ============
pub(crate) fn http_not_found(msg: &str) -> Response {
    let body = json!({"detail": msg});
    (StatusCode::NOT_FOUND, Json(body)).into_response()
}

pub(crate) fn http_error(status: StatusCode, msg: &str) -> Response {
    let body = json!({"detail": msg});
    (status, Json(body)).into_response()
}
