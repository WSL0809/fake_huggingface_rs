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

mod app_state;
mod caches;

use app_state::AppState;
use caches::{
    PATHS_INFO_CACHE, PathsInfoEntry, SHA256_CACHE, SIBLINGS_CACHE, SIDECAR_CACHE, Sha256Entry,
    SiblingsEntry, SidecarMap,
};

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
    axum::serve(listener, app).await.expect("server run");
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = fmt::layer().with_target(false).with_level(true);
    let subscriber = Registry::default().with(env_filter).with(fmt_layer);
    tracing::subscriber::set_global_default(subscriber).ok();
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
                json!(redact_header(&k.to_string(), val, state.log_redact)),
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

    // Optionally log JSON body
    let mut body_snippet: Option<String> = None;
    let should_log_body =
        state.log_body_all || ct.to_ascii_lowercase().contains("application/json");
    if should_log_body {
        let (parts, body) = req.into_parts();
        match axum::body::to_bytes(body, state.log_body_max).await {
            Ok(bytes) => {
                let s = String::from_utf8_lossy(&bytes).to_string();
                body_snippet = if !s.is_empty() { Some(s) } else { None };
                // reassemble request with original body bytes
                req = AxRequest::from_parts(parts, Body::from(bytes));
            }
            Err(_) => {
                req = AxRequest::from_parts(parts, Body::empty());
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
                json!(redact_header(&k.to_string(), val, state.log_redact)),
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
        let revision = parts.last().unwrap_or(&"");
        let repo_id = parts[..parts.len() - 2].join("/");
        let repo_path = state.root.join(repo_id);
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
    let repo_path = state.root.join(repo_id);
    if !repo_path.is_dir() {
        return Err(http_not_found("Repository not found"));
    }
    let cache_key = format!(
        "model:{}",
        dunce::canonicalize(&repo_path)
            .unwrap_or(repo_path.clone())
            .display()
    );
    let now = Instant::now();
    // Try cache
    if let Some(hit) = {
        let cache = SIBLINGS_CACHE.read().await;
        cache.get(&cache_key).cloned()
    } {
        if now.duration_since(hit.at) < state.cache_ttl {
            let fake_sha = revision
                .map(|r| format!("fakesha-{}", r))
                .unwrap_or_else(|| "fakesha1234567890".to_string());
            let val = json!({
                "_id": format!("local/{}", repo_id),
                "id": repo_id,
                "private": false,
                "pipeline_tag": "text-generation",
                "library_name": "transformers",
                "tags": ["transformers", "gpt2", "text-generation"],
                "downloads": 0,
                "likes": 0,
                "modelId": repo_id,
                "author": "local-user",
                "sha": fake_sha,
                "lastModified": "1970-01-01T00:00:00.000Z",
                "createdAt": "1970-01-01T00:00:00.000Z",
                "gated": false,
                "disabled": false,
                "widgetData": [{"text": "Hello"}],
                "model-index": Value::Null,
                "config": {"architectures": ["GPT2LMHeadModel"], "model_type": "gpt2", "tokenizer_config": {}},
                "cardData": {"language": "en", "tags": ["example"], "license": "mit"},
                "transformersInfo": {
                    "auto_model": "AutoModelForCausalLM",
                    "pipeline_tag": "text-generation",
                    "processor": "AutoTokenizer",
                },
                "safetensors": {"parameters": {"F32": 0}, "total": 0},
                "siblings": hit.siblings,
                "spaces": [],
                "usedStorage": (hit.total as i64),
            });
            return Ok(val);
        }
    }

    // Miss or expired: compute
    let mut siblings: Vec<Value> = Vec::new();
    let mut total_size: u64 = 0;
    let mut dirs = vec![repo_path.clone()];
    while let Some(dir) = dirs.pop() {
        if let Ok(mut rd) = fs::read_dir(&dir).await {
            while let Ok(Some(ent)) = rd.next_entry().await {
                let path = ent.path();
                if path.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    if name == ".paths-info.json" {
                        continue;
                    }
                }
                let rel = pathdiff::diff_paths(&path, &repo_path).unwrap_or(path.clone());
                let rel_norm = rel.to_string_lossy().replace('\\', "/");
                siblings.push(json!({"rfilename": rel_norm}));
                if let Ok(md) = ent.metadata().await {
                    total_size = total_size.saturating_add(md.len());
                }
            }
        }
    }
    siblings.sort_by(|a, b| {
        a["rfilename"]
            .as_str()
            .unwrap_or("")
            .cmp(b["rfilename"].as_str().unwrap_or(""))
    });
    // Insert to cache (bounded)
    {
        let mut cache = SIBLINGS_CACHE.write().await;
        if cache.len() >= state.siblings_cache_cap {
            if let Some(first_key) = cache.keys().next().cloned() {
                cache.remove(&first_key);
            }
        }
        cache.insert(
            cache_key,
            SiblingsEntry {
                siblings: siblings.clone(),
                total: total_size,
                at: now,
            },
        );
    }

    let fake_sha = revision
        .map(|r| format!("fakesha-{}", r))
        .unwrap_or_else(|| "fakesha1234567890".to_string());

    let val = json!({
        "_id": format!("local/{}", repo_id),
        "id": repo_id,
        "private": false,
        // "pipeline_tag": "text-generation",
        // "library_name": "transformers",
        // "tags": ["transformers", "gpt2", "text-generation"],
        // "downloads": 0,
        // "likes": 0,
        "modelId": repo_id,
        // "author": "local-user",
        "sha": fake_sha,
        // "lastModified": "1970-01-01T00:00:00.000Z",
        // "createdAt": "1970-01-01T00:00:00.000Z",
        // "gated": false,
        // "disabled": false,
        // "widgetData": [{"text": "Hello"}],
        "model-index": Value::Null,
        // "config": {"architectures": ["GPT2LMHeadModel"], "model_type": "gpt2", "tokenizer_config": {}},
        // "cardData": {"language": "en", "tags": ["example"], "license": "mit"},
        // "transformersInfo": {
        //     "auto_model": "AutoModelForCausalLM",
        //     "pipeline_tag": "text-generation",
        //     "processor": "AutoTokenizer",
        // },
        // "safetensors": {"parameters": {"F32": 0}, "total": 0},
        "siblings": siblings,
        // "spaces": [],
        "usedStorage": (total_size as i64),
    });
    Ok(val)
}

// ============ Datasets ============
async fn get_dataset_catchall_get(
    State(state): State<AppState>,
    AxPath(rest): AxPath<String>,
) -> impl IntoResponse {
    // rest can be "{repo_id}" or "{repo_id}/revision/{revision}"
    let parts: Vec<&str> = rest.split('/').collect();
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
        let ds_path = state.root.join("datasets").join(repo_id);
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
    let ds_path = state.root.join("datasets").join(repo_id);
    if !ds_path.is_dir() {
        return Err(http_not_found("Dataset not found"));
    }
    let cache_key = format!(
        "dataset:{}",
        dunce::canonicalize(&ds_path)
            .unwrap_or(ds_path.clone())
            .display()
    );
    let now = Instant::now();
    if let Some(hit) = {
        let cache = SIBLINGS_CACHE.read().await;
        cache.get(&cache_key).cloned()
    } {
        if now.duration_since(hit.at) < state.cache_ttl {
            let fake_sha = revision
                .map(|r| format!("fakesha-{}", r))
                .unwrap_or_else(|| "fakesha1234567890".to_string());
            let val = json!({
                "_id": format!("local/datasets/{}", repo_id),
                "id": repo_id,
                "private": false,
                "tags": ["dataset"],
                // "downloads": 0,
                // "likes": 0,
                // "author": "local-user",
                "sha": fake_sha,
                // "lastModified": "1970-01-01T00:00:00.000Z",
                // "createdAt": "1970-01-01T00:00:00.000Z",
                // "gated": false,
                // "disabled": false,
                // "cardData": {"license": "mit", "language": ["en"]},
                "siblings": hit.siblings,
                "usedStorage": (hit.total as i64),
            });
            return Ok(val);
        }
    }

    let mut siblings: Vec<Value> = Vec::new();
    let mut total_size: u64 = 0;
    let mut dirs = vec![ds_path.clone()];
    while let Some(dir) = dirs.pop() {
        if let Ok(mut rd) = fs::read_dir(&dir).await {
            while let Ok(Some(ent)) = rd.next_entry().await {
                let path = ent.path();
                if path.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    if name == ".paths-info.json" {
                        continue;
                    }
                }
                let rel = pathdiff::diff_paths(&path, &ds_path).unwrap_or(path.clone());
                let rel_norm = rel.to_string_lossy().replace('\\', "/");
                siblings.push(json!({"rfilename": rel_norm}));
                if let Ok(md) = ent.metadata().await {
                    total_size = total_size.saturating_add(md.len());
                }
            }
        }
    }
    siblings.sort_by(|a, b| {
        a["rfilename"]
            .as_str()
            .unwrap_or("")
            .cmp(b["rfilename"].as_str().unwrap_or(""))
    });
    {
        let mut cache = SIBLINGS_CACHE.write().await;
        if cache.len() >= state.siblings_cache_cap {
            if let Some(first_key) = cache.keys().next().cloned() {
                cache.remove(&first_key);
            }
        }
        cache.insert(
            cache_key,
            SiblingsEntry {
                siblings: siblings.clone(),
                total: total_size,
                at: now,
            },
        );
    }

    let fake_sha = revision
        .map(|r| format!("fakesha-{}", r))
        .unwrap_or_else(|| "fakesha1234567890".to_string());

    let val = json!({
        "_id": format!("local/datasets/{}", repo_id),
        "id": repo_id,
        "private": false,
        "tags": ["dataset"],
        "downloads": 0,
        "likes": 0,
        "author": "local-user",
        "sha": fake_sha,
        "lastModified": "1970-01-01T00:00:00.000Z",
        "createdAt": "1970-01-01T00:00:00.000Z",
        "gated": false,
        "disabled": false,
        "cardData": {"license": "mit", "language": ["en"]},
        "siblings": siblings,
        "usedStorage": (total_size as i64),
    });
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

    // Build cache key
    let base_abs = dunce::canonicalize(base_dir).unwrap_or_else(|_| base_dir.to_path_buf());
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
        cache.get(&cache_key).cloned()
    } {
        if Instant::now().duration_since(hit.at) < state.cache_ttl {
            return Ok(hit.items);
        }
    }

    let mut results: Vec<Value> = Vec::new();
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
                results.extend(collect_paths_info(&base_abs, Some(trimmed)).await?);
            } else {
                let norm_rel = trimmed.trim_start_matches('/');
                let abs_target = normalize_join(&base_abs, norm_rel);
                if abs_target.starts_with(&base_abs) || abs_target == base_abs {
                    if abs_target.is_dir() {
                        results.push(
                            json!({"path": norm_rel.replace('\\', "/"), "type": "directory"}),
                        );
                    } else if abs_target.is_file() {
                        let infos = collect_paths_info(&base_abs, Some(norm_rel)).await?;
                        for it in infos {
                            if it["type"].as_str() == Some("file") {
                                results.push(it);
                                break;
                            }
                        }
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
        if cache.len() >= state.paths_info_cache_cap {
            if let Some(first_key) = cache.keys().next().cloned() {
                cache.remove(&first_key);
            }
        }
        cache.insert(
            cache_key,
            PathsInfoEntry {
                items: unique_clone,
                at: Instant::now(),
            },
        );
    }
    Ok(unique)
}

// (old POST sha256 removed; GET-only implemented in catchall)

async fn collect_paths_info(
    base_dir: &Path,
    rel_prefix: Option<&str>,
) -> Result<Vec<Value>, Response> {
    let base_abs = dunce::canonicalize(base_dir).unwrap_or_else(|_| base_dir.to_path_buf());
    let mut results: Vec<Value> = Vec::new();
    let sidecar_map = get_sidecar_map(&base_abs).await.unwrap_or_default();

    fn build_file_entry(abs_path: &Path, rel_path: &str, sidecar_map: &SidecarMap) -> Value {
        let rel_norm = rel_path.replace('\\', "/");
        if let Some(sc) = sidecar_map.get(&rel_norm) {
            let mut rec = serde_json::Map::new();
            rec.insert("path".to_string(), json!(rel_norm));
            rec.insert("type".to_string(), json!("file"));
            let size = file_size(abs_path).unwrap_or(0);
            rec.insert("size".to_string(), json!(size as i64));
            if let Some(oid) = sc.get("oid").and_then(|v| v.as_str()) {
                rec.insert("oid".to_string(), json!(oid));
            }
            if let Some(lfs) = sc.get("lfs").and_then(|v| v.as_object()) {
                let mut ldict = serde_json::Map::new();
                if let Some(loid) = lfs.get("oid").and_then(|v| v.as_str()) {
                    ldict.insert("oid".to_string(), json!(loid));
                }
                ldict.insert("size".to_string(), json!(size as i64));
                rec.insert("lfs".to_string(), Value::Object(ldict));
            }
            return Value::Object(rec);
        }
        let size = file_size(abs_path).unwrap_or(0);
        json!({
            "path": rel_norm,
            "type": "file",
            "size": (size as i64),
        })
    }

    async fn walk_dir_collect(
        base_dir: &Path,
        start_abs: &Path,
        start_rel: &str,
        sidecar_map: &SidecarMap,
    ) -> io::Result<Vec<Value>> {
        let mut out: Vec<Value> = Vec::new();
        if !start_rel.is_empty() {
            out.push(json!({"path": start_rel.replace('\\', "/"), "type": "directory"}));
        }
        let mut stack = vec![start_abs.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let mut rd = fs::read_dir(&dir).await?;
            while let Ok(Some(ent)) = rd.next_entry().await {
                let path = ent.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    if name == ".paths-info.json" {
                        continue;
                    }
                }
                let rel = pathdiff::diff_paths(&path, base_dir).unwrap_or(path.clone());
                let rel_str = rel.to_string_lossy().to_string();
                out.push(build_file_entry(&path, &rel_str, sidecar_map));
            }
        }
        Ok(out)
    }

    if let Some(prefix) = rel_prefix {
        let norm_rel = prefix.trim().trim_start_matches('/');
        let abs_target = normalize_join(&base_abs, norm_rel);
        if !(abs_target.starts_with(&base_abs) || abs_target == base_abs) {
            return Ok(results);
        }
        if abs_target.is_dir() {
            if let Ok(mut v) =
                walk_dir_collect(&base_abs, &abs_target, norm_rel, &sidecar_map).await
            {
                results.append(&mut v);
            }
        } else if abs_target.is_file() {
            results.push(build_file_entry(&abs_target, norm_rel, &sidecar_map));
        }
        return Ok(results);
    }

    if let Ok(mut v) = walk_dir_collect(&base_abs, &base_abs, "", &sidecar_map).await {
        results.append(&mut v);
    }
    Ok(results)
}

async fn get_sidecar_map(base_dir: &Path) -> io::Result<SidecarMap> {
    let sidecar = base_dir.join(".paths-info.json");
    if !sidecar.is_file() {
        return Ok(SidecarMap::new());
    }
    let md = sidecar.metadata()?;
    let size = md.len();
    let mtime = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let key = (
        dunce::canonicalize(&sidecar).unwrap_or(sidecar.clone()),
        mtime,
        size,
    );
    {
        let cache = SIDECAR_CACHE.read().await;
        if let Some(mp) = cache.inner.get(&key) {
            return Ok(mp.clone());
        }
    }
    let data = fs::read_to_string(&sidecar).await?;
    let parsed: Value = serde_json::from_str(&data).unwrap_or(json!({}));
    let mut map: SidecarMap = SidecarMap::new();
    if let Some(entries) = parsed.get("entries").and_then(|v| v.as_array()) {
        for it in entries {
            if it.get("type").and_then(|v| v.as_str()) == Some("file") {
                if let Some(path) = it.get("path").and_then(|v| v.as_str()) {
                    map.insert(path.to_string(), it.clone());
                }
            }
        }
    }
    let mut cache = SIDECAR_CACHE.write().await;
    cache.inner.insert(key, map.clone());
    Ok(map)
}

fn file_size(p: &Path) -> io::Result<u64> {
    Ok(p.metadata()?.len())
}

fn normalize_join(base: &Path, rel: &str) -> PathBuf {
    let joined = base.join(rel);
    dunce::canonicalize(&joined).unwrap_or(joined)
}

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
    let key = (
        dunce::canonicalize(p).unwrap_or(p.to_path_buf()),
        mtime,
        size,
    );
    if let Some(hit) = {
        let cache = SHA256_CACHE.read().await;
        cache.get(&key).cloned()
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
        if cache.len() >= state.sha256_cache_cap {
            if let Some(first_key) = cache.keys().next().cloned() {
                cache.remove(&first_key);
            }
        }
        cache.insert(
            key,
            Sha256Entry {
                sum: sum.clone(),
                at: Instant::now(),
            },
        );
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
        format!("/{}", rest)
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
        if filename == ".paths-info.json" {
            return http_not_found("File not found");
        }
        let filepath = state.root.join(left).join(filename);
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
    if filename == ".paths-info.json" {
        return http_not_found("File not found");
    }

    let filepath = state.root.join(left).join(filename);
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
        let total = file_size(&filepath).unwrap_or(0);
        match parse_range(&rh, total) {
            RangeParse::Invalid => {
                // ignore range, return full file
                return full_file_response(&filepath, None).await;
            }
            RangeParse::Unsatisfiable => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "Content-Range",
                    HeaderValue::from_str(&format!("bytes */{}", total)).unwrap(),
                );
                headers.insert("Accept-Ranges", HeaderValue::from_static("bytes"));
                return (StatusCode::RANGE_NOT_SATISFIABLE, headers).into_response();
            }
            RangeParse::Ok(start, end) => {
                let length = end - start + 1;
                let stream = stream! {
                    let mut f = match fs::File::open(&filepath).await { Ok(f) => f, Err(e) => { error!("open file: {}", e); yield Err(io::Error::new(io::ErrorKind::Other, "open failed")); return; } };
                    if let Err(e) = f.seek(std::io::SeekFrom::Start(start)).await { error!("seek: {}", e); yield Err(io::Error::new(io::ErrorKind::Other, "seek failed")); return; }
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
                let mut headers = HeaderMap::new();
                headers.insert(
                    "Content-Range",
                    HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, total)).unwrap(),
                );
                headers.insert("Accept-Ranges", HeaderValue::from_static("bytes"));
                headers.insert(
                    "Content-Length",
                    HeaderValue::from_str(&length.to_string()).unwrap(),
                );
                headers.insert(
                    "Content-Type",
                    HeaderValue::from_static("application/octet-stream"),
                );
                headers.insert(
                    "x-repo-commit",
                    HeaderValue::from_str(revision).unwrap_or(HeaderValue::from_static("-")),
                );
                headers.insert(
                    "x-revision",
                    HeaderValue::from_str(revision).unwrap_or(HeaderValue::from_static("-")),
                );
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

    full_file_response(&filepath, None).await
}

async fn full_file_response(path: &Path, _filename: Option<&str>) -> Response {
    // Read entire file into body stream using tokio_util::io::ReaderStream if desired.
    // For simplicity and parity, we use a streaming reader.
    let file = match fs::File::open(path).await {
        Ok(f) => f,
        Err(_) => return http_not_found("File not found"),
    };
    let size = file.metadata().await.ok().map(|m| m.len()).unwrap_or(0);
    let stream = tokio_util::io::ReaderStream::with_capacity(file, CHUNK_SIZE);
    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Length",
        HeaderValue::from_str(&size.to_string()).unwrap(),
    );
    headers.insert(
        "Content-Type",
        HeaderValue::from_static("application/octet-stream"),
    );
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
    state: &AppState,
    repo_id: &str,
    revision: &str,
    filename: &str,
    filepath: &Path,
) -> Response {
    let size = file_size(filepath).unwrap_or(0);
    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Length",
        HeaderValue::from_str(&size.to_string()).unwrap(),
    );
    headers.insert(
        "Content-Type",
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert("Accept-Ranges", HeaderValue::from_static("bytes"));
    headers.insert(
        "x-repo-commit",
        HeaderValue::from_str(revision).unwrap_or(HeaderValue::from_static("-")),
    );
    headers.insert(
        "x-revision",
        HeaderValue::from_str(revision).unwrap_or(HeaderValue::from_static("-")),
    );

    // ETag strictly from sidecar; otherwise 500
    let repo_root = state.root.join(repo_id);
    let rel_path = filename.replace('\\', "/");
    let sc_map = get_sidecar_map(&repo_root).await.unwrap_or_default();
    let mut etag: Option<String> = None;
    if let Some(sc) = sc_map.get(&rel_path) {
        let ok_size = sc
            .get("size")
            .and_then(|v| v.as_u64())
            .map(|s| s == size)
            .unwrap_or(true);
        if ok_size {
            if let Some(lfs_oid) = sc
                .get("lfs")
                .and_then(|v| v.get("oid"))
                .and_then(|v| v.as_str())
            {
                etag = Some(lfs_oid.split(':').last().unwrap_or(lfs_oid).to_string());
            } else if let Some(oid) = sc.get("oid").and_then(|v| v.as_str()) {
                etag = Some(oid.to_string());
            } else if let Some(e) = sc.get("etag").and_then(|v| v.as_str()) {
                etag = Some(e.to_string());
            }
        }
    }
    if etag.is_none() {
        error!("ETag missing for {}@{}:{}", repo_id, revision, rel_path);
        return http_error(StatusCode::INTERNAL_SERVER_ERROR, "ETag not available");
    }
    let quoted = format!("\"{}\"", etag.unwrap());
    headers.insert(
        "ETag",
        HeaderValue::from_str(&quoted).unwrap_or(HeaderValue::from_static("\"-\"")),
    );
    if sc_map
        .get(&rel_path)
        .and_then(|sc| sc.get("lfs"))
        .is_some()
    {
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
    if unit.to_ascii_lowercase() != "bytes" {
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
        return RangeParse::Ok(start, end);
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
        return RangeParse::Ok(start, end);
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
