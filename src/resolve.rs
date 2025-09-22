use std::io;
use std::path::Path;
use std::time::UNIX_EPOCH;

use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::extract::{Path as AxPath, Request as AxRequest, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::error;
use sha2::Digest;

use crate::app_state::AppState;
use crate::caches::{SHA256_CACHE, Sha256Entry};
use crate::utils::headers::{file_headers_common, set_content_range};
use crate::utils::paths::{is_sidecar_path, secure_join};
use crate::utils::sidecar::{etag_from_sidecar, get_sidecar_map};
use crate::{CHUNK_SIZE, http_error, http_not_found};

// ============ Resolve (GET/HEAD) ============
pub(crate) async fn resolve_catchall(
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
                headers.insert("Content-Length", HeaderValue::from_static("0"));
                return (StatusCode::RANGE_NOT_SATISFIABLE, headers).into_response();
            }
            RangeParse::Ok(start, end) => {
                let length = end - start + 1;
                let fp_for_stream = filepath.clone();
                let stream = stream! {
                    let mut f =
                        match tokio::fs::File::open(fp_for_stream).await { Ok(f) => f, Err(e) => { let _ = e; return; } };
                    if let Err(e) = f.seek(std::io::SeekFrom::Start(start)).await {
                        let _ = e; return;
                    }
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
                if let Err(resp) = ensure_and_insert_etag(&mut headers, &filepath, filename, left, revision, total).await {
                    return resp;
                }
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
    repo_id: &str,
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
    if let Err(resp) = ensure_and_insert_etag(&mut headers, path, filename, repo_id, revision, size).await {
        return resp;
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
    if let Err(resp) = ensure_and_insert_etag(&mut headers, filepath, filename, repo_id, revision, size).await {
        return resp;
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

// Compute sha256 with TTL cache keyed by (path, mtime, size)
async fn sha256_file_cached(state: &AppState, p: &Path) -> io::Result<String> {
    let md = tokio::fs::metadata(p).await?;
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
        if std::time::Instant::now().duration_since(hit.at) < state.cache_ttl {
            let fresh = std::time::Instant::now();
            let mut cachew = SHA256_CACHE.write().await;
            let cloned = if let Some(entry) = cachew.inner.get_mut(&key) {
                entry.at = fresh;
                Some(entry.sum.clone())
            } else { None };
            cachew.evict_q.push_back((key.clone(), fresh));
            if let Some(sum) = cloned { return Ok(sum); }
            return Ok(hit.sum);
        }
    }
    let mut file = tokio::fs::File::open(p).await?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        use sha2::Digest;
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
        let now_i = std::time::Instant::now();
        cache.evict_q.push_back((key.clone(), now_i));
        cache.inner.insert(key, Sha256Entry { sum: sum.clone(), at: now_i });
    }
    Ok(sum)
}

// Strictly load ETag from sidecar and inject into headers.
// No fallback permitted: on failure returns an HTTP 500 Response.
async fn ensure_and_insert_etag(
    headers: &mut HeaderMap,
    filepath: &Path,
    filename: &str,
    repo_id: &str,
    revision: &str,
    total_size: u64,
) -> Result<(), Response> {
    // Derive repo root by walking up path components of filename.
    let mut repo_root = filepath.to_path_buf();
    let depth = filename.split('/').count();
    for _ in 0..depth {
        if let Some(parent) = repo_root.parent() {
            repo_root = parent.to_path_buf();
        }
    }
    let sc_map = get_sidecar_map(&repo_root).await.unwrap_or_default();
    let rel_path = filename.replace('\\', "/");
    let etag_pair = etag_from_sidecar(&sc_map, &rel_path, total_size);
    match etag_pair {
        None => {
            error!("ETag missing for {}@{}:{}", repo_id, revision, rel_path);
            Err(http_error(StatusCode::INTERNAL_SERVER_ERROR, "ETag not available"))
        }
        Some((etag, is_lfs)) => {
            let quoted = format!("\"{etag}\"");
            headers.insert(
                "ETag",
                HeaderValue::from_str(&quoted).unwrap_or(HeaderValue::from_static("\"-\"")),
            );
            if is_lfs {
                headers.insert(
                    "x-lfs-size",
                    HeaderValue::from_str(&total_size.to_string()).unwrap(),
                );
            }
            Ok(())
        }
    }
}

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
            log_json_body: false,
            ip_log_retention_secs: 1_800,
            ip_log_per_ip_cap: 200,
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
