use std::time::Instant;

use axum::extract::{Path as AxPath, Request as AxRequest, State};
use axum::response::IntoResponse;
use axum::Json;
use axum::http::StatusCode;
use serde_json::Value;

use crate::app_state::AppState;
use crate::caches::{SIBLINGS_CACHE, SiblingsEntry};
use crate::utils::paths::secure_join;
use crate::utils::repo_json::{build_repo_json, RepoJsonFlavor, RepoKind};
use crate::{http_error, http_not_found, paths_info_response};

pub(crate) async fn get_dataset_catchall_get(
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
        if let Some(vals) = crate::utils::fs_walk::collect_paths_info_from_sidecar(&ds_path).await {
            return Json(vals).into_response();
        }
        return http_error(StatusCode::INTERNAL_SERVER_ERROR, "Sidecar missing or incomplete");
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

pub(crate) async fn get_dataset_paths_info_post(
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
) -> Result<Value, axum::response::Response> {
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
            let fresh = Instant::now();
            let mut cachew = SIBLINGS_CACHE.write().await;
            if let Some(entry) = cachew.inner.get_mut(&cache_key) {
                entry.at = fresh;
                cachew.evict_q.push_back((cache_key.clone(), fresh));
            }
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
        if let Some((s, t)) = crate::utils::fs_walk::siblings_from_sidecar(&ds_path).await {
            (s, t)
        } else {
            return Err(http_error(StatusCode::INTERNAL_SERVER_ERROR, "Sidecar missing or incomplete"));
        };
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

