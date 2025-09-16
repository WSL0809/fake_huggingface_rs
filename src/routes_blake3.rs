use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::extract::{Path as AxPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use tokio::io::AsyncReadExt;
use tracing::warn;

use crate::CHUNK_SIZE;
use crate::app_state::AppState;
use crate::http_error;
use crate::http_not_found;
use crate::utils::paths::{normalize_rel, secure_join};
use crate::utils::sidecar::get_sidecar_map;

pub(crate) async fn get_repo_blake3(
    State(state): State<AppState>,
    AxPath(repo): AxPath<String>,
) -> impl IntoResponse {
    let repo_id = repo.trim_matches('/');
    if repo_id.is_empty() {
        return http_not_found("Repository not found");
    }

    let Some(repo_path) = resolve_repo_path(&state, repo_id).await else {
        return http_not_found("Repository not found");
    };

    let sc_path = repo_path.join(".paths-info.json");
    if !sc_path.is_file() {
        return http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Sidecar missing or incomplete",
        );
    }

    let sc_map = match get_sidecar_map(&repo_path).await {
        Ok(map) => map,
        Err(err) => {
            warn!(target: "fakehub", "load sidecar failed: {}", err);
            return http_error(StatusCode::INTERNAL_SERVER_ERROR, "Failed to read sidecar");
        }
    };

    let mut out: BTreeMap<String, String> = BTreeMap::new();
    for (rel, entry) in sc_map.iter() {
        if let Some(hash) = entry.get("blake3").and_then(|v| v.as_str()) {
            out.insert(rel.clone(), hash.to_string());
            continue;
        }
        match compute_blake3(&repo_path, rel).await {
            Ok(hash) => {
                out.insert(rel.clone(), hash);
            }
            Err(err) => {
                warn!(target: "fakehub", "compute blake3 failed for {}: {}", rel, err);
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to compute BLAKE3",
                );
            }
        }
    }

    Json(out).into_response()
}

async fn resolve_repo_path(state: &AppState, repo_id: &str) -> Option<PathBuf> {
    let base = state.root.as_ref();
    if let Some(candidate) = secure_join(base, repo_id) {
        if dir_exists(&candidate).await {
            return Some(candidate);
        }
    }

    let dataset_base = base.join("datasets");
    if let Some(candidate) = secure_join(&dataset_base, repo_id) {
        if dir_exists(&candidate).await {
            return Some(candidate);
        }
    }
    None
}

async fn dir_exists(p: &Path) -> bool {
    tokio::fs::metadata(p)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
}

async fn compute_blake3(base: &Path, rel: &str) -> Result<String, io::Error> {
    let rel_norm = normalize_rel(rel)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid path"))?;
    let full = base.join(&rel_norm);
    if !full.starts_with(base) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "path escapes repository",
        ));
    }
    let mut file = tokio::fs::File::open(full).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}
