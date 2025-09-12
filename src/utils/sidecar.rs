use std::io;
use std::path::Path;
use std::time::UNIX_EPOCH;

use serde_json::{Value, json};
use tokio::fs;

use crate::caches::SidecarMap;

pub async fn get_sidecar_map(base_dir: &Path) -> io::Result<SidecarMap> {
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
        let cache = crate::caches::SIDECAR_CACHE.read().await;
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
    let mut cache = crate::caches::SIDECAR_CACHE.write().await;
    cache.inner.insert(key, map.clone());
    Ok(map)
}

// Extract an ETag string from a sidecar map for a given relative path, verifying size.
// Returns (etag, is_lfs) if available and consistent.
pub fn etag_from_sidecar(sc_map: &SidecarMap, rel_path: &str, expected_size: u64) -> Option<(String, bool)> {
    let sc = sc_map.get(rel_path)?;
    let ok_size = sc
        .get("size")
        .and_then(|v| v.as_u64())
        .map(|s| s == expected_size)
        .unwrap_or(true);
    if !ok_size {
        return None;
    }
    if let Some(lfs_oid) = sc
        .get("lfs")
        .and_then(|v| v.get("oid"))
        .and_then(|v| v.as_str())
    {
        let etag = lfs_oid.split(':').next_back().unwrap_or(lfs_oid).to_string();
        return Some((etag, true));
    }
    if let Some(oid) = sc.get("oid").and_then(|v| v.as_str()) {
        return Some((oid.to_string(), false));
    }
    if let Some(e) = sc.get("etag").and_then(|v| v.as_str()) {
        return Some((e.to_string(), false));
    }
    None
}
