use std::io;
use std::path::Path;

use serde_json::{Value, json};
use tokio::fs;

use crate::caches::SidecarMap;
use crate::utils::paths::normalize_join_abs;
use crate::utils::sidecar::get_sidecar_map;

pub async fn collect_paths_info(
    base_dir: &Path,
    rel_prefix: Option<&str>,
) -> Result<Vec<Value>, super::super::Response> {
    // Caller passes canonical base_dir (from secure_join); avoid redundant canonicalize
    let base_abs = base_dir.to_path_buf();
    let mut results: Vec<Value> = Vec::new();
    let sidecar_map = get_sidecar_map(&base_abs).await.unwrap_or_default();

    fn build_file_entry_with_size(rel_path: &str, sidecar_map: &SidecarMap, pre_size: Option<u64>) -> Value {
        let rel_norm = rel_path.replace('\\', "/");
        if let Some(sc) = sidecar_map.get(&rel_norm) {
            // Prefer sidecar-provided size (and lfs.size). Fall back to precomputed size if missing.
            let sidecar_size = sc
                .get("size")
                .and_then(|v| v.as_i64())
                .or_else(|| {
                    sc.get("lfs")
                        .and_then(|v| v.get("size"))
                        .and_then(|v| v.as_i64())
                });
            let size_i64 = match (sidecar_size, pre_size) {
                (Some(s), _) if s >= 0 => s,
                (_, Some(ps)) => ps as i64,
                _ => 0,
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
                // Keep lfs.size consistent with top-level size if available; otherwise use sidecar value if present
                let lfs_size = lfs
                    .get("size")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(size_i64);
                ldict.insert("size".to_string(), json!(lfs_size));
                rec.insert("lfs".to_string(), Value::Object(ldict));
            }
            return Value::Object(rec);
        }
        let size = pre_size.unwrap_or(0);
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
                let is_dir = match ent.file_type().await { Ok(t) => t.is_dir(), Err(_) => false };
                if is_dir {
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
                let size = ent.metadata().await.ok().map(|m| m.len());
                out.push(build_file_entry_with_size(&rel_str, sidecar_map, size));
            }
        }
        Ok(out)
    }

    if let Some(prefix) = rel_prefix {
        let norm_rel = prefix.trim().trim_start_matches('/');
        let abs_target = normalize_join_abs(&base_abs, norm_rel);
        if !(abs_target.starts_with(&base_abs) || abs_target == base_abs) {
            return Ok(results);
        }
        match fs::metadata(&abs_target).await {
            Ok(md) if md.is_dir() => {
                if let Ok(mut v) = walk_dir_collect(&base_abs, &abs_target, norm_rel, &sidecar_map).await {
                    results.append(&mut v);
                }
            }
            Ok(md) if md.is_file() => {
                results.push(build_file_entry_with_size(norm_rel, &sidecar_map, Some(md.len())));
            }
            _ => {}
        }
        return Ok(results);
    }

    if let Ok(mut v) = walk_dir_collect(&base_abs, &base_abs, "", &sidecar_map).await {
        results.append(&mut v);
    }
    Ok(results)
}

pub async fn list_siblings_except_sidecar(
    root: &Path,
) -> std::io::Result<(Vec<Value>, u64)> {
    let mut siblings: Vec<Value> = Vec::new();
    let mut total_size: u64 = 0;
    let mut dirs = vec![root.to_path_buf()];
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
                let rel = pathdiff::diff_paths(&path, root).unwrap_or(path.clone());
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
    Ok((siblings, total_size))
}
