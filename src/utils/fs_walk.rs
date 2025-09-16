use std::path::Path;

use serde_json::{Value, json};

use crate::utils::sidecar::get_sidecar_map;


// Fast path: build full file entries from sidecar without hitting filesystem.
// Returns None if sidecar missing/empty; caller should fall back to walking.
pub async fn collect_paths_info_from_sidecar(base_dir: &Path) -> Option<Vec<Value>> {
    let sc_map = get_sidecar_map(base_dir).await.ok()?;
    let mut out: Vec<Value> = Vec::with_capacity(sc_map.len());
    for (rel, v) in sc_map.iter() {
        let mut rec = serde_json::Map::new();
        rec.insert("path".to_string(), json!(rel));
        rec.insert("type".to_string(), json!("file"));
        // Require size present (either top-level or lfs.size); otherwise sidecar is incomplete.
        let Some(size) = v
            .get("size")
            .and_then(|x| x.as_i64())
            .or_else(|| v.get("lfs").and_then(|x| x.get("size")).and_then(|x| x.as_i64()))
        else {
            return None;
        };
        rec.insert("size".to_string(), json!(size));
        if let Some(oid) = v.get("oid").and_then(|x| x.as_str()) {
            rec.insert("oid".to_string(), json!(oid));
        }
        if let Some(lfs) = v.get("lfs").and_then(|x| x.as_object()) {
            let mut ldict = serde_json::Map::new();
            if let Some(loid) = lfs.get("oid").and_then(|x| x.as_str()) {
                ldict.insert("oid".to_string(), json!(loid));
            }
            if let Some(lsz) = lfs.get("size").and_then(|x| x.as_i64()) {
                ldict.insert("size".to_string(), json!(lsz));
            }
            rec.insert("lfs".to_string(), Value::Object(ldict));
        }
        out.push(Value::Object(rec));
    }
    Some(out)
}

// Fast path for repo siblings/total_size using sidecar only.
// Returns None when sidecar missing/empty.
pub async fn siblings_from_sidecar(root: &Path) -> Option<(Vec<Value>, u64)> {
    let sc_map = get_sidecar_map(root).await.ok()?;
    let mut items: Vec<Value> = Vec::with_capacity(sc_map.len());
    let mut total: u64 = 0;
    for (rel, v) in sc_map.iter() {
        items.push(json!({ "rfilename": rel }));
        let Some(sz) = v
            .get("size")
            .and_then(|x| x.as_i64())
            .or_else(|| v.get("lfs").and_then(|x| x.get("size")).and_then(|x| x.as_i64()))
        else {
            return None;
        };
        if sz > 0 { total = total.saturating_add(sz as u64); }
    }
    items.sort_by(|a, b| {
        a["rfilename"].as_str().unwrap_or("").cmp(b["rfilename"].as_str().unwrap_or(""))
    });
    Some((items, total))
}
