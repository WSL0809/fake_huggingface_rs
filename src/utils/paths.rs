use std::path::{Path, PathBuf};
use tokio::fs;

// Normalize a relative path, rejecting absolute segments and attempts to escape root.
pub fn normalize_rel(rel: &str) -> Option<PathBuf> {
    let p = Path::new(rel);
    if p.is_absolute() {
        return None;
    }
    let mut parts: Vec<&str> = Vec::new();
    let cleaned = rel.replace('\\', "/");
    for seg in cleaned.split('/') {
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." {
            parts.pop()?;
        } else {
            parts.push(seg);
        }
    }
    let mut out = PathBuf::new();
    for seg in parts {
        out.push(seg);
    }
    Some(out)
}

// Join base + relative and ensure the result stays under base.
pub fn secure_join(base: &Path, rel: &str) -> Option<PathBuf> {
    let base_abs = dunce::canonicalize(base).ok()?;
    let rel_norm = normalize_rel(rel)?;
    let joined = base_abs.join(&rel_norm);
    let joined_can = dunce::canonicalize(&joined).unwrap_or(joined);
    if joined_can.starts_with(&base_abs) {
        Some(joined_can)
    } else {
        None
    }
}

// Join then try to canonicalize; used when we already know rel is safe.
pub fn normalize_join_abs(base: &Path, rel: &str) -> PathBuf {
    let joined = base.join(rel);
    dunce::canonicalize(&joined).unwrap_or(joined)
}

pub fn file_size(p: &Path) -> std::io::Result<u64> { Ok(p.metadata()?.len()) }

pub async fn file_size_async(p: &Path) -> u64 {
    fs::metadata(p).await.ok().map(|m| m.len()).unwrap_or(0)
}

pub fn is_sidecar_path(p: &str) -> bool {
    let p = Path::new(p);
    p.file_name().and_then(|s| s.to_str()) == Some(".paths-info.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_rel_basic() {
        assert_eq!(normalize_rel("a/b").unwrap(), PathBuf::from("a/b"));
        assert_eq!(normalize_rel("a/./b").unwrap(), PathBuf::from("a/b"));
        assert_eq!(normalize_rel("a/../b").unwrap(), PathBuf::from("b"));
        assert!(normalize_rel("/abs").is_none());
        assert!(normalize_rel("../../etc").is_none());
    }

    #[test]
    fn secure_join_rejects_escape() {
        let base = Path::new(".");
        let ok = secure_join(base, "src/main.rs");
        assert!(ok.is_some());
        let bad = secure_join(base, "../..//etc/passwd");
        assert!(bad.is_none());
    }

    #[test]
    fn detect_sidecar_name() {
        assert!(is_sidecar_path(".paths-info.json"));
        assert!(is_sidecar_path("foo/.paths-info.json"));
        assert!(!is_sidecar_path("paths-info.json"));
    }
}
