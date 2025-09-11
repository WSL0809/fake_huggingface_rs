use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use serde_json::Value;
use tokio::sync::RwLock;

// In-memory sidecar cache
pub type SidecarMap = HashMap<String, Value>; // rel_path (posix) -> entry

#[derive(Default)]
pub struct SidecarCache {
    // key: (abs_path, mtime_secs, size)
    pub inner: HashMap<(PathBuf, u64, u64), SidecarMap>,
}

pub static SIDECAR_CACHE: once_cell::sync::Lazy<RwLock<SidecarCache>> =
    once_cell::sync::Lazy::new(|| RwLock::new(SidecarCache::default()));

#[derive(Clone)]
pub struct SiblingsEntry {
    pub siblings: Vec<Value>,
    pub total: u64,
    pub at: Instant,
}

pub static SIBLINGS_CACHE: once_cell::sync::Lazy<RwLock<HashMap<String, SiblingsEntry>>> =
    once_cell::sync::Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Clone)]
pub struct PathsInfoEntry {
    pub items: Vec<Value>,
    pub at: Instant,
}

pub static PATHS_INFO_CACHE: once_cell::sync::Lazy<RwLock<HashMap<String, PathsInfoEntry>>> =
    once_cell::sync::Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Clone)]
pub struct Sha256Entry {
    pub sum: String,
    pub at: Instant,
}

pub static SHA256_CACHE: once_cell::sync::Lazy<RwLock<HashMap<(PathBuf, u64, u64), Sha256Entry>>> =
    once_cell::sync::Lazy::new(|| RwLock::new(HashMap::new()));
