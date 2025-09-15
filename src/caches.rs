use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::time::Instant;

use serde_json::Value;
use tokio::sync::RwLock;

// In-memory sidecar cache
pub type SidecarMap = std::sync::Arc<HashMap<String, Value>>; // rel_path (posix) -> entry (Arc for cheap clones)

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

pub struct SiblingsCache {
    pub inner: HashMap<String, SiblingsEntry>,
    pub evict_q: VecDeque<(String, Instant)>,
}

impl Default for SiblingsCache {
    fn default() -> Self {
        Self { inner: HashMap::new(), evict_q: VecDeque::new() }
    }
}

pub static SIBLINGS_CACHE: once_cell::sync::Lazy<RwLock<SiblingsCache>> =
    once_cell::sync::Lazy::new(|| RwLock::new(SiblingsCache::default()));

#[derive(Clone)]
pub struct PathsInfoEntry {
    pub items: Vec<Value>,
    pub at: Instant,
}

pub struct PathsInfoCache {
    pub inner: HashMap<String, PathsInfoEntry>,
    pub evict_q: VecDeque<(String, Instant)>,
}

impl Default for PathsInfoCache {
    fn default() -> Self {
        Self { inner: HashMap::new(), evict_q: VecDeque::new() }
    }
}

pub static PATHS_INFO_CACHE: once_cell::sync::Lazy<RwLock<PathsInfoCache>> =
    once_cell::sync::Lazy::new(|| RwLock::new(PathsInfoCache::default()));

#[derive(Clone)]
pub struct Sha256Entry {
    pub sum: String,
    pub at: Instant,
}

pub type Sha256Key = (PathBuf, u64, u64);

pub struct Sha256Cache {
    pub inner: HashMap<Sha256Key, Sha256Entry>,
    pub evict_q: VecDeque<(Sha256Key, Instant)>,
}

impl Default for Sha256Cache {
    fn default() -> Self {
        Self { inner: HashMap::new(), evict_q: VecDeque::new() }
    }
}

pub static SHA256_CACHE: once_cell::sync::Lazy<RwLock<Sha256Cache>> =
    once_cell::sync::Lazy::new(|| RwLock::new(Sha256Cache::default()));
