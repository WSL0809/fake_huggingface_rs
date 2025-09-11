use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct AppState {
    pub root: Arc<PathBuf>,
    // logging options
    pub log_requests: bool,
    pub log_body_max: usize,
    pub log_headers_mode_all: bool,
    pub log_resp_headers: bool,
    pub log_redact: bool,
    pub log_body_all: bool,
    // cache options
    pub cache_ttl: Duration,
    pub paths_info_cache_cap: usize,
    pub siblings_cache_cap: usize,
    pub sha256_cache_cap: usize,
}
