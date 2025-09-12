use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use clap::Parser;
use glob::Pattern;
use percent_encoding::{utf8_percent_encode, percent_decode_str, AsciiSet, CONTROLS};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, USER_AGENT};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use sha2::{Digest as Sha2Digest, Sha256};
use std::time::Duration;

#[derive(Debug, Clone)]
struct TreeItem {
    path: String,
    lfs_oid: Option<String>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum RepoTypeArg {
    Model,
    Dataset,
}

impl RepoTypeArg {
    fn as_plural(&self) -> &'static str {
        match self {
            RepoTypeArg::Model => "models",
            RepoTypeArg::Dataset => "datasets",
        }
    }
    fn as_singular(&self) -> &'static str {
        match self {
            RepoTypeArg::Model => "model",
            RepoTypeArg::Dataset => "dataset",
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "fetch_repo", about = "Skeletonize a real HF repo (structure + filenames only)")]
struct Opt {
    /// Repository ID, e.g., 'gpt2' or 'org/name'
    repo_id: String,

    /// Repository type
    #[arg(short = 't', long = "repo-type", value_enum, default_value_t = RepoTypeArg::Model)]
    repo_type: RepoTypeArg,

    /// Revision/branch/commit (default: main)
    #[arg(short = 'r', long = "revision", default_value = "main")]
    revision: String,

    /// Remote endpoint (default: env HF_REMOTE_ENDPOINT or https://huggingface.co)
    #[arg(short = 'e', long = "endpoint")]
    endpoint: Option<String>,

    /// HF access token (optional)
    #[arg(long = "token")]
    token: Option<String>,

    /// Glob to include (can repeat)
    #[arg(long = "include")]
    include: Vec<String>,

    /// Glob to exclude (can repeat)
    #[arg(long = "exclude")]
    exclude: Vec<String>,

    /// Limit number of files
    #[arg(long = "max-files")]
    max_files: Option<usize>,

    /// Destination root (override default layout)
    #[arg(long = "dst")]
    dst: Option<PathBuf>,

    /// Overwrite existing files
    #[arg(long = "force")]
    force: bool,

    /// Print actions without writing files
    #[arg(long = "dry-run")]
    dry_run: bool,

    /// Fill created files with repeated content instead of empty files
    #[arg(long = "fill")]
    fill: bool,

    /// Per-file size to fill, e.g. '16MiB' (default if --fill is set)
    #[arg(long = "fill-size")]
    fill_size: Option<String>,

    /// Content string to repeat when filling files (default: zeros)
    #[arg(long = "fill-content")]
    fill_content: Option<String>,

    /// Ignore system proxy settings for HTTP(S) requests
    #[arg(long = "no-proxy")]
    no_proxy: bool,
}

fn env_default_endpoint() -> String {
    std::env::var("HF_REMOTE_ENDPOINT")
        .unwrap_or_else(|_| "https://huggingface.co".to_string())
        .trim_end_matches('/')
        .to_string()
}

fn env_default_root() -> PathBuf {
    PathBuf::from(
        std::env::var("FAKE_HUB_ROOT").unwrap_or_else(|_| "fake_hub".to_string()),
    )
}

// Encode set for a single path segment: keep ALPHA / DIGIT / - . _ ~ unescaped
// and escape '/', '%', '?' , '#', spaces and controls.
const SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')  // space
    .add(b'%')
    .add(b'?')
    .add(b'#')
    .add(b'/');

fn quote_segment(seg: &str) -> String {
    utf8_percent_encode(seg, SEGMENT_ENCODE_SET).to_string()
}

fn quote_repo_id(repo_id: &str) -> String {
    repo_id
        .split('/')
        .map(|raw| {
            // If user already provided URL-encoded, decode first to avoid double-encoding
            let decoded = percent_decode_str(raw).decode_utf8_lossy();
            quote_segment(&decoded)
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn fetch_repo_tree(
    endpoint: &str,
    repo_id: &str,
    repo_type: &RepoTypeArg,
    revision: &str,
    token: Option<&str>,
    no_proxy: bool,
) -> Result<Vec<TreeItem>, String> {
    let rid = quote_repo_id(repo_id);
    let rev = quote_segment(revision);
    let url = format!(
        "{}/api/{}/{}/tree/{}?recursive=1&expand=1",
        endpoint.trim_end_matches('/'),
        repo_type.as_plural(),
        rid,
        rev,
    );

    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("fake-hub-skeleton/0.1 (+rust)"),
    );
    if let Some(t) = token {
        if !t.is_empty() {
            let hv = HeaderValue::from_str(&format!("Bearer {}", t)).map_err(|e| e.to_string())?;
            headers.insert(AUTHORIZATION, hv);
        }
    }

    let mut builder = Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(30));
    if no_proxy {
        builder = builder.no_proxy();
    }
    let client = builder.build().map_err(|e| e.to_string())?;

    let resp = client.get(&url).send().map_err(|e| e.to_string())?;
    let status = resp.status();
    let text = resp.text().map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!(
            "HTTP {} calling {}\nResponse: {}",
            status, url, text
        ));
    }

    let data: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    let mut items_val: Value = data.clone();
    if data.is_object() {
        for key in ["tree", "items", "paths"] {
            if let Some(v) = data.get(key) {
                if v.is_array() {
                    items_val = v.clone();
                    break;
                }
            }
        }
    }
    let mut out: Vec<TreeItem> = Vec::new();
    if let Some(arr) = items_val.as_array() {
        for it in arr {
            if let Some(obj) = it.as_object() {
                let p = obj
                    .get("path")
                    .and_then(|v| v.as_str())
                    .or_else(|| obj.get("rfilename").and_then(|v| v.as_str()));
                let t = obj
                    .get("type")
                    .and_then(|v| v.as_str())
                    .or_else(|| obj.get("kind").and_then(|v| v.as_str()));
                if let (Some(path), Some(kind)) = (p, t) {
                    let tnorm = kind.to_ascii_lowercase();
                    if tnorm == "file" || tnorm == "blob" {
                        let mut lfs_oid = None;
                        if let Some(lfs) = obj.get("lfs").and_then(|v| v.as_object()) {
                            lfs_oid = lfs.get("oid").and_then(|v| v.as_str()).map(|s| s.to_string());
                        }
                        out.push(TreeItem {
                            path: path.to_string(),
                            lfs_oid,
                        });
                    }
                }
            }
        }
    }

    if out.is_empty() {
        let kind = repo_type.as_singular();
        return Err(format!(
            "{} tree unavailable or empty for '{}' at {} ({})",
            capitalize(kind), repo_id, revision, endpoint
        ));
    }
    Ok(out)
}

fn capitalize(s: &str) -> String {
    let mut chs = s.chars();
    match chs.next() {
        Some(c) => c.to_uppercase().collect::<String>() + chs.as_str(),
        None => String::new(),
    }
}

fn keep_by_filters(path: &str, includes: &[String], excludes: &[String]) -> bool {
    if !includes.is_empty() {
        let mut any = false;
        for pat in includes {
            if let Ok(p) = Pattern::new(pat) {
                if p.matches(path) {
                    any = true;
                    break;
                }
            }
        }
        if !any {
            return false;
        }
    }
    if !excludes.is_empty() {
        for pat in excludes {
            if let Ok(p) = Pattern::new(pat) {
                if p.matches(path) {
                    return false;
                }
            }
        }
    }
    true
}

fn dest_root(repo_type: &RepoTypeArg, repo_id: &str, override_dst: Option<&Path>) -> PathBuf {
    if let Some(p) = override_dst {
        return p.to_path_buf();
    }
    let base = env_default_root();
    match repo_type {
        RepoTypeArg::Model => base.join(repo_id),
        RepoTypeArg::Dataset => base.join("datasets").join(repo_id),
    }
}

fn normalize_rel(rel: &str) -> Result<PathBuf, String> {
    if Path::new(rel).is_absolute() {
        return Err(format!("Absolute path not allowed: {}", rel));
    }
    // Normalize manually to prevent escaping root
    let mut parts: Vec<&str> = Vec::new();
    let binding = rel.replace('\\', "/");
    for seg in binding.split('/') {
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." {
            if parts.pop().is_none() {
                return Err(format!("Suspicious path outside root: {}", rel));
            }
            continue;
        }
        parts.push(seg);
    }
    let mut out = PathBuf::new();
    for seg in parts {
        out.push(seg);
    }
    Ok(out)
}

fn safe_join(root: &Path, rel: &str) -> Result<PathBuf, String> {
    let nroot = fs::canonicalize(root).map_err(|e| format!("canonicalize root: {}", e))?;
    let norm = normalize_rel(rel)?;
    let joined = nroot.join(&norm);
    // Ensure within root
    let jp = joined
        .to_str()
        .ok_or_else(|| "non-utf8 path".to_string())?
        .to_string();
    let rp = nroot
        .to_str()
        .ok_or_else(|| "non-utf8 path".to_string())?
        .to_string();
    if !(jp.starts_with(&(rp.clone() + std::path::MAIN_SEPARATOR_STR)) || jp == rp) {
        return Err(format!("Suspicious path outside root: {}", rel));
    }
    Ok(joined)
}

fn ensure_dir(p: &Path) -> Result<(), String> {
    fs::create_dir_all(p).map_err(|e| e.to_string())
}

fn touch_empty_file(p: &Path, force: bool) -> Result<(), String> {
    if p.exists() && !force {
        return Ok(());
    }
    if let Some(parent) = p.parent() {
        ensure_dir(parent)?;
    }
    File::create(p).map(|_| ()).map_err(|e| e.to_string())
}

fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Empty size string".into());
    }
    let mut num = String::new();
    let mut unit = String::new();
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            num.push(ch);
        } else if ch == '.' || ch == ',' {
            break; // ignore fractional part
        } else {
            unit.push(ch);
        }
    }
    if num.is_empty() {
        return Err(format!("Invalid size: {}", s));
    }
    let n: u64 = num.parse::<u64>().map_err(|e| e.to_string())?;
    let u = unit.trim().to_ascii_lowercase();
    Ok(match u.as_str() {
        "" | "b" => n,
        "kb" => n * 1000,
        "mb" => n * 1000 * 1000,
        "gb" => n * 1000 * 1000 * 1000,
        "kib" | "ki" => n * 1024,
        "mib" | "mi" => n * 1024 * 1024,
        "gib" | "gi" => n * 1024 * 1024 * 1024,
        _ => return Err(format!("Unknown size unit in: {}", s)),
    })
}

fn write_filled_file(p: &Path, size_bytes: u64, pattern: &[u8], force: bool) -> Result<(), String> {
    if p.exists() && !force {
        return Ok(());
    }
    if let Some(parent) = p.parent() {
        ensure_dir(parent)?;
    }
    if size_bytes == 0 {
        File::create(p).map_err(|e| e.to_string())?;
        return Ok(());
    }
    let pat = if pattern.is_empty() { &[0u8][..] } else { pattern };
    let target_chunk: usize = 1024 * 1024; // 1 MiB
    let reps = std::cmp::max(1, target_chunk / std::cmp::max(1, pat.len()));
    let mut chunk = Vec::with_capacity(target_chunk);
    for _ in 0..reps {
        chunk.extend_from_slice(pat);
        if chunk.len() >= target_chunk {
            break;
        }
    }
    chunk.truncate(target_chunk);

    let mut f = File::create(p).map_err(|e| e.to_string())?;
    let mut written: u64 = 0;
    while (written as usize) + chunk.len() <= size_bytes as usize {
        f.write_all(&chunk).map_err(|e| e.to_string())?;
        written += chunk.len() as u64;
    }
    let remaining = size_bytes - written;
    if remaining > 0 {
        let mut tail = Vec::with_capacity(remaining as usize);
        while tail.len() < remaining as usize {
            let need = (remaining as usize) - tail.len();
            let take = std::cmp::min(need, pat.len());
            tail.extend_from_slice(&pat[..take]);
        }
        f.write_all(&tail).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<(String, String), String> {
    let mut f = File::open(path).map_err(|e| e.to_string())?;
    let mut buf = vec![0u8; 1024 * 1024];
    let mut h1 = Sha1::new();
    let mut h256: Sha256 = Sha2Digest::new();
    loop {
        let n = f.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        h1.update(&buf[..n]);
        h256.update(&buf[..n]);
    }
    Ok((
        hex::encode(h1.finalize()),
        hex::encode(h256.finalize()),
    ))
}

fn write_paths_info_sidecar(
    dst_root: &Path,
    created_paths: &[(PathBuf, bool)],
    dry_run: bool,
) -> Result<Option<PathBuf>, String> {
    // Canonicalize root to ensure we can derive correct relative paths
    let root_abs = dunce::canonicalize(dst_root).map_err(|e| format!("canonicalize root: {}", e))?;
    let mut entries: Vec<Value> = Vec::new();
    for (abs_path, is_lfs) in created_paths {
        if abs_path.is_file() {
            // Prefer robust diff over strip_prefix to handle mixed absolute/relative roots
            let rel_path = pathdiff::diff_paths(abs_path, &root_abs).unwrap_or(abs_path.clone());
            let rel = rel_path.to_string_lossy().replace('\\', "/");
            let size = abs_path.metadata().map_err(|e| e.to_string())?.len();
            let (sha1_hex, sha256_hex) = hash_file(abs_path)?;
            let mut rec = serde_json::Map::new();
            rec.insert("path".to_string(), json!(rel));
            rec.insert("type".to_string(), json!("file"));
            rec.insert("size".to_string(), json!(size as i64));
            rec.insert("oid".to_string(), json!(sha1_hex));
            if *is_lfs {
                rec.insert(
                    "lfs".to_string(),
                    json!({"oid": format!("sha256:{}", sha256_hex), "size": (size as i64)}),
                );
            }
            let rec = Value::Object(rec);
            entries.push(rec);
        }
    }
    if entries.is_empty() {
        return Ok(None);
    }
    let sidecar_path = root_abs.join(".paths-info.json");
    if dry_run {
        return Ok(Some(sidecar_path));
    }
    ensure_dir(&root_abs)?;
    let obj = json!({"version": 1, "entries": entries});
    let s = serde_json::to_string_pretty(&obj).map_err(|e| e.to_string())?;
    fs::write(&sidecar_path, s).map_err(|e| e.to_string())?;
    Ok(Some(sidecar_path))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();

    let endpoint = opt
        .endpoint
        .unwrap_or_else(|| env_default_endpoint());
    let token = opt
        .token
        .or_else(|| std::env::var("HF_TOKEN").ok())
        .or_else(|| std::env::var("HUGGING_FACE_HUB_TOKEN").ok())
        .or_else(|| std::env::var("HUGGINGFACEHUB_API_TOKEN").ok());

    // Fetch remote tree
    let items = match fetch_repo_tree(
        &endpoint,
        &opt.repo_id,
        &opt.repo_type,
        &opt.revision,
        token.as_deref(),
        opt.no_proxy
    ) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}", e);
            return Ok(());
        }
    };

    // Apply include/exclude/max-files filters
    let mut filtered: Vec<&TreeItem> = items
        .iter()
        .filter(|ti| keep_by_filters(&ti.path, &opt.include, &opt.exclude))
        .collect();
    if let Some(m) = opt.max_files {
        filtered.truncate(m);
    }

    // Destination root
    let dst_root = dest_root(&opt.repo_type, &opt.repo_id, opt.dst.as_deref());
    ensure_dir(&dst_root).map_err(|e| format!("create root: {}", e))?;

    // Resolve filler options
    let mut fill_size_bytes: Option<u64> = None;
    let mut fill_pattern: Vec<u8> = Vec::new();
    if opt.fill {
        fill_size_bytes = Some(if let Some(ref s) = opt.fill_size {
            parse_size(s)?
        } else {
            16 * 1024 * 1024
        });
        if let Some(ref s) = opt.fill_content {
            fill_pattern = s.as_bytes().to_vec();
        }
    }

    // Create files
    let mut created_abs: Vec<(PathBuf, bool)> = Vec::new();
    for it in filtered {
        let abs = match safe_join(&dst_root, &it.path) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Warning: {}", e);
                continue;
            }
        };
        let is_lfs = it.lfs_oid.is_some();
        if opt.dry_run {
            created_abs.push((abs, is_lfs));
            continue;
        }
        if let Some(sz) = fill_size_bytes {
            write_filled_file(&abs, sz, &fill_pattern, opt.force)?;
        } else {
            touch_empty_file(&abs, opt.force)?;
        }
        created_abs.push((abs, is_lfs));
    }

    // Write sidecar
    match write_paths_info_sidecar(&dst_root, &created_abs, opt.dry_run) {
        Ok(Some(sc)) => println!("Wrote sidecar: {}", sc.display()),
        Ok(None) => {}
        Err(e) => eprintln!("Warning: failed to write .paths-info.json: {}", e),
    }

    println!("Skeleton root: {}", dst_root.display());
    println!("Files: {}", created_abs.len());
    for (p, _) in &created_abs {
        let rel = p
            .strip_prefix(&dst_root)
            .unwrap_or(p)
            .to_string_lossy()
            .to_string();
        println!("  {}", rel);
    }

    Ok(())
}
