use std::{fs, path::PathBuf};

use clap::Parser;
use reqwest::blocking::Client;
use serde_json::{Value, json};

#[derive(Parser, Debug)]
#[command(name = "fetch_repo", about = "Fetch repo skeleton from HuggingFace")]
struct Opt {
    /// Repository type: models or datasets
    #[arg(long, default_value = "models")]
    repo_type: String,

    /// Repository id, e.g., user/repo
    repo_id: String,

    /// Revision, defaults to main
    #[arg(long, default_value = "main")]
    revision: String,

    /// Destination root directory
    #[arg(long, default_value = "fake_hub")]
    dest: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();

    let base = format!(
        "https://huggingface.co/api/{}/{}/paths-info/{}",
        opt.repo_type, opt.repo_id, opt.revision
    );
    let client = Client::new();
    let resp: Value = client
        .post(&base)
        .json(&serde_json::json!({"expand": true}))
        .send()?
        .error_for_status()?
        .json()?;

    let repo_root = match opt.repo_type.as_str() {
        "datasets" => opt.dest.join("datasets").join(&opt.repo_id),
        _ => opt.dest.join(&opt.repo_id),
    };

    let mut entries: Vec<Value> = Vec::new();
    if let Some(paths) = resp["paths"].as_array() {
        for item in paths {
            let path = item["path"].as_str().unwrap_or("");
            let kind = item["type"].as_str().unwrap_or("file");
            let size = item["size"].as_u64().unwrap_or(0);
            let full = repo_root.join(path);
            if kind == "directory" {
                fs::create_dir_all(&full)?;
            } else {
                if let Some(parent) = full.parent() {
                    fs::create_dir_all(parent)?;
                }
                let file = fs::File::create(&full)?;
                if size > 0 {
                    file.set_len(size)?;
                }

                // Build sidecar entry
                let mut rec = serde_json::Map::new();
                rec.insert("path".to_string(), Value::String(path.to_string()));
                rec.insert("type".to_string(), Value::String("file".to_string()));
                rec.insert("size".to_string(), Value::from(size as i64));
                if let Some(oid) = item.get("oid") {
                    rec.insert("oid".to_string(), oid.clone());
                }
                if let Some(lfs) = item.get("lfs") {
                    rec.insert("lfs".to_string(), lfs.clone());
                }
                entries.push(Value::Object(rec));
            }
        }
    }

    // Write .paths-info.json at repo root so HEAD requests can read ETag
    if !entries.is_empty() {
        let sidecar = json!({ "entries": entries });
        let sc_path = repo_root.join(".paths-info.json");
        if let Some(parent) = sc_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(sc_path, serde_json::to_string_pretty(&sidecar)?)?;
    }

    Ok(())
}
