use serde_json::{Value, json};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RepoKind {
    Model,
    Dataset,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RepoJsonFlavor {
    Minimal,
    Rich,
}

pub fn fake_sha(revision: Option<&str>) -> String {
    revision
        .map(|r| format!("fakesha-{r}"))
        .unwrap_or_else(|| "fakesha1234567890".to_string())
}

pub fn build_repo_json(
    kind: RepoKind,
    repo_id: &str,
    revision: Option<&str>,
    siblings: &[Value],
    total_size: u64,
    flavor: RepoJsonFlavor,
) -> Value {
    let sha = fake_sha(revision);
    match (kind, flavor) {
        (RepoKind::Model, RepoJsonFlavor::Rich) => {
            json!({
                "_id": format!("local/{}", repo_id),
                "id": repo_id,
                "private": false,
                "pipeline_tag": "text-generation",
                "library_name": "transformers",
                "tags": ["transformers", "gpt2", "text-generation"],
                "downloads": 0,
                "likes": 0,
                "modelId": repo_id,
                "author": "local-user",
                "sha": sha,
                "lastModified": "1970-01-01T00:00:00.000Z",
                "createdAt": "1970-01-01T00:00:00.000Z",
                "gated": false,
                "disabled": false,
                "widgetData": [{"text": "Hello"}],
                "model-index": Value::Null,
                "config": {"architectures": ["GPT2LMHeadModel"], "model_type": "gpt2", "tokenizer_config": {}},
                "cardData": {"language": "en", "tags": ["example"], "license": "mit"},
                "transformersInfo": {
                    "auto_model": "AutoModelForCausalLM",
                    "pipeline_tag": "text-generation",
                    "processor": "AutoTokenizer",
                },
                "safetensors": {"parameters": {"F32": 0}, "total": 0},
                "siblings": siblings,
                "spaces": [],
                "usedStorage": (total_size as i64),
            })
        }
        (RepoKind::Model, RepoJsonFlavor::Minimal) => {
            json!({
                "_id": format!("local/{}", repo_id),
                "id": repo_id,
                "private": false,
                "modelId": repo_id,
                "sha": sha,
                "model-index": Value::Null,
                "siblings": siblings,
                "usedStorage": (total_size as i64),
            })
        }
        (RepoKind::Dataset, RepoJsonFlavor::Minimal) => {
            json!({
                "_id": format!("local/datasets/{}", repo_id),
                "id": repo_id,
                "private": false,
                "tags": ["dataset"],
                "sha": sha,
                "siblings": siblings,
                "usedStorage": (total_size as i64),
            })
        }
        (RepoKind::Dataset, RepoJsonFlavor::Rich) => {
            json!({
                "_id": format!("local/datasets/{}", repo_id),
                "id": repo_id,
                "private": false,
                "tags": ["dataset"],
                "downloads": 0,
                "likes": 0,
                "author": "local-user",
                "sha": sha,
                "lastModified": "1970-01-01T00:00:00.000Z",
                "createdAt": "1970-01-01T00:00:00.000Z",
                "gated": false,
                "disabled": false,
                "cardData": {"license": "mit", "language": ["en"]},
                "siblings": siblings,
                "usedStorage": (total_size as i64),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_minimal_shape() {
        let v = build_repo_json(
            RepoKind::Model,
            "foo/bar",
            Some("main"),
            &[],
            123,
            RepoJsonFlavor::Minimal,
        );
        assert_eq!(v["id"], "foo/bar");
        assert_eq!(v["modelId"], "foo/bar");
        assert_eq!(v["_id"], "local/foo/bar");
        assert_eq!(v["usedStorage"], 123);
        assert!(v.get("model-index").is_some());
    }

    #[test]
    fn dataset_rich_shape() {
        let v = build_repo_json(
            RepoKind::Dataset,
            "ds/foo",
            None,
            &[],
            0,
            RepoJsonFlavor::Rich,
        );
        assert_eq!(v["_id"], "local/datasets/ds/foo");
        assert_eq!(v["id"], "ds/foo");
        assert_eq!(v["tags"][0], "dataset");
        assert!(v.get("downloads").is_some());
    }
}
