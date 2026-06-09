use crate::types::{FileRefEntry, FileRefUpdate, FileStamp, RecToolResult};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use sha1::{Digest, Sha1};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HashRefTarget {
    pub filename: String,
    pub small_hash_code: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileRefInjection {
    pub args: Value,
    pub executor: String,
}

pub fn basename(input: &str) -> String {
    let normalized = input.replace('\\', "/");
    normalized
        .split('/')
        .rfind(|part| !part.is_empty())
        .unwrap_or(input)
        .to_string()
}

pub fn file_key_ref(executor: Option<&str>, file_key: &str) -> String {
    format!(
        "{}:{}",
        executor
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .unwrap_or("local"),
        file_key
    )
}

pub fn small_hash_code(file_key_ref: &str, hash_code: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(format!("{file_key_ref}:{hash_code}"));
    let hex = format!("{:x}", hasher.finalize());
    hex.chars().take(4).collect::<String>().to_uppercase()
}

pub fn label_hash_ref(filename: &str, small_hash_code: &str) -> String {
    format!("{} #{}", filename, small_hash_code.to_uppercase())
}

pub fn parse_hash_ref(target: &str) -> Option<HashRefTarget> {
    let trimmed = target.trim();
    let (filename, code) = trimmed.rsplit_once(" #")?;
    let code = code.trim();
    if code.len() != 4 || !code.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return None;
    }
    let filename = filename.trim();
    if filename.is_empty() {
        return None;
    }
    Some(HashRefTarget {
        filename: filename.to_string(),
        small_hash_code: code.to_uppercase(),
    })
}

pub fn make_entry_parts(
    executor: Option<&str>,
    file_key: &str,
    file_path: &str,
    hash_code: &str,
) -> (String, String, String, String) {
    let file_key_ref = file_key_ref(executor, file_key);
    let filename = basename(file_path);
    let small_hash_code = small_hash_code(&file_key_ref, hash_code);
    let label = label_hash_ref(&filename, &small_hash_code);
    (file_key_ref, filename, small_hash_code, label)
}
pub fn inject_file_ref(args: &Value, entry: Option<&FileRefEntry>) -> FileRefInjection {
    let mut object = args.as_object().cloned().unwrap_or_default();
    let executor = entry
        .map(|item| item.executor.clone())
        .or_else(|| string_field(&object, "executor"))
        .or_else(|| string_field(&object, "targetExecutor"))
        .unwrap_or_else(|| "local".to_string());

    if let Some(entry) = entry {
        object.insert(
            "filePath".to_string(),
            Value::String(entry.file_path.clone()),
        );
        object.insert("hashCheckMode".to_string(), Value::Bool(true));
        object.insert(
            "hashCode".to_string(),
            Value::String(entry.hash_code.clone()),
        );
        if entry.executor == "local" {
            object.remove("executor");
            object.remove("targetExecutor");
        } else {
            object.insert(
                "targetExecutor".to_string(),
                Value::String(entry.executor.clone()),
            );
            object.remove("executor");
        }
    }

    FileRefInjection {
        args: Value::Object(object),
        executor,
    }
}

pub fn extract_file_ref_update(
    result: &RecToolResult,
    executor: impl Into<String>,
) -> Option<FileRefUpdate> {
    let metadata = result.metadata.as_object()?;
    let file = metadata.get("file")?.clone();
    let file: FileStamp = serde_json::from_value(file).ok()?;

    if file.kind != "file" {
        return None;
    }
    let hash_code = metadata.get("hashCode")?.as_str()?.to_string();
    Some(FileRefUpdate {
        executor: executor.into(),
        file,
        hash_code,
    })
}

fn string_field(object: &Map<String, Value>, key: &str) -> Option<String> {
    object.get(key)?.as_str().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn injects_hash_ref_into_rec_args() {
        let entry = FileRefEntry {
            executor: "box".into(),
            file_path: "/remote/a.txt".into(),
            hash_code: "sha256:abc".into(),
            file_key_ref: "box:key".into(),
        };
        let injected = inject_file_ref(&json!({ "filePath": "a.txt #ABCD" }), Some(&entry));
        assert_eq!(injected.executor, "box");
        assert_eq!(injected.args["filePath"], "/remote/a.txt");
        assert_eq!(injected.args["hashCheckMode"], true);
        assert_eq!(injected.args["hashCode"], "sha256:abc");
        assert_eq!(injected.args["targetExecutor"], "box");
    }

    #[test]
    fn extracts_file_ref_update_from_rec_result() {
        let result = RecToolResult {
            metadata: json!({
                "hashCode": "sha256:def",
                "file": { "fileKey": "k", "canonicalPath": "/tmp/a.txt", "kind": "file" }
            }),
            output: json!({}),
        };
        let update = extract_file_ref_update(&result, "local").unwrap();
        assert_eq!(update.executor, "local");
        assert_eq!(update.hash_code, "sha256:def");
        assert_eq!(update.file.canonical_path, "/tmp/a.txt");
    }

    #[test]
    fn parses_and_labels_hash_ref_like_opencode() {
        let parsed = parse_hash_ref("src/main.rs #a1b2").unwrap();
        assert_eq!(parsed.filename, "src/main.rs");
        assert_eq!(parsed.small_hash_code, "A1B2");
        assert_eq!(label_hash_ref("main.rs", "a1b2"), "main.rs #A1B2");
        assert!(parse_hash_ref("src/main.rs#a1b2").is_none());
        assert!(parse_hash_ref("src/main.rs #XYZ1").is_none());
    }

    #[test]
    fn computes_opencode_file_key_ref_parts() {
        let (file_key_ref, filename, small, label) =
            make_entry_parts(Some("box"), "file-key", "/remote/path/App.ts", "sha256:abc");
        assert_eq!(file_key_ref, "box:file-key");
        assert_eq!(filename, "App.ts");
        assert_eq!(small.len(), 4);
        assert_eq!(label, format!("App.ts #{}", small));
    }

    #[test]
    fn labels_hash_refs_with_filename_only() {
        let (_file_key_ref, filename, small, label) = make_entry_parts(
            None,
            "file-key",
            ".opencode/remote_executor_infos.json",
            "sha256:abc",
        );
        assert_eq!(filename, "remote_executor_infos.json");
        assert_eq!(label, format!("remote_executor_infos.json #{}", small));
    }
}
