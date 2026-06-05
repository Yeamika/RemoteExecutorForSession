#[cfg(test)]
pub mod integration_tests;
pub mod json_file_host;
pub mod memory_host;

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::demos::json_file_host::JsonFileSessionHost;
    use crate::demos::memory_host::MemorySessionHost;
    use crate::host::{
        ExbashSessionStore, ExbashSyncInput, ExbashWorkdirStore, HashRefSessionStore,
        RemoteExecutorConfigStore, SessionWorkdirProvider,
    };
    use crate::types::FileRefUpdate;
    use serde_json::json;

    fn update() -> FileRefUpdate {
        FileRefUpdate {
            executor: "local".into(),
            file: crate::types::FileStamp {
                file_key: "fk".into(),
                canonical_path: "/tmp/a.txt".into(),
                kind: "file".into(),
                size: Some(1),
                mtime_ms: Some(2),
            },
            hash_code: "sha256:abc".into(),
        }
    }

    fn task_input() -> ExbashSyncInput {
        ExbashSyncInput {
            async_id: Some("rex-1".into()),
            executor: Some("local".into()),
            command: Some("echo hi".into()),
            state: Some("running".into()),
            pid: Some(123),
            started_at: Some(10),
            ..ExbashSyncInput::default()
        }
    }

    #[tokio::test]
    async fn memory_host_round_trips_hashref_and_exbash() {
        let host = MemorySessionHost::new("ses", "/work");
        assert_eq!(host.session_workdir("ses").await.unwrap(), "/work");
        let entry = host.store_hash_ref("ses", update()).await.unwrap();
        let label = crate::refs::label_hash_ref(
            &crate::refs::basename(&entry.file_path),
            &crate::refs::small_hash_code(&entry.file_key_ref, &entry.hash_code),
        );
        let resolved = host.resolve_hash_ref("ses", &label).await.unwrap();
        assert_eq!(resolved.file_path, "/tmp/a.txt");
        let snapshot = host
            .upsert_session_exbash("ses", task_input())
            .await
            .unwrap();
        assert_eq!(snapshot.async_id, "rex-1");
        let wd = host
            .upsert_workdir_exbash("ses", "/work", task_input())
            .await
            .unwrap();
        assert_eq!(wd.workdir.unwrap(), "/work");
        let config = host
            .update_remote_executor_config("/work", json!({"a": 1}))
            .await
            .unwrap();
        assert_eq!(config.config["a"], 1);
    }

    #[tokio::test]
    async fn json_file_host_round_trips_hashref_and_exbash() {
        let dir = tempdir().unwrap();
        let host = JsonFileSessionHost::new("ses", "/work", dir.path());
        let entry = host.store_hash_ref("ses", update()).await.unwrap();
        let label = crate::refs::label_hash_ref(
            &crate::refs::basename(&entry.file_path),
            &crate::refs::small_hash_code(&entry.file_key_ref, &entry.hash_code),
        );
        let resolved = host.resolve_hash_ref("ses", &label).await.unwrap();
        assert_eq!(resolved.executor, "local");
        let snapshot = host
            .upsert_session_exbash("ses", task_input())
            .await
            .unwrap();
        assert_eq!(snapshot.async_id, "rex-1");
        let config = host
            .update_remote_executor_config("/work", json!({"x": true}))
            .await
            .unwrap();
        assert!(config.config["x"].as_bool().unwrap());
    }
}
