use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::host::{ExbashSessionStore, ExbashSyncInput, ExbashWorkdirStore, HashRefSessionStore, RemoteExecutorConfigStore, SessionWorkdirProvider};
use crate::refs::{make_entry_parts, parse_hash_ref, small_hash_code, basename};
use crate::types::{ExbashTaskSnapshot, FileRefEntry, FileRefUpdate, RemoteExecutorConfigSnapshot};

type Key = String;

#[derive(Clone)]
pub struct MemorySessionHost {
    session_id: String,
    workdir: String,
    hash_refs: Arc<RwLock<HashMap<Key, FileRefEntry>>>,
    session_tasks: Arc<RwLock<HashMap<Key, ExbashTaskSnapshot>>>,
    workdir_tasks: Arc<RwLock<HashMap<Key, ExbashTaskSnapshot>>>,
    configs: Arc<RwLock<HashMap<String, Value>>>,
}

impl MemorySessionHost {
    pub fn new(session_id: impl Into<String>, workdir: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            workdir: workdir.into(),
            hash_refs: Arc::new(RwLock::new(HashMap::new())),
            session_tasks: Arc::new(RwLock::new(HashMap::new())),
            workdir_tasks: Arc::new(RwLock::new(HashMap::new())),
            configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl SessionWorkdirProvider for MemorySessionHost {
    type Error = String;
    async fn session_workdir(&self) -> Result<String, Self::Error> { Ok(self.workdir.clone()) }
}

#[async_trait]
impl HashRefSessionStore for MemorySessionHost {
    type Error = String;
    fn session_id(&self) -> &str { &self.session_id }
    fn is_hash_ref(&self, target: &str) -> bool { parse_hash_ref(target).is_some() }

    async fn resolve_hash_ref(&self, target: &str) -> Result<FileRefEntry, Self::Error> {
        let parsed = parse_hash_ref(target).ok_or_else(|| format!("invalid hashRef: {target}"))?;
        let store = self.hash_refs.read().await;
        store.values().find(|entry| basename(&entry.file_path) == parsed.filename && small_hash_code(&entry.file_key_ref, &entry.hash_code) == parsed.small_hash_code).cloned().ok_or_else(|| format!("hashRef not found: {target}"))
    }

    async fn store_hash_ref(&self, update: FileRefUpdate) -> Result<FileRefEntry, Self::Error> {
        let (file_key_ref, _filename, _small, label) = make_entry_parts(Some(&update.executor), &update.file.file_key, &update.file.canonical_path, &update.hash_code);
        let entry = FileRefEntry { executor: update.executor, file_path: update.file.canonical_path, hash_code: update.hash_code, file_key_ref };
        self.hash_refs.write().await.insert(label, entry.clone());
        Ok(entry)
    }

    async fn retouch_hash_ref(&self, file_key_ref: &str, hash_code: &str) -> Result<Option<FileRefEntry>, Self::Error> {
        let mut store = self.hash_refs.write().await;
        if let Some(entry) = store.values_mut().find(|e| e.file_key_ref == file_key_ref) {
            entry.hash_code = hash_code.to_string();
            return Ok(Some(entry.clone()));
        }
        Ok(None)
    }
}

#[async_trait]
impl ExbashSessionStore for MemorySessionHost {
    type Error = String;

    async fn session_exbash_snapshot(&self, async_id: &str, executor: &str) -> Result<Option<ExbashTaskSnapshot>, Self::Error> {
        Ok(self.session_tasks.read().await.get(&format!("{async_id}:{executor}")).cloned())
    }

    async fn upsert_session_exbash(&self, input: ExbashSyncInput) -> Result<ExbashTaskSnapshot, Self::Error> {
        let async_id = input.async_id.unwrap_or_default();
        let executor = input.executor.unwrap_or_else(|| "local".into());
        let key = format!("{async_id}:{executor}");
        let snapshot = ExbashTaskSnapshot { async_id, executor, session_id: Some(self.session_id.clone()), workdir: None, state: input.state, pid: input.pid, exit_code: input.exit_code, started_at: input.started_at, ended_at: input.ended_at, command: input.command, description: input.description, total_output: input.total_output };
        self.session_tasks.write().await.insert(key, snapshot.clone());
        Ok(snapshot)
    }

    async fn remove_session_exbash(&self, async_id: &str, executor: &str) -> Result<bool, Self::Error> {
        Ok(self.session_tasks.write().await.remove(&format!("{async_id}:{executor}")).is_some())
    }
}

#[async_trait]
impl ExbashWorkdirStore for MemorySessionHost {
    type Error = String;

    async fn workdir_exbash_snapshot(&self, workdir: &str, async_id: &str, executor: &str) -> Result<Option<ExbashTaskSnapshot>, Self::Error> {
        Ok(self.workdir_tasks.read().await.get(&format!("{workdir}:{async_id}:{executor}")).cloned())
    }

    async fn upsert_workdir_exbash(&self, workdir: &str, input: ExbashSyncInput) -> Result<ExbashTaskSnapshot, Self::Error> {
        let async_id = input.async_id.unwrap_or_default();
        let executor = input.executor.unwrap_or_else(|| "local".into());
        let key = format!("{workdir}:{async_id}:{executor}");
        let snapshot = ExbashTaskSnapshot { async_id, executor, session_id: input.session_id, workdir: Some(workdir.to_string()), state: input.state, pid: input.pid, exit_code: input.exit_code, started_at: input.started_at, ended_at: input.ended_at, command: input.command, description: input.description, total_output: input.total_output };
        self.workdir_tasks.write().await.insert(key, snapshot.clone());
        Ok(snapshot)
    }

    async fn remove_workdir_exbash(&self, workdir: &str, async_id: &str, executor: &str) -> Result<bool, Self::Error> {
        Ok(self.workdir_tasks.write().await.remove(&format!("{workdir}:{async_id}:{executor}")).is_some())
    }
}

#[async_trait]
impl RemoteExecutorConfigStore for MemorySessionHost {
    type Error = String;

    async fn read_remote_executor_config(&self, workdir: &str) -> Result<RemoteExecutorConfigSnapshot, Self::Error> {
        Ok(RemoteExecutorConfigSnapshot { workdir: workdir.to_string(), config: self.configs.read().await.get(workdir).cloned().unwrap_or_else(|| json!({})) })
    }

    async fn update_remote_executor_config(&self, workdir: &str, patch: Value) -> Result<RemoteExecutorConfigSnapshot, Self::Error> {
        self.configs.write().await.insert(workdir.to_string(), patch.clone());
        Ok(RemoteExecutorConfigSnapshot { workdir: workdir.to_string(), config: patch })
    }
}
