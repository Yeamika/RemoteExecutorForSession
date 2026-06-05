use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::host::{
    ExbashSessionStore, ExbashSyncInput, ExbashWorkdirStore, HashRefSessionStore,
    RemoteExecutorConfigStore, SessionWorkdirProvider,
};
use crate::refs::{basename, make_entry_parts, parse_hash_ref, small_hash_code};
use crate::types::{ExbashTaskSnapshot, FileRefEntry, FileRefUpdate, RemoteExecutorConfigSnapshot};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct JsonSessionFile {
    hash_refs: HashMap<String, FileRefEntry>,
    session_tasks: HashMap<String, ExbashTaskSnapshot>,
    workdir_tasks: HashMap<String, ExbashTaskSnapshot>,
    configs: HashMap<String, Value>,
}

#[derive(Clone)]
pub struct JsonFileSessionHost {
    session_id: String,
    workdir: String,
    session_dir: PathBuf,
    cache: Arc<RwLock<HashMap<String, JsonSessionFile>>>,
}

impl JsonFileSessionHost {
    pub fn new(
        session_id: impl Into<String>,
        workdir: impl Into<String>,
        session_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            workdir: workdir.into(),
            session_dir: session_dir.into(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn file_path(&self, session_id: &str) -> PathBuf {
        self.session_dir.join(format!("{session_id}.json"))
    }

    async fn load_cached(&self, session_id: &str) -> JsonSessionFile {
        {
            let cache = self.cache.read().await;
            if let Some(snapshot) = cache.get(session_id).cloned() {
                return snapshot;
            }
        }
        let path = self.file_path(session_id);
        let snapshot = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => JsonSessionFile::default(),
        };
        let mut cache = self.cache.write().await;
        cache.insert(session_id.to_string(), snapshot.clone());
        snapshot
    }

    async fn save_cached(
        &self,
        session_id: &str,
        snapshot: &JsonSessionFile,
    ) -> Result<(), String> {
        tokio::fs::create_dir_all(&self.session_dir)
            .await
            .map_err(|e| e.to_string())?;
        let json = serde_json::to_string_pretty(snapshot).map_err(|e| e.to_string())?;
        tokio::fs::write(self.file_path(session_id), json)
            .await
            .map_err(|e| e.to_string())?;
        let mut cache = self.cache.write().await;
        cache.insert(session_id.to_string(), snapshot.clone());
        Ok(())
    }
}

#[async_trait]
impl SessionWorkdirProvider for JsonFileSessionHost {
    type Error = String;
    async fn session_workdir(&self, _session_id: &str) -> Result<String, Self::Error> {
        Ok(self.workdir.clone())
    }
}

#[async_trait]
impl HashRefSessionStore for JsonFileSessionHost {
    type Error = String;
    fn is_hash_ref(&self, target: &str) -> bool {
        parse_hash_ref(target).is_some()
    }

    async fn resolve_hash_ref(
        &self,
        session_id: &str,
        target: &str,
    ) -> Result<FileRefEntry, Self::Error> {
        let parsed = parse_hash_ref(target).ok_or_else(|| format!("invalid hashRef: {target}"))?;
        let snapshot = self.load_cached(session_id).await;
        snapshot
            .hash_refs
            .values()
            .find(|entry| {
                basename(&entry.file_path) == parsed.filename
                    && small_hash_code(&entry.file_key_ref, &entry.hash_code)
                        == parsed.small_hash_code
            })
            .cloned()
            .ok_or_else(|| format!("hashRef not found: {target}"))
    }

    async fn store_hash_ref(
        &self,
        session_id: &str,
        update: FileRefUpdate,
    ) -> Result<FileRefEntry, Self::Error> {
        let (file_key_ref, _, _, label) = make_entry_parts(
            Some(&update.executor),
            &update.file.file_key,
            &update.file.canonical_path,
            &update.hash_code,
        );
        let entry = FileRefEntry {
            executor: update.executor,
            file_path: update.file.canonical_path,
            hash_code: update.hash_code,
            file_key_ref,
        };
        let mut snapshot = self.load_cached(session_id).await;
        snapshot.hash_refs.insert(label, entry.clone());
        self.save_cached(session_id, &snapshot).await?;
        Ok(entry)
    }

    async fn retouch_hash_ref(
        &self,
        session_id: &str,
        file_key_ref: &str,
        hash_code: &str,
    ) -> Result<Option<FileRefEntry>, Self::Error> {
        let mut snapshot = self.load_cached(session_id).await;
        if let Some(entry) = snapshot
            .hash_refs
            .values_mut()
            .find(|e| e.file_key_ref == file_key_ref)
        {
            entry.hash_code = hash_code.to_string();
            let next = entry.clone();
            self.save_cached(session_id, &snapshot).await?;
            return Ok(Some(next));
        }
        Ok(None)
    }
}

#[async_trait]
impl ExbashSessionStore for JsonFileSessionHost {
    type Error = String;

    async fn session_exbash_snapshot(
        &self,
        session_id: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<Option<ExbashTaskSnapshot>, Self::Error> {
        Ok(self
            .load_cached(session_id)
            .await
            .session_tasks
            .get(&format!("{async_id}:{executor}"))
            .cloned())
    }

    async fn upsert_session_exbash(
        &self,
        session_id: &str,
        input: ExbashSyncInput,
    ) -> Result<ExbashTaskSnapshot, Self::Error> {
        let async_id = input.async_id.unwrap_or_default();
        let executor = input.executor.unwrap_or_else(|| "local".into());
        let key = format!("{async_id}:{executor}");
        let snapshot_task = ExbashTaskSnapshot {
            async_id,
            executor,
            session_id: Some(session_id.to_string()),
            workdir: None,
            state: input.state,
            pid: input.pid,
            exit_code: input.exit_code,
            started_at: input.started_at,
            ended_at: input.ended_at,
            command: input.command,
            description: input.description,
            total_output: input.total_output,
        };
        let mut snapshot = self.load_cached(session_id).await;
        snapshot.session_tasks.insert(key, snapshot_task.clone());
        self.save_cached(session_id, &snapshot).await?;
        Ok(snapshot_task)
    }

    async fn remove_session_exbash(
        &self,
        session_id: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<bool, Self::Error> {
        let mut snapshot = self.load_cached(session_id).await;
        let removed = snapshot
            .session_tasks
            .remove(&format!("{async_id}:{executor}"))
            .is_some();
        if removed {
            self.save_cached(session_id, &snapshot).await?;
        }
        Ok(removed)
    }

    async fn list_session_exbash(
        &self,
        session_id: &str,
        executor: Option<&str>,
    ) -> Result<Vec<ExbashTaskSnapshot>, Self::Error> {
        let mut tasks = self
            .load_cached(session_id)
            .await
            .session_tasks
            .values()
            .filter(|task| {
                executor
                    .map(|executor| task.executor == executor)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| task.started_at.unwrap_or_default());
        Ok(tasks)
    }
}

#[async_trait]
impl ExbashWorkdirStore for JsonFileSessionHost {
    type Error = String;

    async fn workdir_exbash_snapshot(
        &self,
        _session_id: &str,
        workdir: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<Option<ExbashTaskSnapshot>, Self::Error> {
        let session_id = &self.session_id;
        Ok(self
            .load_cached(session_id)
            .await
            .workdir_tasks
            .get(&format!("{workdir}:{executor}:{async_id}"))
            .cloned())
    }

    async fn upsert_workdir_exbash(
        &self,
        session_id: &str,
        workdir: &str,
        input: ExbashSyncInput,
    ) -> Result<ExbashTaskSnapshot, Self::Error> {
        let async_id = input.async_id.unwrap_or_default();
        let executor = input.executor.unwrap_or_else(|| "local".into());
        let key = format!("{workdir}:{executor}:{async_id}");
        let snapshot_task = ExbashTaskSnapshot {
            async_id,
            executor,
            session_id: input.session_id.or_else(|| Some(session_id.to_string())),
            workdir: Some(workdir.to_string()),
            state: input.state,
            pid: input.pid,
            exit_code: input.exit_code,
            started_at: input.started_at,
            ended_at: input.ended_at,
            command: input.command,
            description: input.description,
            total_output: input.total_output,
        };
        let mut snapshot = self.load_cached(session_id).await;
        snapshot.workdir_tasks.insert(key, snapshot_task.clone());
        self.save_cached(session_id, &snapshot).await?;
        Ok(snapshot_task)
    }

    async fn remove_workdir_exbash(
        &self,
        _session_id: &str,
        workdir: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<bool, Self::Error> {
        let session_id = &self.session_id;
        let mut snapshot = self.load_cached(session_id).await;
        let removed = snapshot
            .workdir_tasks
            .remove(&format!("{workdir}:{executor}:{async_id}"))
            .is_some();
        if removed {
            self.save_cached(session_id, &snapshot).await?;
        }
        Ok(removed)
    }

    async fn list_workdir_exbash(
        &self,
        _session_id: &str,
        workdir: &str,
        executor: Option<&str>,
    ) -> Result<Vec<ExbashTaskSnapshot>, Self::Error> {
        let session_id = &self.session_id;
        let mut tasks = self
            .load_cached(session_id)
            .await
            .workdir_tasks
            .values()
            .filter(|task| {
                task.workdir.as_deref() == Some(workdir)
                    && executor
                        .map(|executor| task.executor == executor)
                        .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| task.started_at.unwrap_or_default());
        Ok(tasks)
    }
}

#[async_trait]
impl RemoteExecutorConfigStore for JsonFileSessionHost {
    type Error = String;

    async fn read_remote_executor_config(
        &self,
        workdir: &str,
    ) -> Result<RemoteExecutorConfigSnapshot, Self::Error> {
        Ok(RemoteExecutorConfigSnapshot {
            workdir: workdir.to_string(),
            config: self
                .load_cached(&self.session_id)
                .await
                .configs
                .get(workdir)
                .cloned()
                .unwrap_or_else(|| json!({})),
        })
    }

    async fn update_remote_executor_config(
        &self,
        workdir: &str,
        patch: Value,
    ) -> Result<RemoteExecutorConfigSnapshot, Self::Error> {
        let mut snapshot = self.load_cached(&self.session_id).await;
        snapshot.configs.insert(workdir.to_string(), patch.clone());
        self.save_cached(&self.session_id, &snapshot).await?;
        Ok(RemoteExecutorConfigSnapshot {
            workdir: workdir.to_string(),
            config: patch,
        })
    }
}
