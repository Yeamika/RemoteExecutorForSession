use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::host::{
    ExbashSessionStore, ExbashSyncInput, ExbashWorkdirStore, HashRefSessionStore,
    RemoteExecutorConfigStore, SessionWorkdirProvider, EXBASH_TASK_STACK_FULL_MESSAGE,
};
use crate::refs::{basename, make_entry_parts, parse_hash_ref, small_hash_code};
use crate::types::{ExbashTaskSnapshot, FileRefEntry, FileRefUpdate, RemoteExecutorConfigSnapshot};

type Key = String;

#[derive(Clone)]
pub struct MemorySessionHost {
    session_id: String,
    workdir: String,
    session_workdirs: Arc<RwLock<HashMap<String, String>>>,
    hash_refs: Arc<RwLock<HashMap<Key, FileRefEntry>>>,
    session_tasks: Arc<RwLock<HashMap<Key, ExbashTaskSnapshot>>>,
    workdir_tasks: Arc<RwLock<HashMap<Key, ExbashTaskSnapshot>>>,
    configs: Arc<RwLock<HashMap<String, Value>>>,
    exbash_task_limit: Option<usize>,
}

impl MemorySessionHost {
    pub fn new(session_id: impl Into<String>, workdir: impl Into<String>) -> Self {
        let session_id = session_id.into();
        let workdir = workdir.into();
        let mut session_workdirs = HashMap::new();
        session_workdirs.insert(session_id.clone(), workdir.clone());
        Self {
            session_id,
            workdir,
            session_workdirs: Arc::new(RwLock::new(session_workdirs)),
            hash_refs: Arc::new(RwLock::new(HashMap::new())),
            session_tasks: Arc::new(RwLock::new(HashMap::new())),
            workdir_tasks: Arc::new(RwLock::new(HashMap::new())),
            configs: Arc::new(RwLock::new(HashMap::new())),
            exbash_task_limit: None,
        }
    }

    pub fn with_exbash_task_limit(mut self, limit: usize) -> Self {
        self.exbash_task_limit = Some(limit);
        self
    }

    fn scoped_key(session_id: &str, key: impl AsRef<str>) -> String {
        format!("{session_id}:{}", key.as_ref())
    }
}

#[async_trait]
impl SessionWorkdirProvider for MemorySessionHost {
    type Error = String;
    async fn session_workdir(&self, session_id: &str) -> Result<String, Self::Error> {
        Ok(self
            .session_workdirs
            .read()
            .await
            .get(session_id)
            .cloned()
            .unwrap_or_else(|| self.workdir.clone()))
    }
}

#[async_trait]
impl HashRefSessionStore for MemorySessionHost {
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
        let store = self.hash_refs.read().await;
        let prefix = format!("{session_id}:");
        store
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(_, entry)| entry)
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
        let (file_key_ref, _filename, _small, label) = make_entry_parts(
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
        self.hash_refs
            .write()
            .await
            .insert(Self::scoped_key(session_id, label), entry.clone());
        Ok(entry)
    }

    async fn retouch_hash_ref(
        &self,
        session_id: &str,
        file_key_ref: &str,
        hash_code: &str,
    ) -> Result<Option<FileRefEntry>, Self::Error> {
        let mut store = self.hash_refs.write().await;
        let prefix = format!("{session_id}:");
        if let Some(entry) = store
            .iter_mut()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(_, entry)| entry)
            .find(|e| e.file_key_ref == file_key_ref)
        {
            entry.hash_code = hash_code.to_string();
            return Ok(Some(entry.clone()));
        }
        Ok(None)
    }
}

#[async_trait]
impl ExbashSessionStore for MemorySessionHost {
    type Error = String;

    async fn check_session_exbash_create(
        &self,
        session_id: &str,
        input: &ExbashSyncInput,
    ) -> Result<(), Self::Error> {
        let Some(limit) = self.exbash_task_limit else {
            return Ok(());
        };
        let executor = input.executor.as_deref().unwrap_or("local");
        if let Some(async_id) = input.async_id.as_deref().filter(|value| !value.is_empty()) {
            if self
                .session_tasks
                .read()
                .await
                .contains_key(&format!("{session_id}:{async_id}:{executor}"))
            {
                return Ok(());
            }
        }
        let prefix = format!("{session_id}:");
        let count = self
            .session_tasks
            .read()
            .await
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .count();
        if count >= limit {
            return Err(EXBASH_TASK_STACK_FULL_MESSAGE.to_string());
        }
        Ok(())
    }

    async fn session_exbash_snapshot(
        &self,
        session_id: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<Option<ExbashTaskSnapshot>, Self::Error> {
        Ok(self
            .session_tasks
            .read()
            .await
            .get(&format!("{session_id}:{async_id}:{executor}"))
            .cloned())
    }

    async fn upsert_session_exbash(
        &self,
        session_id: &str,
        input: ExbashSyncInput,
    ) -> Result<ExbashTaskSnapshot, Self::Error> {
        let async_id = input.async_id.unwrap_or_default();
        let executor = input.executor.unwrap_or_else(|| "local".into());
        let key = format!("{session_id}:{async_id}:{executor}");
        let snapshot = ExbashTaskSnapshot {
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
        self.session_tasks
            .write()
            .await
            .insert(key, snapshot.clone());
        Ok(snapshot)
    }

    async fn list_session_exbash(
        &self,
        session_id: &str,
        executor: Option<&str>,
    ) -> Result<Vec<ExbashTaskSnapshot>, Self::Error> {
        let prefix = format!("{session_id}:");
        let mut tasks = self
            .session_tasks
            .read()
            .await
            .iter()
            .filter(|(key, task)| {
                key.starts_with(&prefix)
                    && executor
                        .map(|executor| task.executor == executor)
                        .unwrap_or(true)
            })
            .map(|(_, task)| task.clone())
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| task.started_at.unwrap_or_default());
        Ok(tasks)
    }

    async fn remove_session_exbash(
        &self,
        session_id: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<bool, Self::Error> {
        Ok(self
            .session_tasks
            .write()
            .await
            .remove(&format!("{session_id}:{async_id}:{executor}"))
            .is_some())
    }
}

#[async_trait]
impl ExbashWorkdirStore for MemorySessionHost {
    type Error = String;

    async fn check_workdir_exbash_create(
        &self,
        _session_id: &str,
        workdir: &str,
        input: &ExbashSyncInput,
    ) -> Result<(), Self::Error> {
        let Some(limit) = self.exbash_task_limit else {
            return Ok(());
        };
        let executor = input.executor.as_deref().unwrap_or("local");
        if let Some(async_id) = input.async_id.as_deref().filter(|value| !value.is_empty()) {
            if self
                .workdir_tasks
                .read()
                .await
                .contains_key(&format!("{workdir}:{executor}:{async_id}"))
            {
                return Ok(());
            }
        }
        let prefix = format!("{workdir}:");
        let count = self
            .workdir_tasks
            .read()
            .await
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .count();
        if count >= limit {
            return Err(EXBASH_TASK_STACK_FULL_MESSAGE.to_string());
        }
        Ok(())
    }

    async fn workdir_exbash_snapshot(
        &self,
        _session_id: &str,
        workdir: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<Option<ExbashTaskSnapshot>, Self::Error> {
        Ok(self
            .workdir_tasks
            .read()
            .await
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
        let snapshot = ExbashTaskSnapshot {
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
        self.workdir_tasks
            .write()
            .await
            .insert(key, snapshot.clone());
        Ok(snapshot)
    }

    async fn list_workdir_exbash(
        &self,
        _session_id: &str,
        workdir: &str,
        executor: Option<&str>,
    ) -> Result<Vec<ExbashTaskSnapshot>, Self::Error> {
        let prefix = format!("{workdir}:");
        let mut tasks = self
            .workdir_tasks
            .read()
            .await
            .iter()
            .filter(|(key, task)| {
                key.starts_with(&prefix)
                    && executor
                        .map(|executor| task.executor == executor)
                        .unwrap_or(true)
            })
            .map(|(_, task)| task.clone())
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| task.started_at.unwrap_or_default());
        Ok(tasks)
    }

    async fn remove_workdir_exbash(
        &self,
        _session_id: &str,
        workdir: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<bool, Self::Error> {
        Ok(self
            .workdir_tasks
            .write()
            .await
            .remove(&format!("{workdir}:{executor}:{async_id}"))
            .is_some())
    }
}

#[async_trait]
impl RemoteExecutorConfigStore for MemorySessionHost {
    type Error = String;

    async fn read_remote_executor_config(
        &self,
        workdir: &str,
    ) -> Result<RemoteExecutorConfigSnapshot, Self::Error> {
        Ok(RemoteExecutorConfigSnapshot {
            workdir: workdir.to_string(),
            config: self
                .configs
                .read()
                .await
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
        self.configs
            .write()
            .await
            .insert(workdir.to_string(), patch.clone());
        Ok(RemoteExecutorConfigSnapshot {
            workdir: workdir.to_string(),
            config: patch,
        })
    }
}
