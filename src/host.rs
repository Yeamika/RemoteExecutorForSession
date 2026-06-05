use crate::types::{ExbashTaskSnapshot, FileRefEntry, FileRefUpdate, RemoteExecutorConfigSnapshot};
use async_trait::async_trait;
use serde_json::Value;
use std::fmt::Display;

#[async_trait]
pub trait SessionHost:
    SessionWorkdirProvider
    + HashRefSessionStore
    + ExbashSessionStore
    + ExbashWorkdirStore
    + RemoteExecutorConfigStore
    + Send
    + Sync
{
}

#[async_trait]
impl<T> SessionHost for T where
    T: SessionWorkdirProvider
        + HashRefSessionStore
        + ExbashSessionStore
        + ExbashWorkdirStore
        + RemoteExecutorConfigStore
        + Send
        + Sync
{
}

#[async_trait]
pub trait SessionWorkdirProvider: Send + Sync {
    type Error: Send + Sync + Display + 'static;

    async fn session_workdir(&self, session_id: &str) -> Result<String, Self::Error>;
}

#[async_trait]
pub trait HashRefSessionStore: Send + Sync {
    type Error: Send + Sync + Display + 'static;

    fn is_hash_ref(&self, target: &str) -> bool;
    async fn resolve_hash_ref(
        &self,
        session_id: &str,
        target: &str,
    ) -> Result<FileRefEntry, Self::Error>;
    async fn store_hash_ref(
        &self,
        session_id: &str,
        update: FileRefUpdate,
    ) -> Result<FileRefEntry, Self::Error>;
    async fn retouch_hash_ref(
        &self,
        session_id: &str,
        file_key_ref: &str,
        hash_code: &str,
    ) -> Result<Option<FileRefEntry>, Self::Error>;
}

#[derive(Clone, Debug, Default)]
pub struct ExbashSyncInput {
    pub async_id: Option<String>,
    pub session_id: Option<String>,
    pub workdir: Option<String>,
    pub executor: Option<String>,
    pub state: Option<String>,
    pub pid: Option<i64>,
    pub exit_code: Option<i32>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub command: Option<String>,
    pub description: Option<String>,
    pub total_output: Option<i64>,
}

#[async_trait]
pub trait ExbashSessionStore: Send + Sync {
    type Error: Send + Sync + Display + 'static;

    async fn session_exbash_snapshot(
        &self,
        session_id: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<Option<ExbashTaskSnapshot>, Self::Error>;
    async fn upsert_session_exbash(
        &self,
        session_id: &str,
        input: ExbashSyncInput,
    ) -> Result<ExbashTaskSnapshot, Self::Error>;
    async fn list_session_exbash(
        &self,
        _session_id: &str,
        _executor: Option<&str>,
    ) -> Result<Vec<ExbashTaskSnapshot>, Self::Error> {
        Ok(Vec::new())
    }
    async fn remove_session_exbash(
        &self,
        session_id: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<bool, Self::Error>;
}

#[async_trait]
pub trait ExbashWorkdirStore: Send + Sync {
    type Error: Send + Sync + Display + 'static;

    async fn workdir_exbash_snapshot(
        &self,
        session_id: &str,
        workdir: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<Option<ExbashTaskSnapshot>, Self::Error>;
    async fn upsert_workdir_exbash(
        &self,
        session_id: &str,
        workdir: &str,
        input: ExbashSyncInput,
    ) -> Result<ExbashTaskSnapshot, Self::Error>;
    async fn list_workdir_exbash(
        &self,
        _session_id: &str,
        _workdir: &str,
        _executor: Option<&str>,
    ) -> Result<Vec<ExbashTaskSnapshot>, Self::Error> {
        Ok(Vec::new())
    }
    async fn remove_workdir_exbash(
        &self,
        session_id: &str,
        workdir: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<bool, Self::Error>;
}

#[async_trait]
pub trait RemoteExecutorConfigStore: Send + Sync {
    type Error: Send + Sync + Display + 'static;

    async fn read_remote_executor_config(
        &self,
        workdir: &str,
    ) -> Result<RemoteExecutorConfigSnapshot, Self::Error>;
    async fn update_remote_executor_config(
        &self,
        workdir: &str,
        patch: Value,
    ) -> Result<RemoteExecutorConfigSnapshot, Self::Error>;
}

/// Optional host-provided IO boundary.
pub trait HostIo: Send + Sync {
    type Error: Send + Sync + 'static;

    fn read_bytes(&self, path: &str) -> Result<Vec<u8>, Self::Error>;
    fn write_bytes(&self, path: &str, bytes: &[u8]) -> Result<(), Self::Error>;
}
