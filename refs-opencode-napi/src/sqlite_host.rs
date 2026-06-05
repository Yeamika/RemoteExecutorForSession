use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Mutex;

use remote_executor_for_session::host::{
    ExbashSessionStore, ExbashSyncInput, ExbashWorkdirStore, HashRefSessionStore,
    RemoteExecutorConfigStore, SessionWorkdirProvider, EXBASH_TASK_STACK_FULL_MESSAGE,
};
use remote_executor_for_session::refs::{make_entry_parts, parse_hash_ref, small_hash_code};
use remote_executor_for_session::types::{
    ExbashTaskSnapshot, FileRefEntry, FileRefUpdate, RemoteExecutorConfigSnapshot,
};

/// SQLite-backed SessionHost that reads/writes the same database as OpenCode.
///
/// Table schemas (copied from OpenCode `session.sql.ts`):
///
/// ```sql
/// session_file_read (
///   session_id TEXT NOT NULL,
///   file_key_ref TEXT NOT NULL,
///   filename TEXT NOT NULL,
///   file_path TEXT NOT NULL,
///   hash_code TEXT NOT NULL,
///   small_hash_code TEXT NOT NULL,
///   read_time INTEGER NOT NULL,
///   PRIMARY KEY (session_id, file_key_ref)
/// )
///
/// exbash_task (
///   async_id TEXT NOT NULL,
///   session_id TEXT NOT NULL,
///   workspace TEXT NOT NULL,
///   scope TEXT NOT NULL,
///   executor TEXT NOT NULL DEFAULT 'local',
///   description TEXT NOT NULL,
///   command TEXT NOT NULL,
///   cwd TEXT NOT NULL,
///   time_start INTEGER NOT NULL,
///   time_end INTEGER,
///   exit_code TEXT,
///   time_created INTEGER NOT NULL,
///   time_updated INTEGER NOT NULL,
///   PRIMARY KEY (session_id, workspace, executor, async_id)
/// )
/// ```
pub struct SqliteSessionHost {
    workdir: String,
    conn: Mutex<Connection>,
}

const EXBASH_TASK_LIMIT: i64 = 10;

impl SqliteSessionHost {
    pub fn new(_session_id: String, workdir: String, db_path: PathBuf) -> anyhow::Result<Self> {
        let conn =
            Connection::open_with_flags(&db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;")?;
        Ok(Self {
            workdir,
            conn: Mutex::new(conn),
        })
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ─── SessionWorkdirProvider ───

#[async_trait]
impl SessionWorkdirProvider for SqliteSessionHost {
    type Error = String;
    async fn session_workdir(&self, session_id: &str) -> Result<String, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let Ok(mut stmt) = conn.prepare("SELECT directory FROM session WHERE id = ?1 LIMIT 1")
        else {
            return Ok(self.workdir.clone());
        };
        let directory = stmt
            .query_row(rusqlite::params![session_id], |row| row.get::<_, String>(0))
            .optional()
            .map_err(|e| e.to_string())?;
        Ok(directory.unwrap_or_else(|| self.workdir.clone()))
    }
}

// ─── HashRefSessionStore ───

#[async_trait]
impl HashRefSessionStore for SqliteSessionHost {
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
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT file_key_ref, file_path, hash_code
                 FROM session_file_read
                 WHERE session_id = ?1 AND filename = ?2 AND small_hash_code = ?3
                 ORDER BY read_time DESC
                 LIMIT 1",
            )
            .map_err(|e| e.to_string())?;
        let entry = stmt
            .query_row(
                rusqlite::params![session_id, parsed.filename, parsed.small_hash_code],
                |row| {
                    let fkr: String = row.get(0)?;
                    let executor = fkr[..fkr.find(':').unwrap_or(0)].to_string();
                    Ok(FileRefEntry {
                        executor,
                        file_path: row.get(1)?,
                        hash_code: row.get(2)?,
                        file_key_ref: fkr,
                    })
                },
            )
            .map_err(|e| format!("hashRef not found: {target} ({e})"))?;
        conn.execute(
            "UPDATE session_file_read
             SET read_time = ?1
             WHERE session_id = ?2 AND file_key_ref = ?3",
            rusqlite::params![now_ms(), session_id, entry.file_key_ref],
        )
        .map_err(|e| e.to_string())?;
        Ok(entry)
    }

    async fn store_hash_ref(
        &self,
        session_id: &str,
        update: FileRefUpdate,
    ) -> Result<FileRefEntry, Self::Error> {
        let (file_key_ref, filename, small_hash, _label) = make_entry_parts(
            Some(&update.executor),
            &update.file.file_key,
            &update.file.canonical_path,
            &update.hash_code,
        );
        let file_path = &update.file.canonical_path;
        let read_time = now_ms();

        let conn = self.conn.lock().unwrap();
        // Delete old entry with same file_key_ref (handles rename)
        conn.execute(
            "DELETE FROM session_file_read WHERE session_id = ?1 AND file_key_ref = ?2",
            rusqlite::params![session_id, file_key_ref],
        )
        .map_err(|e| e.to_string())?;
        // Insert new
        conn.execute(
            "INSERT INTO session_file_read
                (session_id, file_key_ref, filename, file_path, hash_code, small_hash_code, read_time)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                session_id,
                file_key_ref,
                filename,
                file_path,
                update.hash_code,
                small_hash,
                read_time
            ],
        )
        .map_err(|e| e.to_string())?;
        // Keep the latest 64 hash refs for this session by access time.
        conn.execute(
            "DELETE FROM session_file_read
             WHERE session_id = ?1 AND file_key_ref NOT IN (
                SELECT file_key_ref FROM session_file_read
                WHERE session_id = ?1
                ORDER BY read_time DESC LIMIT 64
             )",
            rusqlite::params![session_id],
        )
        .map_err(|e| e.to_string())?;

        Ok(FileRefEntry {
            executor: update.executor,
            file_path: file_path.clone(),
            hash_code: update.hash_code,
            file_key_ref,
        })
    }

    async fn retouch_hash_ref(
        &self,
        session_id: &str,
        file_key_ref: &str,
        hash_code: &str,
    ) -> Result<Option<FileRefEntry>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let existing: Option<(String, String, String)> = conn
            .prepare(
                "SELECT file_key_ref, file_path, hash_code
                 FROM session_file_read
                 WHERE session_id = ?1 AND file_key_ref = ?2",
            )
            .map_err(|e| e.to_string())?
            .query_row(rusqlite::params![session_id, file_key_ref], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .ok();

        let Some((_fkr, _fp, _old_hash)) = existing else {
            return Ok(None);
        };

        let new_small = small_hash_code(file_key_ref, hash_code);
        let read_time = now_ms();

        conn.execute(
            "UPDATE session_file_read
             SET hash_code = ?1, small_hash_code = ?2, read_time = ?3
             WHERE session_id = ?4 AND file_key_ref = ?5",
            rusqlite::params![hash_code, new_small, read_time, session_id, file_key_ref],
        )
        .map_err(|e| e.to_string())?;

        let entry = conn
            .prepare(
                "SELECT file_key_ref, file_path, hash_code
                 FROM session_file_read
                 WHERE session_id = ?1 AND file_key_ref = ?2",
            )
            .map_err(|e| e.to_string())?
            .query_row(rusqlite::params![session_id, file_key_ref], |row| {
                let fkr: String = row.get(0)?;
                Ok(FileRefEntry {
                    executor: fkr[..fkr.find(':').unwrap_or(0)].to_string(),
                    file_path: row.get(1)?,
                    hash_code: row.get(2)?,
                    file_key_ref: fkr,
                })
            })
            .map_err(|e| e.to_string())?;
        Ok(Some(entry))
    }
}

// ─── ExbashSessionStore ───

/// Columns: async_id(0), executor(1), session_id(2), scope(3), state(4),
///          exit_code(5), time_start(6), time_end(7), command(8), description(9), workspace(10)
fn exbash_row_to_snapshot(row: &rusqlite::Row) -> rusqlite::Result<ExbashTaskSnapshot> {
    let async_id: String = row.get(0)?;
    let executor: String = row.get(1)?;
    let session_id: String = row.get(2)?;
    let _scope: String = row.get(3)?;
    let _state: Option<String> = row.get(4)?;
    let exit_code_str: Option<String> = row.get(5)?;
    let time_start: i64 = row.get(6)?;
    let time_end: Option<i64> = row.get(7)?;
    let state = exbash_state(time_end, exit_code_str.as_deref());
    let exit_code: Option<i32> = exit_code_str.as_deref().and_then(|s| s.parse().ok());
    let command: String = row.get(8)?;
    let description: String = row.get(9)?;
    let workspace: String = row.get(10)?;

    Ok(ExbashTaskSnapshot {
        async_id,
        executor,
        session_id: Some(session_id),
        workdir: Some(workspace),
        state,
        pid: None,
        exit_code,
        started_at: Some(time_start),
        ended_at: time_end,
        command: Some(command),
        description: Some(description),
        total_output: None,
    })
}

fn exbash_state(time_end: Option<i64>, exit_code: Option<&str>) -> Option<String> {
    if time_end.is_none() {
        return Some("running".into());
    }
    let Some(value) = exit_code.map(str::trim).filter(|value| !value.is_empty()) else {
        return Some("unknown".into());
    };
    match value {
        "timeout" => Some("timeout".into()),
        "stop" | "stopped" => Some("stop".into()),
        other => other
            .parse::<i32>()
            .map(|code| format!("exit:{code}"))
            .ok()
            .or_else(|| Some("unknown".into())),
    }
}

fn exbash_exit_storage(input: &ExbashSyncInput) -> Option<String> {
    if let Some(code) = input.exit_code {
        return Some(code.to_string());
    }
    let state = input.state.as_deref()?.trim();
    match state {
        "timeout" => Some("timeout".into()),
        "stop" | "stopped" => Some("stop".into()),
        value if value.starts_with("exit:") => value
            .trim_start_matches("exit:")
            .parse::<i32>()
            .map(|code| code.to_string())
            .ok(),
        _ => None,
    }
}

fn exbash_exit_storage_is_terminal(exit_code: Option<&str>) -> bool {
    exit_code
        .map(|value| {
            let value = value.trim();
            value == "timeout"
                || value == "stop"
                || value == "stopped"
                || value.parse::<i32>().is_ok()
        })
        .unwrap_or(false)
}

fn exbash_time_end(input: &ExbashSyncInput, exit_code: Option<&str>) -> Option<i64> {
    input
        .ended_at
        .or_else(|| exbash_exit_storage_is_terminal(exit_code).then(now_ms))
}

#[async_trait]
impl ExbashSessionStore for SqliteSessionHost {
    type Error = String;

    async fn check_session_exbash_create(
        &self,
        session_id: &str,
        input: &ExbashSyncInput,
    ) -> Result<(), Self::Error> {
        let executor = input.executor.as_deref().unwrap_or("local");
        let conn = self.conn.lock().unwrap();
        if let Some(async_id) = input.async_id.as_deref().filter(|value| !value.is_empty()) {
            let existing: i64 = conn
                .query_row(
                    "SELECT COUNT(*)
                     FROM exbash_task
                     WHERE session_id = ?1 AND executor = ?2 AND async_id = ?3 AND scope = 'local'",
                    rusqlite::params![session_id, executor, async_id],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())?;
            if existing > 0 {
                return Ok(());
            }
        }
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM exbash_task
                 WHERE session_id = ?1 AND scope = 'local'",
                rusqlite::params![session_id],
                |row| row.get(0),
            )
            .map_err(|e| e.to_string())?;
        if count >= EXBASH_TASK_LIMIT {
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
        let conn = self.conn.lock().unwrap();
        let result = conn
            .prepare(
                "SELECT async_id, executor, session_id, scope,
                        NULL as state,
                        exit_code, time_start, time_end, command, description, workspace
                 FROM exbash_task
                 WHERE session_id = ?1 AND async_id = ?2 AND executor = ?3 AND scope = 'local'",
            )
            .map_err(|e| e.to_string())?
            .query_row(
                rusqlite::params![session_id, async_id, executor],
                exbash_row_to_snapshot,
            )
            .ok();
        Ok(result)
    }

    async fn upsert_session_exbash(
        &self,
        session_id: &str,
        input: ExbashSyncInput,
    ) -> Result<ExbashTaskSnapshot, Self::Error> {
        let session_id = input
            .session_id
            .clone()
            .unwrap_or_else(|| session_id.to_string());
        let workdir = input
            .workdir
            .clone()
            .unwrap_or_else(|| self.workdir.clone());
        let async_id = input.async_id.clone().unwrap_or_default();
        let executor = input.executor.clone().unwrap_or_else(|| "local".into());
        let command = input.command.clone().unwrap_or_default();
        let description = input.description.clone().unwrap_or_else(|| command.clone());
        let time_start = input.started_at.unwrap_or_else(now_ms);
        let exit_code = exbash_exit_storage(&input);
        let time_end = exbash_time_end(&input, exit_code.as_deref());
        let ts = now_ms();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO exbash_task
                (async_id, session_id, workspace, scope, executor, description, command, cwd,
                 time_start, time_end, exit_code, time_created, time_updated)
             VALUES (?1, ?2, ?3, 'local', ?4, ?5, ?6, ?3, ?7, ?8, ?9, ?10, ?10)
             ON CONFLICT(session_id, workspace, executor, async_id)
             DO UPDATE SET time_end = excluded.time_end,
                           exit_code = excluded.exit_code,
                           time_updated = excluded.time_updated",
            rusqlite::params![
                async_id,
                session_id,
                workdir,
                executor,
                description,
                command,
                time_start,
                time_end,
                exit_code,
                ts
            ],
        )
        .map_err(|e| e.to_string())?;

        Ok(ExbashTaskSnapshot {
            async_id,
            executor,
            session_id: Some(session_id),
            workdir: Some(workdir),
            state: exbash_state(time_end, exit_code.as_deref()),
            pid: input.pid,
            exit_code: input.exit_code,
            started_at: Some(time_start),
            ended_at: time_end,
            command: Some(command),
            description: Some(description),
            total_output: input.total_output,
        })
    }

    async fn list_session_exbash(
        &self,
        session_id: &str,
        executor: Option<&str>,
    ) -> Result<Vec<ExbashTaskSnapshot>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let sql = if executor.is_some() {
            "SELECT async_id, executor, session_id, scope,
                    NULL as state,
                    exit_code, time_start, time_end, command, description, workspace
             FROM exbash_task
             WHERE session_id = ?1 AND executor = ?2 AND scope = 'local'
             ORDER BY time_start ASC"
        } else {
            "SELECT async_id, executor, session_id, scope,
                    NULL as state,
                    exit_code, time_start, time_end, command, description, workspace
             FROM exbash_task
             WHERE session_id = ?1 AND scope = 'local'
             ORDER BY time_start ASC"
        };
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let rows = if let Some(executor) = executor {
            stmt.query_map(
                rusqlite::params![session_id, executor],
                exbash_row_to_snapshot,
            )
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
        } else {
            stmt.query_map(rusqlite::params![session_id], exbash_row_to_snapshot)
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
        }
        .map_err(|e| e.to_string())?;
        Ok(rows)
    }

    async fn remove_session_exbash(
        &self,
        session_id: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<bool, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let rows = conn
            .execute(
                "DELETE FROM exbash_task
                 WHERE session_id = ?1 AND async_id = ?2 AND executor = ?3 AND scope = 'local'",
                rusqlite::params![session_id, async_id, executor],
            )
            .map_err(|e| e.to_string())?;
        Ok(rows > 0)
    }
}

// ─── ExbashWorkdirStore ───

#[async_trait]
impl ExbashWorkdirStore for SqliteSessionHost {
    type Error = String;

    async fn check_workdir_exbash_create(
        &self,
        _session_id: &str,
        workdir: &str,
        input: &ExbashSyncInput,
    ) -> Result<(), Self::Error> {
        let executor = input.executor.as_deref().unwrap_or("local");
        let conn = self.conn.lock().unwrap();
        if let Some(async_id) = input.async_id.as_deref().filter(|value| !value.is_empty()) {
            let existing: i64 = conn
                .query_row(
                    "SELECT COUNT(*)
                     FROM exbash_task
                     WHERE workspace = ?1 AND executor = ?2 AND async_id = ?3 AND scope = 'workspace'",
                    rusqlite::params![workdir, executor, async_id],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())?;
            if existing > 0 {
                return Ok(());
            }
        }
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM exbash_task
                 WHERE workspace = ?1 AND scope = 'workspace'",
                rusqlite::params![workdir],
                |row| row.get(0),
            )
            .map_err(|e| e.to_string())?;
        if count >= EXBASH_TASK_LIMIT {
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
        let conn = self.conn.lock().unwrap();
        let result = conn
            .prepare(
                "SELECT async_id, executor, session_id, scope,
                        NULL as state,
                        exit_code, time_start, time_end, command, description, workspace
                 FROM exbash_task
                 WHERE workspace = ?1 AND async_id = ?2 AND executor = ?3 AND scope = 'workspace'",
            )
            .map_err(|e| e.to_string())?
            .query_row(
                rusqlite::params![workdir, async_id, executor],
                exbash_row_to_snapshot,
            )
            .ok();
        Ok(result)
    }

    async fn upsert_workdir_exbash(
        &self,
        session_id: &str,
        workdir: &str,
        input: ExbashSyncInput,
    ) -> Result<ExbashTaskSnapshot, Self::Error> {
        let async_id = input.async_id.clone().unwrap_or_default();
        let executor = input.executor.clone().unwrap_or_else(|| "local".into());
        let command = input.command.clone().unwrap_or_default();
        let description = input.description.clone().unwrap_or_else(|| command.clone());
        let time_start = input.started_at.unwrap_or_else(now_ms);
        let exit_code = exbash_exit_storage(&input);
        let time_end = exbash_time_end(&input, exit_code.as_deref());
        let session_id = input
            .session_id
            .clone()
            .unwrap_or_else(|| session_id.to_string());
        let ts = now_ms();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM exbash_task
             WHERE workspace = ?1 AND executor = ?2 AND async_id = ?3 AND scope = 'workspace'",
            rusqlite::params![workdir, executor, async_id],
        )
        .map_err(|e| e.to_string())?;
        conn.execute(
            "INSERT INTO exbash_task
                (async_id, session_id, workspace, scope, executor, description, command, cwd,
                 time_start, time_end, exit_code, time_created, time_updated)
             VALUES (?1, ?2, ?3, 'workspace', ?4, ?5, ?6, ?3, ?7, ?8, ?9, ?10, ?10)
             ON CONFLICT(session_id, workspace, executor, async_id)
             DO UPDATE SET time_end = excluded.time_end,
                           exit_code = excluded.exit_code,
                           time_updated = excluded.time_updated",
            rusqlite::params![
                async_id,
                session_id,
                workdir,
                executor,
                description,
                command,
                time_start,
                time_end,
                exit_code,
                ts
            ],
        )
        .map_err(|e| e.to_string())?;

        Ok(ExbashTaskSnapshot {
            async_id,
            executor,
            session_id: Some(session_id),
            workdir: Some(workdir.to_string()),
            state: exbash_state(time_end, exit_code.as_deref()),
            pid: input.pid,
            exit_code: input.exit_code,
            started_at: Some(time_start),
            ended_at: time_end,
            command: Some(command),
            description: Some(description),
            total_output: input.total_output,
        })
    }

    async fn list_workdir_exbash(
        &self,
        _session_id: &str,
        workdir: &str,
        executor: Option<&str>,
    ) -> Result<Vec<ExbashTaskSnapshot>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let sql = if executor.is_some() {
            "SELECT async_id, executor, session_id, scope,
                    NULL as state,
                    exit_code, time_start, time_end, command, description, workspace
             FROM exbash_task
             WHERE workspace = ?1 AND executor = ?2 AND scope = 'workspace'
             ORDER BY time_start ASC"
        } else {
            "SELECT async_id, executor, session_id, scope,
                    NULL as state,
                    exit_code, time_start, time_end, command, description, workspace
             FROM exbash_task
             WHERE workspace = ?1 AND scope = 'workspace'
             ORDER BY time_start ASC"
        };
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let rows = if let Some(executor) = executor {
            stmt.query_map(rusqlite::params![workdir, executor], exbash_row_to_snapshot)
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
        } else {
            stmt.query_map(rusqlite::params![workdir], exbash_row_to_snapshot)
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
        }
        .map_err(|e| e.to_string())?;
        Ok(rows)
    }

    async fn remove_workdir_exbash(
        &self,
        _session_id: &str,
        workdir: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<bool, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let rows = conn
            .execute(
                "DELETE FROM exbash_task
                 WHERE workspace = ?1 AND async_id = ?2 AND executor = ?3 AND scope = 'workspace'",
                rusqlite::params![workdir, async_id, executor],
            )
            .map_err(|e| e.to_string())?;
        Ok(rows > 0)
    }
}

// ─── RemoteExecutorConfigStore ───

#[async_trait]
impl RemoteExecutorConfigStore for SqliteSessionHost {
    type Error = String;

    async fn read_remote_executor_config(
        &self,
        workdir: &str,
    ) -> Result<RemoteExecutorConfigSnapshot, Self::Error> {
        let config_path = std::path::Path::new(workdir)
            .join(".opencode")
            .join("remote_executor_infos.json");
        let config = if config_path.exists() {
            let text = std::fs::read_to_string(&config_path).map_err(|e| e.to_string())?;
            serde_json::from_str(&text).unwrap_or_else(|_| json!({}))
        } else {
            json!({})
        };
        Ok(RemoteExecutorConfigSnapshot {
            workdir: workdir.to_string(),
            config,
        })
    }

    async fn update_remote_executor_config(
        &self,
        workdir: &str,
        patch: Value,
    ) -> Result<RemoteExecutorConfigSnapshot, Self::Error> {
        let config_path = std::path::Path::new(workdir)
            .join(".opencode")
            .join("remote_executor_infos.json");
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(&config_path, serde_json::to_string_pretty(&patch).unwrap())
            .map_err(|e| e.to_string())?;
        Ok(RemoteExecutorConfigSnapshot {
            workdir: workdir.to_string(),
            config: patch,
        })
    }
}
