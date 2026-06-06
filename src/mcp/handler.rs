use crate::host::{ExbashSyncInput, SessionHost};
use crate::jsonrpc::JsonRpcError;
use crate::mcp::{
    EmbeddedMcp, McpCallContext, McpCallResult, McpContentText, McpToolDef, McpToolHandler,
    EXECUTOR_SESSION_PARAM,
};
use crate::rec::{manager_handle, new_manager, Caller, ExecutorRequest, ShellManager, ToolContext};
use crate::refs::{extract_file_ref_update, inject_file_ref, parse_hash_ref};
use crate::types::ExbashTaskSnapshot;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;

type SharedManager = Arc<Caller>;

#[derive(Clone, Debug)]
struct CallScope {
    session_id: String,
    workdir: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RgMode {
    Content,
    Files,
}

pub async fn create_session_mcp<H: SessionHost + 'static>(
    _ctx: ToolContext,
    host: Arc<H>,
    _shell_manager: ShellManager,
) -> Result<EmbeddedMcp<SessionMcpHandler<H>>, anyhow::Error> {
    let manager = OnceCell::<SharedManager>::new();
    manager
        .get_or_try_init(|| async { new_manager().await.map(Arc::new) })
        .await?;
    Ok(EmbeddedMcp::new(SessionMcpHandler { host, manager }))
}

pub fn create_session_mcp_with_manager<H: SessionHost + 'static>(
    _ctx: ToolContext,
    host: Arc<H>,
    manager: SharedManager,
    _shell_manager: ShellManager,
) -> EmbeddedMcp<SessionMcpHandler<H>> {
    let cell = OnceCell::new();
    cell.set(manager).ok();
    EmbeddedMcp::new(SessionMcpHandler {
        host,
        manager: cell,
    })
}

pub async fn create_default_session_mcp(
    ctx: ToolContext,
    shell_manager: ShellManager,
) -> Result<EmbeddedMcp<SessionMcpHandler<DummySessionHost>>, anyhow::Error> {
    create_session_mcp(ctx, Arc::new(DummySessionHost), shell_manager).await
}

pub struct SessionMcpHandler<H: SessionHost> {
    host: Arc<H>,
    manager: OnceCell<SharedManager>,
}

impl<H: SessionHost + 'static> SessionMcpHandler<H> {
    async fn scope(&self, context: McpCallContext) -> Result<CallScope, JsonRpcError> {
        let session_id = context
            .executor_session_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                JsonRpcError::invalid_params(format!("{EXECUTOR_SESSION_PARAM} is required"))
            })?;
        let workdir = self
            .host
            .session_workdir(&session_id)
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?;
        if workdir.trim().is_empty() {
            return Err(JsonRpcError::internal(format!(
                "workdir not found for {EXECUTOR_SESSION_PARAM}={session_id}"
            )));
        }
        Ok(CallScope {
            session_id,
            workdir,
        })
    }

    async fn resolve_file_args(
        &self,
        scope: &CallScope,
        mut arguments: Value,
    ) -> Result<(Value, Option<String>), JsonRpcError> {
        let Some(object) = arguments.as_object_mut() else {
            return Ok((arguments, None));
        };
        let Some(file_key) = object
            .remove("fileKey")
            .and_then(|v| v.as_str().map(str::to_string))
        else {
            return Ok((arguments, None));
        };
        if self.host.is_hash_ref(&file_key) {
            let entry = self
                .host
                .resolve_hash_ref(&scope.session_id, &file_key)
                .await
                .map_err(|err| JsonRpcError::internal(err.to_string()))?;
            let injection = inject_file_ref(&arguments, Some(&entry));
            return Ok((injection.args, Some(entry.file_key_ref)));
        }
        // Not a hashRef, convert fileKey -> filePath for REC
        let mut args = arguments.as_object().cloned().unwrap_or_default();
        args.insert("filePath".to_string(), Value::String(file_key));
        Ok((Value::Object(args), None))
    }

    async fn store_from_result(
        &self,
        scope: &CallScope,
        executor: &str,
        result: &serde_json::Value,
        _file_key_ref: Option<&str>,
    ) -> Option<String> {
        let rec = serde_json::from_value::<crate::types::RecToolResult>(result.clone()).ok()?;
        let update = extract_file_ref_update(&rec, executor)?;
        if let Ok(entry) = self.host.store_hash_ref(&scope.session_id, update).await {
            return Some(crate::refs::label_hash_ref(
                &crate::refs::basename(&entry.file_path),
                &crate::refs::small_hash_code(&entry.file_key_ref, &entry.hash_code),
            ));
        }
        None
    }

    async fn read_back_file_action_result(
        &self,
        scope: &CallScope,
        mut result: Value,
        executor: &str,
    ) -> Value {
        if result
            .pointer("/metadata/file/type")
            .and_then(Value::as_str)
            == Some("delete")
        {
            return result;
        }
        let file_path = result
            .pointer("/metadata/file/newFilePath")
            .or_else(|| result.pointer("/metadata/file/canonicalPath"))
            .or_else(|| result.pointer("/metadata/file/filePath"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if file_path.is_empty() {
            return result;
        }
        let read_args =
            json!({ "filePath": file_path, "executor": executor, "hashCheckMode": true });
        let Ok(read_result) = self.call_via_manager(scope, "read", read_args).await else {
            return result;
        };
        if let Some(hash_code) = read_result.pointer("/metadata/hashCode").cloned() {
            result["metadata"]["hashCode"] = hash_code;
        }
        if let Some(file) = read_result.pointer("/metadata/file").cloned() {
            result["metadata"]["file"] = file;
        }
        result
    }

    async fn upsert_exbash_from_result(
        &self,
        scope: &CallScope,
        result: &serde_json::Value,
        tracking_scope: &str,
        requested_mode: &str,
    ) -> Option<ExbashTaskSnapshot> {
        if !exbash_mode_creates_task(requested_mode) {
            return None;
        }
        let async_id = result
            .pointer("/metadata/asyncID")
            .and_then(Value::as_str)
            .map(str::to_string)?;
        let executor = result
            .pointer("/metadata/executor")
            .and_then(Value::as_str)
            .unwrap_or("local")
            .to_string();
        let command = result
            .pointer("/metadata/command")
            .and_then(Value::as_str)
            .map(str::to_string);
        let description = result
            .pointer("/metadata/description")
            .and_then(Value::as_str)
            .map(str::to_string);
        let state = Some(normalized_exbash_state(result, requested_mode, None));
        let exit_code = metadata_exit_code_i32(result);
        let pid = metadata_i64(result, "pid");
        let started_at = metadata_i64(result, "startedAt");
        let ended_at = metadata_i64(result, "endedAt");
        let total_output = metadata_i64(result, "totalOutput");
        let input = ExbashSyncInput {
            async_id: Some(async_id),
            session_id: None,
            workdir: None,
            executor: Some(executor),
            state,
            pid,
            exit_code,
            started_at,
            ended_at,
            command,
            description,
            total_output,
        };
        if tracking_scope == "remote" {
            return None;
        }
        let snapshot = if tracking_scope == "workspace" {
            let wd_input = ExbashSyncInput {
                session_id: Some(scope.session_id.clone()),
                ..input
            };
            self.host
                .upsert_workdir_exbash(&scope.session_id, &scope.workdir, wd_input)
                .await
                .ok()
        } else {
            self.host
                .upsert_session_exbash(&scope.session_id, input)
                .await
                .ok()
        };
        if let Some(snapshot) = snapshot.as_ref() {
            self.spawn_local_exbash_terminal_sync(scope, tracking_scope, snapshot);
        }
        snapshot
    }

    async fn remove_exbash_tracking(
        &self,
        scope: &CallScope,
        async_id: &str,
        executor: &str,
    ) -> Result<Value, JsonRpcError> {
        let local_removed = self
            .host
            .remove_session_exbash(&scope.session_id, async_id, executor)
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?;
        let workspace_removed = self
            .host
            .remove_workdir_exbash(&scope.session_id, &scope.workdir, async_id, executor)
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?;
        Ok(json!({
            "asyncID": async_id,
            "executor": executor,
            "localRemoved": local_removed,
            "workspaceRemoved": workspace_removed,
        }))
    }

    async fn update_existing_exbash_tracking(
        &self,
        scope: &CallScope,
        result: &serde_json::Value,
        mode: &str,
        async_id: &str,
        executor: &str,
    ) -> Result<Vec<ExbashTaskSnapshot>, JsonRpcError> {
        let mut snapshots = Vec::new();
        if let Some(existing) = self
            .host
            .session_exbash_snapshot(&scope.session_id, async_id, executor)
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?
        {
            let input = exbash_sync_input_from_existing(&existing, result, mode);
            snapshots.push(
                self.host
                    .upsert_session_exbash(&scope.session_id, input)
                    .await
                    .map_err(|err| JsonRpcError::internal(err.to_string()))?,
            );
        }
        if let Some(existing) = self
            .host
            .workdir_exbash_snapshot(&scope.session_id, &scope.workdir, async_id, executor)
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?
        {
            let input = exbash_sync_input_from_existing(&existing, result, mode);
            snapshots.push(
                self.host
                    .upsert_workdir_exbash(&scope.session_id, &scope.workdir, input)
                    .await
                    .map_err(|err| JsonRpcError::internal(err.to_string()))?,
            );
        }
        Ok(snapshots)
    }

    fn spawn_local_exbash_terminal_sync(
        &self,
        scope: &CallScope,
        tracking_scope: &str,
        snapshot: &ExbashTaskSnapshot,
    ) {
        if snapshot.executor != "local" || snapshot.state.as_deref() != Some("running") {
            return;
        }
        let Some(manager) = self.manager.get().cloned() else {
            return;
        };
        let host = Arc::clone(&self.host);
        let scope = scope.clone();
        let tracking_scope = tracking_scope.to_string();
        let async_id = snapshot.async_id.clone();
        let mut exit_rx = match manager.subscribe_local_exit_code(&async_id) {
            Ok(rx) => rx,
            Err(_) => return,
        };
        tokio::spawn(async move {
            if exit_rx.recv().await.is_none() {
                return;
            }
            let Ok(run) = manager.local_exbash_run_detail(&async_id) else {
                return;
            };
            let result = json!({ "metadata": run });
            Self::sync_local_exbash_terminal_result(host, scope, tracking_scope, async_id, result)
                .await;
        });
    }

    async fn sync_local_exbash_terminal_result(
        host: Arc<H>,
        scope: CallScope,
        tracking_scope: String,
        async_id: String,
        result: Value,
    ) {
        if tracking_scope == "workspace" {
            let Ok(Some(existing)) = host
                .workdir_exbash_snapshot(&scope.session_id, &scope.workdir, &async_id, "local")
                .await
            else {
                return;
            };
            if existing.state.as_deref() != Some("running") {
                return;
            }
            let input = exbash_sync_input_from_existing(&existing, &result, "event");
            let _ = host
                .upsert_workdir_exbash(&scope.session_id, &scope.workdir, input)
                .await;
            return;
        }

        let Ok(Some(existing)) = host
            .session_exbash_snapshot(&scope.session_id, &async_id, "local")
            .await
        else {
            return;
        };
        if existing.state.as_deref() != Some("running") {
            return;
        }
        let input = exbash_sync_input_from_existing(&existing, &result, "event");
        let _ = host.upsert_session_exbash(&scope.session_id, input).await;
    }

    async fn call_via_manager(
        &self,
        scope: &CallScope,
        name: &str,
        mut arguments: Value,
    ) -> Result<serde_json::Value, JsonRpcError> {
        // Extract executor from arguments, default to "local"
        let executor = extract_executor(&mut arguments);
        // Remove ExecutorSessionID from arguments
        remove_executor_session_id(&mut arguments);
        // Remove executor from arguments (it goes into ExecutorRequest)
        remove_field(&mut arguments, "executor");

        let manager = self
            .manager
            .get()
            .ok_or_else(|| JsonRpcError::internal("RemoteExecutorManager is not initialized"))?
            .clone();
        let directory = if executor == "local" {
            Some(PathBuf::from(&scope.workdir))
        } else {
            None
        };
        let request = ExecutorRequest {
            id: Value::Number(1.into()),
            method: name.to_string(),
            executor: Some(executor),
            params: arguments,
            directory,
            tool_timeout_ms: None,
        };
        let response = manager_handle(&manager, request).await;
        if !response.ok {
            return Err(JsonRpcError::internal(
                response.error.unwrap_or_else(|| "unknown error".into()),
            ));
        }
        Ok(response.result.unwrap_or(Value::Null))
    }

    async fn list_exbash(
        &self,
        scope: &CallScope,
        arguments: &Value,
    ) -> Result<McpCallResult, JsonRpcError> {
        let list_scope = extract_exbash_scope(arguments)?;
        let executor_filter = optional_string_field(arguments, "executor")
            .or_else(|| optional_string_field(arguments, "targetExecutor"));
        let display_executor = executor_filter.as_deref().unwrap_or("all");
        let local_tasks = self
            .host
            .list_session_exbash(&scope.session_id, executor_filter.as_deref())
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?;
        let workspace_tasks = self
            .host
            .list_workdir_exbash(
                &scope.session_id,
                &scope.workdir,
                executor_filter.as_deref(),
            )
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?;

        if list_scope == "remote" {
            let Some(remote_executor) = executor_filter.as_deref() else {
                return Err(JsonRpcError::invalid_params(
                    "scope=remote requires executor",
                ));
            };
            if remote_executor == "local" {
                return Err(JsonRpcError::invalid_params(
                    "scope=remote requires a remote executor",
                ));
            }
            let remote = self
                .call_via_manager(
                    scope,
                    "exbash",
                    json!({ "mode": "list", "executor": remote_executor }),
                )
                .await?;
            let remote_runs = remote_exbash_runs(&remote);
            let tracked = tracked_async_ids(&local_tasks, &workspace_tasks);
            let untracked = remote_runs
                .into_iter()
                .filter(|run| !remote_run_is_tracked(&tracked, remote_executor, run))
                .collect::<Vec<_>>();
            let mut lines = exbash_list_header(
                local_tasks.len(),
                workspace_tasks.len(),
                remote_executor,
                "remote",
            );
            if untracked.is_empty() {
                lines.push("- none".to_string());
            } else {
                lines.extend(untracked.iter().map(format_remote_exbash_run));
            }
            return Ok(McpCallResult {
                content: vec![McpContentText {
                    kind: "text".to_string(),
                    text: lines.join("\n"),
                }],
                structured_content: Some(json!({
                    "metadata": {
                        "scope": "remote",
                        "executor": remote_executor,
                        "localCount": local_tasks.len(),
                        "workspaceCount": workspace_tasks.len(),
                        "remoteUntracked": untracked,
                    }
                })),
            });
        }

        let visible_tasks = if list_scope == "workspace" {
            &workspace_tasks
        } else {
            &local_tasks
        };
        let mut lines = exbash_list_header(
            local_tasks.len(),
            workspace_tasks.len(),
            display_executor,
            &list_scope,
        );
        if visible_tasks.is_empty() {
            lines.push("- none".to_string());
        } else {
            lines.extend(visible_tasks.iter().map(format_exbash_task));
        }

        let mut remote_untracked_count = None;
        if let Some(remote_executor) = executor_filter.as_deref().filter(|item| *item != "local") {
            if let Ok(remote) = self
                .call_via_manager(
                    scope,
                    "exbash",
                    json!({ "mode": "list", "executor": remote_executor }),
                )
                .await
            {
                let tracked = tracked_async_ids(&local_tasks, &workspace_tasks);
                let count = remote_exbash_runs(&remote)
                    .into_iter()
                    .filter(|run| !remote_run_is_tracked(&tracked, remote_executor, run))
                    .count();
                remote_untracked_count = Some(count);
                if count > 0 {
                    lines.push(format!(
                        "executor={remote_executor} has {count} untracked task(s) (use scope=remote to see)"
                    ));
                }
            }
        }

        Ok(McpCallResult {
            content: vec![McpContentText {
                kind: "text".to_string(),
                text: lines.join("\n"),
            }],
            structured_content: Some(json!({
                "metadata": {
                    "scope": list_scope,
                    "executor": display_executor,
                    "localCount": local_tasks.len(),
                    "workspaceCount": workspace_tasks.len(),
                    "remoteUntrackedCount": remote_untracked_count,
                    "tasks": visible_tasks,
                }
            })),
        })
    }

    async fn check_exbash_create_allowed(
        &self,
        scope: &CallScope,
        tracking_scope: &str,
        arguments: &Value,
    ) -> Result<(), JsonRpcError> {
        let mode = optional_string_field(arguments, "mode").unwrap_or_else(|| "shell".to_string());
        if !exbash_mode_creates_task(&mode) {
            return Ok(());
        }
        let input = ExbashSyncInput {
            async_id: optional_string_field(arguments, "asyncID"),
            session_id: None,
            workdir: optional_string_field(arguments, "workdir"),
            executor: optional_string_field(arguments, "executor")
                .or_else(|| optional_string_field(arguments, "targetExecutor")),
            state: None,
            pid: None,
            exit_code: None,
            started_at: None,
            ended_at: None,
            command: optional_string_field(arguments, "command"),
            description: optional_string_field(arguments, "description"),
            total_output: None,
        };
        if tracking_scope == "workspace" {
            let input = ExbashSyncInput {
                session_id: Some(scope.session_id.clone()),
                workdir: Some(scope.workdir.clone()),
                ..input
            };
            return self
                .host
                .check_workdir_exbash_create(&scope.session_id, &scope.workdir, &input)
                .await
                .map_err(|err| JsonRpcError::invalid_params(err.to_string()));
        }
        self.host
            .check_session_exbash_create(&scope.session_id, &input)
            .await
            .map_err(|err| JsonRpcError::invalid_params(err.to_string()))
    }
}

fn extract_executor(arguments: &mut Value) -> String {
    let Some(object) = arguments.as_object_mut() else {
        return "local".to_string();
    };
    object
        .remove("executor")
        .or_else(|| object.remove("targetExecutor"))
        .and_then(|v| v.as_str().map(str::to_string))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "local".to_string())
}

fn remove_executor_session_id(arguments: &mut Value) {
    if let Some(object) = arguments.as_object_mut() {
        object.remove(EXECUTOR_SESSION_PARAM);
    }
}

fn remove_field(arguments: &mut Value, field: &str) {
    if let Some(object) = arguments.as_object_mut() {
        object.remove(field);
    }
}

fn optional_string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn require_patch_hash_ref(arguments: &Value) -> Result<(), JsonRpcError> {
    if optional_string_field(arguments, "mode").as_deref() != Some("patch") {
        return Ok(());
    }
    let file_key = optional_string_field(arguments, "fileKey").unwrap_or_default();
    if parse_hash_ref(&file_key).is_some() {
        return Ok(());
    }
    Err(JsonRpcError::invalid_params(
        "FileAction mode=patch requires fileKey to be a hashRef returned by read/FileAction, such as `App.ts #A1B2`; call read first and pass the <fileRef> value.",
    ))
}

fn enable_hash_check_mode(arguments: &mut Value) {
    if let Some(object) = arguments.as_object_mut() {
        object
            .entry("hashCheckMode".to_string())
            .or_insert(Value::Bool(true));
    }
}

fn extract_exbash_scope(arguments: &Value) -> Result<String, JsonRpcError> {
    let scope = optional_string_field(arguments, "scope")
        .or_else(|| optional_string_field(arguments, "spoe"))
        .unwrap_or_else(|| "local".to_string());
    match scope.as_str() {
        "local" | "workspace" | "remote" => Ok(scope),
        other => Err(JsonRpcError::invalid_params(format!(
            "invalid exbash scope: {other}; expected local, workspace, or remote"
        ))),
    }
}

fn exbash_mode_creates_task(mode: &str) -> bool {
    matches!(mode, "run" | "shell")
}

fn exbash_error_indicates_missing_or_lost(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("async run not found")
        || message.contains("not found")
        || message.contains("does not exist")
        || message.contains("closed before responding")
        || message.contains("connection")
        || message.contains("disconnected")
        || message.contains("timed out")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn exbash_result_async_id(value: &Value) -> Option<&str> {
    value.pointer("/metadata/asyncID").and_then(Value::as_str)
}

fn exbash_result_executor(value: &Value) -> Option<&str> {
    value.pointer("/metadata/executor").and_then(Value::as_str)
}

fn metadata_string(value: &Value, field: &str) -> Option<String> {
    value
        .get("metadata")
        .and_then(|metadata| metadata.get(field))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn metadata_i64(value: &Value, field: &str) -> Option<i64> {
    json_i64(
        value
            .get("metadata")
            .and_then(|metadata| metadata.get(field))?,
    )
}

fn json_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
        .or_else(|| value.as_str()?.parse::<i64>().ok())
}

fn json_i32(value: &Value) -> Option<i32> {
    value
        .as_i64()
        .and_then(|number| i32::try_from(number).ok())
        .or_else(|| value.as_u64().and_then(|number| i32::try_from(number).ok()))
        .or_else(|| value.as_str()?.parse::<i32>().ok())
}

fn metadata_exit_code_i32(value: &Value) -> Option<i32> {
    let exit_code = value
        .get("metadata")
        .and_then(|metadata| metadata.get("exitCode"))?;
    json_i32(exit_code)
}

fn normalized_exit_state(exit_code: &Value) -> Option<String> {
    if let Some(number) = json_i64(exit_code) {
        return Some(format!("exit:{number}"));
    }
    let text = exit_code.as_str()?.trim();
    if text.is_empty() {
        return None;
    }
    match text {
        "timeout" => Some("timeout".to_string()),
        "stopped" | "stop" => Some("stop".to_string()),
        other => other
            .parse::<i32>()
            .map(|number| format!("exit:{number}"))
            .ok(),
    }
}

fn normalize_exbash_state_for_host(state: &str, exit_code: Option<i32>) -> Option<String> {
    match state.trim() {
        "running" | "timeout" | "stop" => Some(state.trim().to_string()),
        value if value.starts_with("exit:") => Some(value.to_string()),
        "stopped" => Some(
            exit_code
                .map(|code| format!("exit:{code}"))
                .unwrap_or_else(|| "stop".to_string()),
        ),
        "" => exit_code.map(|code| format!("exit:{code}")),
        _ => None,
    }
}

fn normalize_exbash_state_for_display(state: &str, exit_code: Option<i32>) -> String {
    if state == "unknown" {
        return "unknown".to_string();
    }
    normalize_exbash_state_for_host(state, exit_code).unwrap_or_else(|| "unknown".to_string())
}

fn normalized_exbash_state(result: &Value, mode: &str, fallback: Option<&str>) -> String {
    if let Some(state) = result
        .get("metadata")
        .and_then(|metadata| metadata.get("exitCode"))
        .and_then(normalized_exit_state)
    {
        return state;
    }
    if mode == "stop" {
        return "stop".to_string();
    }
    if let Some(state) = metadata_string(result, "state") {
        if let Some(state) = normalize_exbash_state_for_host(&state, metadata_exit_code_i32(result))
        {
            return state;
        }
    }
    if matches!(mode, "run" | "shell") && exbash_result_async_id(result).is_some() {
        return "running".to_string();
    }
    fallback
        .and_then(|state| normalize_exbash_state_for_host(state, metadata_exit_code_i32(result)))
        .unwrap_or_else(|| "running".to_string())
}

fn set_metadata_field(value: &mut Value, field: &str, data: Value) {
    if !value.get("metadata").map(Value::is_object).unwrap_or(false) {
        value["metadata"] = json!({});
    }
    if let Some(metadata) = value.get_mut("metadata").and_then(Value::as_object_mut) {
        metadata.insert(field.to_string(), data);
    }
}

fn exbash_sync_input_from_existing(
    existing: &ExbashTaskSnapshot,
    result: &Value,
    mode: &str,
) -> ExbashSyncInput {
    ExbashSyncInput {
        async_id: Some(existing.async_id.clone()),
        session_id: existing.session_id.clone(),
        workdir: existing.workdir.clone(),
        executor: Some(existing.executor.clone()),
        state: Some(normalized_exbash_state(
            result,
            mode,
            existing.state.as_deref(),
        )),
        pid: metadata_i64(result, "pid").or(existing.pid),
        exit_code: metadata_exit_code_i32(result).or(existing.exit_code),
        started_at: metadata_i64(result, "startedAt").or(existing.started_at),
        ended_at: metadata_i64(result, "endedAt").or_else(|| {
            if mode == "stop" {
                Some(now_ms())
            } else {
                existing.ended_at
            }
        }),
        command: metadata_string(result, "command").or_else(|| existing.command.clone()),
        description: metadata_string(result, "description")
            .or_else(|| existing.description.clone()),
        total_output: metadata_i64(result, "totalOutput").or(existing.total_output),
    }
}

fn extract_rg_mode(arguments: &Value) -> Result<RgMode, JsonRpcError> {
    let mode = optional_string_field(arguments, "mode")
        .or_else(|| optional_string_field(arguments, "type"))
        .unwrap_or_else(|| "content".to_string());
    match mode.as_str() {
        "content" | "text" | "search" => Ok(RgMode::Content),
        "files" | "file" | "paths" | "path" | "glob" => Ok(RgMode::Files),
        other => Err(JsonRpcError::invalid_params(format!(
            "invalid rg mode: {other}; expected content or files"
        ))),
    }
}

fn exbash_list_header(
    local_count: usize,
    workspace_count: usize,
    executor: &str,
    scope: &str,
) -> Vec<String> {
    vec![
        format!("local:{local_count} workspace:{workspace_count}"),
        format!("showing executor={executor} of {scope}"),
    ]
}

fn tracked_async_ids(
    local_tasks: &[ExbashTaskSnapshot],
    workspace_tasks: &[ExbashTaskSnapshot],
) -> HashSet<String> {
    local_tasks
        .iter()
        .chain(workspace_tasks.iter())
        .map(|task| format!("{}:{}", task.executor, task.async_id))
        .collect()
}

fn remote_run_is_tracked(tracked: &HashSet<String>, executor: &str, run: &Value) -> bool {
    run.get("asyncID")
        .and_then(Value::as_str)
        .map(|async_id| tracked.contains(&format!("{executor}:{async_id}")))
        .unwrap_or(false)
}

fn remote_exbash_runs(value: &Value) -> Vec<Value> {
    value
        .pointer("/metadata/runs")
        .or_else(|| value.pointer("/result/metadata/runs"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn format_exbash_task(task: &ExbashTaskSnapshot) -> String {
    let state = task
        .state
        .as_deref()
        .map(|state| normalize_exbash_state_for_display(state, task.exit_code))
        .unwrap_or_else(|| {
            task.exit_code
                .map(|code| format!("exit:{code}"))
                .unwrap_or_else(|| "unknown".to_string())
        });
    let total_output = task.total_output.unwrap_or_default();
    let command = task.command.as_deref().unwrap_or("");
    format!(
        "- {}:{} {} totalOutput={} command={}",
        task.executor, task.async_id, state, total_output, command
    )
}

fn format_remote_exbash_run(run: &Value) -> String {
    let async_id = run
        .get("asyncID")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let exit_code = run.get("exitCode");
    let state = exit_code
        .and_then(normalized_exit_state)
        .or_else(|| {
            run.get("state")
                .and_then(Value::as_str)
                .map(|state| normalize_exbash_state_for_display(state, None))
        })
        .unwrap_or_else(|| "unknown".to_string());
    let total_output = run
        .get("totalOutput")
        .map(json_value_text)
        .unwrap_or_else(|| "0".to_string());
    let command = run.get("command").and_then(Value::as_str).unwrap_or("");
    format!("- {async_id} {state} totalOutput={total_output} command={command}")
}

fn format_exbash_run_or_shell_output(result: &Value) -> String {
    let output = result.get("output");
    let mut text = output
        .and_then(|output| output.get("text"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let message = output
        .and_then(|output| output.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let info = output
        .and_then(|output| output.get("info"))
        .and_then(Value::as_str)
        .unwrap_or("");

    if let Some(exit_code) = result.pointer("/metadata/exitCode") {
        let total_output_bytes = exbash_completed_output_bytes(result, &text);
        append_completion_footer(&mut text, total_output_bytes, &json_value_text(exit_code));
        return text;
    }

    let detached = result
        .pointer("/metadata/detached")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || result.pointer("/metadata/asyncID").is_some();
    if detached {
        if !message.is_empty() {
            append_visible_line(&mut text, message);
        }
        if !info.is_empty() {
            append_visible_line(&mut text, info);
        }
        return text;
    }

    extract_output_text(result)
}

fn format_rg_output(result: &Value) -> String {
    let mut text = extract_output_text(result);
    let mut footer = Vec::new();
    if let Some(matches) = result.pointer("/metadata/matches") {
        footer.push(format!("matches:{}", json_value_text(matches)));
    }
    if let Some(files_walked) = result.pointer("/metadata/filesWalked") {
        footer.push(format!("filesWalked:{}", json_value_text(files_walked)));
    }
    if result
        .pointer("/metadata/timedOut")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        footer.push("timedOut:true".to_string());
    }
    if let Some(code) = result.pointer("/metadata/code") {
        footer.push(format!("code:{}", json_value_text(code)));
    }
    if !footer.is_empty() {
        ensure_blank_line(&mut text);
        text.push_str(&footer.join("\n"));
    }
    text
}

fn normalize_rg_files_result(mut result: Value) -> Value {
    let matches = result
        .pointer("/metadata/count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let code = if matches == 0 { 1 } else { 0 };
    if let Some(metadata) = result.get_mut("metadata").and_then(Value::as_object_mut) {
        metadata.insert("mode".to_string(), json!("files"));
        metadata.insert("matches".to_string(), json!(matches));
        metadata.insert("code".to_string(), json!(code));
    } else if let Some(object) = result.as_object_mut() {
        object.insert(
            "metadata".to_string(),
            json!({ "mode": "files", "matches": matches, "code": code }),
        );
    }
    result
}

fn append_visible_line(text: &mut String, line: &str) {
    if text.lines().any(|existing| existing.trim() == line) {
        return;
    }
    if !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(line);
}

fn append_completion_footer(text: &mut String, total_output_bytes: usize, exit_code: &str) {
    ensure_blank_line(text);
    text.push_str(&format!(
        "totaloutput:{total_output_bytes}bytes\nexitcode:{exit_code}"
    ));
}

fn ensure_blank_line(text: &mut String) {
    if text.is_empty() {
        text.push_str("\n\n");
    } else if text.ends_with("\n\n") || text.ends_with("\r\n\n") || text.ends_with("\r\n\r\n") {
        // Already has a blank line before the footer.
    } else if text.ends_with('\n') {
        text.push('\n');
    } else {
        text.push_str("\n\n");
    }
}

fn exbash_completed_output_bytes(result: &Value, visible_text: &str) -> usize {
    result
        .pointer("/metadata/totalOutput")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .or_else(|| {
            result
                .pointer("/metadata/output")
                .and_then(Value::as_str)
                .map(|value| value.as_bytes().len())
        })
        .unwrap_or_else(|| visible_text.as_bytes().len())
}

fn json_value_text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

#[async_trait]
impl<H: SessionHost + 'static> McpToolHandler for SessionMcpHandler<H> {
    async fn list_tools(&self) -> Result<Vec<McpToolDef>, JsonRpcError> {
        Ok(crate::mcp::mcptooldefs::all_tools())
    }

    async fn call_tool(
        &self,
        name: &str,
        mut arguments: Value,
        context: McpCallContext,
    ) -> Result<McpCallResult, JsonRpcError> {
        let scope = self.scope(context).await?;
        match name {
            "FileAction" | "read" => {
                let file_action_mode = if name == "FileAction" {
                    arguments
                        .get("mode")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                } else {
                    None
                };
                if name == "FileAction" {
                    require_patch_hash_ref(&arguments)?;
                }
                // Resolve hashRef if present
                let (mut args, file_key_ref) = self.resolve_file_args(&scope, arguments).await?;
                if name == "read" {
                    enable_hash_check_mode(&mut args);
                }
                let executor = extract_executor_from_value(&args);
                // Call via Caller
                let result = self.call_via_manager(&scope, name, args).await?;
                // FileAction results use patch-style file metadata. Read back changed files so
                // hashRef storage gets a canonical REC FileStamp and the latest hash.
                let result = if name == "FileAction" {
                    self.read_back_file_action_result(&scope, result, &executor)
                        .await
                } else {
                    result
                };
                // Store fileRef if applicable
                let label = self
                    .store_from_result(&scope, &executor, &result, file_key_ref.as_deref())
                    .await;
                let mut value = result;
                if let Some(label) = label {
                    if let Some(text) = value.pointer_mut("/output/text") {
                        if let Some(existing) = text.as_str() {
                            *text =
                                Value::String(format!("{existing}\n<fileRef>{label}</fileRef>"));
                        }
                    }
                }
                let raw_output_text = extract_output_text(&value);
                let output_text = match file_action_mode.as_deref() {
                    Some("patch" | "rename") => strip_hash_code_lines(&raw_output_text),
                    _ => raw_output_text,
                };
                let structured_content = match file_action_mode.as_deref() {
                    Some("create" | "patch" | "rename" | "delete") => None,
                    _ => Some(strip_output(value)),
                };
                Ok(McpCallResult {
                    content: vec![McpContentText {
                        kind: "text".to_string(),
                        text: output_text,
                    }],
                    structured_content,
                })
            }
            "exbash" => {
                let mode = optional_string_field(&arguments, "mode")
                    .unwrap_or_else(|| "shell".to_string());
                if mode == "list" {
                    return self.list_exbash(&scope, &arguments).await;
                }
                let tracking_scope = extract_exbash_scope(&arguments)?;
                if tracking_scope == "remote" {
                    return Err(JsonRpcError::invalid_params(
                        "scope=remote is only supported for exbash mode=list",
                    ));
                }
                let mut call_args = arguments;
                if let Some(object) = call_args.as_object_mut() {
                    let missing_mode = object
                        .get("mode")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_none();
                    if missing_mode {
                        object.insert("mode".to_string(), Value::String(mode.clone()));
                    }
                }
                let requested_async_id = optional_string_field(&call_args, "asyncID");
                let requested_executor = optional_string_field(&call_args, "executor")
                    .or_else(|| optional_string_field(&call_args, "targetExecutor"))
                    .unwrap_or_else(|| "local".to_string());
                self.check_exbash_create_allowed(&scope, &tracking_scope, &call_args)
                    .await?;
                remove_field(&mut call_args, "scope");
                remove_field(&mut call_args, "spoe");
                let result = match self.call_via_manager(&scope, "exbash", call_args).await {
                    Ok(result) => result,
                    Err(error) => {
                        if mode == "remove"
                            && requested_executor == "local"
                            && exbash_error_indicates_missing_or_lost(&error.message)
                        {
                            if let Some(async_id) = requested_async_id.as_deref() {
                                self.remove_exbash_tracking(&scope, async_id, &requested_executor)
                                    .await?;
                            }
                        }
                        return Err(error);
                    }
                };
                let mut value = result;
                if let Some(snapshot) = self
                    .upsert_exbash_from_result(&scope, &value, &tracking_scope, &mode)
                    .await
                {
                    value["metadata"]["hostSnapshot"] =
                        serde_json::to_value(snapshot).unwrap_or_default();
                } else if let Some(async_id) =
                    exbash_result_async_id(&value).or(requested_async_id.as_deref())
                {
                    let executor = exbash_result_executor(&value)
                        .or(Some(requested_executor.as_str()))
                        .unwrap_or("local");
                    if mode == "remove" {
                        if value.pointer("/metadata/hostCleanup").is_none() {
                            let cleanup = self
                                .remove_exbash_tracking(&scope, async_id, executor)
                                .await?;
                            set_metadata_field(&mut value, "hostCleanup", cleanup);
                        }
                    } else if matches!(mode.as_str(), "stop" | "attach") {
                        let snapshots = self
                            .update_existing_exbash_tracking(
                                &scope, &value, &mode, async_id, executor,
                            )
                            .await?;
                        if !snapshots.is_empty() {
                            set_metadata_field(
                                &mut value,
                                "hostSnapshots",
                                serde_json::to_value(snapshots).unwrap_or_default(),
                            );
                        }
                    }
                }
                let output_text = if mode == "run" || mode == "shell" {
                    format_exbash_run_or_shell_output(&value)
                } else {
                    extract_output_text(&value)
                };
                Ok(McpCallResult {
                    content: vec![McpContentText {
                        kind: "text".to_string(),
                        text: output_text,
                    }],
                    structured_content: Some(strip_output(value)),
                })
            }
            "rg" => {
                let mode = extract_rg_mode(&arguments)?;
                let method = match mode {
                    RgMode::Content => "rg",
                    RgMode::Files => "glob",
                };
                let mut call_args = arguments;
                remove_field(&mut call_args, "mode");
                remove_field(&mut call_args, "type");
                let result = self.call_via_manager(&scope, method, call_args).await?;
                let result = match mode {
                    RgMode::Content => result,
                    RgMode::Files => normalize_rg_files_result(result),
                };
                let output_text = format_rg_output(&result);
                Ok(McpCallResult {
                    content: vec![McpContentText {
                        kind: "text".to_string(),
                        text: output_text,
                    }],
                    structured_content: Some(strip_output(result)),
                })
            }
            "RemoteExecutorManager" => {
                let method = arguments
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if method == "list_shells" || method == "set_executor_shell" {
                    let mut call_args = arguments;
                    remove_field(&mut call_args, "method");
                    let tool_method = if method == "list_shells" {
                        "list_shells"
                    } else {
                        if let Some(object) = call_args.as_object_mut() {
                            let missing_shell = object
                                .get("shell")
                                .and_then(Value::as_str)
                                .map(str::trim)
                                .filter(|value| !value.is_empty())
                                .is_none();
                            if missing_shell {
                                object.insert("shell".to_string(), Value::String("auto".into()));
                            }
                        }
                        "set_default_shell"
                    };
                    let result = self
                        .call_via_manager(&scope, tool_method, call_args)
                        .await?;
                    let output_text = extract_output_text(&result);
                    return Ok(McpCallResult {
                        content: vec![McpContentText {
                            kind: "text".to_string(),
                            text: output_text,
                        }],
                        structured_content: Some(strip_output(result)),
                    });
                }
                // Other manager methods go through Caller
                let executor = extract_executor(&mut arguments);
                remove_executor_session_id(&mut arguments);
                remove_field(&mut arguments, "executor");
                remove_field(&mut arguments, "method");
                let manager = self
                    .manager
                    .get()
                    .ok_or_else(|| {
                        JsonRpcError::internal("RemoteExecutorManager is not initialized")
                    })?
                    .clone();
                let request = ExecutorRequest {
                    id: Value::Number(1.into()),
                    method,
                    executor: Some(executor),
                    params: arguments,
                    directory: None,
                    tool_timeout_ms: None,
                };
                let response = manager_handle(&manager, request).await;
                let result = serde_json::to_value(&response).unwrap();
                let output_text = manager_output_text(&result);
                Ok(McpCallResult {
                    content: vec![McpContentText {
                        kind: "text".to_string(),
                        text: output_text,
                    }],
                    structured_content: Some(strip_output(result)),
                })
            }
            unknown => Err(JsonRpcError::method_not_found(unknown)),
        }
    }
}

fn extract_output_text(result: &Value) -> String {
    let output = result.get("output");
    match output {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Object(obj)) => {
            let parts: Vec<&str> = [
                obj.get("message").and_then(Value::as_str).unwrap_or(""),
                obj.get("text").and_then(Value::as_str).unwrap_or(""),
                obj.get("info").and_then(Value::as_str).unwrap_or(""),
            ]
            .iter()
            .filter(|s| !s.is_empty())
            .copied()
            .collect();
            parts.join("\n")
        }
        _ => "".to_string(),
    }
}

fn manager_output_text(result: &Value) -> String {
    let text = result
        .get("result")
        .and_then(executor_list_output_text)
        .or_else(|| {
            result
                .get("result")
                .map(extract_output_text)
                .filter(|text| !text.is_empty())
        });
    text.unwrap_or_else(|| serde_json::to_string_pretty(result).unwrap_or_default())
}

fn executor_list_output_text(result: &Value) -> Option<String> {
    let metadata = result.get("metadata")?;
    let executors = metadata.get("executors")?.as_array()?;
    let default_executor = metadata
        .get("default")
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut lines = Vec::new();
    if !default_executor.is_empty() {
        lines.push(format!("default executor: {default_executor}"));
    }
    lines.push("executors:".to_string());
    if executors.is_empty() {
        lines.push("- none".to_string());
    }
    for executor in executors {
        let id = executor
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown");
        let mut line = format!("- {id}");
        if id == default_executor {
            line.push_str(" (default)");
        }
        for field in ["system", "device", "url"] {
            if let Some(value) = executor
                .get(field)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                line.push_str(&format!(" {field}={value}"));
            }
        }
        if let Some(labels) = labels_output_text(executor.get("labels")) {
            line.push_str(&format!(" labels={labels}"));
        }
        lines.push(line);
    }
    Some(lines.join("\n"))
}

fn labels_output_text(labels: Option<&Value>) -> Option<String> {
    let object = labels?.as_object()?;
    if object.is_empty() {
        return None;
    }
    let mut entries = object
        .iter()
        .map(|(key, value)| {
            let value = value
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string());
            format!("{key}={value}")
        })
        .collect::<Vec<_>>();
    entries.sort();
    Some(entries.join(","))
}

fn strip_hash_code_lines(text: &str) -> String {
    text.lines()
        .filter(|line| !line.trim_start().starts_with("hashCode:"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Strip `output` from a result value before storing as structuredContent.
/// structuredContent should only contain metadata/title for programmatic use;
/// the model-visible output goes in content[0].text.
fn strip_output(mut result: Value) -> Value {
    if let Some(obj) = result.as_object_mut() {
        obj.remove("output");
        if let Some(nested) = obj.get_mut("result") {
            if let Some(nested_obj) = nested.as_object_mut() {
                nested_obj.remove("output");
            }
        }
    }
    result
}

fn extract_executor_from_value(arguments: &Value) -> String {
    arguments
        .get("executor")
        .or_else(|| arguments.get("targetExecutor"))
        .and_then(Value::as_str)
        .unwrap_or("local")
        .to_string()
}

pub struct DummySessionHost;

#[async_trait]
impl crate::host::SessionWorkdirProvider for DummySessionHost {
    type Error = String;
    async fn session_workdir(&self, _session_id: &str) -> Result<String, Self::Error> {
        Ok("".into())
    }
}

#[async_trait]
impl crate::host::HashRefSessionStore for DummySessionHost {
    type Error = String;
    fn is_hash_ref(&self, target: &str) -> bool {
        parse_hash_ref(target).is_some()
    }
    async fn resolve_hash_ref(
        &self,
        _session_id: &str,
        _target: &str,
    ) -> Result<crate::types::FileRefEntry, Self::Error> {
        Err("hashRef host is not configured".into())
    }
    async fn store_hash_ref(
        &self,
        _session_id: &str,
        _update: crate::types::FileRefUpdate,
    ) -> Result<crate::types::FileRefEntry, Self::Error> {
        Err("hashRef host is not configured".into())
    }
    async fn retouch_hash_ref(
        &self,
        _session_id: &str,
        _file_key_ref: &str,
        _hash_code: &str,
    ) -> Result<Option<crate::types::FileRefEntry>, Self::Error> {
        Err("hashRef host is not configured".into())
    }
}

#[async_trait]
impl crate::host::ExbashSessionStore for DummySessionHost {
    type Error = String;
    async fn session_exbash_snapshot(
        &self,
        _session_id: &str,
        _async_id: &str,
        _executor: &str,
    ) -> Result<Option<ExbashTaskSnapshot>, Self::Error> {
        Ok(None)
    }
    async fn upsert_session_exbash(
        &self,
        session_id: &str,
        input: ExbashSyncInput,
    ) -> Result<ExbashTaskSnapshot, Self::Error> {
        Ok(ExbashTaskSnapshot {
            async_id: input.async_id.unwrap_or_default(),
            executor: input.executor.unwrap_or_else(|| "local".into()),
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
        })
    }
    async fn remove_session_exbash(
        &self,
        _session_id: &str,
        _async_id: &str,
        _executor: &str,
    ) -> Result<bool, Self::Error> {
        Ok(false)
    }
}

#[async_trait]
impl crate::host::ExbashWorkdirStore for DummySessionHost {
    type Error = String;
    async fn workdir_exbash_snapshot(
        &self,
        _session_id: &str,
        _workdir: &str,
        _async_id: &str,
        _executor: &str,
    ) -> Result<Option<ExbashTaskSnapshot>, Self::Error> {
        Ok(None)
    }
    async fn upsert_workdir_exbash(
        &self,
        session_id: &str,
        workdir: &str,
        input: ExbashSyncInput,
    ) -> Result<ExbashTaskSnapshot, Self::Error> {
        Ok(ExbashTaskSnapshot {
            async_id: input.async_id.unwrap_or_default(),
            executor: input.executor.unwrap_or_else(|| "local".into()),
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
        })
    }
    async fn remove_workdir_exbash(
        &self,
        _session_id: &str,
        _workdir: &str,
        _async_id: &str,
        _executor: &str,
    ) -> Result<bool, Self::Error> {
        Ok(false)
    }
}

#[async_trait]
impl crate::host::RemoteExecutorConfigStore for DummySessionHost {
    type Error = String;
    async fn read_remote_executor_config(
        &self,
        workdir: &str,
    ) -> Result<crate::types::RemoteExecutorConfigSnapshot, Self::Error> {
        Ok(crate::types::RemoteExecutorConfigSnapshot {
            workdir: workdir.to_string(),
            config: json!({}),
        })
    }
    async fn update_remote_executor_config(
        &self,
        workdir: &str,
        patch: Value,
    ) -> Result<crate::types::RemoteExecutorConfigSnapshot, Self::Error> {
        Ok(crate::types::RemoteExecutorConfigSnapshot {
            workdir: workdir.to_string(),
            config: patch,
        })
    }
}
