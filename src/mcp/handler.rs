use crate::host::{ExbashSyncInput, SessionHost};
use crate::jsonrpc::JsonRpcError;
use crate::mcp::{
    EmbeddedMcp, McpCallContext, McpCallResult, McpContentText, McpToolDef, McpToolHandler,
    EXECUTOR_SESSION_PARAM,
};
use crate::rec::{
    manager_handle, new_manager, Caller, ConnectExecutorOptions, ExecutorRequest, ShellManager,
    ToolContext,
};
use crate::refs::{extract_file_ref_update, inject_file_ref, parse_hash_ref};
use crate::types::ExbashTaskSnapshot;
use async_trait::async_trait;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;

type SharedManager = Arc<Caller>;
const TMP_RUNNING_DESCRIPTION: &str = "Tmp Running";

#[derive(Clone, Debug)]
struct CallScope {
    session_id: String,
    workdir: String,
}

#[derive(Clone, Debug, Default)]
struct ExbashHostSync {
    snapshot: Option<ExbashTaskSnapshot>,
    warning: Option<Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RgMode {
    Content,
    Files,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RgPathList {
    Single,
    Multi(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct WorkspaceExecutorInfo {
    id: String,
    url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    device: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkspaceExecutorConfig {
    executors: Vec<WorkspaceExecutorInfo>,
}

#[derive(Clone, Debug)]
struct SkillTarget {
    executor: String,
    path: String,
}

#[derive(Clone, Debug)]
struct SkillInfo {
    name: String,
    description: String,
    location: String,
    content: String,
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

    fn manager(&self) -> Result<SharedManager, JsonRpcError> {
        self.manager
            .get()
            .ok_or_else(|| {
                JsonRpcError::internal("RemoteExecutorManager runtime is not initialized")
            })
            .cloned()
    }

    async fn local_executor_metadata(&self) -> Result<Value, JsonRpcError> {
        let response = manager_handle(
            self.manager()?.as_ref(),
            ExecutorRequest {
                id: json!("list-local-executor"),
                method: "list_executor".to_string(),
                executor: None,
                params: json!({}),
                directory: None,
                tool_timeout_ms: None,
            },
        )
        .await;
        if !response.ok {
            return Err(JsonRpcError::internal(
                response
                    .error
                    .unwrap_or_else(|| "list_executor failed".to_string()),
            ));
        }
        let Some(result) = response.result.as_ref() else {
            return Ok(json!({ "id": "local" }));
        };
        Ok(result
            .pointer("/metadata/executors")
            .and_then(Value::as_array)
            .and_then(|executors| {
                executors.iter().find_map(|executor| {
                    (executor.get("id").and_then(Value::as_str) == Some("local"))
                        .then(|| executor.clone())
                })
            })
            .unwrap_or_else(|| json!({ "id": "local" })))
    }

    async fn read_workspace_executor_config(
        &self,
        scope: &CallScope,
    ) -> Result<WorkspaceExecutorConfig, JsonRpcError> {
        let snapshot = self
            .host
            .read_remote_executor_config(&scope.workdir)
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?;
        Ok(workspace_executor_config_from_value(snapshot.config))
    }

    async fn write_workspace_executor_config(
        &self,
        scope: &CallScope,
        config: &WorkspaceExecutorConfig,
    ) -> Result<(), JsonRpcError> {
        self.host
            .update_remote_executor_config(
                &scope.workdir,
                workspace_executor_config_to_value(config),
            )
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?;
        Ok(())
    }

    async fn sync_workspace_executor(
        &self,
        scope: &CallScope,
        executor: &str,
    ) -> Result<(), JsonRpcError> {
        if executor == "local" {
            return Ok(());
        }
        let config = self.read_workspace_executor_config(scope).await?;
        let Some(item) = config.executors.iter().find(|item| item.id == executor) else {
            return Err(JsonRpcError::internal(format!(
                "executor not found in workspace config: {executor}"
            )));
        };
        self.manager()?
            .connect_to_executor(ConnectExecutorOptions {
                id: item.id.clone(),
                url: item.url.clone(),
                system: item.system.clone(),
                device: item.device.clone(),
                labels: item.labels.clone(),
            })
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?;
        Ok(())
    }

    async fn call_workspace_executor_manager(
        &self,
        scope: &CallScope,
        method: &str,
        mut arguments: Value,
    ) -> Result<McpCallResult, JsonRpcError> {
        remove_executor_session_id(&mut arguments);
        remove_field(&mut arguments, "executor");
        remove_field(&mut arguments, "method");

        let result = match method {
            "list_executor" => {
                let config = self.read_workspace_executor_config(scope).await?;
                let local = self.local_executor_metadata().await?;
                workspace_executor_result(&config, local)
            }
            "connect_to_executor" => {
                let mut config = self.read_workspace_executor_config(scope).await?;
                let item = workspace_executor_from_arguments(&arguments)?;
                upsert_workspace_executor(&mut config, item.clone());
                self.write_workspace_executor_config(scope, &config).await?;
                self.manager()?
                    .connect_to_executor(ConnectExecutorOptions {
                        id: item.id,
                        url: item.url,
                        system: item.system,
                        device: item.device,
                        labels: item.labels,
                    })
                    .await
                    .map_err(|err| JsonRpcError::internal(err.to_string()))?;
                let local = self.local_executor_metadata().await?;
                workspace_executor_result(&config, local)
            }
            _ => return Err(JsonRpcError::method_not_found(method)),
        };
        let output_text = extract_output_text(&result);
        Ok(McpCallResult {
            content: vec![McpContentText {
                kind: "text".to_string(),
                text: output_text,
            }],
            structured_content: Some(strip_output(result)),
        })
    }

    async fn resolve_file_args(
        &self,
        scope: &CallScope,
        mut arguments: Value,
        allow_hash_ref_fallback: bool,
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
            let resolved = self
                .host
                .resolve_hash_ref(&scope.session_id, &file_key)
                .await;
            let entry = match resolved {
                Ok(entry) => entry,
                Err(_) if allow_hash_ref_fallback => {
                    let mut args = arguments.as_object().cloned().unwrap_or_default();
                    let target = parse_hash_ref(&file_key)
                        .map(|item| item.filename)
                        .unwrap_or(file_key);
                    args.insert("filePath".to_string(), Value::String(target));
                    return Ok((Value::Object(args), None));
                }
                Err(err) => return Err(JsonRpcError::internal(err.to_string())),
            };
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
        let Ok(read_result) = self.call_executor_tool(scope, "read", read_args).await else {
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
        requested_description: Option<String>,
    ) -> ExbashHostSync {
        if !exbash_mode_creates_task(requested_mode) {
            return ExbashHostSync::default();
        }
        let async_id = result
            .pointer("/metadata/asyncID")
            .and_then(Value::as_str)
            .map(str::to_string);
        let Some(async_id) = async_id else {
            return ExbashHostSync::default();
        };
        let executor = result
            .pointer("/metadata/executor")
            .and_then(Value::as_str)
            .unwrap_or("local")
            .to_string();
        let command = result
            .pointer("/metadata/command")
            .and_then(Value::as_str)
            .map(str::to_string);
        let description = requested_description
            .filter(|description| !description.trim().is_empty())
            .or_else(|| {
                result
                    .pointer("/metadata/description")
                    .and_then(Value::as_str)
                    .filter(|description| !description.trim().is_empty())
                    .map(str::to_string)
            })
            .or_else(|| Some(TMP_RUNNING_DESCRIPTION.to_string()));
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
            return ExbashHostSync::default();
        }
        let mut sync = ExbashHostSync::default();
        let snapshot = match self
            .upsert_exbash_tracking(scope, tracking_scope, input)
            .await
        {
            Ok(snapshot) => Some(snapshot),
            Err(message) => {
                sync.warning = Some(json!({
                    "code": "hostTrackingWriteFailed",
                    "message": message,
                }));
                None
            }
        };
        if let Some(snapshot) = snapshot.as_ref() {
            self.spawn_local_exbash_terminal_sync(scope, tracking_scope, snapshot);
        }
        sync.snapshot = snapshot;
        sync
    }

    async fn upsert_exbash_tracking(
        &self,
        scope: &CallScope,
        tracking_scope: &str,
        input: ExbashSyncInput,
    ) -> Result<ExbashTaskSnapshot, String> {
        if tracking_scope == "workspace" {
            let wd_input = ExbashSyncInput {
                session_id: Some(scope.session_id.clone()),
                ..input
            };
            return self
                .host
                .upsert_workdir_exbash(&scope.session_id, &scope.workdir, wd_input)
                .await
                .map_err(|err| err.to_string());
        }
        self.host
            .upsert_session_exbash(&scope.session_id, input)
            .await
            .map_err(|err| err.to_string())
    }

    async fn prune_temporary_stopped_exbash(
        &self,
        scope: &CallScope,
        tracking_scope: &str,
        reason: &str,
    ) -> Result<Option<Value>, JsonRpcError> {
        let tasks = if tracking_scope == "workspace" {
            self.host
                .list_workdir_exbash(&scope.session_id, &scope.workdir, None)
                .await
                .map_err(|err| JsonRpcError::internal(err.to_string()))?
        } else {
            self.host
                .list_session_exbash(&scope.session_id, None)
                .await
                .map_err(|err| JsonRpcError::internal(err.to_string()))?
        };
        let candidates = tasks
            .into_iter()
            .filter(exbash_cleanup_candidate)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(None);
        }
        let mut removed = Vec::new();
        for task in candidates {
            let did_remove = if tracking_scope == "workspace" {
                self.host
                    .remove_workdir_exbash(
                        &scope.session_id,
                        &scope.workdir,
                        &task.async_id,
                        &task.executor,
                    )
                    .await
                    .map_err(|err| JsonRpcError::internal(err.to_string()))?
            } else {
                self.host
                    .remove_session_exbash(&scope.session_id, &task.async_id, &task.executor)
                    .await
                    .map_err(|err| JsonRpcError::internal(err.to_string()))?
            };
            if did_remove {
                removed.push(json!({
                    "asyncID": task.async_id,
                    "executor": task.executor,
                    "state": task.state,
                    "startedAt": task.started_at,
                    "endedAt": task.ended_at,
                    "totalOutput": task.total_output,
                }));
            }
        }
        if removed.is_empty() {
            return Ok(None);
        }
        Ok(Some(json!({
            "scope": tracking_scope,
            "reason": "exbashStorageWriteRejected",
            "message": reason,
            "removedCount": removed.len(),
            "removed": removed,
        })))
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

    async fn call_executor_tool(
        &self,
        scope: &CallScope,
        name: &str,
        mut arguments: Value,
    ) -> Result<serde_json::Value, JsonRpcError> {
        // Extract executor from arguments, default to "local"
        let executor = extract_executor(&mut arguments);
        let routed_executor = executor.clone();
        // Remove ExecutorSessionID from arguments
        remove_executor_session_id(&mut arguments);
        // Remove executor from arguments (it goes into ExecutorRequest)
        remove_field(&mut arguments, "executor");

        self.sync_workspace_executor(scope, &executor).await?;
        let manager = self.manager()?;
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
        let mut result = response.result.unwrap_or(Value::Null);
        if let Some(object) = result.as_object_mut() {
            let metadata = object.entry("metadata").or_insert_with(|| json!({}));
            if let Some(metadata) = metadata.as_object_mut() {
                metadata
                    .entry("executor".to_string())
                    .or_insert(Value::String(routed_executor));
            }
        }
        Ok(result)
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
        let mut local_tasks = self
            .host
            .list_session_exbash(&scope.session_id, executor_filter.as_deref())
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?;
        let mut workspace_tasks = self
            .host
            .list_workdir_exbash(
                &scope.session_id,
                &scope.workdir,
                executor_filter.as_deref(),
            )
            .await
            .map_err(|err| JsonRpcError::internal(err.to_string()))?;
        sort_exbash_tasks(&mut local_tasks);
        sort_exbash_tasks(&mut workspace_tasks);

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
                .call_executor_tool(
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
                .call_executor_tool(
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

    async fn call_skill(
        &self,
        scope: &CallScope,
        arguments: Value,
    ) -> Result<McpCallResult, JsonRpcError> {
        let mode = optional_string_field(&arguments, "mode").unwrap_or_else(|| "list".to_string());
        let target = skill_target(&arguments)?;
        let name = optional_string_field(&arguments, "name");
        if mode == "read" && name.is_none() {
            return Err(JsonRpcError::invalid_params(
                "skill mode=read requires name regex",
            ));
        }
        let pattern = Regex::new(name.as_deref().unwrap_or(".*")).map_err(|err| {
            JsonRpcError::invalid_params(format!("invalid skill name regex: {err}"))
        })?;
        let mut skills = self.skill_infos(scope, &target).await?;
        skills.retain(|skill| pattern.is_match(&skill.name));
        skills.sort_by(|a, b| {
            a.name
                .cmp(&b.name)
                .then_with(|| a.location.cmp(&b.location))
        });

        let output = if mode == "list" {
            format_skill_list(&skills)
        } else if mode == "read" {
            let [skill] = skills.as_slice() else {
                if skills.is_empty() {
                    return Err(JsonRpcError::invalid_params("skill not found"));
                }
                return Err(JsonRpcError::invalid_params(format!(
                    "skill name regex matched multiple skills: {}",
                    skills
                        .iter()
                        .map(|skill| skill.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            };
            let files = self.skill_summary_files(scope, &target, skill).await?;
            format_skill_read(skill, &files)
        } else {
            return Err(JsonRpcError::invalid_params(format!(
                "invalid skill mode: {mode}; expected list or read"
            )));
        };

        Ok(McpCallResult {
            content: vec![McpContentText {
                kind: "text".to_string(),
                text: output,
            }],
            structured_content: None,
        })
    }

    async fn skill_infos(
        &self,
        scope: &CallScope,
        target: &SkillTarget,
    ) -> Result<Vec<SkillInfo>, JsonRpcError> {
        let files = if skill_path_is_file(&target.path) {
            vec![target.path.clone()]
        } else {
            let result = self
                .call_executor_tool(
                    scope,
                    "glob",
                    json!({
                        "executor": target.executor,
                        "path": target.path,
                        "pattern": "**/SKILL.md",
                        "timeout": 30_000,
                    }),
                )
                .await?;
            skill_files_from_glob(&extract_output_text(&result))
        };
        let mut result = Vec::new();
        for file in files {
            if let Some(skill) = self.skill_info(scope, target, &file).await? {
                result.push(skill);
            }
        }
        Ok(result)
    }

    async fn skill_info(
        &self,
        scope: &CallScope,
        target: &SkillTarget,
        file: &str,
    ) -> Result<Option<SkillInfo>, JsonRpcError> {
        let result = self
            .call_executor_tool(
                scope,
                "read",
                json!({
                    "executor": target.executor,
                    "filePath": file,
                    "mode": "text",
                    "limit": 20_000,
                }),
            )
            .await?;
        let location = result
            .pointer("/metadata/file/canonicalPath")
            .and_then(Value::as_str)
            .unwrap_or(file);
        Ok(parse_skill_info(
            &plain_read_text(
                result
                    .pointer("/output/text")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
            ),
            location,
        ))
    }

    async fn skill_summary_files(
        &self,
        scope: &CallScope,
        target: &SkillTarget,
        skill: &SkillInfo,
    ) -> Result<Vec<String>, JsonRpcError> {
        let dir = skill_dir(&skill.location);
        if dir.is_empty() {
            return Ok(Vec::new());
        }
        let result = self
            .call_executor_tool(
                scope,
                "glob",
                json!({
                    "executor": target.executor,
                    "path": dir,
                    "pattern": "**/*",
                    "timeout": 10_000,
                }),
            )
            .await?;
        Ok(skill_files_from_glob(&extract_output_text(&result))
            .into_iter()
            .filter(|file| !skill_path_is_file(file))
            .take(10)
            .collect())
    }

    async fn check_exbash_create_allowed(
        &self,
        scope: &CallScope,
        tracking_scope: &str,
        arguments: &Value,
    ) -> Result<Option<Value>, JsonRpcError> {
        let mode = optional_string_field(arguments, "mode").unwrap_or_else(|| "shell".to_string());
        if !exbash_mode_creates_task(&mode) {
            return Ok(None);
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
        self.check_exbash_create_with_cleanup(scope, tracking_scope, input)
            .await
    }

    async fn check_exbash_create(
        &self,
        scope: &CallScope,
        tracking_scope: &str,
        input: &ExbashSyncInput,
    ) -> Result<(), String> {
        if tracking_scope == "workspace" {
            let input = ExbashSyncInput {
                session_id: Some(scope.session_id.clone()),
                workdir: Some(scope.workdir.clone()),
                ..input.clone()
            };
            return self
                .host
                .check_workdir_exbash_create(&scope.session_id, &scope.workdir, &input)
                .await
                .map_err(|err| err.to_string());
        }
        self.host
            .check_session_exbash_create(&scope.session_id, input)
            .await
            .map_err(|err| err.to_string())
    }

    async fn check_exbash_create_with_cleanup(
        &self,
        scope: &CallScope,
        tracking_scope: &str,
        input: ExbashSyncInput,
    ) -> Result<Option<Value>, JsonRpcError> {
        match self
            .check_exbash_create(scope, tracking_scope, &input)
            .await
        {
            Ok(()) => Ok(None),
            Err(message) if exbash_error_indicates_storage_rejection(&message) => {
                let cleanup = self
                    .prune_temporary_stopped_exbash(scope, tracking_scope, &message)
                    .await?;
                let Some(cleanup) = cleanup else {
                    return Err(JsonRpcError::invalid_params(message));
                };
                self.check_exbash_create(scope, tracking_scope, &input)
                    .await
                    .map_err(JsonRpcError::invalid_params)?;
                Ok(Some(cleanup))
            }
            Err(message) => Err(JsonRpcError::invalid_params(message)),
        }
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

fn workspace_executor_config_from_value(value: Value) -> WorkspaceExecutorConfig {
    let entries = match value {
        Value::Array(entries) => entries,
        Value::Object(mut object) => object
            .remove("executors")
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    let mut seen = HashSet::new();
    let executors = entries
        .into_iter()
        .filter_map(|entry| serde_json::from_value::<WorkspaceExecutorInfo>(entry).ok())
        .filter(|entry| {
            !entry.id.trim().is_empty()
                && entry.id != "local"
                && !entry.url.trim().is_empty()
                && seen.insert(entry.id.clone())
        })
        .collect::<Vec<_>>();
    WorkspaceExecutorConfig { executors }
}

fn workspace_executor_config_to_value(config: &WorkspaceExecutorConfig) -> Value {
    json!({
        "executors": config.executors,
    })
}

fn workspace_executor_metadata(config: &WorkspaceExecutorConfig, local: Value) -> Value {
    let mut executors = vec![local];
    executors.extend(
        config
            .executors
            .iter()
            .map(|entry| serde_json::to_value(entry).unwrap_or_else(|_| json!({}))),
    );
    json!({
        "default": "local",
        "executors": executors,
    })
}

fn workspace_executor_result(config: &WorkspaceExecutorConfig, local: Value) -> Value {
    let metadata = workspace_executor_metadata(config, local);
    let text = executor_list_output_text(&json!({ "metadata": metadata.clone() }))
        .unwrap_or_else(|| "executors:\n- none".to_string());
    json!({
        "metadata": metadata,
        "output": {
            "message": "",
            "text": text,
            "info": "",
        }
    })
}

fn workspace_executor_from_arguments(
    arguments: &Value,
) -> Result<WorkspaceExecutorInfo, JsonRpcError> {
    let id = optional_string_field(arguments, "id")
        .ok_or_else(|| JsonRpcError::invalid_params("executor id is required"))?;
    if id == "local" {
        return Err(JsonRpcError::invalid_params("local executor is reserved"));
    }
    let url = optional_string_field(arguments, "url")
        .ok_or_else(|| JsonRpcError::invalid_params("executor url is required"))?;
    Ok(WorkspaceExecutorInfo {
        id,
        url: normalize_workspace_executor_url(&url),
        system: optional_string_field(arguments, "system"),
        device: optional_string_field(arguments, "device"),
        labels: workspace_executor_labels(arguments.get("labels")),
    })
}

fn workspace_executor_labels(value: Option<&Value>) -> BTreeMap<String, String> {
    value
        .and_then(Value::as_object)
        .map(|object| {
            object
                .iter()
                .filter_map(|(key, value)| {
                    value.as_str().map(|value| (key.clone(), value.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_workspace_executor_url(url: &str) -> String {
    if url.starts_with("ws://") || url.starts_with("wss://") {
        url.to_string()
    } else {
        format!("ws://{url}")
    }
}

fn upsert_workspace_executor(config: &mut WorkspaceExecutorConfig, entry: WorkspaceExecutorInfo) {
    if let Some(existing) = config
        .executors
        .iter_mut()
        .find(|existing| existing.id == entry.id)
    {
        *existing = entry;
        return;
    }
    config.executors.push(entry);
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

fn skill_target(arguments: &Value) -> Result<SkillTarget, JsonRpcError> {
    let path = optional_string_field(arguments, "path")
        .ok_or_else(|| JsonRpcError::invalid_params("skill path is required"))?;
    let Some((executor, rest)) = path.split_once(':') else {
        return Ok(SkillTarget {
            executor: "local".to_string(),
            path,
        });
    };
    let drive = executor.len() == 1 && executor.as_bytes()[0].is_ascii_alphabetic();
    if drive || rest.is_empty() {
        return Ok(SkillTarget {
            executor: "local".to_string(),
            path: format!("{executor}:{rest}"),
        });
    }
    Ok(SkillTarget {
        executor: executor.to_string(),
        path: rest.to_string(),
    })
}

fn skill_path_is_file(path: &str) -> bool {
    let normalized = path.replace('\\', "/");
    normalized == "SKILL.md" || normalized.ends_with("/SKILL.md")
}

fn skill_files_from_glob(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter(|line| *line != "No files found")
        .filter(|line| !line.starts_with("(Results are truncated"))
        .filter(|line| !line.starts_with("matches:"))
        .filter(|line| !line.starts_with("filesWalked:"))
        .filter(|line| !line.starts_with("code:"))
        .map(str::to_string)
        .collect()
}

fn plain_read_text(text: &str) -> String {
    text.lines()
        .map(|line| {
            line.split_once(": ")
                .and_then(|(prefix, rest)| {
                    prefix.chars().all(|ch| ch.is_ascii_digit()).then_some(rest)
                })
                .unwrap_or(line)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn strip_frontmatter_quotes(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 {
        let first = value.as_bytes()[0];
        let last = value.as_bytes()[value.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return value[1..value.len() - 1].trim().to_string();
        }
    }
    value.to_string()
}

fn parse_skill_info(text: &str, location: &str) -> Option<SkillInfo> {
    let text = text.trim_start();
    let rest = text.strip_prefix("---")?.trim_start_matches(['\r', '\n']);
    let (frontmatter, content) = rest.split_once("\n---")?;
    let mut name = None;
    let mut description = None;
    for line in frontmatter.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        match key.trim() {
            "name" => name = Some(strip_frontmatter_quotes(value)),
            "description" => description = Some(strip_frontmatter_quotes(value)),
            _ => {}
        }
    }
    let name = name?.trim().to_string();
    if name.is_empty() {
        return None;
    }
    Some(SkillInfo {
        name,
        description: description.unwrap_or_default(),
        location: location.to_string(),
        content: content
            .trim_start_matches(['-', '\r', '\n'])
            .trim()
            .to_string(),
    })
}

fn skill_dir(location: &str) -> String {
    let slash = location.rfind('/');
    let backslash = location.rfind('\\');
    let Some(idx) = slash.into_iter().chain(backslash).max() else {
        return ".".to_string();
    };
    location[..idx].to_string()
}

fn format_skill_list(skills: &[SkillInfo]) -> String {
    if skills.is_empty() {
        return "No skills found.".to_string();
    }
    skills
        .iter()
        .map(|skill| {
            format!(
                "name: {}\ndescription: {}\npath: {}",
                skill.name, skill.description, skill.location
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn format_skill_read(skill: &SkillInfo, files: &[String]) -> String {
    let base = skill_dir(&skill.location);
    let list = if files.is_empty() {
        "  <file>SKILL.md</file>".to_string()
    } else {
        files
            .iter()
            .map(|file| format!("  <file>{file}</file>"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    [
        format!(r#"<skill_content name="{}">"#, skill.name),
        format!("# Skill: {}", skill.name),
        String::new(),
        skill.content.trim().to_string(),
        String::new(),
        format!("Base directory for this skill: {base}"),
        "Relative paths in this skill (e.g., scripts/, reference/) are relative to this base directory.".to_string(),
        "Note: file list is sampled.".to_string(),
        "<skill_files>".to_string(),
        list,
        "</skill_files>".to_string(),
        "</skill_content>".to_string(),
    ]
    .join("\n")
}

fn require_patch_hash_ref(arguments: &Value) -> Result<(), JsonRpcError> {
    if optional_string_field(arguments, "mode").as_deref() != Some("patch") {
        return Ok(());
    }
    if optional_string_field(arguments, "patchMode").as_deref() == Some("binary") {
        return Ok(());
    }
    let file_key = optional_string_field(arguments, "fileKey").unwrap_or_default();
    if parse_hash_ref(&file_key).is_some() {
        return Ok(());
    }
    Err(JsonRpcError::invalid_params(
        "FileAction mode=patch requires fileKey to be the hashRef label from a prior read/FileAction result, such as `App.ts #A1B2`. Do not pass a direct file path for patch; call read first and pass the exact text inside <fileRef>...</fileRef>.",
    ))
}

fn validate_file_action_patch_text(arguments: &Value) -> Result<(), JsonRpcError> {
    if optional_string_field(arguments, "mode").as_deref() != Some("patch") {
        return Ok(());
    }
    if optional_string_field(arguments, "patchMode").as_deref() == Some("binary") {
        return Ok(());
    }
    let Some(text) = arguments.get("patchText").and_then(Value::as_str) else {
        return Ok(());
    };
    if text.trim().is_empty() {
        return Ok(());
    }
    Ok(())
}

fn enable_hash_check_mode(arguments: &mut Value) {
    if let Some(object) = arguments.as_object_mut() {
        object
            .entry("hashCheckMode".to_string())
            .or_insert(Value::Bool(true));
    }
}

fn disable_hash_check_mode(arguments: &mut Value) {
    if let Some(object) = arguments.as_object_mut() {
        object.remove("hashCheckMode");
        object.remove("hashCode");
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

fn exbash_error_indicates_storage_rejection(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("task stack is full")
        || message.contains("stack is full")
        || message.contains("quota")
        || message.contains("capacity")
        || message.contains("limit")
        || message.contains("storage")
        || message.contains("write")
        || message.contains("rejected")
        || message.contains("refused")
}

fn exbash_remove_warning_code(message: &str) -> &'static str {
    let message = message.to_ascii_lowercase();
    if message.contains("closed before responding")
        || message.contains("connection")
        || message.contains("disconnected")
        || message.contains("timed out")
    {
        return "notReplayable";
    }
    "asyncTaskNotFound"
}

fn exbash_cleanup_candidate(task: &ExbashTaskSnapshot) -> bool {
    exbash_task_is_temporary(task) && exbash_task_is_stopped(task)
}

fn exbash_task_is_temporary(task: &ExbashTaskSnapshot) -> bool {
    let description = task.description.as_deref().map(str::trim).unwrap_or("");
    description.is_empty() || description == TMP_RUNNING_DESCRIPTION
}

fn sort_exbash_tasks(tasks: &mut [ExbashTaskSnapshot]) {
    tasks.sort_by(|a, b| {
        exbash_task_is_temporary(a)
            .cmp(&exbash_task_is_temporary(b))
            .then_with(|| {
                a.started_at
                    .unwrap_or_default()
                    .cmp(&b.started_at.unwrap_or_default())
            })
    });
}

fn exbash_task_is_stopped(task: &ExbashTaskSnapshot) -> bool {
    if let Some(state) = task.state.as_deref().map(str::trim) {
        if state == "running" || state == "unknown" || state.is_empty() {
            return false;
        }
        if matches!(state, "stop" | "stopped" | "timeout") || state.starts_with("exit:") {
            return true;
        }
    }
    task.ended_at.is_some() || task.exit_code.is_some()
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
        command: existing
            .command
            .clone()
            .or_else(|| metadata_string(result, "command")),
        description: existing.description.clone(),
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

fn prepare_rg_arguments(scope: &CallScope, mode: RgMode, arguments: &mut Value) {
    if mode != RgMode::Content {
        return;
    }
    let Some(object) = arguments.as_object_mut() else {
        return;
    };
    let executor = object
        .get("executor")
        .or_else(|| object.get("targetExecutor"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("local");
    if executor == "local" {
        object
            .entry("root".to_string())
            .or_insert_with(|| Value::String(scope.workdir.clone()));
    }
    expand_basename_globs(arguments);
}

fn extract_rg_paths(arguments: &mut Value) -> RgPathList {
    let Some(object) = arguments.as_object_mut() else {
        return RgPathList::Single;
    };
    if let Some(paths) = object.remove("paths").and_then(|value| match value {
        Value::Array(values) => Some(
            values
                .into_iter()
                .filter_map(|value| value.as_str().map(str::trim).map(str::to_string))
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>(),
        ),
        Value::String(value) => Some(split_rg_path_list(&value, true)),
        _ => None,
    }) {
        if paths.len() > 1 {
            object.remove("path");
            return RgPathList::Multi(paths);
        }
        if let Some(path) = paths.into_iter().next() {
            object.insert("path".to_string(), Value::String(path));
        }
        return RgPathList::Single;
    }

    let Some(path) = object.get("path").and_then(Value::as_str) else {
        return RgPathList::Single;
    };
    let paths = split_rg_path_list(path, false);
    if paths.len() <= 1 {
        return RgPathList::Single;
    }
    object.remove("path");
    RgPathList::Multi(paths)
}

fn split_rg_path_list(path: &str, explicit_multi: bool) -> Vec<String> {
    let values = path
        .split_whitespace()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if values.len() <= 1
        || (!explicit_multi && !values.iter().all(|value| looks_like_absolute_path(value)))
    {
        return vec![path.trim().to_string()]
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect();
    }
    values
}

fn looks_like_absolute_path(value: &str) -> bool {
    value.starts_with('/')
        || value.starts_with('\\')
        || value.as_bytes().get(1).is_some_and(|byte| *byte == b':')
}

async fn call_rg_paths<H: SessionHost + 'static>(
    handler: &SessionMcpHandler<H>,
    scope: &CallScope,
    method: &str,
    mut args: Value,
    paths: RgPathList,
) -> Result<Value, JsonRpcError> {
    let RgPathList::Multi(paths) = paths else {
        return handler.call_executor_tool(scope, method, args).await;
    };
    let mut results = Vec::new();
    let mut remaining = args
        .get("max_count")
        .and_then(Value::as_u64)
        .map(|value| value as usize);
    for path in paths {
        if remaining == Some(0) {
            break;
        }
        if let Some(object) = args.as_object_mut() {
            object.insert("path".to_string(), Value::String(path));
            if let Some(remaining) = remaining {
                object.insert("max_count".to_string(), json!(remaining));
            }
        }
        let result = handler
            .call_executor_tool(scope, method, args.clone())
            .await?;
        if let Some(value) = remaining.as_mut() {
            *value = value.saturating_sub(rg_result_match_count(&result));
        }
        results.push(result);
    }
    Ok(merge_rg_results(results))
}

fn rg_result_match_count(result: &Value) -> usize {
    result
        .pointer("/metadata/matches")
        .or_else(|| result.pointer("/metadata/count"))
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize
}

fn merge_rg_results(results: Vec<Value>) -> Value {
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut matches = 0u64;
    let mut files_walked = 0u64;
    let mut timed_out = false;
    let mut count = 0u64;
    for result in results {
        if let Some(text) = result.pointer("/output/text").and_then(Value::as_str) {
            stdout.push_str(text);
            if !stdout.is_empty() && !stdout.ends_with('\n') {
                stdout.push('\n');
            }
        }
        if let Some(text) = result.pointer("/metadata/stderr").and_then(Value::as_str) {
            stderr.push_str(text);
        }
        matches += result
            .pointer("/metadata/matches")
            .or_else(|| result.pointer("/metadata/count"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        files_walked += result
            .pointer("/metadata/filesWalked")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        timed_out |= result
            .pointer("/metadata/timedOut")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        count += result
            .pointer("/metadata/count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
    }
    json!({
        "output": { "text": stdout },
        "metadata": {
            "stdout": stdout,
            "stderr": stderr,
            "matches": matches,
            "filesWalked": files_walked,
            "timedOut": timed_out,
            "code": if matches > 0 { 0 } else { 1 },
            "count": count,
        }
    })
}

fn expand_basename_globs(arguments: &mut Value) {
    let Some(globs) = arguments
        .as_object_mut()
        .and_then(|object| object.get_mut("globs"))
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let mut additions = Vec::new();
    for glob in globs.iter().filter_map(Value::as_str) {
        if !needs_recursive_basename_glob(glob) {
            continue;
        }
        let expanded = format!("**/{glob}");
        if globs
            .iter()
            .any(|existing| existing.as_str() == Some(&expanded))
            || additions.iter().any(|existing| existing == &expanded)
        {
            continue;
        }
        additions.push(expanded);
    }
    globs.extend(additions.into_iter().map(Value::String));
}

fn needs_recursive_basename_glob(glob: &str) -> bool {
    let glob = glob.trim();
    !glob.is_empty() && !glob.starts_with("**/") && !glob.contains('/') && !glob.contains('\\')
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
    let description = task.description.as_deref().unwrap_or("");
    format!(
        "- {}:{} {} totalOutput={} description={} command={}",
        task.executor,
        task.async_id,
        state,
        total_output,
        clipped_exbash_list_text(description),
        clipped_exbash_list_command(command)
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
    let description = run.get("description").and_then(Value::as_str).unwrap_or("");
    let description = clipped_exbash_list_text(description);
    let command = clipped_exbash_list_command(command);
    format!(
        "- {async_id} {state} totalOutput={total_output} description={description} command={command}"
    )
}

fn clipped_exbash_list_command(command: &str) -> String {
    clipped_exbash_list_text(command)
}

fn clipped_exbash_list_text(text: &str) -> String {
    single_line_exbash_list_text(text)
        .chars()
        .take(30)
        .collect()
}

fn single_line_exbash_list_text(text: &str) -> String {
    text.replace("\r\n", "\\n").replace(['\n', '\r'], "\\n")
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
        let mut footer = exbash_execution_footer_lines(result);
        footer.push(format!("totaloutput:{total_output_bytes}bytes"));
        footer.push(format!("exitcode:{}", json_value_text(exit_code)));
        append_footer_lines(&mut text, footer);
        return text;
    }

    let detached = result
        .pointer("/metadata/detached")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || result.pointer("/metadata/asyncID").is_some();
    if detached {
        append_footer_lines(&mut text, exbash_execution_footer_lines(result));
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

fn append_footer_lines(text: &mut String, lines: Vec<String>) {
    if lines.is_empty() {
        return;
    }
    ensure_blank_line(text);
    text.push_str(&lines.join("\n"));
}

fn exbash_execution_footer_lines(result: &Value) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(command) = result.pointer("/metadata/command").and_then(Value::as_str) {
        lines.push(format!("command:{command}"));
    }
    if let Some(cwd) = result.pointer("/metadata/cwd").and_then(Value::as_str) {
        lines.push(format!("cwd:{cwd}"));
    }
    lines
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
                .map(|value| value.len())
        })
        .unwrap_or(visible_text.len())
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
        arguments: Value,
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
                    validate_file_action_patch_text(&arguments)?;
                }
                // Resolve hashRef if present
                let (mut args, file_key_ref) = self
                    .resolve_file_args(&scope, arguments, name == "read")
                    .await?;
                if name == "read" {
                    enable_hash_check_mode(&mut args);
                }
                if name == "FileAction"
                    && file_action_mode.as_deref() == Some("patch")
                    && optional_string_field(&args, "patchMode").as_deref() == Some("binary")
                {
                    disable_hash_check_mode(&mut args);
                }
                let executor = extract_executor_from_value(&args);
                // Dispatch through the executor manager.
                let result = self.call_executor_tool(&scope, name, args).await?;
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
                let structured_content = Some(strip_output(value));
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
                let requested_description = optional_string_field(&call_args, "description");
                let pre_create_cleanup = self
                    .check_exbash_create_allowed(&scope, &tracking_scope, &call_args)
                    .await?;
                remove_field(&mut call_args, "scope");
                remove_field(&mut call_args, "spoe");
                let result = match self.call_executor_tool(&scope, "exbash", call_args).await {
                    Ok(result) => result,
                    Err(error) => {
                        if mode == "remove"
                            && exbash_error_indicates_missing_or_lost(&error.message)
                        {
                            if let Some(async_id) = requested_async_id.as_deref() {
                                let cleanup = self
                                    .remove_exbash_tracking(&scope, async_id, &requested_executor)
                                    .await?;
                                let code = exbash_remove_warning_code(&error.message);
                                let output_text = format!(
                                    "warning: {code}\nasyncID:{async_id}\nexecutor:{requested_executor}\nmessage:{}\nhostCleanup:{cleanup}",
                                    error.message
                                );
                                return Ok(McpCallResult {
                                    content: vec![McpContentText {
                                        kind: "text".to_string(),
                                        text: output_text,
                                    }],
                                    structured_content: Some(json!({
                                        "metadata": {
                                            "asyncID": async_id,
                                            "executor": requested_executor,
                                            "warning": {
                                                "code": code,
                                                "message": error.message,
                                            },
                                            "hostCleanup": cleanup,
                                        }
                                    })),
                                });
                            }
                        }
                        return Err(error);
                    }
                };
                let mut value = result;
                if let Some(cleanup) = pre_create_cleanup {
                    set_metadata_field(&mut value, "hostCleanup", cleanup);
                }
                let host_sync = self
                    .upsert_exbash_from_result(
                        &scope,
                        &value,
                        &tracking_scope,
                        &mode,
                        requested_description,
                    )
                    .await;
                if let Some(warning) = host_sync.warning {
                    set_metadata_field(&mut value, "hostWarning", warning);
                }
                if let Some(snapshot) = host_sync.snapshot {
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
            "skill" => self.call_skill(&scope, arguments).await,
            "rg" => {
                let mode = extract_rg_mode(&arguments)?;
                let method = match mode {
                    RgMode::Content => "rg",
                    RgMode::Files => "glob",
                };
                let mut call_args = arguments;
                prepare_rg_arguments(&scope, mode, &mut call_args);
                let paths = extract_rg_paths(&mut call_args);
                remove_field(&mut call_args, "mode");
                remove_field(&mut call_args, "type");
                let result = call_rg_paths(self, &scope, method, call_args, paths).await?;
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
                if matches!(method.as_str(), "list_executor" | "connect_to_executor") {
                    return self
                        .call_workspace_executor_manager(&scope, &method, arguments)
                        .await;
                }
                if method == "list_shells"
                    || method == "set_executor_shell"
                    || method == "request_reload"
                {
                    let mut call_args = arguments;
                    remove_field(&mut call_args, "method");
                    let tool_method = match method.as_str() {
                        "list_shells" => "list_shells",
                        "request_reload" => "request_reload",
                        _ => {
                            if let Some(object) = call_args.as_object_mut() {
                                let missing_shell = object
                                    .get("shell")
                                    .and_then(Value::as_str)
                                    .map(str::trim)
                                    .filter(|value| !value.is_empty())
                                    .is_none();
                                if missing_shell {
                                    object
                                        .insert("shell".to_string(), Value::String("auto".into()));
                                }
                            }
                            "set_default_shell"
                        }
                    };
                    let result = self
                        .call_executor_tool(&scope, tool_method, call_args)
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
                Err(JsonRpcError::method_not_found(&method))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exbash_task_list_line_clips_description_and_command() {
        let line = format_exbash_task(&ExbashTaskSnapshot {
            async_id: "rex-test".to_string(),
            executor: "local".to_string(),
            session_id: Some("session".to_string()),
            workdir: None,
            state: Some("running".to_string()),
            pid: None,
            exit_code: None,
            started_at: None,
            ended_at: None,
            command: Some("012345678901234567890123456789EXTRA\nnext".to_string()),
            description: Some("abcdefghijklmnopqrstuvwxyz123456EXTRA\nnext".to_string()),
            total_output: Some(12),
        });

        assert!(line.contains(
            "totalOutput=12 description=abcdefghijklmnopqrstuvwxyz1234 command=012345678901234567890123456789"
        ));
        assert!(!line.contains("EXTRA"));
        assert!(!line.contains("description=abcdefghijklmnopqrstuvwxyz123456"));
    }

    #[test]
    fn remote_exbash_list_line_clips_description_and_command() {
        let line = format_remote_exbash_run(&serde_json::json!({
            "asyncID": "rex-remote",
            "state": "running",
            "totalOutput": 8,
            "description": "012345678901234567890123456789EXTRA\nnext",
            "command": "abcdefghijklmnopqrstuvwxyz123456EXTRA"
        }));

        assert_eq!(
            line,
            "- rex-remote running totalOutput=8 description=012345678901234567890123456789 command=abcdefghijklmnopqrstuvwxyz1234"
        );
    }

    #[test]
    fn exbash_task_sort_places_tmp_running_last() {
        let mut tasks = vec![
            ExbashTaskSnapshot {
                async_id: "tmp".to_string(),
                executor: "local".to_string(),
                session_id: Some("session".to_string()),
                workdir: None,
                state: Some("running".to_string()),
                pid: None,
                exit_code: None,
                started_at: Some(3),
                ended_at: None,
                command: Some("sleep 1".to_string()),
                description: Some("Tmp Running".to_string()),
                total_output: None,
            },
            ExbashTaskSnapshot {
                async_id: "described".to_string(),
                executor: "local".to_string(),
                session_id: Some("session".to_string()),
                workdir: None,
                state: Some("stop".to_string()),
                pid: None,
                exit_code: None,
                started_at: Some(1),
                ended_at: Some(2),
                command: Some("true".to_string()),
                description: Some("described".to_string()),
                total_output: None,
            },
        ];

        sort_exbash_tasks(&mut tasks);
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.async_id.as_str())
                .collect::<Vec<_>>(),
            vec!["described", "tmp"]
        );
    }

    #[test]
    fn rg_content_basename_globs_are_recursive() {
        let mut args = json!({ "globs": ["small.txt", "*.rs", "src/*.ts", "**/*.md"] });
        expand_basename_globs(&mut args);
        assert_eq!(
            args["globs"],
            json!([
                "small.txt",
                "*.rs",
                "src/*.ts",
                "**/*.md",
                "**/small.txt",
                "**/*.rs"
            ])
        );
    }

    #[test]
    fn rg_extracts_paths_array() {
        let mut args = json!({ "paths": ["one", "two"], "path": "ignored" });
        assert_eq!(
            extract_rg_paths(&mut args),
            RgPathList::Multi(vec!["one".to_string(), "two".to_string()])
        );
        assert!(args.get("path").is_none());
    }

    #[test]
    fn rg_splits_whitespace_separated_absolute_path() {
        let mut args = json!({ "path": "/one /two" });
        assert_eq!(
            extract_rg_paths(&mut args),
            RgPathList::Multi(vec!["/one".to_string(), "/two".to_string()])
        );
    }

    #[test]
    fn rg_keeps_single_path_with_spaces() {
        let mut args = json!({ "path": "dir with spaces" });
        assert_eq!(extract_rg_paths(&mut args), RgPathList::Single);
        assert_eq!(args["path"], "dir with spaces");
    }

    #[test]
    fn local_rg_content_defaults_to_session_workdir_root() {
        let scope = CallScope {
            session_id: "ses".to_string(),
            workdir: "/workspace/current".to_string(),
        };
        let mut args = json!({ "pattern": "needle" });
        prepare_rg_arguments(&scope, RgMode::Content, &mut args);
        assert_eq!(args["root"], "/workspace/current");
    }

    #[test]
    fn remote_rg_content_does_not_inject_local_workdir_root() {
        let scope = CallScope {
            session_id: "ses".to_string(),
            workdir: "/workspace/current".to_string(),
        };
        let mut args = json!({ "pattern": "needle", "executor": "exec_1" });
        prepare_rg_arguments(&scope, RgMode::Content, &mut args);
        assert!(args.get("root").is_none());
    }
}
