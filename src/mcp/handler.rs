use crate::host::{ExbashSyncInput, SessionHost};
use crate::mcp::{EmbeddedMcp, McpCallContext, McpCallResult, McpContentText, McpToolDef, McpToolHandler, EXECUTOR_SESSION_PARAM};
use crate::jsonrpc::JsonRpcError;
use crate::rec::{manager_handle, new_manager, Caller, ExecutorRequest, ShellManager, ToolContext};
use crate::refs::{extract_file_ref_update, inject_file_ref, parse_hash_ref};
use crate::types::ExbashTaskSnapshot;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::OnceCell;

type SharedManager = Arc<Caller>;

pub async fn create_session_mcp<H: SessionHost + 'static>(ctx: ToolContext, host: Arc<H>, shell_manager: ShellManager) -> Result<EmbeddedMcp<SessionMcpHandler<H>>, anyhow::Error> {
    let manager = OnceCell::<SharedManager>::new();
    manager.get_or_try_init(|| async { new_manager().await.map(Arc::new) }).await?;
    Ok(EmbeddedMcp::new(SessionMcpHandler { ctx: ctx.with_shell_manager(shell_manager), host, manager }))
}

pub fn create_session_mcp_with_manager<H: SessionHost + 'static>(ctx: ToolContext, host: Arc<H>, manager: SharedManager, shell_manager: ShellManager) -> EmbeddedMcp<SessionMcpHandler<H>> {
    let cell = OnceCell::new();
    cell.set(manager).ok();
    EmbeddedMcp::new(SessionMcpHandler { ctx: ctx.with_shell_manager(shell_manager), host, manager: cell })
}

pub async fn create_default_session_mcp(ctx: ToolContext, shell_manager: ShellManager) -> Result<EmbeddedMcp<SessionMcpHandler<DummySessionHost>>, anyhow::Error> {
    create_session_mcp(ctx, Arc::new(DummySessionHost), shell_manager).await
}

pub struct SessionMcpHandler<H: SessionHost> {
    ctx: ToolContext,
    host: Arc<H>,
    manager: OnceCell<SharedManager>,
}

impl<H: SessionHost> SessionMcpHandler<H> {
    async fn resolve_file_args(&self, mut arguments: Value) -> Result<(Value, Option<String>), JsonRpcError> {
        let Some(object) = arguments.as_object_mut() else {
            return Ok((arguments, None));
        };
        let Some(file_key) = object.remove("fileKey").and_then(|v| v.as_str().map(str::to_string)) else {
            return Ok((arguments, None));
        };
        if self.host.is_hash_ref(&file_key) {
            let entry = self.host.resolve_hash_ref(&file_key).await.map_err(|err| JsonRpcError::internal(err.to_string()))?;
            let injection = inject_file_ref(&arguments, Some(&entry));
            return Ok((injection.args, Some(entry.file_key_ref)));
        }
        // Not a hashRef, convert fileKey -> filePath for REC
        let mut args = arguments.as_object().cloned().unwrap_or_default();
        args.insert("filePath".to_string(), Value::String(file_key));
        Ok((Value::Object(args), None))
    }

    async fn store_from_result(&self, executor: &str, result: &serde_json::Value, _file_key_ref: Option<&str>) -> Option<String> {
        let rec = serde_json::from_value::<crate::types::RecToolResult>(result.clone()).ok()?;
        let update = extract_file_ref_update(&rec, executor)?;
        if let Ok(entry) = self.host.store_hash_ref(update).await {
            return Some(crate::refs::label_hash_ref(&crate::refs::basename(&entry.file_path), &crate::refs::small_hash_code(&entry.file_key_ref, &entry.hash_code)));
        }
        None
    }

    async fn read_back_file_action_result(
        &self,
        mut result: Value,
        executor: &str,
    ) -> Value {
        if result.pointer("/metadata/file/type").and_then(Value::as_str) == Some("delete") {
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
        let read_args = json!({ "filePath": file_path, "executor": executor, "hashCheckMode": true });
        let Ok(read_result) = self.call_via_manager("read", read_args).await else {
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

    fn current_scope(&self) -> Option<String> {
        let dir = self.ctx.directory.to_string_lossy().to_string();
        if dir.is_empty() { None } else { Some(dir) }
    }

    async fn upsert_exbash_from_result(&self, result: &serde_json::Value) -> Option<ExbashTaskSnapshot> {
        let mode = result.pointer("/metadata/mode").and_then(Value::as_str).unwrap_or("unknown");
        let async_id = result.pointer("/metadata/asyncID").and_then(Value::as_str).map(str::to_string)?;
        let executor = result.pointer("/metadata/executor").and_then(Value::as_str).unwrap_or("local").to_string();
        let command = result.pointer("/metadata/command").and_then(Value::as_str).map(str::to_string);
        let description = result.pointer("/metadata/description").and_then(Value::as_str).map(str::to_string);
        let state = result.pointer("/metadata/state").and_then(Value::as_str).map(str::to_string);
        let exit_code = result.pointer("/metadata/exitCode").and_then(Value::as_i64).map(|n| n as i32);
        let pid = result.pointer("/metadata/pid").and_then(Value::as_i64);
        let started_at = result.pointer("/metadata/startedAt").and_then(Value::as_i64);
        let ended_at = result.pointer("/metadata/endedAt").and_then(Value::as_i64);
        let total_output = result.pointer("/metadata/totalOutput").and_then(Value::as_i64);
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
        let session = self.host.upsert_session_exbash(input.clone()).await.ok()?;
        if mode == "list" || mode == "attach" {
            return Some(session);
        }
        if let Some(workdir) = self.current_scope() {
            let wd_input = ExbashSyncInput { session_id: Some(self.host.session_id().to_string()), ..input };
            return self.host.upsert_workdir_exbash(&workdir, wd_input).await.ok();
        }
        Some(session)
    }

    async fn call_via_manager(&self, name: &str, mut arguments: Value) -> Result<serde_json::Value, JsonRpcError> {
        // Extract executor from arguments, default to "local"
        let executor = extract_executor(&mut arguments);
        // Remove ExecutorSessionID from arguments
        remove_executor_session_id(&mut arguments);
        // Remove executor from arguments (it goes into ExecutorRequest)
        remove_field(&mut arguments, "executor");

        let manager = self.manager.get().ok_or_else(|| JsonRpcError::internal("RemoteExecutorManager is not initialized"))?.clone();
        let directory = if executor == "local" {
            Some(self.ctx.directory.clone())
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
            return Err(JsonRpcError::internal(response.error.unwrap_or_else(|| "unknown error".into())));
        }
        Ok(response.result.unwrap_or(Value::Null))
    }

    fn list_shells(&self) -> Result<Value, JsonRpcError> {
        let settings = self.ctx.settings_store().ok_or_else(|| JsonRpcError::internal("settings store not available"))?;
        let settings = settings.settings().map_err(|e| JsonRpcError::internal(e.to_string()))?;
        let profiles: Vec<Value> = settings.shells.profiles.iter().map(|(name, profile)| {
            json!({
                "name": name,
                "candidates": profile.candidates,
                "commandArgs": profile.command_args,
                "interactiveArgs": profile.interactive_args,
            })
        }).collect();
        Ok(json!({
            "default": settings.shells.default,
            "interactive": settings.shells.interactive,
            "profiles": profiles,
        }))
    }
}

fn extract_executor(arguments: &mut Value) -> String {
    let Some(object) = arguments.as_object_mut() else {
        return "local".to_string();
    };
    object.remove("executor")
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

#[async_trait]
impl<H: SessionHost + 'static> McpToolHandler for SessionMcpHandler<H> {
    async fn list_tools(&self) -> Result<Vec<McpToolDef>, JsonRpcError> {
        Ok(crate::mcp::mcptooldefs::all_tools())
    }

    async fn call_tool(&self, name: &str, mut arguments: Value, _context: McpCallContext) -> Result<McpCallResult, JsonRpcError> {
        match name {
            "FileAction" | "read" if !self.host.session_id().is_empty() => {
                // Resolve hashRef if present
                let (args, file_key_ref) = self.resolve_file_args(arguments).await?;
                let executor = extract_executor_from_value(&args);
                // Call via Caller
                let result = self.call_via_manager(name, args).await?;
                // FileAction results use patch-style file metadata. Read back changed files so
                // hashRef storage gets a canonical REC FileStamp and the latest hash.
                let result = if name == "FileAction" {
                    self.read_back_file_action_result(result, &executor).await
                } else {
                    result
                };
                // Store fileRef if applicable
                let label = self.store_from_result(&executor, &result, file_key_ref.as_deref()).await;
                let mut value = result;
                if let Some(label) = label {
                    if let Some(text) = value.pointer_mut("/output/text") {
                        if let Some(existing) = text.as_str() {
                            *text = Value::String(format!("{existing}\n<fileRef>{label}</fileRef>"));
                        }
                    }
                }
                let output_text = extract_output_text(&value);
                Ok(McpCallResult { content: vec![McpContentText { kind: "text".to_string(), text: output_text }], structured_content: Some(strip_output(value)) })
            }
            "exbash" => {
                let result = self.call_via_manager("exbash", arguments).await?;
                let mut value = result;
                if let Some(snapshot) = self.upsert_exbash_from_result(&value).await {
                    value["metadata"]["hostSnapshot"] = serde_json::to_value(snapshot).unwrap_or_default();
                }
                let output_text = extract_output_text(&value);
                Ok(McpCallResult { content: vec![McpContentText { kind: "text".to_string(), text: output_text }], structured_content: Some(strip_output(value)) })
            }
            "rg" => {
                let result = self.call_via_manager("rg", arguments).await?;
                let output_text = extract_output_text(&result);
                Ok(McpCallResult { content: vec![McpContentText { kind: "text".to_string(), text: output_text }], structured_content: Some(strip_output(result)) })
            }
            "RemoteExecutorManager" => {
                let method = arguments.get("method").and_then(Value::as_str).unwrap_or("").to_string();
                if method == "list_shells" {
                    let shells = self.list_shells()?;
                    return Ok(McpCallResult { content: vec![McpContentText { kind: "text".to_string(), text: serde_json::to_string_pretty(&shells).unwrap() }], structured_content: Some(shells) });
                }
                if method == "set_executor_shell" {
                    let executor = extract_executor(&mut arguments);
                    let shell = arguments.get("shell").and_then(Value::as_str).unwrap_or("auto").to_string();
                    remove_executor_session_id(&mut arguments);
                    let manager = self.manager.get().ok_or_else(|| JsonRpcError::internal("RemoteExecutorManager is not initialized"))?.clone();
                    let directory = if executor == "local" {
                        Some(self.ctx.directory.clone())
                    } else {
                        None
                    };
                    let request = ExecutorRequest {
                        id: Value::Number(1.into()),
                        method: "set_default_shell".to_string(),
                        executor: Some(executor),
                        params: json!({ "shell": shell }),
                        directory,
                        tool_timeout_ms: None,
                    };
                    let response = manager_handle(&manager, request).await;
                    let result = serde_json::to_value(&response).unwrap();
                    let output_text = manager_output_text(&result);
                    return Ok(McpCallResult { content: vec![McpContentText { kind: "text".to_string(), text: output_text }], structured_content: Some(strip_output(result)) });
                }
                // Other manager methods go through Caller
                let executor = extract_executor(&mut arguments);
                remove_executor_session_id(&mut arguments);
                remove_field(&mut arguments, "executor");
                remove_field(&mut arguments, "method");
                let manager = self.manager.get().ok_or_else(|| JsonRpcError::internal("RemoteExecutorManager is not initialized"))?.clone();
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
                Ok(McpCallResult { content: vec![McpContentText { kind: "text".to_string(), text: output_text }], structured_content: Some(strip_output(result)) })
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
        .map(extract_output_text)
        .filter(|text| !text.is_empty());
    text.unwrap_or_else(|| serde_json::to_string_pretty(result).unwrap_or_default())
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
    async fn session_workdir(&self) -> Result<String, Self::Error> { Ok("".into()) }
}

#[async_trait]
impl crate::host::HashRefSessionStore for DummySessionHost {
    type Error = String;
    fn session_id(&self) -> &str { "" }
    fn is_hash_ref(&self, target: &str) -> bool { parse_hash_ref(target).is_some() }
    async fn resolve_hash_ref(&self, _target: &str) -> Result<crate::types::FileRefEntry, Self::Error> { Err("hashRef host is not configured".into()) }
    async fn store_hash_ref(&self, _update: crate::types::FileRefUpdate) -> Result<crate::types::FileRefEntry, Self::Error> { Err("hashRef host is not configured".into()) }
    async fn retouch_hash_ref(&self, _file_key_ref: &str, _hash_code: &str) -> Result<Option<crate::types::FileRefEntry>, Self::Error> { Err("hashRef host is not configured".into()) }
}

#[async_trait]
impl crate::host::ExbashSessionStore for DummySessionHost {
    type Error = String;
    async fn session_exbash_snapshot(&self, _async_id: &str, _executor: &str) -> Result<Option<ExbashTaskSnapshot>, Self::Error> { Ok(None) }
    async fn upsert_session_exbash(&self, input: ExbashSyncInput) -> Result<ExbashTaskSnapshot, Self::Error> {
        Ok(ExbashTaskSnapshot {
            async_id: input.async_id.unwrap_or_default(),
            executor: input.executor.unwrap_or_else(|| "local".into()),
            session_id: None,
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
    async fn remove_session_exbash(&self, _async_id: &str, _executor: &str) -> Result<bool, Self::Error> { Ok(false) }
}

#[async_trait]
impl crate::host::ExbashWorkdirStore for DummySessionHost {
    type Error = String;
    async fn workdir_exbash_snapshot(&self, _workdir: &str, _async_id: &str, _executor: &str) -> Result<Option<ExbashTaskSnapshot>, Self::Error> { Ok(None) }
    async fn upsert_workdir_exbash(&self, _workdir: &str, input: ExbashSyncInput) -> Result<ExbashTaskSnapshot, Self::Error> {
        Ok(ExbashTaskSnapshot {
            async_id: input.async_id.unwrap_or_default(),
            executor: input.executor.unwrap_or_else(|| "local".into()),
            session_id: input.session_id,
            workdir: Some("".into()),
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
    async fn remove_workdir_exbash(&self, _workdir: &str, _async_id: &str, _executor: &str) -> Result<bool, Self::Error> { Ok(false) }
}

#[async_trait]
impl crate::host::RemoteExecutorConfigStore for DummySessionHost {
    type Error = String;
    async fn read_remote_executor_config(&self, workdir: &str) -> Result<crate::types::RemoteExecutorConfigSnapshot, Self::Error> { Ok(crate::types::RemoteExecutorConfigSnapshot { workdir: workdir.to_string(), config: json!({}) }) }
    async fn update_remote_executor_config(&self, workdir: &str, patch: Value) -> Result<crate::types::RemoteExecutorConfigSnapshot, Self::Error> { Ok(crate::types::RemoteExecutorConfigSnapshot { workdir: workdir.to_string(), config: patch }) }
}
