#![deny(clippy::all)]

use napi_derive::napi;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

use remote_executor::SettingsStore;
use remote_executor_for_session::jsonrpc::JsonRpcEndpoint;
use remote_executor_for_session::mcp::{
    create_session_mcp_with_manager, EmbeddedMcp, SessionMcpHandler,
};
use remote_executor_for_session::rec::{
    manager_handle, new_manager, Caller, ExecutorRequest, ShellManager, ToolContext,
};

mod sqlite_host;
use sqlite_host::SqliteSessionHost;

/// Handle to a session MCP instance. Holds the JSON-RPC endpoint and Caller alive.
#[napi]
pub struct SessionMcpHandle {
    ep: JsonRpcEndpoint<EmbeddedMcp<SessionMcpHandler<SqliteSessionHost>>>,
    manager: Arc<Caller>,
    runtime: Runtime,
}

#[napi]
impl SessionMcpHandle {
    /// List available MCP tool definitions.
    ///
    /// Returns JSON: `{ "tools": [ { "name": "...", "description": "...", "inputSchema": {...} } ] }`
    #[napi]
    pub fn list_tools(&self) -> napi::Result<String> {
        let resp = self
            .runtime
            .block_on(self.ep.handle_value(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list"
            })));
        serde_json::to_string_pretty(&resp).map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Call an MCP tool by name with JSON arguments.
    ///
    /// Returns the full JSON-RPC response object.
    #[napi]
    pub fn call_tool(&self, name: String, arguments: String) -> napi::Result<String> {
        let args: Value =
            serde_json::from_str(&arguments).unwrap_or_else(|_| serde_json::json!({}));
        let resp = self
            .runtime
            .block_on(self.ep.handle_value(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": name,
                    "arguments": args
                }
            })));
        serde_json::to_string_pretty(&resp).map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Call an MCP tool and return only the content[0].text (the model-visible output).
    #[napi]
    pub fn call_tool_text(&self, name: String, arguments: String) -> napi::Result<String> {
        let args: Value =
            serde_json::from_str(&arguments).unwrap_or_else(|_| serde_json::json!({}));
        let resp = self
            .runtime
            .block_on(self.ep.handle_value(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": name,
                    "arguments": args
                }
            })));
        let text = resp
            .pointer("/result/content/0/text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(text)
    }

    /// Return the current executor list as JSON from the underlying manager.
    ///
    /// This is an OpenCode host helper; model-visible MCP tool calls should keep
    /// using plaintext `content[0].text`.
    #[napi]
    pub fn list_executors_json(&self) -> napi::Result<String> {
        let response = self.runtime.block_on(manager_handle(
            self.manager.as_ref(),
            ExecutorRequest {
                id: serde_json::json!(1),
                method: "list_executor".to_string(),
                executor: Some("local".to_string()),
                params: serde_json::json!({}),
                directory: None,
                tool_timeout_ms: None,
            },
        ));
        if !response.ok {
            return Err(napi::Error::from_reason(
                response
                    .error
                    .unwrap_or_else(|| "list_executor failed".to_string()),
            ));
        }
        let metadata = response
            .result
            .as_ref()
            .and_then(|result| result.get("metadata"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!({ "executors": [] }));
        serde_json::to_string_pretty(&metadata).map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Handle a raw JSON-RPC request (supports batch).
    #[napi]
    pub fn handle_raw(&self, request: String) -> napi::Result<String> {
        let value: Value =
            serde_json::from_str(&request).map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let resp = self.runtime.block_on(self.ep.handle_value(value));
        serde_json::to_string_pretty(&resp).map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

/// Create a new session MCP handler backed by OpenCode's SQLite database.
///
/// - `db_path`: path to the SQLite database file (e.g. `~/.local/share/opencode/opencode.db`)
/// - `session_id`: the current session ID
/// - `workdir`: the current working directory
///
/// Returns a `SessionMcpHandle` that can be used to call tools.
#[napi]
pub fn create_session_mcp(
    db_path: String,
    session_id: String,
    workdir: String,
) -> napi::Result<SessionMcpHandle> {
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let host = SqliteSessionHost::new(session_id, workdir.clone(), PathBuf::from(&db_path))
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let host = Arc::new(host);
    let shell_manager = ShellManager::default_shell(80, 24);
    let ctx = ToolContext::new(Some(PathBuf::from(&workdir)))
        .with_settings_store(SettingsStore::load_default_lossy());

    let manager = runtime
        .block_on(new_manager())
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let shared_manager = Arc::new(manager);

    let mcp = create_session_mcp_with_manager(ctx, host, shared_manager.clone(), shell_manager);
    let ep = JsonRpcEndpoint::new(mcp);

    Ok(SessionMcpHandle {
        ep,
        manager: shared_manager,
        runtime,
    })
}

/// Get the default SQLite database path used by OpenCode.
#[napi]
pub fn default_db_path() -> napi::Result<String> {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    let data_dir =
        std::env::var("XDG_DATA_HOME").unwrap_or_else(|_| format!("{home}/.local/share"));
    Ok(format!("{data_dir}/opencode/opencode.db"))
}
