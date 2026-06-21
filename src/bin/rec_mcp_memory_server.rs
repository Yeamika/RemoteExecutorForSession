use remote_executor_for_session::demos::memory_host::MemorySessionHost;
use remote_executor_for_session::jsonrpc::JsonRpcEndpoint;
use remote_executor_for_session::mcp::create_session_mcp_with_manager;
use remote_executor_for_session::ptyt::{RefsPtytGateway, RefsPtytScheduler};
use remote_executor_for_session::rec::{new_manager, ShellManager, ToolContext};
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;

const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let workdir = std::env::var("REFS_MCP_WORKDIR")
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir()?);
    let session_id =
        std::env::var("REFS_MCP_SESSION_ID").unwrap_or_else(|_| "mcp-demo".to_string());

    let caller = new_manager().await?;
    let shared = Arc::new(caller);
    let shell = ShellManager::default_shell(80, 24);
    let host = Arc::new(MemorySessionHost::new(
        session_id,
        workdir.to_string_lossy().to_string(),
    ));
    let ctx = ToolContext::new(Some(workdir.clone()));
    let endpoint = JsonRpcEndpoint::new(create_session_mcp_with_manager(ctx, host, shared, shell));
    let gateway = RefsPtytGateway::new(Arc::new(endpoint), RefsPtytScheduler::default());

    eprintln!("rec_mcp_memory_server ready: workdir={}", workdir.display());

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<Value>(&line) {
            Ok(input) => handle_message(&gateway, input).await,
            Err(error) => Some(error_response(Value::Null, -32700, error.to_string())),
        };
        if let Some(output) = response {
            println!("{}", serde_json::to_string(&output)?);
            io::stdout().flush()?;
        }
    }

    Ok(())
}

async fn handle_message(
    gateway: &RefsPtytGateway<impl remote_executor_for_session::JsonRpcHandler>,
    input: Value,
) -> Option<Value> {
    if let Some(batch) = input.as_array() {
        let mut responses = Vec::new();
        for item in batch {
            if let Some(response) = handle_single(gateway, item.clone()).await {
                responses.push(response);
            }
        }
        return (!responses.is_empty()).then_some(Value::Array(responses));
    }
    handle_single(gateway, input).await
}

async fn handle_single(
    gateway: &RefsPtytGateway<impl remote_executor_for_session::JsonRpcHandler>,
    input: Value,
) -> Option<Value> {
    let id = input.get("id").cloned()?;
    let method = input
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();

    match method {
        "initialize" => Some(initialize_response(
            id,
            input.get("params").cloned().unwrap_or(Value::Null),
        )),
        "ping" => Some(json!({ "jsonrpc": "2.0", "id": id, "result": {} })),
        "tools/list" | "tools/call" => Some(gateway.handle_endpoint_value(input).await),
        _ => Some(error_response(
            id,
            -32601,
            format!("method not found: {method}"),
        )),
    }
}

fn initialize_response(id: Value, params: Value) -> Value {
    let protocol_version = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_PROTOCOL_VERSION);

    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": protocol_version,
            "capabilities": {
                "tools": {
                    "listChanged": false
                }
            },
            "serverInfo": {
                "name": "rec-mcp-memory-server",
                "version": env!("CARGO_PKG_VERSION")
            },
            "instructions": "REFS exposes file, search, terminal, and executor tools. Each tools/call must include ExecutorSessionID; for manual MCP tests use any non-empty placeholder such as codex-mcp-test."
        }
    })
}

fn error_response(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message.into()
        }
    })
}
