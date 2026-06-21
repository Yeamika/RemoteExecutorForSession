use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use remote_executor_for_session::demos::memory_host::MemorySessionHost;
use remote_executor_for_session::jsonrpc::{JsonRpcEndpoint, JsonRpcHandler};
use remote_executor_for_session::mcp::create_session_mcp_with_manager;
use remote_executor_for_session::ptyt::{RefsPtytGateway, RefsPtytRegistration, RefsPtytScheduler};
use remote_executor_for_session::rec::{new_manager, ShellManager, ToolContext};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";

#[tokio::main]
async fn main() -> Result<()> {
    let workdir = std::env::var("REFS_MCP_WORKDIR")
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir()?);
    let session_id =
        std::env::var("REFS_MCP_SESSION_ID").unwrap_or_else(|_| "mcp-demo".to_string());
    let listen = std::env::var("REFS_MCP_WS_LISTEN").unwrap_or_else(|_| "127.0.0.1:0".to_string());

    let caller = new_manager().await?;
    let shared = Arc::new(caller);
    let shell = ShellManager::default_shell(80, 24);
    let host = Arc::new(MemorySessionHost::new(
        session_id,
        workdir.to_string_lossy().to_string(),
    ));
    let ctx = ToolContext::new(Some(workdir.clone()));
    let endpoint = Arc::new(JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx, host, shared, shell,
    )));
    let gateway = Arc::new(RefsPtytGateway::new(endpoint, RefsPtytScheduler::default()));

    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("bind REFS MCP WebSocket listener on {listen}"))?;
    let addr = listener.local_addr()?;
    println!("ws://{addr}");
    eprintln!(
        "rec_mcp_memory_ws_server ready: url=ws://{addr} workdir={}",
        workdir.display()
    );

    loop {
        let (stream, peer) = listener.accept().await?;
        let gateway = gateway.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(gateway, stream).await {
                eprintln!("rec_mcp_memory_ws_server client {peer} error: {error:#}");
            }
        });
    }
}

async fn handle_connection<H: JsonRpcHandler>(
    gateway: Arc<RefsPtytGateway<H>>,
    stream: TcpStream,
) -> Result<()> {
    let ws = accept_async(stream).await?;
    let (mut write, mut read) = ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let mut registration = None::<RefsPtytRegistration>;

    loop {
        tokio::select! {
            outbound = rx.recv() => {
                let Some(outbound) = outbound else {
                    break;
                };
                write.send(Message::Text(outbound.into())).await?;
            }
            message = read.next() => {
                let Some(message) = message else {
                    break;
                };
                match message? {
                    Message::Text(text) => {
                        match gateway.handle_client_text(&text, tx.clone()).await {
                            Ok(Some(next)) => {
                                registration = Some(next);
                            }
                            Ok(None) => {
                                let response = match serde_json::from_str::<Value>(&text) {
                                    Ok(input) => handle_message(&gateway, input).await,
                                    Err(error) => Some(error_response(Value::Null, -32700, error.to_string())),
                                };
                                if let Some(output) = response {
                                    write
                                        .send(Message::Text(serde_json::to_string(&output)?.into()))
                                        .await?;
                                }
                            }
                            Err(error) => {
                                write
                                    .send(Message::Text(
                                        error_response(Value::Null, -32602, error.to_string()).to_string().into(),
                                    ))
                                    .await?;
                            }
                        }
                    }
                    Message::Ping(data) => write.send(Message::Pong(data)).await?,
                    Message::Close(frame) => {
                        let _ = write.send(Message::Close(frame)).await;
                        break;
                    }
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }
    if let Some(registration) = registration {
        gateway.unregister(&registration).await;
    }

    Ok(())
}

async fn handle_message<H: JsonRpcHandler>(
    gateway: &RefsPtytGateway<H>,
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

async fn handle_single<H: JsonRpcHandler>(
    gateway: &RefsPtytGateway<H>,
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
                "name": "rec-mcp-memory-ws-server",
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
