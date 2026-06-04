use remote_executor_for_session::mcp::create_session_mcp_with_manager;
use remote_executor_for_session::demos::memory_host::MemorySessionHost;
use remote_executor_for_session::jsonrpc::{JsonRpcEndpoint, JsonRpcHandler};
use remote_executor_for_session::rec::{new_manager, ShellManager, ToolContext};
use std::sync::Arc;
use serde_json::json;

#[tokio::main]
async fn main() {
    let caller = new_manager().await.unwrap();
    let shared = Arc::new(caller);
    let shell = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(MemorySessionHost::new("demo", dir.path().to_string_lossy()));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let mcp = create_session_mcp_with_manager(ctx, host, shared, shell);
    let ep = JsonRpcEndpoint::new(mcp);

    let req = |method: &str, params: serde_json::Value| {
        json!({"jsonrpc":"2.0","id":1,"method":method,"params":params})
    };

    // 1. FileAction/create (direct path)
    let r = ep.handle_value(req("tools/call", json!({"name":"FileAction","arguments":{
        "mode":"create",
        "fileKey":"demo.rs",
        "content":"fn main() {\n    println!(\"hello\");\n}\n"
    }}))).await;
    println!("=== FileAction/create (direct path) ===\n{}\n", serde_json::to_string_pretty(&r).unwrap());

    // 2. read (direct path, with hashCheckMode)
    let r = ep.handle_value(req("tools/call", json!({"name":"read","arguments":{
        "fileKey":"demo.rs",
        "hashCheckMode": true
    }}))).await;
    println!("=== read (direct path) ===\n{}\n", serde_json::to_string_pretty(&r).unwrap());

    // 3. read again to get fileRef, then use hashRef
    let r1 = ep.handle_value(req("tools/call", json!({"name":"read","arguments":{
        "fileKey":"demo.rs",
        "hashCheckMode": true
    }}))).await;
    // Extract fileRef from output text (after <fileRef> tag)
    let output_text = r1["result"]["content"][0]["text"].as_str().unwrap_or("");
    let file_ref = output_text.lines()
        .find(|line| line.contains("#"))
        .map(|line| line.trim().to_string())
        .unwrap_or_default();
    println!("=== extracted fileRef ===\n{}\n", file_ref);

    // 4. read using hashRef (if we got one)
    if !file_ref.is_empty() {
        let r = ep.handle_value(req("tools/call", json!({"name":"read","arguments":{
            "fileKey": file_ref
        }}))).await;
        println!("=== read (hashRef) ===\n{}\n", serde_json::to_string_pretty(&r).unwrap());
    }

    // 5. FileAction/patch using hashRef
    if !file_ref.is_empty() {
        let r = ep.handle_value(req("tools/call", json!({"name":"FileAction","arguments":{
            "mode":"patch",
            "fileKey": file_ref,
            "patchText":"insert -1\n+// patched\n"
        }}))).await;
        println!("=== FileAction/patch (hashRef) ===\n{}\n", serde_json::to_string_pretty(&r).unwrap());
    }

    // 6. rg
    let r = ep.handle_value(req("tools/call", json!({"name":"rg","arguments":{
        "pattern":"fn",
        "path":dir.path().to_string_lossy()
    }}))).await;
    println!("=== rg ===\n{}\n", serde_json::to_string_pretty(&r).unwrap());

    // 7. exbash
    let r = ep.handle_value(req("tools/call", json!({"name":"exbash","arguments":{
        "mode":"run",
        "command":"echo hello from exbash",
        "read_timeout":5000
    }}))).await;
    println!("=== exbash ===\n{}\n", serde_json::to_string_pretty(&r).unwrap());

    // 8. RemoteExecutorManager/list_executor
    let r = ep.handle_value(req("tools/call", json!({"name":"RemoteExecutorManager","arguments":{
        "method":"list_executor",
        "id":0,
        "params":{}
    }}))).await;
    println!("=== RemoteExecutorManager ===\n{}\n", serde_json::to_string_pretty(&r).unwrap());
}
