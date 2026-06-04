use crate::mcp::McpToolDef;
use crate::mcp::EXECUTOR_SESSION_PARAM;
use serde_json::{json, Map, Value};

fn prop(name: &str, schema: Value) -> (String, Value) {
    (name.to_string(), schema)
}

fn string_prop(description: &str) -> Value {
    json!({ "type": "string", "description": description })
}

fn string_default(description: &str, default: &str) -> Value {
    json!({ "type": "string", "description": description, "default": default })
}

fn string_enum_prop(values: &[&str]) -> Value {
    json!({ "type": "string", "enum": values })
}

fn string_enum_default(values: &[&str], default: &str) -> Value {
    json!({ "type": "string", "enum": values, "default": default })
}

fn integer_prop(description: &str) -> Value {
    json!({ "type": "integer", "description": description })
}

fn boolean_prop(description: &str) -> Value {
    json!({ "type": "boolean", "description": description })
}

fn boolean_default(description: &str, default: bool) -> Value {
    json!({ "type": "boolean", "description": description, "default": default })
}

fn array_prop(item_type: &str) -> Value {
    json!({ "type": "array", "items": { "type": item_type } })
}

fn object_prop(additional: &str) -> Value {
    json!({ "type": "object", "additionalProperties": { "type": additional } })
}

fn exec_session_prop() -> (String, Value) {
    prop(EXECUTOR_SESSION_PARAM, string_prop("OpenCode executor/session routing id"))
}

fn executor_prop() -> (String, Value) {
    prop("executor", string_default("Target executor id", "local"))
}

fn tool_def(name: &str, description: &str, required: Vec<&str>, properties: Vec<(String, Value)>) -> McpToolDef {
    let mut props = Map::new();
    for (k, v) in properties {
        props.insert(k, v);
    }
    let schema = json!({
        "type": "object",
        "required": required,
        "properties": Value::Object(props),
    });
    McpToolDef {
        name: name.to_string(),
        description: Some(description.to_string()),
        input_schema: schema,
    }
}

/// FileAction: patch, create, delete, or rename a file.
///
/// Real MCP JSON-RPC response (create):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [{ "type": "text", "text": "Success. Created file:\nC demo.rs" }],
///     "structuredContent": {
///       "metadata": {
///         "diagnostics": {},
///         "file": {
///           "additions": 37,
///           "deletions": 0,
///           "filePath": "/tmp/.tmpXXX/demo.rs",
///           "relativePath": "demo.rs",
///           "type": "create"
///         }
///       },
///       "output": { "message": "", "text": "Success. Created file:\nC demo.rs", "info": "" }
///     }
///   }
/// }
/// ```
pub fn file_action() -> McpToolDef {
    tool_def(
        "FileAction",
        "REC file action: patch, create, delete, or rename a file",
        vec!["mode"],
        vec![
            exec_session_prop(),
            executor_prop(),
            prop("mode", string_enum_prop(&["patch", "create", "delete", "rename"])),
            prop("fileKey", string_prop("File key or read reference (\"App.ts #A1B2\")")),
            prop("newFilePath", string_prop("Destination path for mode=rename")),
            prop("patchText", string_prop("REC patch text for mode=patch")),
            prop("content", string_prop("New file content for mode=create. With patchMode=binary, this is hex.")),
            prop("patchMode", string_enum_default(&["text", "binary"], "text")),
        ],
    )
}

/// Read a file via REC. Supports file references ("filename #ABCD") and direct paths.
///
/// Real MCP JSON-RPC response:
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [{ "type": "text", "text": "1: fn main() {\n2:     println!(\"hello\");\n3: }\ntotal 3 lines" }],
///     "structuredContent": {
///       "metadata": {
///         "file": {
///           "canonicalPath": "/tmp/.tmpXXX/demo.rs",
///           "fileKey": "file-id:Inode { device_id: 138, inode_number: 413089 }",
///           "kind": "file",
///           "mtimeMs": 1780511126227,
///           "size": 37
///         }
///       },
///       "output": { "message": "", "text": "1: fn main() {\n2:     println!(\"hello\");\n3: }", "info": "total 3 lines" }
///     }
///   }
/// }
/// ```
pub fn read() -> McpToolDef {
    tool_def(
        "read",
        "Read a file via REC. Supports file references (\"filename #ABCD\") and direct paths.",
        vec!["fileKey"],
        vec![
            exec_session_prop(),
            executor_prop(),
            prop("fileKey", string_prop("File key or read reference (\"App.ts #A1B2\")")),
            prop("mode", string_enum_default(&["text", "binary"], "text")),
            prop("offset", integer_prop("Start offset. Text: 1-based line. Binary: 0-based byte.")),
            prop("limit", integer_prop("Max items. Text: lines. Binary: bytes (max 128).")),
        ],
    )
}

/// REC ripgrep-like search. Returns matching lines with file/line/column metadata.
///
/// Real MCP JSON-RPC response:
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [{ "type": "text", "text": "/tmp/.tmpXXX/demo.rs:1:1:fn main() {\n" }],
///     "structuredContent": {
///       "metadata": {
///         "code": 0,
///         "matches": 1
///       },
///       "output": { "message": "", "text": "/tmp/.tmpXXX/demo.rs:1:1:fn main() {\n", "info": "" }
///     }
///   }
/// }
/// ```
pub fn rg() -> McpToolDef {
    tool_def(
        "rg",
        "REC ripgrep-like search. Returns matching lines with file/line/column metadata.",
        vec!["pattern"],
        vec![
            exec_session_prop(),
            executor_prop(),
            prop("pattern", string_prop("Regex pattern to search for")),
            prop("path", string_prop("Specific file or directory to search")),
            prop("globs", array_prop("string")),
            prop("case_sensitive", boolean_default("Case-sensitive matching", true)),
            prop("max_count", integer_prop("Maximum number of matches to return")),
        ],
    )
}

/// REC exbash PTY tool. Run shell commands, attach to running sessions, list/stop/remove tasks.
///
/// Real MCP JSON-RPC response (run, command completes immediately):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [{ "type": "text", "text": "hello from exbash\r\n" }],
///     "structuredContent": {
///       "metadata": {
///         "exitCode": 0,
///         "output": "hello from exbash\r\n"
///       },
///       "output": { "message": "", "text": "hello from exbash\r\n", "info": "" }
///     }
///   }
/// }
/// ```
pub fn exbash() -> McpToolDef {
    tool_def(
        "exbash",
        "REC exbash PTY tool. Run shell commands, attach to running sessions, list/stop/remove tasks.",
        vec![],
        vec![
            exec_session_prop(),
            executor_prop(),
            prop("mode", string_enum_default(&["run", "shell", "attach", "list", "stop", "remove"], "shell")),
            prop("command", string_prop("Shell command for mode=run")),
            prop("shell", string_default("Shell profile name for mode=shell", "auto")),
            prop("description", string_prop("Task description")),
            prop("timeout", integer_prop("Command timeout in milliseconds")),
            prop("read_timeout", integer_prop("Read timeout in milliseconds")),
            prop("asyncID", string_prop("Task ID for attach/stop/remove")),
            prop("text", string_prop("Text to write to PTY stdin (attach mode)")),
            prop("filePath", string_prop("File path for file-based input")),
            prop("workdir", string_prop("Working directory for the shell command")),
            prop("showRawPretty", boolean_prop("Show raw and pretty output")),
        ],
    )
}

/// Manage RemoteExecutor: list/connect executors, set default, list shell profiles.
///
/// Real MCP JSON-RPC response (list_executor):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [{ "type": "text", "text": "{\n  \"executor\": \"caller\",\n  \"id\": 1,\n  \"ok\": true,\n  \"result\": {\n    \"metadata\": {\n      \"default\": \"local\",\n      \"executors\": [\n        {\n          \"device\": \"maintainer\",\n          \"id\": \"local\",\n          \"labels\": {},\n          \"system\": \"linux\",\n          \"url\": \"ws://127.0.0.1:43495\"\n        }\n      ]\n    }\n  }\n}" }],
///     "structuredContent": {
///       "executor": "caller",
///       "id": 1,
///       "ok": true,
///       "result": {
///         "metadata": {
///           "default": "local",
///           "executors": [
///             {
///               "device": "maintainer",
///               "id": "local",
///               "labels": {},
///               "system": "linux",
///               "url": "ws://127.0.0.1:43495"
///             }
///           ]
///         }
///       }
///     }
///   }
/// }
/// ```
pub fn remote_executor_manager() -> McpToolDef {
    tool_def(
        "RemoteExecutorManager",
        "Manage RemoteExecutor: list/connect executors, set default, list shell profiles.",
        vec!["method"],
        vec![
            exec_session_prop(),
            prop("method", string_enum_prop(&["list_executor", "connect_to_executor", "list_shells", "set_executor_shell"])),
            prop("id", string_prop("Executor ID")),
            prop("url", string_prop("WebSocket URL for connect_to_executor")),
            prop("system", string_prop("System label")),
            prop("device", string_prop("Device label")),
            prop("labels", object_prop("string")),
            prop("shell", string_default("Shell profile name for set_executor_shell", "auto")),
        ],
    )
}

pub fn all_tools() -> Vec<McpToolDef> {
    vec![
        file_action(),
        read(),
        rg(),
        exbash(),
        remote_executor_manager(),
    ]
}
