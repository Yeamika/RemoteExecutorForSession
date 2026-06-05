use crate::mcp::{McpToolDef, EXECUTOR_SESSION_PARAM, INCLUDE_STRUCTURED_CONTENT_PARAM};
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

fn string_enum_default_desc(description: &str, values: &[&str], default: &str) -> Value {
    json!({ "type": "string", "description": description, "enum": values, "default": default })
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

fn array_string_desc(description: &str) -> Value {
    json!({ "type": "array", "description": description, "items": { "type": "string" } })
}

fn object_prop(additional: &str) -> Value {
    json!({ "type": "object", "additionalProperties": { "type": additional } })
}

fn exec_session_prop() -> (String, Value) {
    prop(
        EXECUTOR_SESSION_PARAM,
        string_prop("Session id; filled by the host system."),
    )
}

fn include_structured_content_schema() -> Value {
    boolean_default(
        "Return structuredContent metadata in addition to plaintext content.",
        false,
    )
}

fn executor_prop() -> (String, Value) {
    prop("executor", string_default("Target executor id", "local"))
}

fn executor_empty_default_prop(description: &str) -> (String, Value) {
    prop("executor", string_default(description, ""))
}

fn tool_def(
    name: &str,
    description: &str,
    mut required: Vec<&str>,
    properties: Vec<(String, Value)>,
) -> McpToolDef {
    let mut has_executor_session = false;
    let mut props = Map::new();
    for (k, v) in properties {
        if k == EXECUTOR_SESSION_PARAM {
            has_executor_session = true;
            continue;
        }
        if k == INCLUDE_STRUCTURED_CONTENT_PARAM {
            continue;
        }
        props.insert(k, v);
    }
    if has_executor_session {
        let mut ordered = Map::new();
        ordered.insert(
            EXECUTOR_SESSION_PARAM.to_string(),
            string_prop("Session id; filled by the host system."),
        );
        ordered.insert(
            INCLUDE_STRUCTURED_CONTENT_PARAM.to_string(),
            include_structured_content_schema(),
        );
        for (k, v) in props {
            ordered.insert(k, v);
        }
        props = ordered;
        required.retain(|item| *item != EXECUTOR_SESSION_PARAM);
        required.insert(0, EXECUTOR_SESSION_PARAM);
    } else {
        props.insert(
            INCLUDE_STRUCTURED_CONTENT_PARAM.to_string(),
            include_structured_content_schema(),
        );
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
/// Captured MCP input (create):
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "method": "tools/call",
///   "params": {
///     "name": "FileAction",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "mode": "create",
///       "fileKey": "demo.rs",
///       "content": "fn main() {
///     println!(\"hello\");
/// }
/// ",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "Success. Created file:
/// C demo.rs
/// <fileRef>demo.rs #8EBE</fileRef>"
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (patch):
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 2,
///   "method": "tools/call",
///   "params": {
///     "name": "FileAction",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "mode": "patch",
///       "fileKey": "demo.rs #8EBE",
///       "patchText": "insert -1
/// +// patched
/// ",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 2,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "Success. Updated file:
/// M demo.rs
/// <fileRef>demo.rs #8EBE</fileRef>"
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (rename):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 3,
///   "method": "tools/call",
///   "params": {
///     "name": "FileAction",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "mode": "rename",
///       "fileKey": "demo.rs #8EBE",
///       "newFilePath": "/tmp/refs-mcp-examples-vtOsrZ/renamed.rs",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 3,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "Success. Renamed file:
/// R demo.rs -> renamed.rs
/// <fileRef>renamed.rs #8EBE</fileRef>"
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (delete):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 4,
///   "method": "tools/call",
///   "params": {
///     "name": "FileAction",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "mode": "delete",
///       "fileKey": "renamed.rs #8EBE",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 4,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "Success. Deleted file:
/// D renamed.rs"
///       }
///     ]
///   }
/// }
/// ```
pub fn file_action() -> McpToolDef {
    tool_def(
        "FileAction",
        "REC file action: patch, create, delete, or rename a file. For stale-safe edits, prefer the hashRef label returned by read/FileAction, e.g. `App.ts #A1B2`, as `fileKey`; this resolves the file and applies hash checking before the mutation.",
        vec!["mode"],
        vec![
            exec_session_prop(),
            executor_prop(),
            prop(
                "mode",
                string_enum_prop(&["patch", "create", "delete", "rename"]),
            ),
            prop(
                "fileKey",
                string_prop("Direct path, REC file key, or hashRef label. If tool output contains `<fileRef>App.ts #A1B2</fileRef>`, pass the inner `App.ts #A1B2` value here."),
            ),
            prop(
                "newFilePath",
                string_prop("Destination path for mode=rename"),
            ),
            prop("patchText", string_prop("REC patch text for mode=patch")),
            prop(
                "content",
                string_prop(
                    "New file content for mode=create. With patchMode=binary, this is hex.",
                ),
            ),
            prop(
                "patchMode",
                string_enum_default(&["text", "binary"], "text"),
            ),
        ],
    )
}

/// Read a file via REC. Supports file references ("filename #ABCD") and direct paths.
///
/// Captured MCP input (mode=text):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "method": "tools/call",
///   "params": {
///     "name": "read",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "fileKey": "demo.rs",
///       "mode": "text",
///       "limit": 3,
///       "hashCheckMode": true,
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "1: fn main() {
/// 2:     println!(\"hello\");
/// 3: }
/// <fileRef>demo.rs #8EBE</fileRef>
/// total 3 lines"
///       }
///     ],
///     "structuredContent": {
///       "metadata": {
///         "file": {
///           "fileKey": "file-id:Inode { device_id: 138, inode_number: 419297 }",
///           "canonicalPath": "/tmp/.tmp8V7Cxa/demo.rs",
///           "kind": "file",
///           "size": 37,
///           "mtimeMs": 1780575799221
///         },
///         "hashCode": "sha256:35e0393811f794547c34763eb5773d6cddb295dc4f372180ed4aae67da3ea45f"
///       }
///     }
///   }
/// }
/// ```
///
/// Captured MCP input (mode=binary):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 2,
///   "method": "tools/call",
///   "params": {
///     "name": "read",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "fileKey": "demo.rs #8EBE",
///       "mode": "binary",
///       "offset": 0,
///       "limit": 8,
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 2,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "00000000  66 6E 20 6D 61 69 6E 28                          |fn main(|
/// <fileRef>demo.rs #8EBE</fileRef>
/// Showing bytes 0-7 of 37. Use offset=8 to continue."
///       }
///     ],
///     "structuredContent": {
///       "metadata": {
///         "file": {
///           "fileKey": "file-id:Inode { device_id: 138, inode_number: 419297 }",
///           "canonicalPath": "/tmp/refs-mcp-examples-vtOsrZ/demo.rs",
///           "kind": "file",
///           "size": 37,
///           "mtimeMs": 1780576006283
///         },
///         "hashCode": "sha256:35e0393811f794547c34763eb5773d6cddb295dc4f372180ed4aae67da3ea45f"
///       }
///     }
///   }
/// }
/// ```
pub fn read() -> McpToolDef {
    tool_def(
        "read",
        "Read a file via REC. Accepts direct paths, REC file keys, and hashRef labels such as `filename #ABCD`. File reads may return `<fileRef>filename #ABCD</fileRef>`; use that inner label as `fileKey` for later read/FileAction calls to preserve file identity and hash safety.",
        vec!["fileKey"],
        vec![
            exec_session_prop(),
            executor_prop(),
            prop(
                "fileKey",
                string_prop("Direct path, REC file key, or hashRef label such as `App.ts #A1B2`. If a prior tool returned `<fileRef>...</fileRef>`, pass the inner label without XML tags."),
            ),
            prop("mode", string_enum_default(&["text", "binary"], "text")),
            prop(
                "offset",
                integer_prop("Start offset. Text: 1-based line. Binary: 0-based byte."),
            ),
            prop(
                "limit",
                integer_prop("Max items. Text: lines. Binary: bytes (max 128)."),
            ),
        ],
    )
}

/// REC ripgrep-like search. mode=content searches file contents; mode=files matches file paths by glob pattern.
///
/// Captured MCP input (mode=content):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "method": "tools/call",
///   "params": {
///     "name": "rg",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "pattern": "fn",
///       "path": "/tmp/.tmp8V7Cxa",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "/tmp/.tmp8V7Cxa/demo.rs:1:1:fn main() {
///
/// matches:1
/// code:0"
///       }
///     ],
///     "structuredContent": {
///       "metadata": {
///         "matches": 1,
///         "code": 0
///       }
///     }
///   }
/// }
/// ```
///
/// Captured MCP input (mode=files):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "method": "tools/call",
///   "params": {
///     "name": "rg",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "mode": "files",
///       "pattern": "*.rs",
///       "path": "/tmp/.tmp8V7Cxa",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output (mode=files):
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "/tmp/.tmp8V7Cxa/demo.rs
///
/// matches:1
/// code:0"
///       }
///     ],
///     "structuredContent": {
///       "metadata": {
///         "count": 1,
///         "truncated": false,
///         "mode": "files",
///         "matches": 1,
///         "code": 0
///       }
///     }
///   }
/// }
/// ```
pub fn rg() -> McpToolDef {
    tool_def(
        "rg",
        "REC rg search. mode=content searches file contents and returns matching lines. mode=files matches file paths by glob pattern and returns paths. Search results are paths, not hashRefs; call `read` on a path first when a later edit should use a stale-safe `fileKey`.",
        vec!["pattern"],
        vec![
            exec_session_prop(),
            executor_prop(),
            prop(
                "mode",
                string_enum_default_desc(
                    "content: search file contents. files: match file paths by glob pattern.",
                    &["content", "files"],
                    "content",
                ),
            ),
            prop(
                "type",
                string_enum_default_desc(
                    "Alias for mode; prefer mode.",
                    &["content", "files"],
                    "content",
                ),
            ),
            prop(
                "pattern",
                string_prop(
                    "content mode: regex pattern to search for. files mode: glob pattern such as `*.rs` or `src/**/*.ts`.",
                ),
            ),
            prop("path", string_prop("Specific file or directory to search")),
            prop("globs", array_string_desc("Content mode file glob filters")),
            prop(
                "case_sensitive",
                boolean_default("Content mode case-sensitive matching", true),
            ),
            prop(
                "max_count",
                integer_prop("Content mode maximum number of matches to return"),
            ),
        ],
    )
}

/// REC exbash PTY tool. Run shell commands, attach to running sessions, list/stop/remove tasks.
///
/// Task states shown in tracked lists use these protocol values:
/// `running`, `exit:<code>`, `timeout`, and `stop`. `unknown` is only a display
/// fallback for abnormal persisted state; REFS does not actively write it.
///
/// Remote executor state is lazy-tracked. Stored local/workspace entries for a
/// remote executor keep their previous state until a real executor call succeeds.
/// Successful `attach`, `stop`, or `remove` calls update/clear matching tracked
/// entries. Failed remote calls leave tracked state unchanged. Use
/// `scope=remote` with a non-local `executor` to query live untracked remote
/// tasks without changing stored tracking.
///
/// Captured MCP input (run, command completes immediately):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "method": "tools/call",
///   "params": {
///     "name": "exbash",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "mode": "run",
///       "command": "echo hello from exbash",
///       "read_timeout": 5000,
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "hello from exbash
///
/// totaloutput:19bytes
/// exitcode:0"
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (mode=shell):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 2,
///   "method": "tools/call",
///   "params": {
///     "name": "exbash",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "mode": "shell",
///       "command": "echo shell-ok",
///       "read_timeout": 5000,
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 2,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "shell-ok
///
/// totaloutput:10bytes
/// exitcode:0"
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (mode=run, detached):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 3,
///   "method": "tools/call",
///   "params": {
///     "name": "exbash",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "mode": "run",
///       "command": "sleep 5",
///       "read_timeout": 10,
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 3,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "
/// rex-1780576006395-3 detached"
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (mode=list):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 4,
///   "method": "tools/call",
///   "params": {
///     "name": "exbash",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "mode": "list",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 4,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "local:1 workspace:0
/// showing executor=local of local
/// - local:rex-1780576006395-3 running totalOutput=0 command=sleep 5"
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (mode=list, live remote view):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 5,
///   "method": "tools/call",
///   "params": {
///     "name": "exbash",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "mode": "list",
///       "scope": "remote",
///       "executor": "exec_1"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 5,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "local:1 workspace:0
/// showing executor=exec_1 of remote
/// - rex-1780576007000-1 running totalOutput=0 command=sleep 30"
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (mode=attach):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 5,
///   "method": "tools/call",
///   "params": {
///     "name": "exbash",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "mode": "attach",
///       "asyncID": "rex-1780576006395-3",
///       "read_timeout": 0,
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 5,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": ""
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (mode=stop):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 6,
///   "method": "tools/call",
///   "params": {
///     "name": "exbash",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "mode": "stop",
///       "asyncID": "rex-1780576006395-3",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 6,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": ""
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (mode=list, after stop):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 7,
///   "method": "tools/call",
///   "params": {
///     "name": "exbash",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "mode": "list",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 7,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "local:1 workspace:0
/// showing executor=local of local
/// - local:rex-1780576006395-3 stop totalOutput=0 command=sleep 5"
///       }
///     ]
///   }
/// }
/// ```
///
/// Captured MCP input (mode=remove):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 7,
///   "method": "tools/call",
///   "params": {
///     "name": "exbash",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "mode": "remove",
///       "asyncID": "rex-1780576006395-3",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 7,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "ok"
///       }
///     ]
///   }
/// }
/// ```
pub fn exbash() -> McpToolDef {
    tool_def(
        "exbash",
        "PTY-backed background terminal. `shell` is the default mode and should be used for normal terminal syntax, shell operators, environment expansion, scripts, and configured shell profiles. `run` directly starts a program by splitting `command` into executable + argv, without shell interpretation. If the command finishes within `read_timeout`, the tool returns the output immediately. If it keeps running, the tool returns a detached snapshot with `asyncID`; use `attach`, `list`, `stop`, or `remove` to manage that run later. Tracked task states are `running`, `exit:<code>`, `timeout`, or `stop`. Remote executor state is lazy-tracked: successful calls update matching tracked entries, failed remote calls leave stored state unchanged.",
        vec![],
        vec![
            exec_session_prop(),
            executor_empty_default_prop(
                "`run` and `shell` default to local when omitted. `list` defaults to all tracked executors when omitted. `scope=remote` requires a non-local executor and performs a live untracked-task query.",
            ),
            prop(
                "mode",
                string_enum_default_desc(
                    "Operation selector. `shell` is the default terminal path. `run` directly starts a program and splits `command` into executable + argv without shell parsing.",
                    &["run", "shell", "attach", "list", "stop", "remove"],
                    "shell",
                ),
            ),
            prop(
                "scope",
                string_enum_default_desc(
                    "Task tracking view for `list`, `run`, and `shell`. `local` tracks the current session; `workspace` tracks the current workdir; `remote` is only valid with `mode=list`, requires a non-local executor, and queries live remote tasks without changing stored tracking.",
                    &["local", "workspace", "remote"],
                    "local",
                ),
            ),
            prop(
                "command",
                string_prop(
                    "Command text. In `shell` mode it is sent to the configured shell profile. In `run` mode it is parsed as executable + argv and shell syntax is not interpreted.",
                ),
            ),
            prop(
                "shell",
                string_default(
                    "Shell profile name for `shell` mode. Empty or omitted uses the settings default.",
                    "auto",
                ),
            ),
            prop("description", string_prop("Optional display text for the run. The description is shown in run listings and detached snapshots.")),
            prop("timeout", integer_prop("Total lifetime timeout in milliseconds. Omit, 0, or -1 to leave the run unmanaged.")),
            prop("read_timeout", integer_prop("How long to wait before returning. If the process is still running at timeout, the tool returns a detached snapshot with `asyncID`.")),
            prop("asyncID", string_prop("Run id returned by detached `run` or `attach`; required for `attach`, `list`, `stop`, and `remove`.")),
            prop("text", string_prop("Text to write to PTY stdin in `attach` mode. Common escape sequences are interpreted.")),
            prop("filePath", string_prop("File path for `attach` mode input. Mutually exclusive with `text`.")),
            prop("workdir", string_prop("Working directory for `run` and `shell` commands.")),
            prop("showRawPretty", boolean_prop("Include raw PTY text in attach metadata.")),
        ],
    )
}

/// Manage RemoteExecutor: list/connect executors, set default, list shell profiles.
///
/// Captured MCP input (list_executor):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "method": "tools/call",
///   "params": {
///     "name": "RemoteExecutorManager",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "method": "list_executor",
///       "id": "0",
///       "executor": "local"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "default executor: local
/// executors:
/// - local (default) system=linux device=maintainer url=ws://127.0.0.1:43555"
///       }
///     ],
///     "structuredContent": {
///       "id": 1,
///       "ok": true,
///       "result": {
///         "metadata": {
///           "default": "local",
///           "executors": [
///             {
///               "id": "local",
///               "system": "linux",
///               "device": "maintainer",
///               "labels": {},
///               "url": "ws://127.0.0.1:43555"
///             }
///           ]
///         }
///       },
///       "executor": "caller"
///     }
///   }
/// }
/// ```
///
/// Captured MCP input (connect_to_executor):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 2,
///   "method": "tools/call",
///   "params": {
///     "name": "RemoteExecutorManager",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "method": "connect_to_executor",
///       "id": "loopback",
///       "url": "ws://127.0.0.1:44881",
///       "system": "linux",
///       "device": "loopback",
///       "labels": {
///         "demo": "true"
///       }
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 2,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "default executor: local
/// executors:
/// - local (default) system=linux device=maintainer url=ws://127.0.0.1:44881
/// - loopback system=linux device=loopback url=ws://127.0.0.1:44881 labels=demo=true"
///       }
///     ],
///     "structuredContent": {
///       "id": 1,
///       "ok": true,
///       "result": {
///         "metadata": {
///           "default": "local",
///           "executors": [
///             {
///               "id": "local",
///               "system": "linux",
///               "device": "maintainer",
///               "labels": {},
///               "url": "ws://127.0.0.1:44881"
///             },
///             {
///               "id": "loopback",
///               "system": "linux",
///               "device": "loopback",
///               "labels": {
///                 "demo": "true"
///               },
///               "url": "ws://127.0.0.1:44881"
///             }
///           ]
///         }
///       },
///       "executor": "caller"
///     }
///   }
/// }
/// ```
///
/// Captured MCP input (list_shells):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 3,
///   "method": "tools/call",
///   "params": {
///     "name": "RemoteExecutorManager",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "method": "list_shells"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 3,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "default:auto
/// interactive:auto
/// settingsPath:/workspace/OSG-Project/RemoteExecutorForSession/.re-setting.json
/// profiles:
/// - bash: candidates=.venv/bin/bash bash /bin/bash sh commandArgs=-lc {command} interactiveArgs=-l
/// - node: candidates=node nodejs commandArgs=-e {command} interactiveArgs=<none>
/// - powershell: candidates=pwsh powershell.exe commandArgs=-NoLogo -NoProfile -NonInteractive -Command {command} interactiveArgs=-NoLogo
/// - python: candidates=.venv/bin/python .venv/Scripts/python.exe venv/bin/python venv/Scripts/python.exe python3 python commandArgs=-c {command} interactiveArgs=<none>"
///       }
///     ],
///     "structuredContent": {
///       "metadata": {
///         "default": "auto",
///         "interactive": "auto",
///         "profiles": {
///           "bash": {
///             "candidates": [
///               ".venv/bin/bash",
///               "bash",
///               "/bin/bash",
///               "sh"
///             ],
///             "commandArgs": [
///               "-lc",
///               "{command}"
///             ],
///             "interactiveArgs": [
///               "-l"
///             ]
///           },
///           "node": {
///             "candidates": [
///               "node",
///               "nodejs"
///             ],
///             "commandArgs": [
///               "-e",
///               "{command}"
///             ],
///             "interactiveArgs": []
///           },
///           "powershell": {
///             "candidates": [
///               "pwsh",
///               "powershell.exe"
///             ],
///             "commandArgs": [
///               "-NoLogo",
///               "-NoProfile",
///               "-NonInteractive",
///               "-Command",
///               "{command}"
///             ],
///             "interactiveArgs": [
///               "-NoLogo"
///             ]
///           },
///           "python": {
///             "candidates": [
///               ".venv/bin/python",
///               ".venv/Scripts/python.exe",
///               "venv/bin/python",
///               "venv/Scripts/python.exe",
///               "python3",
///               "python"
///             ],
///             "commandArgs": [
///               "-c",
///               "{command}"
///             ],
///             "interactiveArgs": []
///           }
///         },
///         "settingsPath": "/workspace/OSG-Project/RemoteExecutorForSession/.re-setting.json"
///       }
///     }
///   }
/// }
/// ```
///
/// Captured MCP input (set_executor_shell):
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 4,
///   "method": "tools/call",
///   "params": {
///     "name": "RemoteExecutorManager",
///     "arguments": {
///       "ExecutorSessionID": "codex-mcp-test",
///       "includeStructuredContent": true,
///       "method": "set_executor_shell",
///       "executor": "local",
///       "shell": "auto"
///     }
///   }
/// }
/// ```
///
/// Captured MCP output:
/// ```text
/// {
///   "jsonrpc": "2.0",
///   "id": 4,
///   "result": {
///     "content": [
///       {
///         "type": "text",
///         "text": "defaultShell:auto
/// settingsPath:/workspace/OSG-Project/RemoteExecutorForSession/.re-setting.json
/// resolution: requested=auto profile=bash program=bash args=-lc <empty> settingsPath=/workspace/OSG-Project/RemoteExecutorForSession/.re-setting.json"
///       }
///     ],
///     "structuredContent": {
///       "metadata": {
///         "defaultShell": "auto",
///         "resolution": {
///           "requested": "auto",
///           "profile": "bash",
///           "program": "bash",
///           "args": [
///             "-lc",
///             ""
///           ],
///           "settingsPath": "/workspace/OSG-Project/RemoteExecutorForSession/.re-setting.json"
///         },
///         "settingsPath": "/workspace/OSG-Project/RemoteExecutorForSession/.re-setting.json"
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
            executor_prop(),
            prop(
                "method",
                string_enum_prop(&[
                    "list_executor",
                    "connect_to_executor",
                    "list_shells",
                    "set_executor_shell",
                ]),
            ),
            prop("id", string_prop("Executor ID")),
            prop("url", string_prop("WebSocket URL for connect_to_executor")),
            prop("system", string_prop("System label")),
            prop("device", string_prop("Device label")),
            prop("labels", object_prop("string")),
            prop(
                "shell",
                string_default("Shell profile name for set_executor_shell", "auto"),
            ),
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
