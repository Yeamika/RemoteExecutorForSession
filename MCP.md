# MCP in RemoteExecutorForSession

This crate provides an in-process MCP-style interface for session-aware hosts.

It does not start MCP transports. It provides:

- a JSON-RPC endpoint for batch/parallel calls,
- an embedded MCP adapter for `tools/list` and `tools/call`,
- built-in tool wiring for:
  - `FileAction`
  - `read`
  - `rg`
  - `file_transfer`
  - `exbash`
  - `RemoteExecutorManager`

## What hosts must provide

- session id
- hashRef storage read/write
- hashRef policy such as `filename #ABCD`
- permission prompts
- UI rendering

## What this crate provides

- `EmbeddedMcp`
- `JsonRpcEndpoint`
- `create_default_session_mcp()`
- `SessionMcpHandler`
- `McpToolHandler`
- `ExecutorSessionID` argument extraction from `tools/call`

## `ExecutorSessionID`

Each built-in MCP tool declares `ExecutorSessionID` in its schema manually.

The MCP adapter removes `ExecutorSessionID` from `arguments` before `call_tool` and passes it through `McpCallContext`.

This allows hosts to track the target executor/session without forcing schema-level injection by the adapter.
