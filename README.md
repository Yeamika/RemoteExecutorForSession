# RemoteExecutorForSession

`RemoteExecutorForSession` is one Rust library around the `remote_executor` library API.

It exposes two faces from the same library:

- high-speed in-process JSON-RPC function API for batch/parallel calls,
- typed SDK functions for direct host calls,
- embedded MCP-style function calls (`list_tools` / `call_tool`) without stdio, HTTP, SSE, or child-process MCP transport.

It calls RE/REC internals directly through the Rust crate API.

The host platform, such as OpenCode, must provide:

- the current session id,
- hashRef storage reads/writes,
- hashRef parsing policy such as `filename #ABCD`,
- RemoteExecutorManager state and calls,
- RemoteExecutorManager / executor configuration UI and storage,
- exbash task storage,
- permission prompts,
- UI rendering.
- direct RE API call helpers,
- REC output merging helpers,
- hashRef argument injection helpers,
- thin session-aware wrappers around embedded RE calls.
- trait definitions for host-provided hashRef storage and optional host IO,
 - thin session-aware wrappers around embedded RE calls.
- direct RE API call helpers,
- REC output merging helpers,
- fileRef/hash argument injection helpers,
- file stamp/hash extraction helpers,
- thin session-aware wrappers around embedded REC calls.

It should keep REC naming intact: `FileAction`, `mode`, `newFilePath`, `content`, `patchText`, `hashCheckMode`, `hashCode`.

## In-process JSON-RPC

The JSON-RPC API is an in-memory function interface, not a transport.

- no stdio,
- no HTTP,
- no child process,
- batch requests are executed concurrently,
- handlers must be `Send + Sync`.

Hosts can expose it through NAPI, a binary ABI, or any other local binding.

`RemoteExecutorManager` is intentionally host-owned. In OpenCode it maps to the current `executor_manager` tool and should not be reimplemented inside this crate.

RemoteExecutorManager is owned by this library. Hosts may provide UI and persistence policy, but manager calls such as listing, connecting, and setting the default executor are handled here through the embedded RE `Caller` API.
