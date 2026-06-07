# RemoteExecutorForSession

`RemoteExecutorForSession` is one Rust library around the `remote_executor` library API.

It exposes three faces from the same library:

- high-speed in-process JSON-RPC function API for batch/parallel calls,
- typed SDK functions for direct host calls,
- embedded MCP-style function calls (`list_tools` / `call_tool`) without stdio, HTTP, SSE, or child-process MCP transport.

It calls RE/REC internals directly through the Rust crate API.

The host platform, such as OpenCode, must provide:

- the current session id,
- hashRef storage reads/writes,
- hashRef parsing policy such as `filename #ABCD`,
- exbash task storage,
- permission prompts,
- UI rendering,
- executor configuration UI and storage policy.

This crate provides:

- direct RE API call helpers,
- REC output merging helpers,
- fileRef/hash argument injection helpers,
- file stamp/hash extraction helpers,
- trait definitions for host-provided hashRef storage and optional host IO,
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

`RemoteExecutorManager` calls are handled by this library through the embedded RE `Caller` API. Hosts own the UI, session wiring, and persistence policy around those calls, but should not reimplement manager operations such as listing, connecting, or setting the default executor.
