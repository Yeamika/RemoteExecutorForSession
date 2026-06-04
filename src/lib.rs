pub mod host;
pub mod jsonrpc;
pub mod output;
pub mod mcp;
pub mod refs;
pub mod types;
pub mod rec;
pub mod demos;

pub use host::{
    ExbashSessionStore, ExbashSyncInput, ExbashWorkdirStore, HashRefSessionStore, HostIo,
    RemoteExecutorConfigStore, SessionHost, SessionWorkdirProvider,
};
pub use jsonrpc::{JsonRpcEndpoint, JsonRpcError, JsonRpcErrorObject, JsonRpcHandler, JsonRpcRequest, JsonRpcResponse};
pub use refs::{basename, extract_file_ref_update, file_key_ref, inject_file_ref, label_hash_ref, make_entry_parts, parse_hash_ref, small_hash_code, FileRefInjection, HashRefTarget};
pub use mcp::{text_result, EmbeddedMcp, McpCallContext, McpCallResult, McpContentText, McpToolDef, McpToolHandler, SessionMcpHandler, DummySessionHost, create_session_mcp, create_session_mcp_with_manager, create_default_session_mcp, EXECUTOR_SESSION_PARAM};
pub use types::{ExbashTaskSnapshot, FileRefEntry, FileRefUpdate, FileStamp, RecToolResult, RemoteExecutorConfigSnapshot};
pub use rec::{manager_handle, new_manager};
pub use demos::memory_host::MemorySessionHost;
pub use demos::json_file_host::JsonFileSessionHost;
