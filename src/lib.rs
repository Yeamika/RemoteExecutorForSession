pub mod demos;
pub mod host;
pub mod jsonrpc;
pub mod mcp;
pub mod output;
pub mod ptyt;
pub mod rec;
pub mod refs;
pub mod types;

pub use demos::json_file_host::JsonFileSessionHost;
pub use demos::memory_host::MemorySessionHost;
pub use host::{
    ExbashSessionStore, ExbashSyncInput, ExbashWorkdirStore, HashRefSessionStore, HostIo,
    RemoteExecutorConfigStore, SessionHost, SessionWorkdirProvider,
};
pub use jsonrpc::{
    JsonRpcEndpoint, JsonRpcError, JsonRpcErrorObject, JsonRpcHandler, JsonRpcRequest,
    JsonRpcResponse,
};
pub use mcp::{
    create_default_session_mcp, create_session_mcp, create_session_mcp_with_manager, text_result,
    DummySessionHost, EmbeddedMcp, McpCallContext, McpCallResult, McpContentText, McpToolDef,
    McpToolHandler, SessionMcpHandler, EXECUTOR_SESSION_PARAM,
};
pub use ptyt::{
    active_task_from_ptyt_response, prepare_input_for_ptyt_schedule,
    restore_output_after_ptyt_schedule, RefsPtytActiveTask, RefsPtytGateway, RefsPtytRegistration,
    RefsPtytScheduler, RefsPtytSender,
};
pub use rec::{manager_handle, new_manager};
pub use refs::{
    basename, extract_file_ref_update, file_key_ref, inject_file_ref, label_hash_ref,
    make_entry_parts, parse_hash_ref, small_hash_code, FileRefInjection, HashRefTarget,
};
pub use types::{
    ExbashTaskSnapshot, FileRefEntry, FileRefUpdate, FileStamp, RecToolResult,
    RemoteExecutorConfigSnapshot,
};
