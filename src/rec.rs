pub use remote_executor::{
    dispatch_tool, exbash, file_action, file_hash_code, file_stamp, glob_paths, hash_bytes,
    read_path, rg_search, stat_path, Caller, ConnectExecutorOptions, ExbashOptions, ExbashOutput,
    ExecutorRequest, ExecutorResponse, FileActionMode, FileActionOptions, FileKind,
    FileStamp as RecFileStamp, GlobOptions, PatchFile, PatchMode, ReadMode, ReadOptions, RgOptions,
    RgOutput, SetDefaultExecutorOptions, ShellManager, StatOptions, ToolContext, ToolResult,
};
use serde_json::Value;

pub async fn call_tool(
    method: &str,
    params: Value,
    ctx: &ToolContext,
) -> anyhow::Result<ToolResult> {
    dispatch_tool(method, params, ctx).await
}

pub async fn call_file_action(
    options: FileActionOptions,
    ctx: &ToolContext,
) -> anyhow::Result<ToolResult> {
    file_action(options, ctx).await
}

pub fn call_read(options: ReadOptions, ctx: &ToolContext) -> anyhow::Result<ToolResult> {
    read_path(options, ctx)
}

pub async fn call_rg(options: RgOptions, ctx: &ToolContext) -> anyhow::Result<ToolResult> {
    dispatch_tool("rg", serde_json::to_value(options)?, ctx).await
}

pub async fn call_exbash(options: ExbashOptions, ctx: &ToolContext) -> anyhow::Result<ToolResult> {
    exbash(options, ctx).await
}

pub async fn new_manager() -> anyhow::Result<Caller> {
    Caller::new().await
}

pub async fn manager_handle(manager: &Caller, request: ExecutorRequest) -> ExecutorResponse {
    manager.handle(request).await
}
