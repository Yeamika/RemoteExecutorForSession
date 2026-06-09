use std::collections::HashSet;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use crate::demos::memory_host::MemorySessionHost;
use crate::host::{
    ExbashSessionStore, ExbashSyncInput, ExbashWorkdirStore, RemoteExecutorConfigStore,
    EXBASH_TASK_STACK_FULL_MESSAGE,
};
use crate::jsonrpc::{JsonRpcEndpoint, JsonRpcHandler};
use crate::mcp::{create_session_mcp_with_manager, EXECUTOR_SESSION_PARAM};
use crate::rec::{new_manager, ShellManager, ToolContext};
use serde_json::{json, Value};

const SESSION_COUNT: usize = 64;
const CALLS_PER_SESSION: usize = 16;
const EXECUTOR_COUNT: usize = 4;
const DIR_COUNT: usize = 8;
const MATRIX_SESSION_COUNT: usize = 128;
const MATRIX_CALLS_PER_SESSION: usize = 16;
const MATRIX_FILES_PER_SESSION: usize = 4;
const MATRIX_CONFLICT_FILE_COUNT: usize = 16;

fn matrix_conflict_attempts_per_session() -> usize {
    MATRIX_CALLS_PER_SESSION / 2
}

fn matrix_unique_patch_count() -> usize {
    (MATRIX_CALLS_PER_SESSION / 2) - MATRIX_FILES_PER_SESSION
}

fn matrix_session_id(session_idx: usize) -> String {
    format!("matrix_{session_idx:04}")
}

fn matrix_unique_file_name(session_idx: usize, file_idx: usize) -> String {
    format!("matrix_s{session_idx:04}_f{file_idx:02}.txt")
}

fn matrix_unique_patch_target(session_idx: usize, patch_idx: usize) -> usize {
    rng(0xA11CE + session_idx * 4099 + patch_idx * 131) % MATRIX_FILES_PER_SESSION
}

fn matrix_unique_marker(session_idx: usize, file_idx: usize, patch_idx: usize) -> String {
    format!("unique session={session_idx:04} file={file_idx:02} patch={patch_idx:02}\n")
}

fn matrix_conflict_target(session_idx: usize, attempt_idx: usize) -> usize {
    (session_idx * 37 + attempt_idx * 11 + rng(0xC0FFEE + attempt_idx)) % MATRIX_CONFLICT_FILE_COUNT
}

fn matrix_conflict_marker(session_idx: usize, attempt_idx: usize, file_idx: usize) -> String {
    format!("conflict session={session_idx:04} attempt={attempt_idx:02} file={file_idx:02}\n")
}

fn re_shell_settings(default_shell: &str) -> String {
    json!({
        "version": 1,
        "shells": {
            "default": default_shell,
            "interactive": "one",
            "profiles": {
                "one": {
                    "candidates": ["sh"],
                    "commandArgs": ["-c", "echo one-marker; {command}"],
                    "interactiveArgs": []
                },
                "two": {
                    "candidates": ["sh"],
                    "commandArgs": ["-c", "echo two-marker; {command}"],
                    "interactiveArgs": []
                }
            }
        }
    })
    .to_string()
}

fn rng(seed: usize) -> usize {
    let mut x = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x
}

fn executor_name(i: usize) -> &'static str {
    match i % EXECUTOR_COUNT {
        0 => "local",
        1 => "exec_1",
        2 => "exec_2",
        3 => "exec_3",
        _ => "local",
    }
}

fn remote_executor_config(url: &str) -> Value {
    let executors = (1..EXECUTOR_COUNT)
        .map(|i| {
            json!({
                "id": executor_name(i),
                "url": url,
            })
        })
        .collect::<Vec<_>>();
    json!({ "executors": executors })
}

fn one_remote_executor_config(id: &str, url: &str) -> Value {
    json!({
        "executors": [
            {
                "id": id,
                "url": url,
            }
        ]
    })
}

async fn call(
    ep: &JsonRpcEndpoint<impl JsonRpcHandler>,
    session_id: &str,
    tool: &str,
    mut args: Value,
) -> Value {
    if let Some(object) = args.as_object_mut() {
        object.insert(
            EXECUTOR_SESSION_PARAM.to_string(),
            Value::String(session_id.to_string()),
        );
    }
    ep.handle_value(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":tool,"arguments":args}})).await
}

async fn call_structured(
    ep: &JsonRpcEndpoint<impl JsonRpcHandler>,
    session_id: &str,
    tool: &str,
    mut args: Value,
) -> Value {
    if let Some(object) = args.as_object_mut() {
        object.insert("includeStructuredContent".to_string(), Value::Bool(true));
    }
    call(ep, session_id, tool, args).await
}

fn text(r: &Value) -> String {
    r["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("")
        .to_string()
}
fn meta(r: &Value) -> Value {
    r["result"]["structuredContent"].clone()
}
fn ok(r: &Value) -> bool {
    r["error"].is_null()
}
fn is_transient_error(r: &Value) -> bool {
    let msg = r["error"]["message"].as_str().unwrap_or("");
    msg.contains("PTY")
        || msg.contains("ShellManager")
        || msg.contains("another write operation is already running")
}

fn is_write_busy(r: &Value) -> bool {
    r["error"]["message"]
        .as_str()
        .unwrap_or("")
        .contains("another write operation is already running")
}

fn is_hash_mismatch(r: &Value) -> bool {
    r["error"]["message"]
        .as_str()
        .unwrap_or("")
        .contains("hash mismatch")
}

fn assert_remove_warning(r: &Value, async_id: &str, code: &str) {
    assert!(
        r["error"].is_null(),
        "stale remove should return warning success, got: {:?}",
        r["error"]
    );
    let output = text(r);
    assert!(
        output.contains(&format!("warning: {code}"))
            && output.contains(&format!("asyncID:{async_id}")),
        "stale remove warning text should mention code and asyncID, got: {:?}",
        output
    );
    let structured = meta(r);
    assert_eq!(structured["metadata"]["warning"]["code"], code);
    assert_eq!(structured["metadata"]["asyncID"], async_id);
    assert!(
        structured["metadata"]["hostCleanup"]["localRemoved"]
            .as_bool()
            .unwrap_or(false)
            || structured["metadata"]["hostCleanup"]["workspaceRemoved"]
                .as_bool()
                .unwrap_or(false),
        "stale remove should delete at least one host tracking row, got: {structured:?}"
    );
}

fn file_ref_from_text(output: &str) -> Option<String> {
    const START: &str = "<fileRef>";
    const END: &str = "</fileRef>";
    output.lines().find_map(|line| {
        let start = line.find(START)?;
        let rest = &line[start + START.len()..];
        let end = rest.find(END)?;
        let label = rest[..end].trim();
        (!label.is_empty()).then(|| label.to_string())
    })
}

fn binary_append_patch(text: &str) -> String {
    let mut hex = String::with_capacity(text.len() * 2);
    for byte in text.as_bytes() {
        write!(&mut hex, "{byte:02X}").unwrap();
    }
    format!("insert -1\n+{hex}")
}

async fn call_retry_write_busy(
    ep: &JsonRpcEndpoint<impl JsonRpcHandler>,
    session_id: &str,
    tool: &str,
    args: Value,
    seed: usize,
) -> (Value, usize) {
    let mut attempts = 0usize;
    loop {
        attempts += 1;
        let resp = call(ep, session_id, tool, args.clone()).await;
        if !is_write_busy(&resp) {
            return (resp, attempts);
        }
        assert!(
            attempts < 20_000,
            "write stayed busy after {attempts} attempts for {tool}"
        );
        let delay_ms = 1 + (rng(seed.wrapping_add(attempts)) % 9) as u64;
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

fn exbash_limit_task(id: usize) -> ExbashSyncInput {
    ExbashSyncInput {
        async_id: Some(format!("full-{id}")),
        executor: Some("local".to_string()),
        state: Some("stopped".to_string()),
        started_at: Some(id as i64),
        ended_at: Some(id as i64 + 1),
        command: Some(format!("echo {id}")),
        description: Some(format!("task {id}")),
        ..ExbashSyncInput::default()
    }
}

fn exbash_tmp_stopped_task(id: usize) -> ExbashSyncInput {
    ExbashSyncInput {
        async_id: Some(format!("tmp-{id}")),
        executor: Some("local".to_string()),
        state: Some(if id.is_multiple_of(2) {
            "stop".to_string()
        } else {
            "exit:0".to_string()
        }),
        started_at: Some(id as i64),
        ended_at: Some(id as i64 + 1),
        command: Some(format!("echo tmp {id}")),
        description: None,
        ..ExbashSyncInput::default()
    }
}

async fn wait_for_session_exbash_state(
    host: &MemorySessionHost,
    session_id: &str,
    async_id: &str,
    expected_state: &str,
) -> crate::types::ExbashTaskSnapshot {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(snapshot) = host
            .session_exbash_snapshot(session_id, async_id, "local")
            .await
            .unwrap()
        {
            if snapshot.state.as_deref() == Some(expected_state) {
                return snapshot;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for session task {async_id} to reach {expected_state}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_for_workdir_exbash_state(
    host: &MemorySessionHost,
    session_id: &str,
    workdir: &str,
    async_id: &str,
    expected_state: &str,
) -> crate::types::ExbashTaskSnapshot {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(snapshot) = host
            .workdir_exbash_snapshot(session_id, workdir, async_id, "local")
            .await
            .unwrap()
        {
            if snapshot.state.as_deref() == Some(expected_state) {
                return snapshot;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for workdir task {async_id} to reach {expected_state}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[derive(Clone, Debug)]
enum Action {
    Create { name: String, content: String },
    Read { file_idx: usize },
    Patch { file_idx: usize },
    Rename { file_idx: usize, new_name: String },
    Delete { file_idx: usize },
    ExbashRun { command: String },
    ExbashList,
    ExbashStop { task_idx: usize },
    ExbashRemove { task_idx: usize },
    Rg { pattern: String, path: String },
    ManagerList,
}

fn plan_session(si: usize, dir: &str) -> Vec<Action> {
    let mut actions = Vec::with_capacity(CALLS_PER_SESSION);
    let mut live_files: Vec<usize> = Vec::new();
    let mut live_tasks: Vec<usize> = Vec::new();
    let mut stopped_tasks: Vec<usize> = Vec::new();
    let mut file_seq = 0usize;
    let mut task_seq = 0usize;

    for ci in 0..CALLS_PER_SESSION {
        let r = rng(si * 10000 + ci);
        match r % 11 {
            0 => {
                let idx = actions.len();
                live_files.push(idx);
                actions.push(Action::Create {
                    name: format!("s{}_f{}.rs", si, file_seq),
                    content: format!("fn f{}(){{}}\n", file_seq),
                });
                file_seq += 1;
            }
            1 if !live_files.is_empty() => {
                actions.push(Action::Read {
                    file_idx: live_files[r % live_files.len()],
                });
            }
            1 => {
                let idx = actions.len();
                live_tasks.push(idx);
                actions.push(Action::ExbashRun {
                    command: format!("echo t{}", task_seq),
                });
                task_seq += 1;
            }
            2 if !live_files.is_empty() => {
                actions.push(Action::Patch {
                    file_idx: live_files[r % live_files.len()],
                });
            }
            2 => {
                let idx = actions.len();
                live_files.push(idx);
                actions.push(Action::Create {
                    name: format!("s{}_f{}.rs", si, file_seq),
                    content: format!("fn f{}(){{}}\n", file_seq),
                });
                file_seq += 1;
            }
            3 if !live_files.is_empty() => {
                let fi = r % live_files.len();
                actions.push(Action::Rename {
                    file_idx: live_files[fi],
                    new_name: format!("rn_{}_{}.rs", si, file_seq),
                });
                file_seq += 1;
                live_files.remove(fi);
            }
            3 => {
                let idx = actions.len();
                live_tasks.push(idx);
                actions.push(Action::ExbashRun {
                    command: format!("echo t{}", task_seq),
                });
                task_seq += 1;
            }
            4 if !live_files.is_empty() => {
                let fi = r % live_files.len();
                actions.push(Action::Delete {
                    file_idx: live_files[fi],
                });
                live_files.remove(fi);
            }
            4 => {
                let idx = actions.len();
                live_tasks.push(idx);
                actions.push(Action::ExbashRun {
                    command: format!("echo t{}", task_seq),
                });
                task_seq += 1;
            }
            5 => {
                let idx = actions.len();
                live_tasks.push(idx);
                actions.push(Action::ExbashRun {
                    command: format!("echo t{}", task_seq),
                });
                task_seq += 1;
            }
            6 => actions.push(Action::ExbashList),
            7 if !live_tasks.is_empty() => {
                let ti = r % live_tasks.len();
                let idx = live_tasks[ti];
                stopped_tasks.push(idx);
                live_tasks.remove(ti);
                actions.push(Action::ExbashStop { task_idx: idx });
            }
            7 => actions.push(Action::ExbashList),
            8 if !stopped_tasks.is_empty() => {
                let ti = r % stopped_tasks.len();
                let idx = stopped_tasks[ti];
                stopped_tasks.remove(ti);
                actions.push(Action::ExbashRemove { task_idx: idx });
            }
            8 if !live_tasks.is_empty() => {
                let ti = r % live_tasks.len();
                let idx = live_tasks[ti];
                live_tasks.remove(ti);
                actions.push(Action::ExbashRemove { task_idx: idx });
            }
            8 => actions.push(Action::ExbashList),
            9 => actions.push(Action::Rg {
                pattern: "fn".into(),
                path: dir.to_string(),
            }),
            _ => actions.push(Action::ManagerList),
        }
    }
    actions
}

struct SessionResult {
    pass: usize,
    fail: usize,
    pty_skipped: usize,
    files_created: usize,
    file_refs: Vec<Option<String>>,
    task_ids: Vec<Option<String>>,
}

async fn run_session(
    si: usize,
    ep: &JsonRpcEndpoint<impl JsonRpcHandler>,
    dir: &std::path::Path,
    plan: &[Action],
) -> SessionResult {
    let session_id = format!("ses_{:04}", si);
    let mut file_paths: Vec<Option<PathBuf>> = vec![None; plan.len()];
    let mut file_refs: Vec<Option<String>> = vec![None; plan.len()];
    let mut file_executors: Vec<Option<&'static str>> = vec![None; plan.len()];
    let mut task_ids: Vec<Option<String>> = vec![None; plan.len()];
    let mut pass = 0usize;
    let mut fail = 0usize;
    let mut pty_skipped = 0usize;

    for ci in 0..plan.len() {
        let action = &plan[ci];
        let executor = executor_name(rng(si * 1000 + ci));

        let resp = match action {
            Action::Create { name, content } => {
                call(ep, &session_id, "FileAction", json!({"mode":"create","fileKey":dir.join(name).to_string_lossy(),"content":content,"executor":executor})).await
            }
            Action::Read { file_idx } => {
                let file_executor = file_executors[*file_idx].unwrap_or(executor);
                // Prefer hashRef, fallback to direct path
                let key = file_refs[*file_idx].clone()
                    .or_else(|| file_paths[*file_idx].as_ref().map(|p| p.to_string_lossy().to_string()))
                    .unwrap_or_default();
                if key.is_empty() {
                    pty_skipped += 1;
                    continue;
                }
                call(ep, &session_id, "read", json!({"fileKey":key,"executor":file_executor})).await
            }
            Action::Patch { file_idx } => {
                let file_executor = file_executors[*file_idx].unwrap_or(executor);
                let Some(key) = file_refs[*file_idx].clone() else {
                    pty_skipped += 1;
                    continue;
                };
                let patch_text = format!(
                    "***APPEND_HEAD*** 1\n// patched session={si} call={ci}\n***APPEND_END***\n"
                );
                call(ep, &session_id, "FileAction", json!({"mode":"patch","fileKey":key,"patchText":patch_text,"executor":file_executor})).await
            }
            Action::Rename { file_idx, new_name } => {
                let file_executor = file_executors[*file_idx].unwrap_or(executor);
                let key = file_refs[*file_idx].clone()
                    .or_else(|| file_paths[*file_idx].as_ref().map(|p| p.to_string_lossy().to_string()))
                    .unwrap_or_default();
                if key.is_empty() {
                    pty_skipped += 1;
                    continue;
                }
                call(ep, &session_id, "FileAction", json!({"mode":"rename","fileKey":key,"newFilePath":dir.join(new_name).to_string_lossy(),"executor":file_executor})).await
            }
            Action::Delete { file_idx } => {
                let file_executor = file_executors[*file_idx].unwrap_or(executor);
                let key = file_refs[*file_idx].clone()
                    .or_else(|| file_paths[*file_idx].as_ref().map(|p| p.to_string_lossy().to_string()))
                    .unwrap_or_default();
                if key.is_empty() {
                    pty_skipped += 1;
                    continue;
                }
                call(ep, &session_id, "FileAction", json!({"mode":"delete","fileKey":key,"executor":file_executor})).await
            }
            Action::ExbashRun { command } => call_structured(ep, &session_id, "exbash", json!({"mode":"run","command":command,"read_timeout":5000,"executor":executor})).await,
            Action::ExbashList => call(ep, &session_id, "exbash", json!({"mode":"list","executor":executor})).await,
            Action::ExbashStop { task_idx } => match task_ids[*task_idx].as_deref() {
                Some(aid) => call(ep, &session_id, "exbash", json!({"mode":"stop","asyncID":aid,"executor":executor})).await,
                None => { pty_skipped += 1; continue; }
            },
            Action::ExbashRemove { task_idx } => match task_ids[*task_idx].as_deref() {
                Some(aid) => call(ep, &session_id, "exbash", json!({"mode":"remove","asyncID":aid,"executor":executor})).await,
                None => { pty_skipped += 1; continue; }
            },
            Action::Rg { pattern, path } => call(ep, &session_id, "rg", json!({"pattern":pattern,"path":path,"executor":executor})).await,
            Action::ManagerList => call(ep, &session_id, "RemoteExecutorManager", json!({"method":"list_executor","id":si})).await,
        };

        if is_transient_error(&resp) {
            pty_skipped += 1;
            continue;
        }

        let matched = match action {
            Action::Create { name, content: _ } => {
                if ok(&resp) {
                    file_paths[ci] = Some(dir.join(name));
                    file_executors[ci] = Some(executor);
                    let output = text(&resp);
                    let fref = output
                        .lines()
                        .find(|l| l.contains("#"))
                        .map(|l| l.trim().replace("<fileRef>", "").replace("</fileRef>", ""))
                        .filter(|s| !s.is_empty())
                        .unwrap_or_default();
                    if !fref.is_empty() {
                        file_refs[ci] = Some(fref);
                    }
                    output.contains("Created file") || output.contains("Success")
                } else {
                    false
                }
            }
            Action::Read { .. } => ok(&resp),
            Action::Patch { file_idx } => {
                if ok(&resp) {
                    let output = text(&resp);
                    let new_fref = output
                        .lines()
                        .find(|l| l.contains("#"))
                        .map(|l| l.trim().replace("<fileRef>", "").replace("</fileRef>", ""))
                        .filter(|s| !s.is_empty());
                    if let Some(fref) = new_fref {
                        file_refs[*file_idx] = Some(fref);
                    }
                    true
                } else {
                    false
                }
            }
            Action::Rename { file_idx, new_name } => {
                if ok(&resp) {
                    let output = text(&resp);
                    // Extract new fileRef from rename response
                    let new_fref = output
                        .lines()
                        .find(|l| l.contains("#"))
                        .map(|l| l.trim().replace("<fileRef>", "").replace("</fileRef>", ""))
                        .filter(|s| !s.is_empty());
                    if let Some(fref) = new_fref {
                        file_refs[*file_idx] = Some(fref);
                    }
                    // Update file path to new location
                    file_paths[*file_idx] = Some(dir.join(new_name));
                    output.contains("Renamed") || output.contains("Success")
                } else {
                    false
                }
            }
            Action::Delete { file_idx } => {
                if ok(&resp) {
                    file_refs[*file_idx] = None;
                    file_paths[*file_idx] = None;
                    file_executors[*file_idx] = None;
                    true
                } else {
                    false
                }
            }
            Action::ExbashRun { .. } => {
                if ok(&resp) {
                    let m = meta(&resp);
                    let md = m.get("metadata").cloned().unwrap_or(Value::Null);
                    if let Some(aid) = md.get("asyncID").and_then(|v| v.as_str()) {
                        task_ids[ci] = Some(aid.to_string());
                    }
                    let state = md.get("state").and_then(|v| v.as_str());
                    let exit_code = md.get("exitCode").and_then(|v| v.as_i64());
                    state
                        .map(|s| s == "running" || s == "exited")
                        .unwrap_or(exit_code.is_some())
                } else {
                    false
                }
            }
            Action::ExbashList | Action::ExbashStop { .. } | Action::ExbashRemove { .. } => {
                ok(&resp)
            }
            Action::Rg { .. } => ok(&resp),
            Action::ManagerList => ok(&resp) && text(&resp).contains("local"),
        };

        if matched {
            pass += 1;
        } else {
            fail += 1;
            if fail <= 5 {
                eprintln!(
                    "FAIL s{} c{} executor={}: action={:?} resp_error={:?}",
                    si, ci, executor, action, resp["error"]
                );
            }
        }
    }

    SessionResult {
        pass,
        fail,
        pty_skipped,
        files_created: file_paths.iter().flatten().count(),
        file_refs,
        task_ids,
    }
}

struct MatrixUniqueResult {
    busy_retries: usize,
}

struct MatrixConflictResult {
    busy_retries: usize,
    success_markers: Vec<(usize, String)>,
    hash_mismatches: usize,
}

async fn run_matrix_unique_session(
    si: usize,
    ep: &JsonRpcEndpoint<impl JsonRpcHandler>,
    dir: &std::path::Path,
) -> MatrixUniqueResult {
    let session_id = matrix_session_id(si);
    let mut file_refs = Vec::with_capacity(MATRIX_FILES_PER_SESSION);
    let mut expected_markers = vec![Vec::<String>::new(); MATRIX_FILES_PER_SESSION];
    let mut busy_retries = 0usize;

    for file_idx in 0..MATRIX_FILES_PER_SESSION {
        let file_name = matrix_unique_file_name(si, file_idx);
        let content = format!("base session={si:04} file={file_idx:02}\n");
        let (resp, attempts) = call_retry_write_busy(
            ep,
            &session_id,
            "FileAction",
            json!({
                "mode": "create",
                "fileKey": dir.join(&file_name).to_string_lossy(),
                "content": content,
                "executor": "local"
            }),
            0x1000 + si * 97 + file_idx,
        )
        .await;
        busy_retries += attempts - 1;
        assert!(
            resp["error"].is_null(),
            "matrix create failed for {file_name}: {:?}",
            resp["error"]
        );
        let file_ref = file_ref_from_text(&text(&resp))
            .unwrap_or_else(|| panic!("create did not return fileRef for {file_name}: {resp:?}"));
        file_refs.push(file_ref);
    }

    for patch_idx in 0..matrix_unique_patch_count() {
        let file_idx = matrix_unique_patch_target(si, patch_idx);
        let marker = matrix_unique_marker(si, file_idx, patch_idx);
        let (resp, attempts) = call_retry_write_busy(
            ep,
            &session_id,
            "FileAction",
            json!({
                "mode": "patch",
                "fileKey": file_refs[file_idx],
                "patchMode": "binary",
                "patchText": binary_append_patch(&marker),
                "executor": "local"
            }),
            0x2000 + si * 131 + patch_idx,
        )
        .await;
        busy_retries += attempts - 1;
        assert!(
            resp["error"].is_null(),
            "matrix unique patch failed for session={si} file={file_idx}: {:?}",
            resp["error"]
        );
        let file_ref = file_ref_from_text(&text(&resp)).unwrap_or_else(|| {
            panic!("unique patch did not return fileRef for session={si} file={file_idx}: {resp:?}")
        });
        file_refs[file_idx] = file_ref;
        expected_markers[file_idx].push(marker);
    }

    for (file_idx, markers) in expected_markers.iter().enumerate() {
        let file_name = matrix_unique_file_name(si, file_idx);
        let content = std::fs::read_to_string(dir.join(&file_name))
            .unwrap_or_else(|err| panic!("failed to read {file_name}: {err}"));
        assert!(
            content.starts_with(&format!("base session={si:04} file={file_idx:02}\n")),
            "unique file {file_name} lost its base content: {content:?}"
        );
        for marker in markers {
            assert!(
                content.contains(marker),
                "unique file {file_name} missing marker {marker:?}; content={content:?}"
            );
        }
        let actual_marker_count = content
            .lines()
            .filter(|line| line.starts_with("unique session="))
            .count();
        assert_eq!(
            actual_marker_count,
            markers.len(),
            "unique file {file_name} has unexpected marker count; content={content:?}"
        );
    }

    MatrixUniqueResult { busy_retries }
}

async fn read_matrix_conflict_refs(
    si: usize,
    ep: &JsonRpcEndpoint<impl JsonRpcHandler>,
    dir: &std::path::Path,
) -> Vec<Option<String>> {
    let session_id = matrix_session_id(si);
    let mut refs = vec![None; MATRIX_CONFLICT_FILE_COUNT];
    let targets = (0..matrix_conflict_attempts_per_session())
        .map(|attempt_idx| matrix_conflict_target(si, attempt_idx))
        .collect::<HashSet<_>>();

    for file_idx in targets {
        let file_name = format!("matrix_conflict_{file_idx:02}.txt");
        let resp = call(
            ep,
            &session_id,
            "read",
            json!({
                "fileKey": dir.join(&file_name).to_string_lossy(),
                "hashCheckMode": true,
                "executor": "local"
            }),
        )
        .await;
        assert!(
            resp["error"].is_null(),
            "matrix conflict read failed for session={si} file={file_idx}: {:?}",
            resp["error"]
        );
        refs[file_idx] = Some(file_ref_from_text(&text(&resp)).unwrap_or_else(|| {
            panic!(
                "read did not return conflict fileRef for session={si} file={file_idx}: {resp:?}"
            )
        }));
    }

    refs
}

async fn run_matrix_conflict_session(
    si: usize,
    ep: &JsonRpcEndpoint<impl JsonRpcHandler>,
    refs: Vec<Option<String>>,
) -> MatrixConflictResult {
    let session_id = matrix_session_id(si);
    let mut busy_retries = 0usize;
    let mut success_markers = Vec::new();
    let mut hash_mismatches = 0usize;

    for attempt_idx in 0..matrix_conflict_attempts_per_session() {
        let file_idx = matrix_conflict_target(si, attempt_idx);
        let marker = matrix_conflict_marker(si, attempt_idx, file_idx);
        let file_ref = refs[file_idx]
            .as_ref()
            .unwrap_or_else(|| panic!("missing conflict ref session={si} file={file_idx}"));
        let (resp, attempts) = call_retry_write_busy(
            ep,
            &session_id,
            "FileAction",
            json!({
                "mode": "patch",
                "fileKey": file_ref,
                "patchMode": "binary",
                "patchText": binary_append_patch(&marker),
                "executor": "local"
            }),
            0x3000 + si * 193 + attempt_idx,
        )
        .await;
        busy_retries += attempts - 1;
        if resp["error"].is_null() {
            success_markers.push((file_idx, marker));
        } else if is_hash_mismatch(&resp) {
            hash_mismatches += 1;
        } else {
            panic!(
                "matrix conflict patch returned unexpected error for session={si} file={file_idx}: {:?}",
                resp["error"]
            );
        }
    }

    MatrixConflictResult {
        busy_retries,
        success_markers,
        hash_mismatches,
    }
}

#[tokio::test]
async fn stress_rpc_128x16_parallel_random_writes() {
    let start = Instant::now();
    let dir = tempfile::tempdir().unwrap();
    for file_idx in 0..MATRIX_CONFLICT_FILE_COUNT {
        std::fs::write(
            dir.path()
                .join(format!("matrix_conflict_{file_idx:02}.txt")),
            format!("conflict base file={file_idx:02}\n"),
        )
        .unwrap();
    }

    let shared_manager = Arc::new(new_manager().await.unwrap());
    let shell_manager = ShellManager::default_shell(80, 24);
    let mut eps: Vec<JsonRpcEndpoint<_>> = Vec::with_capacity(MATRIX_SESSION_COUNT);
    for si in 0..MATRIX_SESSION_COUNT {
        let session_id = matrix_session_id(si);
        let host = Arc::new(MemorySessionHost::new(
            session_id,
            dir.path().to_string_lossy(),
        ));
        let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
        eps.push(JsonRpcEndpoint::new(create_session_mcp_with_manager(
            ctx,
            host,
            shared_manager.clone(),
            shell_manager.clone(),
        )));
    }

    let lists = futures_util::future::join_all(
        eps.iter()
            .map(|ep| ep.handle_value(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))),
    )
    .await;
    for r in &lists {
        assert_eq!(r["result"]["tools"].as_array().unwrap().len(), 5);
    }

    let unique_started = Instant::now();
    let unique_results = futures_util::future::join_all(
        (0..MATRIX_SESSION_COUNT).map(|si| run_matrix_unique_session(si, &eps[si], dir.path())),
    )
    .await;
    let unique_elapsed = unique_started.elapsed();
    let unique_busy_retries: usize = unique_results
        .iter()
        .map(|result| result.busy_retries)
        .sum();

    let refs_started = Instant::now();
    let conflict_refs = futures_util::future::join_all(
        (0..MATRIX_SESSION_COUNT).map(|si| read_matrix_conflict_refs(si, &eps[si], dir.path())),
    )
    .await;
    let refs_elapsed = refs_started.elapsed();

    let conflict_started = Instant::now();
    let conflict_results = futures_util::future::join_all(
        (0..MATRIX_SESSION_COUNT)
            .map(|si| run_matrix_conflict_session(si, &eps[si], conflict_refs[si].clone())),
    )
    .await;
    let conflict_elapsed = conflict_started.elapsed();

    let mut success_by_file = vec![Vec::<String>::new(); MATRIX_CONFLICT_FILE_COUNT];
    let mut conflict_busy_retries = 0usize;
    let mut hash_mismatches = 0usize;
    for result in &conflict_results {
        conflict_busy_retries += result.busy_retries;
        hash_mismatches += result.hash_mismatches;
        for (file_idx, marker) in &result.success_markers {
            success_by_file[*file_idx].push(marker.clone());
        }
    }
    let conflict_successes: usize = success_by_file.iter().map(Vec::len).sum();
    let conflict_attempts = MATRIX_SESSION_COUNT * matrix_conflict_attempts_per_session();
    assert_eq!(
        conflict_successes + hash_mismatches,
        conflict_attempts,
        "all conflict writes should either succeed once per stale hash or fail by hash mismatch"
    );
    assert!(
        conflict_successes <= MATRIX_CONFLICT_FILE_COUNT,
        "each conflict file should accept at most one stale-hash write"
    );
    assert!(
        conflict_successes > 0,
        "conflict matrix should exercise successful writes before stale hashes are rejected"
    );
    assert!(
        hash_mismatches > 0,
        "conflict matrix should exercise stale hash rejection"
    );

    for (file_idx, expected_markers) in success_by_file.iter().enumerate() {
        assert!(
            expected_markers.len() <= 1,
            "conflict file {file_idx} accepted multiple stale-hash writes: {expected_markers:?}"
        );
        let file_name = format!("matrix_conflict_{file_idx:02}.txt");
        let content = std::fs::read_to_string(dir.path().join(&file_name)).unwrap();
        assert!(
            content.starts_with(&format!("conflict base file={file_idx:02}\n")),
            "conflict file {file_name} lost base content: {content:?}"
        );
        for marker in expected_markers {
            assert!(
                content.contains(marker),
                "conflict file {file_name} missing successful marker {marker:?}: {content:?}"
            );
        }
        let actual_markers = content
            .lines()
            .filter(|line| line.starts_with("conflict session="))
            .collect::<Vec<_>>();
        assert_eq!(
            actual_markers.len(),
            expected_markers.len(),
            "conflict file {file_name} contains untracked writes: {content:?}"
        );
        for marker in actual_markers {
            assert!(
                expected_markers
                    .iter()
                    .any(|expected| expected.trim_end() == marker),
                "conflict file {file_name} contains unexpected marker {marker:?}: {content:?}"
            );
        }
    }

    let elapsed = start.elapsed();
    let matrix_calls = MATRIX_SESSION_COUNT * MATRIX_CALLS_PER_SESSION;
    let ms_per_matrix_call = elapsed.as_millis() as f64 / matrix_calls as f64;
    println!(
        "stress 128x16 random writes: {} matrix calls, {} unique busy retries, {} conflict successes, {} hash mismatches, {} conflict busy retries, {:.1}ms/matrix-call, {:.1}s total (unique {:.1}s, ref-read {:.1}s, conflict {:.1}s)",
        matrix_calls,
        unique_busy_retries,
        conflict_successes,
        hash_mismatches,
        conflict_busy_retries,
        ms_per_matrix_call,
        elapsed.as_secs_f64(),
        unique_elapsed.as_secs_f64(),
        refs_elapsed.as_secs_f64(),
        conflict_elapsed.as_secs_f64()
    );
    assert!(
        ms_per_matrix_call < 300.0,
        "average matrix call latency {:.1}ms exceeds 300ms target",
        ms_per_matrix_call
    );
}

#[tokio::test]
async fn stress_rpc_64x16_parallel() {
    let start = Instant::now();

    // === 4 executors ===
    let caller = new_manager().await.unwrap();
    let lr = crate::rec::manager_handle(
        &caller,
        serde_json::from_value(json!({"id":1,"tool":"list_executor","params":{}})).unwrap(),
    )
    .await;
    let url = serde_json::to_value(&lr.result).unwrap()["metadata"]["executors"][0]["url"]
        .as_str()
        .unwrap()
        .to_string();
    let exec_config = remote_executor_config(&url);

    // === 8 dirs, 64 sessions (shared Caller + ShellManager) ===
    let dirs: Vec<TempDir> = (0..DIR_COUNT)
        .map(|_| tempfile::tempdir().unwrap())
        .collect();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let mut eps: Vec<JsonRpcEndpoint<_>> = Vec::with_capacity(SESSION_COUNT);
    for i in 0..SESSION_COUNT {
        let d = i % DIR_COUNT;
        let host = Arc::new(MemorySessionHost::new(
            format!("ses_{:04}", i),
            dirs[d].path().to_string_lossy(),
        ));
        host.update_remote_executor_config(&dirs[d].path().to_string_lossy(), exec_config.clone())
            .await
            .unwrap();
        let ctx = ToolContext::new(Some(dirs[d].path().to_path_buf()));
        eps.push(JsonRpcEndpoint::new(create_session_mcp_with_manager(
            ctx,
            host,
            shared_manager.clone(),
            shell_manager.clone(),
        )));
    }

    // === tools/list all 64 ===
    let lists: Vec<_> = eps
        .iter()
        .map(|ep| ep.handle_value(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})))
        .collect();
    let lists = futures_util::future::join_all(lists).await;
    for r in &lists {
        assert_eq!(r["result"]["tools"].as_array().unwrap().len(), 5);
    }

    // === Pre-generate plans ===
    let plans: Vec<Vec<Action>> = (0..SESSION_COUNT)
        .map(|si| plan_session(si, &dirs[si % DIR_COUNT].path().to_string_lossy()))
        .collect();
    let total_calls: usize = plans.iter().map(|p| p.len()).sum();
    assert_eq!(total_calls, SESSION_COUNT * CALLS_PER_SESSION);

    // === Run all 64 sessions in parallel ===
    let futures: Vec<_> = (0..SESSION_COUNT)
        .map(|si| run_session(si, &eps[si], dirs[si % DIR_COUNT].path(), &plans[si]))
        .collect();
    let results = futures_util::future::join_all(futures).await;

    let elapsed = start.elapsed();
    let total_pass: usize = results.iter().map(|r| r.pass).sum();
    let total_fail: usize = results.iter().map(|r| r.fail).sum();
    let total_pty: usize = results.iter().map(|r| r.pty_skipped).sum();
    let total_files: usize = results.iter().map(|r| r.files_created).sum();
    let total_tasks: usize = results
        .iter()
        .map(|r| r.task_ids.iter().flatten().count())
        .sum();
    let total_refs: usize = results
        .iter()
        .map(|r| r.file_refs.iter().flatten().count())
        .sum();
    let ms_per_call = elapsed.as_millis() as f64 / total_calls as f64;

    // === Batch RPC ===
    let batch = eps[0].handle_value(json!([
        {"jsonrpc":"2.0","id":1,"method":"tools/list"},
        {"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exbash","arguments":{"mode":"list", EXECUTOR_SESSION_PARAM:"ses_0000"}}},
        {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"RemoteExecutorManager","arguments":{"method":"list_executor","id":"0", EXECUTOR_SESSION_PARAM:"ses_0000"}}}
    ])).await;
    assert_eq!(batch.as_array().unwrap().len(), 3);
    assert!(batch[0]["result"]["tools"].is_array());

    println!("stress 64x16 parallel: {} calls, {} pass, {} fail, {} pty_skipped, {} files, {} hashRefs, {} tasks, {:.1}ms/call, {:.1}s total",
        total_calls, total_pass, total_fail, total_pty, total_files, total_refs, total_tasks, ms_per_call, elapsed.as_secs_f64());
    assert_eq!(total_fail, 0, "{} calls failed", total_fail);
    assert!(
        ms_per_call < 100.0,
        "average call latency {:.1}ms exceeds 100ms target",
        ms_per_call
    );
}

#[tokio::test]
async fn exbash_workdir_tracks_specified_directory() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);

    let session_dir = tempfile::tempdir().unwrap();
    let workdir = tempfile::tempdir().unwrap();

    let host = Arc::new(MemorySessionHost::new(
        "ses_workdir",
        session_dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(session_dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let resp = call(
        &ep,
        "ses_workdir",
        "exbash",
        json!({"mode":"run","command":"pwd","read_timeout":5000}),
    )
    .await;
    assert!(
        resp["error"].is_null(),
        "exbash run without workdir failed: {:?}",
        resp["error"]
    );
    let output = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    println!("exbash without workdir: pwd output = {}", output.trim());

    let resp = call(&ep, "ses_workdir", "exbash", json!({"mode":"run","command":"pwd","workdir":workdir.path().to_string_lossy(),"read_timeout":5000})).await;
    assert!(
        resp["error"].is_null(),
        "exbash run with workdir failed: {:?}",
        resp["error"]
    );

    let output_text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        resp["result"]["structuredContent"].is_null(),
        "structuredContent should be omitted by default"
    );
    assert!(
        output_text.ends_with("exitcode:0"),
        "exbash with workdir should exit 0, got: {}",
        output_text.trim()
    );
    let workdir_path = workdir.path().to_string_lossy();
    println!("exbash with workdir: pwd output = {}", output_text.trim());
    println!("expected workdir = {}", workdir_path);
    assert!(
        output_text.trim().contains(workdir_path.as_ref()),
        "pwd output should contain workdir path, got: {}",
        output_text.trim()
    );

    let session_path = session_dir.path().to_string_lossy();
    assert_ne!(
        session_path.as_ref(),
        workdir_path.as_ref(),
        "session dir and workdir should be different"
    );

    let list = call(&ep, "ses_workdir", "exbash", json!({"mode":"list"})).await;
    assert!(
        list["error"].is_null(),
        "exbash list failed: {:?}",
        list["error"]
    );

    println!(
        "exbash workdir test passed: session_dir={}, workdir={}",
        session_path, workdir_path
    );
}

#[tokio::test]
async fn exbash_session_stack_full_rejects_new_run_before_spawn() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(
        MemorySessionHost::new("ses_session_limit", dir.path().to_string_lossy())
            .with_exbash_task_limit(10),
    );
    for id in 0..10 {
        host.upsert_session_exbash("ses_session_limit", exbash_limit_task(id))
            .await
            .unwrap();
    }
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let blocked = call(
        &ep,
        "ses_session_limit",
        "exbash",
        json!({"mode":"run","command":"echo blocked","read_timeout":5000}),
    )
    .await;
    assert_eq!(blocked["error"]["code"], -32602);
    assert_eq!(
        blocked["error"]["message"].as_str().unwrap_or(""),
        EXBASH_TASK_STACK_FULL_MESSAGE
    );

    let list = call(&ep, "ses_session_limit", "exbash", json!({"mode":"list"})).await;
    assert!(list["error"].is_null(), "list failed: {:?}", list["error"]);
}

#[tokio::test]
async fn exbash_session_stack_full_prunes_tmp_tasks_before_spawn() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(
        MemorySessionHost::new("ses_session_cleanup", dir.path().to_string_lossy())
            .with_exbash_task_limit(10),
    );
    for id in 0..10 {
        host.upsert_session_exbash("ses_session_cleanup", exbash_tmp_stopped_task(id))
            .await
            .unwrap();
    }
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let created = call_structured(
        &ep,
        "ses_session_cleanup",
        "exbash",
        json!({"mode":"run","command":"echo allowed","read_timeout":5000}),
    )
    .await;
    assert!(
        created["error"].is_null(),
        "cleanup should allow exbash create, got: {:?}",
        created["error"]
    );
    assert!(text(&created).contains("allowed"));
    let cleanup = &meta(&created)["metadata"]["hostCleanup"];
    assert_eq!(cleanup["scope"], "local");
    assert_eq!(cleanup["removedCount"], 10);

    let list = call(&ep, "ses_session_cleanup", "exbash", json!({"mode":"list"})).await;
    let list_text = text(&list);
    assert!(
        list_text.contains("local:0 workspace:0") && !list_text.contains("tmp-"),
        "tmp tasks should be removed after cleanup create, got: {list_text:?}"
    );
}

#[tokio::test]
async fn exbash_workspace_stack_full_rejects_new_run_before_spawn() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(
        MemorySessionHost::new("ses_workspace_limit", dir.path().to_string_lossy())
            .with_exbash_task_limit(10),
    );
    let workdir = dir.path().to_string_lossy();
    for id in 0..10 {
        host.upsert_workdir_exbash("ses_workspace_limit", &workdir, exbash_limit_task(id))
            .await
            .unwrap();
    }
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let blocked = call(
        &ep,
        "ses_workspace_limit",
        "exbash",
        json!({"mode":"run","scope":"workspace","command":"echo blocked","read_timeout":5000}),
    )
    .await;
    assert_eq!(blocked["error"]["code"], -32602);
    assert_eq!(
        blocked["error"]["message"].as_str().unwrap_or(""),
        EXBASH_TASK_STACK_FULL_MESSAGE
    );

    let list = call(
        &ep,
        "ses_workspace_limit",
        "exbash",
        json!({"mode":"list","scope":"workspace"}),
    )
    .await;
    assert!(list["error"].is_null(), "list failed: {:?}", list["error"]);
}

#[tokio::test]
async fn exbash_workspace_stack_full_prunes_tmp_tasks_before_spawn() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(
        MemorySessionHost::new("ses_workspace_cleanup", dir.path().to_string_lossy())
            .with_exbash_task_limit(10),
    );
    let workdir = dir.path().to_string_lossy();
    for id in 0..10 {
        host.upsert_workdir_exbash(
            "ses_workspace_cleanup",
            &workdir,
            exbash_tmp_stopped_task(id),
        )
        .await
        .unwrap();
    }
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let created = call_structured(
        &ep,
        "ses_workspace_cleanup",
        "exbash",
        json!({"mode":"run","scope":"workspace","command":"echo allowed-workspace","read_timeout":5000}),
    )
    .await;
    assert!(
        created["error"].is_null(),
        "workspace cleanup should allow exbash create, got: {:?}",
        created["error"]
    );
    assert!(text(&created).contains("allowed-workspace"));
    let cleanup = &meta(&created)["metadata"]["hostCleanup"];
    assert_eq!(cleanup["scope"], "workspace");
    assert_eq!(cleanup["removedCount"], 10);

    let list = call(
        &ep,
        "ses_workspace_cleanup",
        "exbash",
        json!({"mode":"list","scope":"workspace"}),
    )
    .await;
    let list_text = text(&list);
    assert!(
        list_text.contains("local:0 workspace:0") && !list_text.contains("tmp-"),
        "workspace tmp tasks should be removed after cleanup create, got: {list_text:?}"
    );
}

#[tokio::test]
async fn exbash_plaintext_exitcode_defaults_and_list_scope() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(MemorySessionHost::new(
        "ses_exbash_plain",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let run = call(
        &ep,
        "ses_exbash_plain",
        "exbash",
        json!({"mode":"run","command":"echo complete-run","read_timeout":5000}),
    )
    .await;
    assert!(
        run["error"].is_null(),
        "exbash completed run failed: {:?}",
        run["error"]
    );
    let run_text = text(&run);
    assert!(
        run_text.contains("complete-run")
            && run_text.contains("\ncommand:echo complete-run")
            && run_text.contains("\ncwd:")
            && run_text.contains("\ntotaloutput:")
            && run_text.ends_with("bytes\nexitcode:0"),
        "completed run text should include output, command, cwd, totaloutput, and exitcode, got: {:?}",
        run_text
    );

    let default_shell = call(
        &ep,
        "ses_exbash_plain",
        "exbash",
        json!({"command":"echo default-shell","read_timeout":5000}),
    )
    .await;
    assert!(
        default_shell["error"].is_null(),
        "default shell call failed: {:?}",
        default_shell["error"]
    );
    let default_shell_text = text(&default_shell);
    assert!(
        default_shell_text.contains("default-shell")
            && default_shell_text.contains("\ncommand:")
            && default_shell_text.contains("\ncwd:")
            && default_shell_text.contains("\ntotaloutput:")
            && default_shell_text.ends_with("bytes\nexitcode:0"),
        "default shell text should include output, command, cwd, totaloutput, and exitcode, got: {:?}",
        default_shell_text
    );

    let detached = call_structured(
        &ep,
        "ses_exbash_plain",
        "exbash",
        json!({"mode":"run","command":"sleep 5","read_timeout":10}),
    )
    .await;
    assert!(
        detached["error"].is_null(),
        "detached run failed: {:?}",
        detached["error"]
    );
    let detached_text = text(&detached);
    assert!(
        detached_text.starts_with('\n')
            && detached_text.contains("\ncommand:sleep 5")
            && detached_text.contains("\ncwd:")
            && detached_text.contains(" detached"),
        "detached text should show actual command/cwd and detached marker, got: {:?}",
        detached_text
    );
    let async_id = meta(&detached)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();

    let detached_shell = call_structured(
        &ep,
        "ses_exbash_plain",
        "exbash",
        json!({"mode":"shell","command":"sleep 5","read_timeout":10}),
    )
    .await;
    assert!(
        detached_shell["error"].is_null(),
        "detached shell failed: {:?}",
        detached_shell["error"]
    );
    let detached_shell_text = text(&detached_shell);
    assert!(
        detached_shell_text.contains("\ncommand:")
            && detached_shell_text.contains("sleep 5")
            && detached_shell_text.contains("\ncwd:")
            && detached_shell_text.contains(" detached"),
        "detached shell text should show resolved shell command/cwd, got: {:?}",
        detached_shell_text
    );
    let shell_async_id = meta(&detached_shell)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();

    let list = call(&ep, "ses_exbash_plain", "exbash", json!({"mode":"list"})).await;
    assert!(list["error"].is_null(), "list failed: {:?}", list["error"]);
    let list_text = text(&list);
    assert!(
        list_text.contains("local:2 workspace:0")
            && list_text.contains("showing executor=all of local")
            && list_text.contains(&format!("- local:{async_id} running"))
            && list_text.contains(&format!("- local:{shell_async_id} running")),
        "list should be plaintext session view, got: {:?}",
        list_text
    );

    let remote_without_executor = call(
        &ep,
        "ses_exbash_plain",
        "exbash",
        json!({"mode":"list","scope":"remote"}),
    )
    .await;
    assert_eq!(remote_without_executor["error"]["code"], -32602);
    assert!(remote_without_executor["error"]["message"]
        .as_str()
        .unwrap_or("")
        .contains("scope=remote requires executor"));

    let _ = call(
        &ep,
        "ses_exbash_plain",
        "exbash",
        json!({"mode":"stop","asyncID":async_id}),
    )
    .await;
    let _ = call(
        &ep,
        "ses_exbash_plain",
        "exbash",
        json!({"mode":"stop","asyncID":shell_async_id}),
    )
    .await;
}

#[tokio::test]
async fn exbash_local_terminal_events_sync_host_tracking() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(MemorySessionHost::new(
        "ses_exbash_events",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host.clone(),
        shared_manager,
        shell_manager,
    ));

    let exited = call_structured(
        &ep,
        "ses_exbash_events",
        "exbash",
        json!({"mode":"run","command":"sh -lc 'sleep 0.05; exit 7'","read_timeout":0}),
    )
    .await;
    assert!(
        exited["error"].is_null(),
        "detached exit run failed: {:?}",
        exited["error"]
    );
    let exited_id = meta(&exited)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();
    let exited_snapshot =
        wait_for_session_exbash_state(host.as_ref(), "ses_exbash_events", &exited_id, "exit:7")
            .await;
    assert_eq!(exited_snapshot.exit_code, Some(7));
    assert!(exited_snapshot.ended_at.is_some());
    assert!(exited_snapshot.description.is_none());

    let list = call(&ep, "ses_exbash_events", "exbash", json!({"mode":"list"})).await;
    let list_text = text(&list);
    assert!(
        list_text.contains("description= command=sh -lc sleep 0.05; exit 7")
            && !list_text.contains("description=sh -lc"),
        "list should keep missing description empty, got: {:?}",
        list_text
    );

    let timed_out = call_structured(
        &ep,
        "ses_exbash_events",
        "exbash",
        json!({"mode":"run","command":"sleep 5","timeout":50,"read_timeout":0}),
    )
    .await;
    assert!(
        timed_out["error"].is_null(),
        "detached timeout run failed: {:?}",
        timed_out["error"]
    );
    let timed_out_id = meta(&timed_out)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();
    let timed_out_snapshot =
        wait_for_session_exbash_state(host.as_ref(), "ses_exbash_events", &timed_out_id, "timeout")
            .await;
    assert!(timed_out_snapshot.ended_at.is_some());

    let workspace = call_structured(
        &ep,
        "ses_exbash_events",
        "exbash",
        json!({"mode":"run","scope":"workspace","command":"sh -lc 'sleep 0.05; exit 0'","read_timeout":0}),
    )
    .await;
    assert!(
        workspace["error"].is_null(),
        "detached workspace run failed: {:?}",
        workspace["error"]
    );
    let workspace_id = meta(&workspace)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();
    let workdir = dir.path().to_string_lossy();
    let workspace_snapshot = wait_for_workdir_exbash_state(
        host.as_ref(),
        "ses_exbash_events",
        &workdir,
        &workspace_id,
        "exit:0",
    )
    .await;
    assert_eq!(workspace_snapshot.exit_code, Some(0));
}

#[tokio::test]
async fn exbash_detached_run_returns_before_event_sync() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(MemorySessionHost::new(
        "ses_exbash_perf",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host.clone(),
        shared_manager,
        shell_manager,
    ));

    let started = Instant::now();
    let detached = call_structured(
        &ep,
        "ses_exbash_perf",
        "exbash",
        json!({"mode":"run","command":"sh -lc 'sleep 0.4; exit 0'","read_timeout":0}),
    )
    .await;
    let elapsed = started.elapsed();
    println!(
        "detached run returned in {:.2}ms",
        elapsed.as_secs_f64() * 1000.0
    );
    assert!(
        detached["error"].is_null(),
        "detached run failed: {:?}",
        detached["error"]
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "detached run should not wait for process exit, elapsed={elapsed:?}"
    );
    let async_id = meta(&detached)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();

    let snapshot =
        wait_for_session_exbash_state(host.as_ref(), "ses_exbash_perf", &async_id, "exit:0").await;
    assert_eq!(snapshot.exit_code, Some(0));
}

#[tokio::test]
async fn exbash_cleanup_syncs_host_tracking() {
    let caller = new_manager().await.unwrap();
    let remote_caller = new_manager().await.unwrap();
    let remote_list = crate::rec::manager_handle(
        &remote_caller,
        serde_json::from_value(json!({"id":1,"tool":"list_executor","params":{}})).unwrap(),
    )
    .await;
    let remote_url = serde_json::to_value(&remote_list.result).unwrap()["metadata"]["executors"][0]
        ["url"]
        .as_str()
        .unwrap()
        .to_string();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(MemorySessionHost::new(
        "ses_exbash_cleanup",
        dir.path().to_string_lossy(),
    ));
    host.update_remote_executor_config(
        &dir.path().to_string_lossy(),
        one_remote_executor_config("exec_cleanup", &remote_url),
    )
    .await
    .unwrap();
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host.clone(),
        shared_manager.clone(),
        shell_manager,
    ));

    let local = call_structured(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"run","command":"sleep 20","read_timeout":10}),
    )
    .await;
    assert!(
        local["error"].is_null(),
        "local detached run failed: {:?}",
        local["error"]
    );
    let local_async_id = meta(&local)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();

    let remove = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"remove","asyncID":local_async_id}),
    )
    .await;
    assert!(
        remove["error"].is_null(),
        "local remove failed: {:?}",
        remove["error"]
    );
    let list = call(&ep, "ses_exbash_cleanup", "exbash", json!({"mode":"list"})).await;
    let list_text = text(&list);
    assert!(
        list_text.contains("local:0 workspace:0") && !list_text.contains(&local_async_id),
        "local remove should clear host tracking, got: {:?}",
        list_text
    );

    let workspace = call_structured(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"run","scope":"workspace","command":"sleep 20","read_timeout":10}),
    )
    .await;
    assert!(
        workspace["error"].is_null(),
        "workspace detached run failed: {:?}",
        workspace["error"]
    );
    let workspace_async_id = meta(&workspace)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();

    let stop = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"stop","asyncID":workspace_async_id}),
    )
    .await;
    assert!(
        stop["error"].is_null(),
        "workspace stop without scope failed: {:?}",
        stop["error"]
    );
    let local_list = call(&ep, "ses_exbash_cleanup", "exbash", json!({"mode":"list"})).await;
    let local_list_text = text(&local_list);
    assert!(
        local_list_text.contains("local:0 workspace:1"),
        "stop without scope must not create local tracking, got: {:?}",
        local_list_text
    );
    let workspace_list = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"list","scope":"workspace"}),
    )
    .await;
    let workspace_list_text = text(&workspace_list);
    assert!(
        workspace_list_text.contains(&workspace_async_id) && workspace_list_text.contains("stop"),
        "workspace stop should mark existing workspace tracking stopped as stop, got: {:?}",
        workspace_list_text
    );

    let remove = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"remove","asyncID":workspace_async_id}),
    )
    .await;
    assert!(
        remove["error"].is_null(),
        "workspace remove without scope failed: {:?}",
        remove["error"]
    );
    let workspace_list = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"list","scope":"workspace"}),
    )
    .await;
    let workspace_list_text = text(&workspace_list);
    assert!(
        workspace_list_text.contains("local:0 workspace:0")
            && !workspace_list_text.contains(&workspace_async_id),
        "workspace remove should clear host tracking, got: {:?}",
        workspace_list_text
    );

    let stale = call_structured(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"run","scope":"workspace","command":"sleep 20","read_timeout":10}),
    )
    .await;
    assert!(
        stale["error"].is_null(),
        "stale source run failed: {:?}",
        stale["error"]
    );
    let stale_async_id = meta(&stale)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();
    let workspace_list = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"list","scope":"workspace"}),
    )
    .await;
    assert!(
        text(&workspace_list).contains(&stale_async_id),
        "source workspace tracking missing before stale cleanup"
    );

    let other_host = Arc::new(MemorySessionHost::new(
        "ses_exbash_cleanup_other",
        dir.path().to_string_lossy(),
    ));
    other_host
        .update_remote_executor_config(
            &dir.path().to_string_lossy(),
            one_remote_executor_config("exec_cleanup", &remote_url),
        )
        .await
        .unwrap();
    let other_ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let other_ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        other_ctx,
        other_host,
        shared_manager,
        ShellManager::default_shell(80, 24),
    ));
    let external_remove = call(
        &other_ep,
        "ses_exbash_cleanup_other",
        "exbash",
        json!({"mode":"remove","asyncID":stale_async_id}),
    )
    .await;
    assert!(
        external_remove["error"].is_null(),
        "second MCP remove failed: {:?}",
        external_remove["error"]
    );
    let workspace_list = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"list","scope":"workspace"}),
    )
    .await;
    assert!(
        text(&workspace_list).contains(&stale_async_id),
        "source workspace tracking should remain stale before cleanup"
    );

    let remove = call_structured(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"remove","asyncID":stale_async_id}),
    )
    .await;
    assert_remove_warning(&remove, &stale_async_id, "asyncTaskNotFound");
    let workspace_list = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"list","scope":"workspace"}),
    )
    .await;
    let workspace_list_text = text(&workspace_list);
    assert!(
        workspace_list_text.contains("local:0 workspace:0")
            && !workspace_list_text.contains(&stale_async_id),
        "stale remove should clear host tracking through MCP, got: {:?}",
        workspace_list_text
    );

    let remote = call_structured(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"run","command":"sleep 20","read_timeout":10,"executor":"exec_cleanup"}),
    )
    .await;
    assert!(
        remote["error"].is_null(),
        "remote detached run failed: {:?}",
        remote["error"]
    );
    let remote_async_id = meta(&remote)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();
    let remote_list = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"list","executor":"exec_cleanup"}),
    )
    .await;
    assert!(
        text(&remote_list).contains(&remote_async_id),
        "source remote tracking missing before stale cleanup"
    );

    let external_remote_remove = call(
        &other_ep,
        "ses_exbash_cleanup_other",
        "exbash",
        json!({"mode":"remove","asyncID":remote_async_id,"executor":"exec_cleanup"}),
    )
    .await;
    assert!(
        external_remote_remove["error"].is_null(),
        "second MCP remote remove failed: {:?}",
        external_remote_remove["error"]
    );
    let remote_list = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"list","executor":"exec_cleanup"}),
    )
    .await;
    assert!(
        text(&remote_list).contains(&remote_async_id),
        "source remote tracking should remain stale before cleanup"
    );

    let remote_remove = call_structured(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"remove","asyncID":remote_async_id,"executor":"exec_cleanup"}),
    )
    .await;
    assert_remove_warning(&remote_remove, &remote_async_id, "asyncTaskNotFound");
    let remote_list = call(
        &ep,
        "ses_exbash_cleanup",
        "exbash",
        json!({"mode":"list","executor":"exec_cleanup"}),
    )
    .await;
    let remote_list_text = text(&remote_list);
    assert!(
        remote_list_text.contains("local:0 workspace:0")
            && !remote_list_text.contains(&remote_async_id),
        "stale remote remove should clear host tracking through MCP, got: {:?}",
        remote_list_text
    );
}

#[tokio::test]
async fn manager_executor_config_uses_workspace_host_store() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(MemorySessionHost::new(
        "ses_manager_config",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host.clone(),
        shared_manager,
        shell_manager,
    ));

    let list = call_structured(
        &ep,
        "ses_manager_config",
        "RemoteExecutorManager",
        json!({"method":"list_executor"}),
    )
    .await;
    assert!(list["error"].is_null(), "initial list failed: {list:?}");
    assert_eq!(meta(&list)["metadata"]["default"], "local");
    assert_eq!(
        meta(&list)["metadata"]["executors"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let connect = call_structured(
        &ep,
        "ses_manager_config",
        "RemoteExecutorManager",
        json!({
            "method":"connect_to_executor",
            "id":"exec_cfg",
            "url":"127.0.0.1:65535",
            "system":"linux",
            "device":"cfg-device",
            "labels":{"role":"test"}
        }),
    )
    .await;
    assert!(connect["error"].is_null(), "connect failed: {connect:?}");
    let output = text(&connect);
    assert!(
        output.contains("- exec_cfg")
            && output.contains("url=ws://127.0.0.1:65535")
            && output.contains("labels=role=test"),
        "{output}"
    );
    let stored = host
        .read_remote_executor_config(&dir.path().to_string_lossy())
        .await
        .unwrap()
        .config;
    assert_eq!(stored["default"], "local");
    assert_eq!(stored["executors"][0]["id"], "exec_cfg");
    assert_eq!(stored["executors"][0]["url"], "ws://127.0.0.1:65535");

    let set = call_structured(
        &ep,
        "ses_manager_config",
        "RemoteExecutorManager",
        json!({"method":"set_default_executor","id":"exec_cfg"}),
    )
    .await;
    assert!(set["error"].is_null(), "set default failed: {set:?}");
    assert_eq!(meta(&set)["metadata"]["default"], "exec_cfg");
    let stored = host
        .read_remote_executor_config(&dir.path().to_string_lossy())
        .await
        .unwrap()
        .config;
    assert_eq!(stored["default"], "exec_cfg");
}

#[tokio::test]
async fn manager_executor_config_normalizes_legacy_array_on_write() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(MemorySessionHost::new(
        "ses_manager_legacy_config",
        dir.path().to_string_lossy(),
    ));
    host.update_remote_executor_config(
        &dir.path().to_string_lossy(),
        json!([
            {
                "id": "legacy_exec",
                "url": "ws://127.0.0.1:65535"
            }
        ]),
    )
    .await
    .unwrap();
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host.clone(),
        shared_manager,
        shell_manager,
    ));

    let list = call_structured(
        &ep,
        "ses_manager_legacy_config",
        "RemoteExecutorManager",
        json!({"method":"list_executor"}),
    )
    .await;
    assert!(list["error"].is_null(), "legacy list failed: {list:?}");
    assert_eq!(meta(&list)["metadata"]["default"], "local");
    assert!(text(&list).contains("legacy_exec"));

    let set = call_structured(
        &ep,
        "ses_manager_legacy_config",
        "RemoteExecutorManager",
        json!({"method":"set_default_executor","id":"legacy_exec"}),
    )
    .await;
    assert!(set["error"].is_null(), "legacy set failed: {set:?}");
    let stored = host
        .read_remote_executor_config(&dir.path().to_string_lossy())
        .await
        .unwrap()
        .config;
    assert_eq!(stored["default"], "legacy_exec");
    assert_eq!(stored["executors"][0]["id"], "legacy_exec");
}

#[tokio::test]
async fn manager_list_shells_routes_through_executor() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(MemorySessionHost::new(
        "ses_manager_shells",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let response = call(
        &ep,
        "ses_manager_shells",
        "RemoteExecutorManager",
        json!({"method":"list_shells","executor":"local"}),
    )
    .await;
    assert!(
        response["error"].is_null(),
        "list_shells should route through executor, got: {:?}",
        response["error"]
    );
    let output = text(&response);
    assert!(
        output.starts_with("default:")
            && output.contains("\ninteractive:")
            && output.contains("\nsettingsPath:")
            && output.contains("\nprofiles:\n- "),
        "list_shells output should contain plaintext shell settings, got: {:?}",
        output
    );
    assert!(
        response["result"]["structuredContent"].is_null(),
        "structuredContent should be omitted by default"
    );
}

#[tokio::test]
async fn manager_request_reload_rebuilds_workspace_settings_and_reports_plaintext_errors() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let settings_path = dir.path().join(".re-setting.json");
    std::fs::write(&settings_path, re_shell_settings("one")).unwrap();
    let host = Arc::new(MemorySessionHost::new(
        "ses_manager_reload",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let first = call(
        &ep,
        "ses_manager_reload",
        "RemoteExecutorManager",
        json!({"method":"list_shells","executor":"local"}),
    )
    .await;
    assert!(
        first["error"].is_null(),
        "first list failed: {:?}",
        first["error"]
    );
    assert!(text(&first).starts_with("default:one\n"), "{first:?}");

    std::fs::write(&settings_path, re_shell_settings("two")).unwrap();
    let before_reload = call(
        &ep,
        "ses_manager_reload",
        "RemoteExecutorManager",
        json!({"method":"list_shells","executor":"local"}),
    )
    .await;
    assert!(
        before_reload["error"].is_null(),
        "pre-reload list failed: {:?}",
        before_reload["error"]
    );
    assert!(
        text(&before_reload).starts_with("default:one\n"),
        "{before_reload:?}"
    );

    let reload = call(
        &ep,
        "ses_manager_reload",
        "RemoteExecutorManager",
        json!({"method":"request_reload","executor":"local"}),
    )
    .await;
    assert!(
        reload["error"].is_null(),
        "reload failed: {:?}",
        reload["error"]
    );
    let reload_text = text(&reload);
    assert!(
        reload_text.starts_with("reloaded:true\ndefault:two\n"),
        "{reload_text}"
    );

    std::fs::write(&settings_path, "{ broken").unwrap();
    let bad_reload = call(
        &ep,
        "ses_manager_reload",
        "RemoteExecutorManager",
        json!({"method":"request_reload","executor":"local"}),
    )
    .await;
    let error = bad_reload["error"]["message"].as_str().unwrap_or("");
    assert!(error.contains("reloaded:false"), "{bad_reload:?}");
    assert!(
        error.contains(&format!("settingsPath:{}", settings_path.to_string_lossy())),
        "{bad_reload:?}"
    );
    assert!(
        error.contains("error:failed to parse settings file"),
        "{bad_reload:?}"
    );
    assert!(error.contains("cause:"), "{bad_reload:?}");
    assert!(error.contains("line"), "{bad_reload:?}");

    let after_bad = call(
        &ep,
        "ses_manager_reload",
        "RemoteExecutorManager",
        json!({"method":"list_shells","executor":"local"}),
    )
    .await;
    assert!(
        after_bad["error"].is_null(),
        "list after bad reload failed: {:?}",
        after_bad["error"]
    );
    let after_bad_text = text(&after_bad);
    assert!(
        after_bad_text.starts_with("default:two\n"),
        "{after_bad_text}"
    );
    assert!(
        after_bad_text.contains("settingsError:reloaded:false")
            && after_bad_text.contains("error:failed to parse settings file"),
        "{after_bad_text}"
    );
}

#[tokio::test]
async fn file_action_patch_requires_hash_ref() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("patch_requires_hash_ref.txt");
    std::fs::write(&file, "base\n").unwrap();

    let host = Arc::new(MemorySessionHost::new(
        "ses_patch_hash_ref",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let rejected = call(
        &ep,
        "ses_patch_hash_ref",
        "FileAction",
        json!({
            "mode": "patch",
            "fileKey": file.to_string_lossy(),
            "patchText": "***APPEND_HEAD*** 1\ndirect should not write\n***APPEND_END***\n",
            "executor": "local"
        }),
    )
    .await;
    assert_eq!(rejected["error"]["code"], json!(-32602));
    let error_message = rejected["error"]["message"].as_str().unwrap_or("");
    assert!(
        error_message.contains("requires fileKey to be the hashRef label")
            && error_message.contains("Do not pass a direct file path"),
        "direct patch should explain hashRef requirement: {rejected:?}"
    );
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "base\n");

    let read = call(
        &ep,
        "ses_patch_hash_ref",
        "read",
        json!({"fileKey": file.to_string_lossy(), "executor": "local"}),
    )
    .await;
    assert!(read["error"].is_null(), "read failed: {:?}", read["error"]);
    let file_ref = file_ref_from_text(&text(&read))
        .unwrap_or_else(|| panic!("read did not return fileRef: {read:?}"));

    let invalid_patch = call(
        &ep,
        "ses_patch_hash_ref",
        "FileAction",
        json!({
            "mode": "patch",
            "fileKey": file_ref,
            "patchText": "this is not a line patch",
            "executor": "local"
        }),
    )
    .await;
    assert!(
        !invalid_patch["error"].is_null(),
        "invalid line patch should fail: {invalid_patch:?}"
    );
    assert!(
        invalid_patch["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("invalid patchText instruction"),
        "invalid hashRef patch should explain line patch requirement: {invalid_patch:?}"
    );
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "base\n");

    let read = call(
        &ep,
        "ses_patch_hash_ref",
        "read",
        json!({"fileKey": file.to_string_lossy(), "executor": "local"}),
    )
    .await;
    assert!(read["error"].is_null(), "read failed: {:?}", read["error"]);
    let file_ref = file_ref_from_text(&text(&read))
        .unwrap_or_else(|| panic!("read did not return fileRef after invalid patch: {read:?}"));

    let bad_line_patch = call(
        &ep,
        "ses_patch_hash_ref",
        "FileAction",
        json!({
            "mode": "patch",
            "fileKey": file_ref,
            "patchText": "***DELETE*** 2-2\n",
            "executor": "local"
        }),
    )
    .await;
    assert!(
        !bad_line_patch["error"].is_null(),
        "bad line patch should fail: {bad_line_patch:?}"
    );
    let bad_line_message = bad_line_patch["error"]["message"].as_str().unwrap_or("");
    assert!(
        bad_line_message.contains("DELETE line range 2-2 is out of range"),
        "bad line patch should explain exact line mismatch: {bad_line_patch:?}"
    );
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "base\n");

    let read = call(
        &ep,
        "ses_patch_hash_ref",
        "read",
        json!({"fileKey": file.to_string_lossy(), "executor": "local"}),
    )
    .await;
    assert!(read["error"].is_null(), "read failed: {:?}", read["error"]);
    let file_ref = file_ref_from_text(&text(&read))
        .unwrap_or_else(|| panic!("read did not return fileRef after bad line patch: {read:?}"));

    let patched = call(
        &ep,
        "ses_patch_hash_ref",
        "FileAction",
        json!({
            "mode": "patch",
            "fileKey": file_ref,
            "patchText": "***APPEND_HEAD*** 1\nhashRef write\n***APPEND_END***\n",
            "executor": "local"
        }),
    )
    .await;
    assert!(
        patched["error"].is_null(),
        "hashRef patch failed: {:?}",
        patched["error"]
    );
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "base\nhashRef write\n"
    );

    let read = call(
        &ep,
        "ses_patch_hash_ref",
        "read",
        json!({"fileKey": file.to_string_lossy(), "executor": "local"}),
    )
    .await;
    assert!(read["error"].is_null(), "read failed: {:?}", read["error"]);
    let file_ref = file_ref_from_text(&text(&read))
        .unwrap_or_else(|| panic!("read did not return fileRef after valid patch: {read:?}"));

    let structured = call_structured(
        &ep,
        "ses_patch_hash_ref",
        "FileAction",
        json!({
            "mode": "patch",
            "fileKey": file_ref,
            "patchText": "***APPEND_HEAD*** -1\nstructured diff\n***APPEND_END***\n",
            "executor": "local"
        }),
    )
    .await;
    assert!(
        structured["error"].is_null(),
        "structured hashRef patch failed: {:?}",
        structured["error"]
    );
    let diff = structured["result"]["structuredContent"]["metadata"]["diff"]
        .as_str()
        .unwrap_or("");
    assert!(
        diff.contains("+structured diff"),
        "FileAction structuredContent should include diff metadata, got: {structured:?}"
    );
    assert!(
        structured["result"]["structuredContent"]
            .get("output")
            .is_none(),
        "structuredContent should not duplicate model-visible output"
    );
}

#[tokio::test]
async fn rg_plaintext_includes_matches_and_code_footer() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("demo.rs");
    std::fs::write(&file, "fn main() {}\nlet value = 1;\n").unwrap();

    let host = Arc::new(MemorySessionHost::new(
        "ses_rg_plain",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let response = call(
        &ep,
        "ses_rg_plain",
        "rg",
        json!({"pattern":"fn","path":dir.path().to_string_lossy()}),
    )
    .await;
    assert!(
        response["error"].is_null(),
        "rg failed: {:?}",
        response["error"]
    );
    let output = text(&response);
    assert!(
        output.contains("demo.rs:1:1:fn main() {}")
            && output.ends_with("matches:1\nfilesWalked:1\ncode:0"),
        "rg text should include matches/filesWalked/code footer, got: {:?}",
        output
    );
}

#[tokio::test]
async fn rg_content_searches_multiple_paths() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let one = dir.path().join("one");
    let two = dir.path().join("two");
    let three = dir.path().join("three");
    std::fs::create_dir_all(&one).unwrap();
    std::fs::create_dir_all(&two).unwrap();
    std::fs::create_dir_all(&three).unwrap();
    std::fs::write(one.join("a.txt"), "refs-multi-marker one\n").unwrap();
    std::fs::write(two.join("b.txt"), "refs-multi-marker two\n").unwrap();
    std::fs::write(three.join("c.txt"), "refs-multi-marker three\n").unwrap();

    let host = Arc::new(MemorySessionHost::new(
        "ses_rg_multi",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let paths_array = call(
        &ep,
        "ses_rg_multi",
        "rg",
        json!({"pattern":"refs-multi-marker","paths":["one","two"],"max_count":120}),
    )
    .await;
    assert!(
        paths_array["error"].is_null(),
        "rg paths array failed: {:?}",
        paths_array["error"]
    );
    let output = text(&paths_array);
    assert!(
        output.contains("one/a.txt:1:1:refs-multi-marker one")
            && output.contains("two/b.txt:1:1:refs-multi-marker two")
            && output.ends_with("matches:2\nfilesWalked:2\ncode:0"),
        "rg paths array should merge output and footer, got: {output:?}"
    );

    let spaced_path = format!("{} {}", one.to_string_lossy(), two.to_string_lossy());
    let spaced_paths = call(
        &ep,
        "ses_rg_multi",
        "rg",
        json!({"pattern":"refs-multi-marker","path":spaced_path,"max_count":1}),
    )
    .await;
    assert!(
        spaced_paths["error"].is_null(),
        "rg spaced paths failed: {:?}",
        spaced_paths["error"]
    );
    let output = text(&spaced_paths);
    assert!(
        output.contains("one/a.txt:1:1:refs-multi-marker one")
            && !output.contains("two/b.txt")
            && output.ends_with("matches:1\nfilesWalked:1\ncode:0"),
        "rg spaced paths should split absolute paths and honor max_count, got: {output:?}"
    );
}

#[tokio::test]
async fn rg_content_defaults_to_session_workdir_for_local_search() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("small.txt");
    std::fs::write(&file, "refs-local-workdir-marker\n").unwrap();

    let host = Arc::new(MemorySessionHost::new(
        "ses_rg_workdir",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let no_path = call(
        &ep,
        "ses_rg_workdir",
        "rg",
        json!({"pattern":"refs-local-workdir-marker"}),
    )
    .await;
    assert!(
        no_path["error"].is_null(),
        "rg without path failed: {:?}",
        no_path["error"]
    );
    assert!(
        text(&no_path).contains("small.txt:1:1:refs-local-workdir-marker"),
        "rg without path should search the session workdir, got: {:?}",
        text(&no_path)
    );

    let dot_path = call(
        &ep,
        "ses_rg_workdir",
        "rg",
        json!({"pattern":"refs-local-workdir-marker","path":"."}),
    )
    .await;
    assert!(
        dot_path["error"].is_null(),
        "rg with path=. failed: {:?}",
        dot_path["error"]
    );
    assert!(
        text(&dot_path).contains("small.txt:1:1:refs-local-workdir-marker"),
        "rg path=. should search the session workdir, got: {:?}",
        text(&dot_path)
    );
}

#[tokio::test]
async fn rg_content_basename_globs_match_inside_directory_search() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let search_dir = dir.path().join("search-root");
    std::fs::create_dir_all(&search_dir).unwrap();
    std::fs::write(search_dir.join("small.txt"), "refs-glob-marker\n").unwrap();
    std::fs::write(search_dir.join("other.txt"), "refs-glob-marker\n").unwrap();

    let host = Arc::new(MemorySessionHost::new(
        "ses_rg_globs",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let response = call(
        &ep,
        "ses_rg_globs",
        "rg",
        json!({"pattern":"refs-glob-marker","path":"search-root","globs":["small.txt"]}),
    )
    .await;
    assert!(
        response["error"].is_null(),
        "rg with basename glob failed: {:?}",
        response["error"]
    );
    let output = text(&response);
    assert!(
        output.contains("small.txt:1:1:refs-glob-marker") && !output.contains("other.txt"),
        "basename globs should recursively match small.txt only, got: {:?}",
        output
    );
}

#[tokio::test]
async fn rg_files_mode_matches_paths_by_glob_pattern() {
    let caller = new_manager().await.unwrap();
    let shared_manager = Arc::new(caller);
    let shell_manager = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let rust_file = dir.path().join("demo.rs");
    let text_file = dir.path().join("notes.txt");
    std::fs::write(&rust_file, "fn main() {}\n").unwrap();
    std::fs::write(&text_file, "demo\n").unwrap();

    let host = Arc::new(MemorySessionHost::new(
        "ses_rg_files",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(
        ctx,
        host,
        shared_manager,
        shell_manager,
    ));

    let response = call_structured(
        &ep,
        "ses_rg_files",
        "rg",
        json!({"mode":"files","pattern":"*.rs","path":dir.path().to_string_lossy()}),
    )
    .await;
    assert!(
        response["error"].is_null(),
        "rg files mode failed: {:?}",
        response["error"]
    );
    let output = text(&response);
    assert!(
        output.contains("demo.rs")
            && !output.contains("notes.txt")
            && output.ends_with("matches:1\nfilesWalked:2\ncode:0"),
        "rg files mode should return matching paths and footer, got: {:?}",
        output
    );
    assert_eq!(meta(&response)["metadata"]["mode"], "files");
}

#[tokio::test]
async fn executor_routing_with_two_executors() {
    let caller1 = new_manager().await.unwrap();
    let caller2 = new_manager().await.unwrap();

    let list2 = crate::rec::manager_handle(
        &caller2,
        serde_json::from_value(json!({"id":1,"tool":"list_executor","params":{}})).unwrap(),
    )
    .await;
    let url2 = serde_json::to_value(&list2.result).unwrap()["metadata"]["executors"][0]["url"]
        .as_str()
        .unwrap()
        .to_string();

    let shared = Arc::new(caller1);
    let shell = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(MemorySessionHost::new(
        "exec_test",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(ctx, host, shared, shell));

    let connect = call_structured(
        &ep,
        "exec_test",
        "RemoteExecutorManager",
        json!({"method":"connect_to_executor","id":"exec_1","url":url2}),
    )
    .await;
    assert!(
        connect["error"].is_null(),
        "connect exec_1 failed: {:?}",
        connect["error"]
    );
    let execs = meta(&connect)["metadata"]["executors"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(execs.len(), 2);
    assert!(execs.iter().any(|e| e["id"] == "local"));
    assert!(execs.iter().any(|e| e["id"] == "exec_1"));

    let local_file = dir.path().join("local.rs");
    let remote_file = dir.path().join("remote.rs");
    let remote_small_file = dir.path().join("remote-small.txt");
    std::fs::write(&remote_small_file, "refs-remote-glob-marker\n").unwrap();

    let r = call(&ep, "exec_test", "FileAction", json!({"mode":"create","fileKey":local_file.to_string_lossy(),"content":"fn local() {}\n","executor":"local"})).await;
    assert!(
        r["error"].is_null(),
        "FileAction/create on local failed: {:?}",
        r["error"]
    );
    assert!(text(&r).contains("local.rs"));

    let r = call(&ep, "exec_test", "FileAction", json!({"mode":"create","fileKey":remote_file.to_string_lossy(),"content":"fn remote() {}\n","executor":"exec_1"})).await;
    assert!(
        r["error"].is_null(),
        "FileAction/create on exec_1 failed: {:?}",
        r["error"]
    );
    assert!(text(&r).contains("remote.rs"));

    let r = call(
        &ep,
        "exec_test",
        "read",
        json!({"fileKey":local_file.to_string_lossy(),"executor":"local"}),
    )
    .await;
    assert!(
        r["error"].is_null(),
        "read on local failed: {:?}",
        r["error"]
    );
    assert!(text(&r).contains("fn local"));

    let r = call(
        &ep,
        "exec_test",
        "read",
        json!({"fileKey":remote_file.to_string_lossy(),"executor":"exec_1"}),
    )
    .await;
    assert!(
        r["error"].is_null(),
        "read on exec_1 failed: {:?}",
        r["error"]
    );
    assert!(text(&r).contains("fn remote"));

    let r = call(
        &ep,
        "exec_test",
        "exbash",
        json!({"mode":"run","command":"echo from-local","executor":"local","read_timeout":5000}),
    )
    .await;
    assert!(
        r["error"].is_null(),
        "exbash on local failed: {:?}",
        r["error"]
    );
    assert!(text(&r).contains("from-local"));

    let r = call(
        &ep,
        "exec_test",
        "exbash",
        json!({"mode":"run","command":"echo from-remote","executor":"exec_1","read_timeout":5000}),
    )
    .await;
    assert!(
        r["error"].is_null(),
        "exbash on exec_1 failed: {:?}",
        r["error"]
    );
    assert!(text(&r).contains("from-remote"));

    let r = call(
        &ep,
        "exec_test",
        "rg",
        json!({"pattern":"fn local","path":dir.path().to_string_lossy(),"executor":"local"}),
    )
    .await;
    assert!(r["error"].is_null(), "rg on local failed: {:?}", r["error"]);

    let r = call(
        &ep,
        "exec_test",
        "rg",
        json!({"pattern":"fn remote","path":dir.path().to_string_lossy(),"executor":"exec_1"}),
    )
    .await;
    assert!(
        r["error"].is_null(),
        "rg on exec_1 failed: {:?}",
        r["error"]
    );

    let r = call(
        &ep,
        "exec_test",
        "rg",
        json!({"mode":"files","pattern":"remote.rs","path":dir.path().to_string_lossy(),"executor":"exec_1"}),
    )
    .await;
    assert!(
        r["error"].is_null(),
        "rg files mode on exec_1 failed: {:?}",
        r["error"]
    );
    assert!(text(&r).contains("remote.rs"));

    let r = call(
        &ep,
        "exec_test",
        "rg",
        json!({
            "pattern":"refs-remote-glob-marker",
            "path":dir.path().to_string_lossy(),
            "globs":["remote-small.txt"],
            "executor":"exec_1"
        }),
    )
    .await;
    assert!(
        r["error"].is_null(),
        "rg content basename glob on exec_1 failed: {:?}",
        r["error"]
    );
    assert!(text(&r).contains("remote-small.txt"));

    println!("executor routing test passed: local and exec_1 both work");
}
