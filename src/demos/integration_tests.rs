use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

use crate::demos::memory_host::MemorySessionHost;
use crate::host::{
    ExbashSessionStore, ExbashSyncInput, ExbashWorkdirStore, EXBASH_TASK_STACK_FULL_MESSAGE,
};
use crate::jsonrpc::{JsonRpcEndpoint, JsonRpcHandler};
use crate::mcp::{create_session_mcp_with_manager, EXECUTOR_SESSION_PARAM};
use crate::rec::{new_manager, ShellManager, ToolContext};
use serde_json::{json, Value};

const SESSION_COUNT: usize = 64;
const CALLS_PER_SESSION: usize = 16;
const EXECUTOR_COUNT: usize = 4;
const DIR_COUNT: usize = 8;

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
                let key = file_refs[*file_idx].clone()
                    .or_else(|| file_paths[*file_idx].as_ref().map(|p| p.to_string_lossy().to_string()))
                    .unwrap_or_default();
                if key.is_empty() {
                    pty_skipped += 1;
                    continue;
                }
                call(ep, &session_id, "FileAction", json!({"mode":"patch","fileKey":key,"patchText":"insert -1\n+// patched\n","executor":file_executor})).await
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
            Action::Create { name, .. } => {
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
    for i in 1..EXECUTOR_COUNT {
        let r = crate::rec::manager_handle(&caller, serde_json::from_value(json!({"id":i+1,"tool":"connect_to_executor","params":{"id":executor_name(i),"url":url}})).unwrap()).await;
        assert!(r.ok);
    }
    let all = crate::rec::manager_handle(
        &caller,
        serde_json::from_value(json!({"id":999,"tool":"list_executor","params":{}})).unwrap(),
    )
    .await;
    assert_eq!(
        serde_json::to_value(&all.result).unwrap()["metadata"]["executors"]
            .as_array()
            .unwrap()
            .len(),
        EXECUTOR_COUNT
    );

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
            && run_text.contains("\ntotaloutput:")
            && run_text.ends_with("bytes\nexitcode:0"),
        "completed run text should include output, totaloutput, and exitcode, got: {:?}",
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
            && default_shell_text.contains("\ntotaloutput:")
            && default_shell_text.ends_with("bytes\nexitcode:0"),
        "default shell text should include output, totaloutput, and exitcode, got: {:?}",
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
        detached_text.starts_with('\n') && detached_text.contains(" detached"),
        "detached text should start on a new line, got: {:?}",
        detached_text
    );
    let async_id = meta(&detached)["metadata"]["asyncID"]
        .as_str()
        .unwrap()
        .to_string();

    let list = call(&ep, "ses_exbash_plain", "exbash", json!({"mode":"list"})).await;
    assert!(list["error"].is_null(), "list failed: {:?}", list["error"]);
    let list_text = text(&list);
    assert!(
        list_text.contains("local:1 workspace:0")
            && list_text.contains("showing executor=all of local")
            && list_text.contains(&format!("- local:{async_id} running")),
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
        output.contains("demo.rs:1:1:fn main() {}") && output.ends_with("matches:1\ncode:0"),
        "rg text should include matches/code footer, got: {:?}",
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
            && output.ends_with("matches:1\ncode:0"),
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

    let connect = crate::rec::manager_handle(
        &caller1,
        serde_json::from_value(
            json!({"id":2,"tool":"connect_to_executor","params":{"id":"exec_1","url":url2}}),
        )
        .unwrap(),
    )
    .await;
    assert!(connect.ok, "connect exec_1 failed: {:?}", connect.error);

    let list1 = crate::rec::manager_handle(
        &caller1,
        serde_json::from_value(json!({"id":3,"tool":"list_executor","params":{}})).unwrap(),
    )
    .await;
    let execs = serde_json::to_value(&list1.result).unwrap()["metadata"]["executors"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(execs.len(), 2);
    assert!(execs.iter().any(|e| e["id"] == "local"));
    assert!(execs.iter().any(|e| e["id"] == "exec_1"));

    let shared = Arc::new(caller1);
    let shell = ShellManager::default_shell(80, 24);
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(MemorySessionHost::new(
        "exec_test",
        dir.path().to_string_lossy(),
    ));
    let ctx = ToolContext::new(Some(dir.path().to_path_buf()));
    let ep = JsonRpcEndpoint::new(create_session_mcp_with_manager(ctx, host, shared, shell));

    let local_file = dir.path().join("local.rs");
    let remote_file = dir.path().join("remote.rs");

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

    println!("executor routing test passed: local and exec_1 both work");
}
