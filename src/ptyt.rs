use crate::jsonrpc::{JsonRpcEndpoint, JsonRpcHandler};
use crate::types::ExbashTaskSnapshot;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub type RefsPtytSender = mpsc::UnboundedSender<String>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefsPtytRegistration {
    pub session_id: String,
    pub slot_id: String,
}

#[derive(Clone, Debug)]
pub struct RefsPtytActiveTask {
    pub task: ExbashTaskSnapshot,
    pub scope: String,
}

#[derive(Clone, Debug)]
pub struct RefsPtytScheduleCall {
    pub mode: String,
    pub session_id: String,
    pub scope: String,
    include_structured_content: bool,
}

#[derive(Clone, Default)]
pub struct RefsPtytScheduler {
    inner: Arc<Mutex<SchedulerInner>>,
}

#[derive(Clone)]
pub struct RefsPtytGateway<H: JsonRpcHandler> {
    endpoint: Arc<JsonRpcEndpoint<H>>,
    scheduler: RefsPtytScheduler,
}

#[derive(Default)]
struct SchedulerInner {
    slots: BTreeMap<String, PtytSlot>,
    tick: u64,
}

struct PtytSlot {
    session_id: String,
    slot_id: String,
    schedulable: bool,
    assigned_task: Option<String>,
    last_active: u64,
    sender: RefsPtytSender,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum PtytClientMessage {
    #[serde(rename = "refs-ptyt.register")]
    Register {
        #[serde(rename = "sessionID")]
        session_id: String,
        #[serde(rename = "slotID")]
        slot_id: String,
        #[serde(default = "default_schedulable")]
        schedulable: bool,
    },
}

fn default_schedulable() -> bool {
    true
}

impl RefsPtytScheduler {
    pub async fn handle_client_text(
        &self,
        text: &str,
        sender: RefsPtytSender,
    ) -> Result<Option<RefsPtytRegistration>, serde_json::Error> {
        let value: Value = match serde_json::from_str(text) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        if value.get("type").and_then(Value::as_str) != Some("refs-ptyt.register") {
            return Ok(None);
        }
        let PtytClientMessage::Register {
            session_id,
            slot_id,
            schedulable,
        } = serde_json::from_value(value)?;
        let registration = RefsPtytRegistration {
            session_id,
            slot_id,
        };
        self.register(registration.clone(), schedulable, sender)
            .await;
        Ok(Some(registration))
    }

    pub async fn register(
        &self,
        registration: RefsPtytRegistration,
        schedulable: bool,
        sender: RefsPtytSender,
    ) {
        let mut inner = self.inner.lock().await;
        inner.tick = inner.tick.saturating_add(1);
        let last_active = inner.tick;
        let key = slot_key(&registration.session_id, &registration.slot_id);
        inner.slots.insert(
            key,
            PtytSlot {
                session_id: registration.session_id.clone(),
                slot_id: registration.slot_id.clone(),
                schedulable,
                assigned_task: None,
                last_active,
                sender: sender.clone(),
            },
        );
        let _ = sender.send(
            json!({
                "type": "refs-ptyt.registered",
                "slotID": registration.slot_id,
            })
            .to_string(),
        );
    }

    pub async fn unregister(&self, registration: &RefsPtytRegistration) {
        self.inner
            .lock()
            .await
            .slots
            .remove(&slot_key(&registration.session_id, &registration.slot_id));
    }

    pub async fn assign_active_task(&self, active: &RefsPtytActiveTask) -> Option<String> {
        let session_id = active.task.session_id.as_deref()?.to_string();
        let message = refs_ptyt_assign_message(active)?;
        let task_key = active_task_key(active);
        loop {
            let target = {
                let mut inner = self.inner.lock().await;
                let key = select_slot_key(&inner, &session_id)?;
                inner.tick = inner.tick.saturating_add(1);
                let tick = inner.tick;
                let slot = inner.slots.get_mut(&key)?;
                slot.last_active = tick;
                slot.assigned_task = Some(task_key.clone());
                (key, slot.slot_id.clone(), slot.sender.clone())
            };
            if target.2.send(message.clone()).is_ok() {
                return Some(target.1);
            }
            self.inner.lock().await.slots.remove(&target.0);
        }
    }
}

impl<H: JsonRpcHandler> RefsPtytGateway<H> {
    pub fn new(endpoint: Arc<JsonRpcEndpoint<H>>, scheduler: RefsPtytScheduler) -> Self {
        Self {
            endpoint,
            scheduler,
        }
    }

    pub fn scheduler(&self) -> &RefsPtytScheduler {
        &self.scheduler
    }

    pub async fn handle_client_text(
        &self,
        text: &str,
        sender: RefsPtytSender,
    ) -> Result<Option<RefsPtytRegistration>, serde_json::Error> {
        self.scheduler.handle_client_text(text, sender).await
    }

    pub async fn unregister(&self, registration: &RefsPtytRegistration) {
        self.scheduler.unregister(registration).await;
    }

    pub async fn handle_endpoint_value(&self, input: Value) -> Value {
        if let Some(batch) = input.as_array() {
            let mut output = Vec::with_capacity(batch.len());
            for item in batch {
                output.push(self.handle_endpoint_single(item.clone()).await);
            }
            return Value::Array(output);
        }
        self.handle_endpoint_single(input).await
    }

    async fn handle_endpoint_single(&self, input: Value) -> Value {
        let mut input = input;
        let schedule = prepare_input_for_ptyt_schedule(&mut input);
        let mut output = self.endpoint.handle_value(input).await;
        if let Some(schedule) = schedule {
            if let Some(active) = active_task_from_ptyt_response(&schedule, &output) {
                self.scheduler.assign_active_task(&active).await;
            }
            restore_output_after_ptyt_schedule(&mut output, &schedule);
        }
        output
    }
}

pub fn prepare_input_for_ptyt_schedule(input: &mut Value) -> Option<RefsPtytScheduleCall> {
    if input.get("method").and_then(Value::as_str) != Some("tools/call") {
        return None;
    }
    if input.pointer("/params/name").and_then(Value::as_str) != Some("exbash") {
        return None;
    }
    let arguments = input
        .pointer_mut("/params/arguments")
        .and_then(Value::as_object_mut)?;
    if arguments
        .get("refsPtytResolve")
        .and_then(boolish_value)
        .unwrap_or(false)
    {
        return None;
    }
    let mode = arguments
        .get("mode")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("shell")
        .to_string();
    if !matches!(mode.as_str(), "run" | "shell" | "attach") {
        return None;
    }
    let session_id = arguments
        .get("ExecutorSessionID")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    let scope = arguments
        .get("scope")
        .or_else(|| arguments.get("spoe"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("local")
        .to_string();
    let include_structured_content = arguments
        .get("includeStructuredContent")
        .and_then(boolish_value)
        .unwrap_or(false);
    arguments.insert("includeStructuredContent".to_string(), Value::Bool(true));
    Some(RefsPtytScheduleCall {
        mode,
        session_id,
        scope,
        include_structured_content,
    })
}

pub fn restore_output_after_ptyt_schedule(output: &mut Value, call: &RefsPtytScheduleCall) {
    if call.include_structured_content {
        return;
    }
    if let Some(result) = output.get_mut("result").and_then(Value::as_object_mut) {
        result.remove("structuredContent");
    }
}

pub fn active_task_from_ptyt_response(
    call: &RefsPtytScheduleCall,
    output: &Value,
) -> Option<RefsPtytActiveTask> {
    let metadata = output.pointer("/result/structuredContent/metadata")?;
    let task = metadata
        .get("hostSnapshot")
        .and_then(|value| active_task_from_snapshot_value(call, value))
        .or_else(|| {
            metadata
                .get("hostSnapshots")
                .and_then(Value::as_array)
                .and_then(|values| {
                    values
                        .iter()
                        .filter_map(|value| active_task_from_snapshot_value(call, value))
                        .find(|active| active.scope == call.scope)
                        .or_else(|| {
                            values
                                .iter()
                                .find_map(|value| active_task_from_snapshot_value(call, value))
                        })
                })
        })
        .or_else(|| active_task_from_metadata(call, metadata))?;
    Some(task)
}

fn active_task_from_snapshot_value(
    call: &RefsPtytScheduleCall,
    value: &Value,
) -> Option<RefsPtytActiveTask> {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        if !object.contains_key("asyncId") {
            if let Some(async_id) = object.get("asyncID").cloned() {
                object.insert("asyncId".to_string(), async_id);
            }
        }
    }
    let mut task = serde_json::from_value::<ExbashTaskSnapshot>(value).ok()?;
    if task.session_id.is_none() {
        task.session_id = Some(call.session_id.clone());
    }
    let scope = if task.workdir.is_some() {
        "workspace"
    } else {
        call.scope.as_str()
    };
    Some(RefsPtytActiveTask {
        task,
        scope: scope.to_string(),
    })
}

fn active_task_from_metadata(
    call: &RefsPtytScheduleCall,
    metadata: &Value,
) -> Option<RefsPtytActiveTask> {
    let async_id = metadata
        .get("asyncID")
        .or_else(|| metadata.get("asyncId"))
        .and_then(Value::as_str)?
        .to_string();
    let executor = metadata
        .get("executor")
        .and_then(Value::as_str)
        .unwrap_or("local")
        .to_string();
    let task = ExbashTaskSnapshot {
        async_id,
        executor,
        session_id: Some(call.session_id.clone()),
        workdir: (call.scope == "workspace").then(String::new),
        state: metadata
            .get("state")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| Some("running".to_string())),
        pid: metadata.get("pid").and_then(json_i64),
        exit_code: metadata
            .get("exitCode")
            .and_then(json_i64)
            .and_then(|value| i32::try_from(value).ok()),
        started_at: metadata.get("startedAt").and_then(json_i64),
        ended_at: metadata.get("endedAt").and_then(json_i64),
        command: metadata
            .get("command")
            .and_then(Value::as_str)
            .map(str::to_string),
        description: metadata
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_string),
        total_output: metadata.get("totalOutput").and_then(json_i64),
    };
    Some(RefsPtytActiveTask {
        task,
        scope: call.scope.clone(),
    })
}

fn select_slot_key(inner: &SchedulerInner, session_id: &str) -> Option<String> {
    inner
        .slots
        .iter()
        .filter(|(_, slot)| slot.session_id == session_id && slot.schedulable)
        .min_by_key(|(_, slot)| (slot.assigned_task.is_some(), slot.last_active))
        .map(|(key, _)| key.clone())
}

fn refs_ptyt_assign_message(active: &RefsPtytActiveTask) -> Option<String> {
    let mut task = serde_json::to_value(&active.task).ok()?;
    let object = task.as_object_mut()?;
    object.insert("scope".to_string(), Value::String(active.scope.clone()));
    serde_json::to_string(&json!({
        "type": "refs-ptyt.assign",
        "task": task,
    }))
    .ok()
}

fn active_task_key(active: &RefsPtytActiveTask) -> String {
    format!(
        "{}:{}:{}",
        active.scope, active.task.executor, active.task.async_id
    )
}

fn slot_key(session_id: &str, slot_id: &str) -> String {
    format!("{session_id}:{slot_id}")
}

fn boolish_value(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::String(value) => Some(value.eq_ignore_ascii_case("true")),
        _ => None,
    }
}

fn json_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse::<i64>().ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JsonRpcError;
    use async_trait::async_trait;
    use serde_json::json;

    fn task(async_id: &str, session_id: &str) -> RefsPtytActiveTask {
        RefsPtytActiveTask {
            task: ExbashTaskSnapshot {
                async_id: async_id.to_string(),
                executor: "local".to_string(),
                session_id: Some(session_id.to_string()),
                workdir: None,
                state: Some("running".to_string()),
                pid: None,
                exit_code: None,
                started_at: Some(1),
                ended_at: None,
                command: Some("sleep 10".to_string()),
                description: Some("demo".to_string()),
                total_output: None,
            },
            scope: "local".to_string(),
        }
    }

    struct ExbashEndpoint;

    #[async_trait]
    impl JsonRpcHandler for ExbashEndpoint {
        async fn call(&self, method: &str, params: Value) -> Result<Value, JsonRpcError> {
            if method != "tools/call" {
                return Err(JsonRpcError::method_not_found(method));
            }
            assert_eq!(params["name"], "exbash");
            assert_eq!(params["arguments"]["includeStructuredContent"], true);
            Ok(json!({
                "content": [],
                "structuredContent": {
                    "metadata": {
                        "hostSnapshot": {
                            "asyncId": "rex-gateway",
                            "executor": "local",
                            "sessionId": "ses",
                            "state": "running"
                        }
                    }
                }
            }))
        }
    }

    #[tokio::test]
    async fn scheduler_fills_empty_slots_before_lru_replacement() {
        let scheduler = RefsPtytScheduler::default();
        let (first_tx, mut first_rx) = mpsc::unbounded_channel();
        let (second_tx, mut second_rx) = mpsc::unbounded_channel();
        scheduler
            .register(
                RefsPtytRegistration {
                    session_id: "ses".to_string(),
                    slot_id: "a".to_string(),
                },
                true,
                first_tx,
            )
            .await;
        scheduler
            .register(
                RefsPtytRegistration {
                    session_id: "ses".to_string(),
                    slot_id: "b".to_string(),
                },
                true,
                second_tx,
            )
            .await;
        assert!(first_rx.recv().await.unwrap().contains("registered"));
        assert!(second_rx.recv().await.unwrap().contains("registered"));

        assert_eq!(
            scheduler.assign_active_task(&task("one", "ses")).await,
            Some("a".to_string())
        );
        assert!(first_rx
            .recv()
            .await
            .unwrap()
            .contains("\"asyncId\":\"one\""));
        assert_eq!(
            scheduler.assign_active_task(&task("two", "ses")).await,
            Some("b".to_string())
        );
        assert!(second_rx
            .recv()
            .await
            .unwrap()
            .contains("\"asyncId\":\"two\""));
        assert_eq!(
            scheduler.assign_active_task(&task("three", "ses")).await,
            Some("a".to_string())
        );
        assert!(first_rx
            .recv()
            .await
            .unwrap()
            .contains("\"asyncId\":\"three\""));
    }

    #[test]
    fn schedule_prepare_forces_and_restores_structured_content() {
        let mut input = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "exbash",
                "arguments": {
                    "ExecutorSessionID": "ses",
                    "mode": "shell"
                }
            }
        });
        let call = prepare_input_for_ptyt_schedule(&mut input).unwrap();
        assert_eq!(
            input["params"]["arguments"]["includeStructuredContent"],
            json!(true)
        );
        let mut output = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "content": [],
                "structuredContent": {
                    "metadata": {
                        "hostSnapshot": {
                            "asyncId": "rex-1",
                            "executor": "local",
                            "sessionId": "ses",
                            "state": "running"
                        }
                    }
                }
            }
        });
        let active = active_task_from_ptyt_response(&call, &output).unwrap();
        assert_eq!(active.task.async_id, "rex-1");
        restore_output_after_ptyt_schedule(&mut output, &call);
        assert!(output["result"].get("structuredContent").is_none());
    }

    #[tokio::test]
    async fn gateway_schedules_active_exbash_without_leaking_structured_content() {
        let gateway = RefsPtytGateway::new(
            Arc::new(JsonRpcEndpoint::new(ExbashEndpoint)),
            RefsPtytScheduler::default(),
        );
        let (tx, mut rx) = mpsc::unbounded_channel();
        let registered = gateway
            .handle_client_text(
                r#"{"type":"refs-ptyt.register","sessionID":"ses","slotID":"slot-a"}"#,
                tx,
            )
            .await
            .unwrap();
        assert_eq!(
            registered,
            Some(RefsPtytRegistration {
                session_id: "ses".to_string(),
                slot_id: "slot-a".to_string(),
            })
        );
        assert!(rx.recv().await.unwrap().contains("registered"));

        let output = gateway
            .handle_endpoint_value(json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "exbash",
                    "arguments": {
                        "ExecutorSessionID": "ses",
                        "mode": "shell"
                    }
                }
            }))
            .await;
        assert_eq!(output["id"], 9);
        assert!(output["result"].get("structuredContent").is_none());
        let assign = rx.recv().await.unwrap();
        assert!(assign.contains("\"refs-ptyt.assign\""));
        assert!(assign.contains("\"asyncId\":\"rex-gateway\""));
    }
}
