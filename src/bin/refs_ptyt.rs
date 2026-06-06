use anyhow::{anyhow, Context, Result};
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::style::{Attribute, SetAttribute};
use crossterm::terminal::{self, ClearType};
use crossterm::{cursor, execute, queue};
use futures_util::{SinkExt, StreamExt};
use pty_t_protocol::{clamp_size, ClientText, ServerText};
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::{stdout, Stdout, Write};
use std::thread;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

type ClientWsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
    Message,
>;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    server_url: String,

    #[arg(long)]
    session: String,
}

#[derive(Clone, Debug, Default)]
struct TaskList {
    local_count: usize,
    workspace_count: usize,
    tasks: Vec<ExbashTask>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExbashTask {
    #[serde(rename = "asyncId", alias = "asyncID")]
    async_id: String,
    #[serde(default = "default_scope")]
    scope: String,
    #[serde(default = "default_executor")]
    executor: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    exit_code: Option<Value>,
    #[serde(default)]
    started_at: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    AttachInput,
    AttachLink,
    AttachCommand,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommandTarget {
    Input,
    Identity,
    List,
    Stop,
    Remove,
    Quit,
}

#[derive(Clone, Debug)]
struct AttachTarget {
    task: ExbashTask,
    pty_url: String,
}

#[derive(Clone, Debug)]
struct AttachView {
    id: String,
    role: String,
    pty_cols: u16,
    pty_rows: u16,
    exit_code: Option<u32>,
    local_cols: u16,
    local_rows: u16,
    mode: Mode,
    command_target: CommandTarget,
    ctrl_c_count: u8,
    message: Option<String>,
}

#[derive(Clone, Copy)]
struct TerminalSize {
    local_cols: u16,
    local_rows: u16,
    desired_cols: u16,
    desired_rows: u16,
}

#[derive(Clone, Debug)]
enum LocalEvent {
    Key(KeyEvent),
    Resize { cols: u16, rows: u16 },
    Quit,
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self> {
        terminal::enable_raw_mode()?;
        execute!(
            stdout(),
            terminal::EnterAlternateScreen,
            event::EnableMouseCapture,
            cursor::Hide
        )?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(
            stdout(),
            cursor::Show,
            event::DisableMouseCapture,
            terminal::LeaveAlternateScreen
        );
        let _ = terminal::disable_raw_mode();
    }
}

#[derive(Clone)]
struct RefsMcpClient {
    endpoint: WsMcpEndpoint,
    session: String,
}

#[derive(Clone)]
struct WsMcpEndpoint {
    url: String,
}

#[derive(Default)]
struct Metrics {
    tx_bytes: u64,
    rx_bytes: u64,
    last_output: Option<Instant>,
    rtt: Option<Duration>,
    next_ping_seq: u64,
    pending_ping: Option<(u64, Instant)>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = RefsMcpClient::new(&args.server_url, args.session.clone())?;
    let size = resolve_terminal_size()?;
    let _guard = TerminalGuard::enter()?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    spawn_input_thread(tx);

    let mut out = stdout();
    let mut selected = 0usize;
    let mut scroll = 0usize;
    let mut message = None::<String>;
    let mut list = load_task_list(&client).await.unwrap_or_else(|err| {
        message = Some(err.to_string());
        TaskList::default()
    });
    draw_list(&mut out, &list, selected, scroll, message.as_deref())?;

    loop {
        let Some(event) = rx.recv().await else {
            break;
        };
        match event {
            LocalEvent::Quit => break,
            LocalEvent::Resize { .. } => {
                draw_list(&mut out, &list, selected, scroll, message.as_deref())?;
            }
            LocalEvent::Key(key) => match key.code {
                KeyCode::Char('q') | KeyCode::Char('Q') => break,
                KeyCode::Char('r') | KeyCode::Char('R') => {
                    message = Some("refreshing".to_string());
                    draw_list(&mut out, &list, selected, scroll, message.as_deref())?;
                    match load_task_list(&client).await {
                        Ok(next) => {
                            list = next;
                            if selected >= list.tasks.len() {
                                selected = list.tasks.len().saturating_sub(1);
                            }
                            scroll = clamp_scroll(scroll, selected, list.tasks.len());
                            message = None;
                        }
                        Err(err) => message = Some(err.to_string()),
                    }
                    draw_list(&mut out, &list, selected, scroll, message.as_deref())?;
                }
                KeyCode::Up => {
                    selected = selected.saturating_sub(1);
                    scroll = clamp_scroll(scroll, selected, list.tasks.len());
                    draw_list(&mut out, &list, selected, scroll, message.as_deref())?;
                }
                KeyCode::Down => {
                    if selected + 1 < list.tasks.len() {
                        selected += 1;
                    }
                    scroll = clamp_scroll(scroll, selected, list.tasks.len());
                    draw_list(&mut out, &list, selected, scroll, message.as_deref())?;
                }
                KeyCode::Enter => {
                    let Some(task) = list.tasks.get(selected).cloned() else {
                        continue;
                    };
                    message = Some("connecting".to_string());
                    draw_list(&mut out, &list, selected, scroll, message.as_deref())?;
                    match resolve_attach_target(&client, task).await {
                        Ok(target) => {
                            let action = run_attach(&client, target, &mut rx, size).await;
                            match action {
                                AttachAction::Quit => break,
                                AttachAction::List => {
                                    match load_task_list(&client).await {
                                        Ok(next) => {
                                            list = next;
                                            if selected >= list.tasks.len() {
                                                selected = list.tasks.len().saturating_sub(1);
                                            }
                                            scroll =
                                                clamp_scroll(scroll, selected, list.tasks.len());
                                            message = None;
                                        }
                                        Err(err) => message = Some(err.to_string()),
                                    }
                                    draw_list(
                                        &mut out,
                                        &list,
                                        selected,
                                        scroll,
                                        message.as_deref(),
                                    )?;
                                }
                            }
                        }
                        Err(err) => {
                            message = Some(err.to_string());
                            draw_list(&mut out, &list, selected, scroll, message.as_deref())?;
                        }
                    }
                }
                _ => {}
            },
        }
    }

    Ok(())
}

async fn load_task_list(client: &RefsMcpClient) -> Result<TaskList> {
    let local = client.exbash_list("local").await?;
    let workspace = client.exbash_list("workspace").await?;
    let local_meta = list_metadata(&local)?;
    let workspace_meta = list_metadata(&workspace)?;
    let local_tasks = tasks_from_metadata(local_meta, "local")?;
    let workspace_tasks = tasks_from_metadata(workspace_meta, "workspace")?;
    let local_count = usize_field(local_meta, "localCount").unwrap_or(local_tasks.len());
    let workspace_count =
        usize_field(workspace_meta, "workspaceCount").unwrap_or(workspace_tasks.len());
    let mut tasks = Vec::new();
    tasks.extend(workspace_tasks);
    tasks.extend(local_tasks);
    tasks.sort_by(|a, b| {
        let ar = is_running(a);
        let br = is_running(b);
        br.cmp(&ar).then_with(|| {
            b.started_at
                .unwrap_or_default()
                .cmp(&a.started_at.unwrap_or_default())
        })
    });
    Ok(TaskList {
        local_count,
        workspace_count,
        tasks,
    })
}

async fn resolve_attach_target(client: &RefsMcpClient, task: ExbashTask) -> Result<AttachTarget> {
    let _ = client
        .call_tool(
            "exbash",
            json!({
                "mode": "attach",
                "asyncID": task.async_id,
                "executor": task.executor,
                "scope": task.scope,
                "read_timeout": 0
            }),
        )
        .await?;
    let url = client
        .executor_url(&task.executor)
        .await?
        .ok_or_else(|| anyhow!("executor URL not found: {}", task.executor))?;
    Ok(AttachTarget { task, pty_url: url })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttachAction {
    List,
    Quit,
}

async fn run_attach(
    client: &RefsMcpClient,
    target: AttachTarget,
    rx: &mut mpsc::UnboundedReceiver<LocalEvent>,
    size: TerminalSize,
) -> AttachAction {
    match run_attach_inner(client, target, rx, size).await {
        Ok(action) => action,
        Err(err) => {
            let mut out = stdout();
            let _ = draw_error_message(&mut out, &err.to_string());
            time::sleep(Duration::from_millis(1200)).await;
            AttachAction::List
        }
    }
}

async fn run_attach_inner(
    client: &RefsMcpClient,
    target: AttachTarget,
    rx: &mut mpsc::UnboundedReceiver<LocalEvent>,
    size: TerminalSize,
) -> Result<AttachAction> {
    let TerminalSize {
        local_cols,
        local_rows,
        desired_cols,
        desired_rows,
    } = size;

    let (ws, _) = connect_async(&target.pty_url)
        .await
        .with_context(|| format!("connect {}", target.pty_url))?;
    let (mut ws_write, mut ws_read) = ws.split();
    let id = random_client_id();
    let hello = ClientText::Hello {
        id: id.clone(),
        pty: target.task.async_id.clone(),
        cols: desired_cols,
        rows: desired_rows,
    };
    ws_write
        .send(Message::Text(serde_json::to_string(&hello)?.into()))
        .await?;

    let mut out = stdout();
    let mut parser = vt100::Parser::new(desired_rows, desired_cols, 2000);
    let mut view = AttachView {
        id,
        role: "Viewer".to_string(),
        pty_cols: desired_cols,
        pty_rows: desired_rows,
        exit_code: exit_u32(&target.task),
        local_cols,
        local_rows,
        mode: Mode::AttachInput,
        command_target: CommandTarget::Input,
        ctrl_c_count: 0,
        message: None,
    };
    let mut metrics = Metrics::default();
    metrics.next_ping_seq = 1;
    render_attach(&mut out, &parser, &view, &target.task, &metrics)?;

    let mut ping_tick = time::interval_at(
        time::Instant::now() + Duration::from_secs(3),
        Duration::from_secs(3),
    );
    ping_tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut ctrl_c_streak = 0u8;

    loop {
        tokio::select! {
            event = rx.recv() => {
                let Some(event) = event else { return Ok(AttachAction::List); };
                match event {
                    LocalEvent::Quit => return Ok(AttachAction::Quit),
                    LocalEvent::Resize { cols, rows } => {
                        view.local_cols = cols;
                        view.local_rows = rows;
                        let pty_rows = rows.saturating_sub(1).max(1);
                        let resize = ClientText::Resize { cols, rows: pty_rows };
                        let text = serde_json::to_string(&resize)?;
                        metrics.record_tx(text.len());
                        ws_write.send(Message::Text(text.into())).await?;
                        render_attach(&mut out, &parser, &view, &target.task, &metrics)?;
                    }
                    LocalEvent::Key(key) => {
                        match process_attach_key(
                            client,
                            key,
                            &parser,
                            &mut view,
                            &target.task,
                            &mut metrics,
                            &mut out,
                            &mut ws_write,
                            &mut ctrl_c_streak,
                        ).await? {
                            AttachActionRequest::Continue => {}
                            AttachActionRequest::List => return Ok(AttachAction::List),
                            AttachActionRequest::Quit => return Ok(AttachAction::Quit),
                        }
                    }
                }
            }
            _ = ping_tick.tick() => {
                if metrics.ping_due() {
                    let seq = metrics.note_ping_sent();
                    metrics.record_tx(8);
                    ws_write.send(Message::Ping(seq.to_be_bytes().to_vec().into())).await?;
                }
            }
            msg = ws_read.next() => {
                let Some(msg) = msg else { return Ok(AttachAction::List); };
                match msg? {
                    Message::Binary(data) => {
                        metrics.record_rx(data.len(), true);
                        parser.process(&data);
                        render_attach(&mut out, &parser, &view, &target.task, &metrics)?;
                    }
                    Message::Text(text) => {
                        metrics.record_rx(text.len(), false);
                        match serde_json::from_str::<ServerText>(&text) {
                            Ok(ServerText::Meta { id, pty: _, role, cols, rows, exit_code }) => {
                                view.id = id;
                                view.role = role;
                                view.pty_cols = cols;
                                view.pty_rows = rows;
                                view.exit_code = exit_code;
                                parser.screen_mut().set_size(rows, cols);
                                render_attach(&mut out, &parser, &view, &target.task, &metrics)?;
                            }
                            Ok(ServerText::Error { message }) | Ok(ServerText::Info { message }) => {
                                view.message = Some(message);
                                render_attach(&mut out, &parser, &view, &target.task, &metrics)?;
                            }
                            Ok(ServerText::Sessions { .. }) | Ok(ServerText::Session { .. }) => {}
                            Err(_) => {}
                        }
                    }
                    Message::Close(_) => return Ok(AttachAction::List),
                    Message::Ping(data) => {
                        metrics.record_rx(data.len(), false);
                        metrics.record_tx(data.len());
                        ws_write.send(Message::Pong(data)).await?;
                    }
                    Message::Pong(data) => {
                        metrics.record_rx(data.len(), false);
                        if metrics.note_pong(&data) {
                            render_attach(&mut out, &parser, &view, &target.task, &metrics)?;
                        }
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttachActionRequest {
    Continue,
    List,
    Quit,
}

async fn process_attach_key(
    client: &RefsMcpClient,
    key: KeyEvent,
    parser: &vt100::Parser,
    view: &mut AttachView,
    task: &ExbashTask,
    metrics: &mut Metrics,
    out: &mut Stdout,
    ws_write: &mut ClientWsSink,
    ctrl_c_streak: &mut u8,
) -> Result<AttachActionRequest> {
    match view.mode {
        Mode::AttachInput | Mode::AttachLink => {
            if key.modifiers.contains(KeyModifiers::CONTROL)
                && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'))
            {
                *ctrl_c_streak = ctrl_c_streak.saturating_add(1);
                view.ctrl_c_count = *ctrl_c_streak;
                let bytes = vec![0x03];
                metrics.record_tx(bytes.len());
                ws_write.send(Message::Binary(bytes.into())).await?;
                if *ctrl_c_streak >= 3 {
                    *ctrl_c_streak = 0;
                    view.ctrl_c_count = 0;
                    view.mode = Mode::AttachCommand;
                    view.command_target = CommandTarget::Input;
                }
                render_attach(out, parser, view, task, metrics)?;
                return Ok(AttachActionRequest::Continue);
            }

            let had_ctrl_c_hint = *ctrl_c_streak > 0;
            *ctrl_c_streak = 0;
            view.ctrl_c_count = 0;

            if matches!(key.code, KeyCode::Tab) {
                view.mode = if view.mode == Mode::AttachInput {
                    Mode::AttachLink
                } else {
                    Mode::AttachInput
                };
                let bytes = b"\t".to_vec();
                metrics.record_tx(bytes.len());
                ws_write.send(Message::Binary(bytes.into())).await?;
                render_attach(out, parser, view, task, metrics)?;
                return Ok(AttachActionRequest::Continue);
            }

            if let Some(bytes) = key_to_bytes(key) {
                metrics.record_tx(bytes.len());
                ws_write.send(Message::Binary(bytes.into())).await?;
            }
            if had_ctrl_c_hint {
                render_attach(out, parser, view, task, metrics)?;
            }
            Ok(AttachActionRequest::Continue)
        }
        Mode::AttachCommand => {
            *ctrl_c_streak = 0;
            view.ctrl_c_count = 0;
            match key.code {
                KeyCode::Esc => {
                    view.mode = Mode::AttachInput;
                    view.command_target = CommandTarget::Input;
                }
                KeyCode::Enter => match view.command_target {
                    CommandTarget::Input => view.mode = Mode::AttachInput,
                    CommandTarget::Identity => {
                        let msg = serde_json::to_string(&ClientText::RequestControl)?;
                        metrics.record_tx(msg.len());
                        ws_write.send(Message::Text(msg.into())).await?;
                        view.mode = Mode::AttachInput;
                    }
                    CommandTarget::List => return Ok(AttachActionRequest::List),
                    CommandTarget::Stop => {
                        view.message = Some("stopping".to_string());
                        render_attach(out, parser, view, task, metrics)?;
                        match client.exbash_control("stop", task).await {
                            Ok(_) => view.message = Some("stopped".to_string()),
                            Err(err) => view.message = Some(err.to_string()),
                        }
                    }
                    CommandTarget::Remove => {
                        view.message = Some("removing".to_string());
                        render_attach(out, parser, view, task, metrics)?;
                        match client.exbash_control("remove", task).await {
                            Ok(_) => return Ok(AttachActionRequest::List),
                            Err(err) => view.message = Some(err.to_string()),
                        }
                    }
                    CommandTarget::Quit => return Ok(AttachActionRequest::Quit),
                },
                KeyCode::Left => view.command_target = previous_command(view.command_target),
                KeyCode::Right | KeyCode::Tab => {
                    view.command_target = next_command(view.command_target)
                }
                _ => {}
            }
            render_attach(out, parser, view, task, metrics)?;
            Ok(AttachActionRequest::Continue)
        }
    }
}

impl RefsMcpClient {
    fn new(server_url: &str, session: String) -> Result<Self> {
        Ok(Self {
            endpoint: WsMcpEndpoint::parse(server_url)?,
            session,
        })
    }

    async fn exbash_list(&self, scope: &str) -> Result<Value> {
        self.call_tool(
            "exbash",
            json!({
                "mode": "list",
                "scope": scope
            }),
        )
        .await
    }

    async fn exbash_control(&self, mode: &str, task: &ExbashTask) -> Result<Value> {
        self.call_tool(
            "exbash",
            json!({
                "mode": mode,
                "asyncID": task.async_id,
                "executor": task.executor,
                "scope": task.scope
            }),
        )
        .await
    }

    async fn executor_url(&self, executor: &str) -> Result<Option<String>> {
        let result = self
            .call_tool(
                "RemoteExecutorManager",
                json!({
                    "method": "list_executor"
                }),
            )
            .await?;
        let executors = result
            .pointer("/structuredContent/result/metadata/executors")
            .or_else(|| result.pointer("/structuredContent/metadata/executors"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(executors.into_iter().find_map(|item| {
            let id = item.get("id")?.as_str()?;
            if id != executor {
                return None;
            }
            item.get("url").and_then(Value::as_str).map(str::to_string)
        }))
    }

    async fn call_tool(&self, name: &str, mut arguments: Value) -> Result<Value> {
        let object = arguments
            .as_object_mut()
            .ok_or_else(|| anyhow!("tool arguments must be an object"))?;
        object.insert(
            "ExecutorSessionID".to_string(),
            Value::String(self.session.clone()),
        );
        object.insert("includeStructuredContent".to_string(), Value::Bool(true));
        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments
            }
        });
        let response = self.endpoint.call_json(&request).await?;
        if let Some(error) = response.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("REFS MCP error");
            return Err(anyhow!(message.to_string()));
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }
}

impl WsMcpEndpoint {
    fn parse(input: &str) -> Result<Self> {
        if !(input.starts_with("ws://") || input.starts_with("wss://")) {
            return Err(anyhow!(
                "refs-ptyt requires a ws:// or wss:// REFS MCP endpoint"
            ));
        }
        Ok(Self { url: input.into() })
    }

    async fn call_json(&self, value: &Value) -> Result<Value> {
        let request_id = value.get("id").cloned().unwrap_or(Value::Null);
        let (ws, _) = connect_async(&self.url)
            .await
            .with_context(|| format!("connect {}", self.url))?;
        let (mut write, mut read) = ws.split();
        write
            .send(Message::Text(serde_json::to_string(value)?.into()))
            .await?;

        while let Some(message) = read.next().await {
            match message? {
                Message::Text(text) => {
                    let response: Value = serde_json::from_str(&text)?;
                    if response.get("id") == Some(&request_id) || request_id.is_null() {
                        return Ok(response);
                    }
                }
                Message::Ping(data) => write.send(Message::Pong(data)).await?,
                Message::Close(_) => break,
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        Err(anyhow!("REFS MCP WebSocket closed before responding"))
    }
}

impl Metrics {
    fn record_tx(&mut self, len: usize) {
        self.tx_bytes += len as u64;
    }

    fn record_rx(&mut self, len: usize, output: bool) {
        self.rx_bytes += len as u64;
        if output {
            self.last_output = Some(Instant::now());
        }
    }

    fn ping_due(&self) -> bool {
        match self.pending_ping {
            None => true,
            Some((_, sent_at)) => sent_at.elapsed() >= Duration::from_secs(5),
        }
    }

    fn note_ping_sent(&mut self) -> u64 {
        let seq = self.next_ping_seq;
        self.next_ping_seq = self.next_ping_seq.wrapping_add(1);
        self.pending_ping = Some((seq, Instant::now()));
        seq
    }

    fn note_pong(&mut self, data: &[u8]) -> bool {
        if data.len() != 8 {
            return false;
        }
        let mut seq_bytes = [0u8; 8];
        seq_bytes.copy_from_slice(data);
        let seq = u64::from_be_bytes(seq_bytes);
        let Some((pending_seq, sent_at)) = self.pending_ping.take() else {
            return false;
        };
        if seq != pending_seq {
            self.pending_ping = Some((pending_seq, sent_at));
            return false;
        }
        self.rtt = Some(sent_at.elapsed());
        true
    }

    fn latency_text(&self) -> String {
        self.rtt
            .map(format_duration)
            .unwrap_or_else(|| "?".to_string())
    }

    fn idle_text(&self) -> String {
        self.last_output
            .map(|instant| format_duration(instant.elapsed()))
            .unwrap_or_else(|| "?".to_string())
    }
}

fn list_metadata(result: &Value) -> Result<&Value> {
    result
        .pointer("/structuredContent/metadata")
        .ok_or_else(|| anyhow!("REFS list response did not include structuredContent metadata"))
}

fn tasks_from_metadata(metadata: &Value, scope: &str) -> Result<Vec<ExbashTask>> {
    let tasks = metadata
        .get("tasks")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    tasks
        .into_iter()
        .map(|mut value| {
            if let Some(object) = value.as_object_mut() {
                object.insert("scope".to_string(), Value::String(scope.to_string()));
            }
            serde_json::from_value(value)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn usize_field(value: &Value, field: &str) -> Option<usize> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

fn draw_list(
    out: &mut Stdout,
    list: &TaskList,
    selected: usize,
    scroll: usize,
    message: Option<&str>,
) -> Result<()> {
    let (cols, rows) = terminal::size()?;
    queue!(out, cursor::Hide, terminal::Clear(ClearType::All))?;
    let body_rows = rows.saturating_sub(1) as usize;
    let visible_slots = body_rows / 2;
    for (slot, task) in list
        .tasks
        .iter()
        .skip(scroll)
        .take(visible_slots)
        .enumerate()
    {
        let index = scroll + slot;
        let y = (slot * 2) as u16;
        let selected_row = index == selected;
        queue!(
            out,
            cursor::MoveTo(0, y),
            terminal::Clear(ClearType::CurrentLine)
        )?;
        if selected_row {
            queue!(out, SetAttribute(Attribute::Reverse))?;
        }
        write!(
            out,
            "{} {}",
            task_state_icon(task),
            trim_to_width(&task_title(task), cols.saturating_sub(5) as usize)
        )?;
        queue!(out, SetAttribute(Attribute::Reset))?;
        if y + 1 < rows.saturating_sub(1) {
            queue!(
                out,
                cursor::MoveTo(0, y + 1),
                terminal::Clear(ClearType::CurrentLine)
            )?;
            if selected_row {
                queue!(out, SetAttribute(Attribute::Reverse))?;
            }
            write!(
                out,
                "    {}",
                trim_to_width(&task_subtitle(task), cols.saturating_sub(4) as usize)
            )?;
            queue!(out, SetAttribute(Attribute::Reset))?;
        }
    }
    if list.tasks.is_empty() && body_rows > 0 {
        queue!(out, cursor::MoveTo(0, 0))?;
        write!(out, "No exbash tasks")?;
    }
    if let Some(message) = message {
        let y = rows.saturating_sub(2);
        queue!(
            out,
            cursor::MoveTo(0, y),
            terminal::Clear(ClearType::CurrentLine)
        )?;
        write!(out, "{}", trim_to_width(message, cols as usize))?;
    }
    draw_list_status(out, list.local_count, list.workspace_count, cols, rows)?;
    out.flush()?;
    Ok(())
}

fn draw_list_status(
    out: &mut Stdout,
    local: usize,
    workspace: usize,
    cols: u16,
    rows: u16,
) -> Result<()> {
    let left = format!("local {local}; workspace {workspace}  | enter attach  r refresh  q quit");
    draw_status_with_marker(out, rows.saturating_sub(1), cols, &left, "[#]")
}

fn render_attach(
    out: &mut Stdout,
    parser: &vt100::Parser,
    view: &AttachView,
    task: &ExbashTask,
    metrics: &Metrics,
) -> Result<()> {
    let content_rows = view.local_rows.saturating_sub(1);
    let draw_cols = view.local_cols.min(view.pty_cols);
    queue!(out, cursor::Hide, SetAttribute(Attribute::Reset))?;
    let rows = parser
        .screen()
        .rows_formatted(0, draw_cols)
        .take(content_rows as usize)
        .collect::<Vec<_>>();
    for y in 0..content_rows {
        queue!(
            out,
            cursor::MoveTo(0, y),
            SetAttribute(Attribute::Reset),
            terminal::Clear(ClearType::CurrentLine)
        )?;
        if let Some(row) = rows.get(y as usize) {
            out.write_all(row)?;
        }
    }
    if let Some(message) = view.message.as_deref() {
        draw_status_with_marker(
            out,
            view.local_rows.saturating_sub(1),
            view.local_cols,
            message,
            "[#]",
        )?;
    } else {
        draw_attach_status(out, view, task, metrics)?;
    }
    if view.mode == Mode::AttachCommand {
        queue!(
            out,
            cursor::MoveTo(0, view.local_rows.saturating_sub(1)),
            cursor::Show
        )?;
    } else {
        let (cur_row, cur_col) = parser.screen().cursor_position();
        if cur_row < content_rows && cur_col < view.local_cols {
            queue!(out, cursor::MoveTo(cur_col, cur_row), cursor::Show)?;
        }
    }
    out.flush()?;
    Ok(())
}

fn draw_attach_status(
    out: &mut Stdout,
    view: &AttachView,
    task: &ExbashTask,
    metrics: &Metrics,
) -> Result<()> {
    let role = role_symbol(&view.role);
    let state = attach_state_icon(view, task);
    let body = match view.mode {
        Mode::AttachInput if view.ctrl_c_count > 0 => {
            format!("[ctrl c x{}] x3 to command mode", view.ctrl_c_count)
        }
        Mode::AttachInput => format!(
            "[>] [{role}:{}]  {state} {}:{}  {}",
            view.id,
            task.executor,
            task.async_id,
            task_title(task)
        ),
        Mode::AttachLink => format!(
            "[~] [{role}:{}]  rtt={} rx={} tx={} idle={}  {state} {}:{}",
            view.id,
            metrics.latency_text(),
            format_bytes(metrics.rx_bytes),
            format_bytes(metrics.tx_bytes),
            metrics.idle_text(),
            task.executor,
            task.async_id
        ),
        Mode::AttachCommand => format!(
            "[:] {}  {state} {}:{}",
            command_targets_text(view.command_target),
            task.executor,
            task.async_id
        ),
    };
    draw_status_with_marker(
        out,
        view.local_rows.saturating_sub(1),
        view.local_cols,
        &body,
        "[#]",
    )
}

fn draw_status_with_marker(
    out: &mut Stdout,
    row: u16,
    cols: u16,
    left: &str,
    marker: &str,
) -> Result<()> {
    queue!(
        out,
        cursor::MoveTo(0, row),
        SetAttribute(Attribute::Reverse),
        terminal::Clear(ClearType::CurrentLine)
    )?;
    let width = cols as usize;
    let marker_len = marker.chars().count();
    let max_left = width.saturating_sub(marker_len + 1);
    let left = trim_to_width(left, max_left);
    let left_len = left.chars().count();
    if left_len + 1 + marker_len <= width {
        write!(
            out,
            "{left}{:>pad$}",
            marker,
            pad = width.saturating_sub(left_len)
        )?;
    } else {
        write!(out, "{}", trim_to_width(marker, width))?;
    }
    queue!(out, SetAttribute(Attribute::Reset))?;
    Ok(())
}

fn draw_error_message(out: &mut Stdout, message: &str) -> Result<()> {
    let (cols, rows) = terminal::size()?;
    draw_status_with_marker(
        out,
        rows.saturating_sub(1),
        cols,
        &format!("error: {message}"),
        "[#]",
    )?;
    out.flush()?;
    Ok(())
}

fn spawn_input_thread(tx: mpsc::UnboundedSender<LocalEvent>) {
    thread::spawn(move || {
        while let Ok(event) = event::read() {
            match event {
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    if key.code == KeyCode::Char(']')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        let _ = tx.send(LocalEvent::Quit);
                        break;
                    }
                    let _ = tx.send(LocalEvent::Key(key));
                }
                Event::Resize(cols, rows) => {
                    let _ = tx.send(LocalEvent::Resize { cols, rows });
                }
                _ => {}
            }
        }
    });
}

fn key_to_bytes(key: KeyEvent) -> Option<Vec<u8>> {
    let mut bytes = match key.code {
        KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
            ctrl_byte(c).map(|b| vec![b])?
        }
        KeyCode::Char(c) => {
            let mut buf = [0u8; 4];
            c.encode_utf8(&mut buf).as_bytes().to_vec()
        }
        KeyCode::Enter => b"\r".to_vec(),
        KeyCode::Backspace => vec![0x7f],
        KeyCode::Tab => b"\t".to_vec(),
        KeyCode::Esc => vec![0x1b],
        KeyCode::Up => b"\x1b[A".to_vec(),
        KeyCode::Down => b"\x1b[B".to_vec(),
        KeyCode::Right => b"\x1b[C".to_vec(),
        KeyCode::Left => b"\x1b[D".to_vec(),
        KeyCode::Home => b"\x1b[H".to_vec(),
        KeyCode::End => b"\x1b[F".to_vec(),
        KeyCode::PageUp => b"\x1b[5~".to_vec(),
        KeyCode::PageDown => b"\x1b[6~".to_vec(),
        KeyCode::Delete => b"\x1b[3~".to_vec(),
        KeyCode::Insert => b"\x1b[2~".to_vec(),
        _ => return None,
    };
    if key.modifiers.contains(KeyModifiers::ALT) {
        let mut prefixed = vec![0x1b];
        prefixed.append(&mut bytes);
        Some(prefixed)
    } else {
        Some(bytes)
    }
}

fn ctrl_byte(c: char) -> Option<u8> {
    let c = c.to_ascii_lowercase();
    match c {
        'a'..='z' => Some((c as u8) - b'a' + 1),
        '[' => Some(0x1b),
        '\\' => Some(0x1c),
        ']' => Some(0x1d),
        '^' => Some(0x1e),
        '_' => Some(0x1f),
        '?' => Some(0x7f),
        _ => None,
    }
}

fn resolve_terminal_size() -> Result<TerminalSize> {
    let (local_cols, local_rows) = terminal::size()?;
    let desired_cols = local_cols;
    let desired_rows = local_rows.saturating_sub(1).max(1);
    let (desired_cols, desired_rows) = clamp_size(desired_cols, desired_rows);
    Ok(TerminalSize {
        local_cols,
        local_rows,
        desired_cols,
        desired_rows,
    })
}

fn clamp_scroll(scroll: usize, selected: usize, total: usize) -> usize {
    let rows = terminal::size().map(|(_, rows)| rows).unwrap_or(24);
    let slots = (rows.saturating_sub(1) as usize / 2).max(1);
    if selected < scroll {
        selected
    } else if selected >= scroll + slots {
        selected.saturating_sub(slots - 1)
    } else if total <= slots {
        0
    } else {
        scroll
    }
}

fn task_title(task: &ExbashTask) -> String {
    let text = task.description.as_str();
    if text.trim().is_empty() {
        "running command".to_string()
    } else {
        single_line(text)
    }
}

fn task_subtitle(task: &ExbashTask) -> String {
    let command = single_line(&task.command)
        .chars()
        .take(30)
        .collect::<String>();
    format!(
        "{} · {} [{}/{}] command={}",
        task.async_id, task.cwd, task.scope, task.executor, command
    )
}

fn task_state_icon(task: &ExbashTask) -> &'static str {
    match task_state(task).as_str() {
        "running" => "[▶]",
        "exit:0" => "[✓]",
        "timeout" | "stop" => "[!]",
        "unknown" => "[?]",
        state if state.starts_with("exit:") => "[E]",
        _ => "[?]",
    }
}

fn attach_state_icon(view: &AttachView, task: &ExbashTask) -> &'static str {
    match view.exit_code {
        None => task_state_icon(task),
        Some(0) => "[✓]",
        Some(_) => "[E]",
    }
}

fn task_state(task: &ExbashTask) -> String {
    if let Some(state) = task.state.as_deref().filter(|state| !state.is_empty()) {
        return normalize_state(state, task.exit_code.as_ref());
    }
    if let Some(exit_code) = task.exit_code.as_ref() {
        return normalize_exit_code(exit_code);
    }
    "unknown".to_string()
}

fn normalize_state(state: &str, exit_code: Option<&Value>) -> String {
    match state {
        "running" | "timeout" | "unknown" => state.to_string(),
        "stop" | "stopped" => "stop".to_string(),
        state if state.starts_with("exit:") => state.to_string(),
        _ => exit_code
            .map(normalize_exit_code)
            .unwrap_or_else(|| "unknown".to_string()),
    }
}

fn normalize_exit_code(value: &Value) -> String {
    if let Some(number) = value.as_i64().or_else(|| value.as_str()?.parse().ok()) {
        return format!("exit:{number}");
    }
    match value.as_str().unwrap_or_default() {
        "timeout" => "timeout".to_string(),
        "stop" | "stopped" => "stop".to_string(),
        _ => "unknown".to_string(),
    }
}

fn is_running(task: &ExbashTask) -> bool {
    task_state(task) == "running"
}

fn exit_u32(task: &ExbashTask) -> Option<u32> {
    let state = task_state(task);
    let code = state.strip_prefix("exit:")?.parse::<i64>().ok()?;
    u32::try_from(code).ok()
}

fn role_symbol(role: &str) -> &'static str {
    if role == "Controller" {
        "◆"
    } else {
        "◇"
    }
}

fn command_targets_text(selected: CommandTarget) -> String {
    let items = [
        (CommandTarget::Input, "INPUT"),
        (CommandTarget::Identity, "IDENTITY"),
        (CommandTarget::List, "LIST"),
        (CommandTarget::Stop, "STOP"),
        (CommandTarget::Remove, "REMOVE"),
        (CommandTarget::Quit, "QUIT"),
    ];
    items
        .into_iter()
        .map(|(target, label)| {
            if target == selected {
                format!("[{label}]")
            } else {
                label.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("  ")
}

fn next_command(command: CommandTarget) -> CommandTarget {
    match command {
        CommandTarget::Input => CommandTarget::Identity,
        CommandTarget::Identity => CommandTarget::List,
        CommandTarget::List => CommandTarget::Stop,
        CommandTarget::Stop => CommandTarget::Remove,
        CommandTarget::Remove => CommandTarget::Quit,
        CommandTarget::Quit => CommandTarget::Input,
    }
}

fn previous_command(command: CommandTarget) -> CommandTarget {
    match command {
        CommandTarget::Input => CommandTarget::Quit,
        CommandTarget::Identity => CommandTarget::Input,
        CommandTarget::List => CommandTarget::Identity,
        CommandTarget::Stop => CommandTarget::List,
        CommandTarget::Remove => CommandTarget::Stop,
        CommandTarget::Quit => CommandTarget::Remove,
    }
}

fn trim_to_width(text: &str, width: usize) -> String {
    text.chars().take(width).collect()
}

fn single_line(text: &str) -> String {
    text.replace("\r\n", "\\n")
        .replace('\n', "\\n")
        .replace('\r', "\\n")
}

fn format_duration(duration: Duration) -> String {
    let ms = duration.as_millis();
    if ms < 1000 {
        format!("{ms}ms")
    } else {
        format!("{:.1}s", duration.as_secs_f64())
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}{}", bytes, UNITS[unit])
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

fn random_client_id() -> String {
    format!("client-{:016x}", rand::random::<u64>())
}

fn default_scope() -> String {
    "local".to_string()
}

fn default_executor() -> String {
    "local".to_string()
}
