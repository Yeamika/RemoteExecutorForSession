use anyhow::{anyhow, Context, Result};
use clap::Parser;
use crossterm::event::{
    self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use crossterm::style::{Attribute, SetAttribute};
use crossterm::terminal::{self, ClearType};
use crossterm::{cursor, execute, queue};
use futures_util::{SinkExt, StreamExt};
use pty_t_protocol::{clamp_size, ClientText, ServerText};
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::{stdout, Stdout, Write};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

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

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
enum ControlMessage {
    #[serde(rename = "refs-ptyt.registered")]
    Registered {
        #[serde(rename = "slotID")]
        slot_id: String,
    },
    #[serde(rename = "refs-ptyt.assign")]
    Assign { task: Box<ExbashTask> },
    #[serde(rename = "refs-ptyt.unassign")]
    Unassign {
        #[serde(rename = "asyncID", alias = "asyncId")]
        async_id: String,
        #[serde(default = "default_executor")]
        executor: String,
    },
    #[serde(rename = "refs-ptyt.message")]
    Message { message: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Input,
    Link,
    Command,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommandTarget {
    Input,
    List,
    Quit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlCommand {
    SetSchedulable(bool),
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
    mouse_tracking: bool,
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
    Mouse(MouseEvent),
    Resize { cols: u16, rows: u16 },
    Assign(Box<ExbashTask>),
    Unassign { async_id: String, executor: String },
    Notice(String),
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
    spawn_input_thread(tx.clone());
    let control_tx = spawn_control_connection(client.clone(), tx.clone());

    let mut out = stdout();
    let mut auto_schedule = true;
    let mut selected = 0usize;
    let mut scroll = 0usize;
    let mut message = None::<String>;
    let mut list = load_task_list(&client).await.unwrap_or_else(|err| {
        message = Some(err.to_string());
        TaskList::default()
    });
    draw_list(
        &mut out,
        &list,
        selected,
        scroll,
        message.as_deref(),
        auto_schedule,
    )?;
    let mut spinner_tick = time::interval(Duration::from_millis(250));
    spinner_tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                match event {
                    LocalEvent::Quit => break,
                    LocalEvent::Resize { .. } => {
                        draw_list(
                            &mut out,
                            &list,
                            selected,
                            scroll,
                            message.as_deref(),
                            auto_schedule,
                        )?;
                    }
                    LocalEvent::Notice(text) => {
                        message = Some(text);
                        draw_list(
                            &mut out,
                            &list,
                            selected,
                            scroll,
                            message.as_deref(),
                            auto_schedule,
                        )?;
                    }
                    LocalEvent::Unassign { async_id, executor } => {
                        message = Some(format!("{executor}:{async_id} unassigned"));
                        refresh_task_list(&client, &mut list, &mut selected, &mut scroll, &mut message)
                            .await;
                        draw_list(
                            &mut out,
                            &list,
                            selected,
                            scroll,
                            message.as_deref(),
                            auto_schedule,
                        )?;
                    }
                    LocalEvent::Assign(task) => {
                        if !auto_schedule {
                            continue;
                        }
                        message = Some(format!("assigned {}:{}", task.executor, task.async_id));
                        draw_list(
                            &mut out,
                            &list,
                            selected,
                            scroll,
                            message.as_deref(),
                            auto_schedule,
                        )?;
                        match attach_loop(
                            &client,
                            *task,
                            &mut rx,
                            size,
                            &mut auto_schedule,
                            &control_tx,
                        ).await {
                            AttachAction::Quit => break,
                            AttachAction::List | AttachAction::Switch(_) => {
                                refresh_task_list(
                                    &client,
                                    &mut list,
                                    &mut selected,
                                    &mut scroll,
                                    &mut message,
                                )
                                .await;
                                draw_list(
                                    &mut out,
                                    &list,
                                    selected,
                                    scroll,
                                    message.as_deref(),
                                    auto_schedule,
                                )?;
                            }
                        }
                    }
                    LocalEvent::Key(key) => match key.code {
                        KeyCode::Char('q') | KeyCode::Char('Q') => break,
                        KeyCode::Char('r') | KeyCode::Char('R') => {
                            message = Some("refreshing".to_string());
                            draw_list(
                                &mut out,
                                &list,
                                selected,
                                scroll,
                                message.as_deref(),
                                auto_schedule,
                            )?;
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
                            draw_list(
                                &mut out,
                                &list,
                                selected,
                                scroll,
                                message.as_deref(),
                                auto_schedule,
                            )?;
                        }
                        KeyCode::Up => {
                            selected = selected.saturating_sub(1);
                            scroll = clamp_scroll(scroll, selected, list.tasks.len());
                            draw_list(
                                &mut out,
                                &list,
                                selected,
                                scroll,
                                message.as_deref(),
                                auto_schedule,
                            )?;
                        }
                        KeyCode::Down => {
                            if selected + 1 < list.tasks.len() {
                                selected += 1;
                            }
                            scroll = clamp_scroll(scroll, selected, list.tasks.len());
                            draw_list(
                                &mut out,
                                &list,
                                selected,
                                scroll,
                                message.as_deref(),
                                auto_schedule,
                            )?;
                        }
                        KeyCode::Enter => {
                            let Some(task) = list.tasks.get(selected).cloned() else {
                                continue;
                            };
                            message = Some("connecting".to_string());
                            draw_list(
                                &mut out,
                                &list,
                                selected,
                                scroll,
                                message.as_deref(),
                                auto_schedule,
                            )?;
                            match attach_loop(
                                &client,
                                task,
                                &mut rx,
                                size,
                                &mut auto_schedule,
                                &control_tx,
                            ).await {
                                AttachAction::Quit => break,
                                AttachAction::List | AttachAction::Switch(_) => {
                                    refresh_task_list(
                                        &client,
                                        &mut list,
                                        &mut selected,
                                        &mut scroll,
                                        &mut message,
                                    )
                                    .await;
                                    draw_list(
                                        &mut out,
                                        &list,
                                        selected,
                                        scroll,
                                        message.as_deref(),
                                        auto_schedule,
                                    )?;
                                }
                            }
                        }
                        _ => {}
                    },
                    LocalEvent::Mouse(mouse) => {
                        if list_auto_hit(auto_schedule, mouse) {
                            toggle_auto_schedule(&mut auto_schedule, &control_tx);
                            draw_list(
                                &mut out,
                                &list,
                                selected,
                                scroll,
                                message.as_deref(),
                                auto_schedule,
                            )?;
                        }
                    }
                }
            }
            _ = spinner_tick.tick(), if has_running_task(&list) => {
                draw_list(
                    &mut out,
                    &list,
                    selected,
                    scroll,
                    message.as_deref(),
                    auto_schedule,
                )?;
            }
        }
    }

    Ok(())
}

async fn refresh_task_list(
    client: &RefsMcpClient,
    list: &mut TaskList,
    selected: &mut usize,
    scroll: &mut usize,
    message: &mut Option<String>,
) {
    match load_task_list(client).await {
        Ok(next) => {
            *list = next;
            if *selected >= list.tasks.len() {
                *selected = list.tasks.len().saturating_sub(1);
            }
            *scroll = clamp_scroll(*scroll, *selected, list.tasks.len());
            *message = None;
        }
        Err(err) => *message = Some(err.to_string()),
    }
}

fn spawn_control_connection(
    client: RefsMcpClient,
    tx: mpsc::UnboundedSender<LocalEvent>,
) -> mpsc::UnboundedSender<ControlCommand> {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let slot_id = random_client_id();
        let mut schedulable = true;
        while !tx.is_closed() {
            match run_control_connection(&client, &slot_id, &tx, &mut cmd_rx, &mut schedulable)
                .await
            {
                Ok(()) => {
                    if tx.is_closed() {
                        break;
                    }
                    let _ = tx.send(LocalEvent::Notice("scheduler disconnected".to_string()));
                }
                Err(err) => {
                    if tx.is_closed() {
                        break;
                    }
                    let _ = tx.send(LocalEvent::Notice(format!("scheduler: {err}")));
                }
            }
            time::sleep(Duration::from_secs(1)).await;
        }
    });
    cmd_tx
}

async fn run_control_connection(
    client: &RefsMcpClient,
    slot_id: &str,
    tx: &mpsc::UnboundedSender<LocalEvent>,
    cmd_rx: &mut mpsc::UnboundedReceiver<ControlCommand>,
    schedulable: &mut bool,
) -> Result<()> {
    let (ws, _) = connect_async(&client.endpoint.url)
        .await
        .with_context(|| format!("connect {}", client.endpoint.url))?;
    let (mut write, mut read) = ws.split();
    drain_control_commands(schedulable, cmd_rx);
    send_control_register(&mut write, client, slot_id, *schedulable).await?;

    loop {
        tokio::select! {
            command = cmd_rx.recv() => {
                let Some(command) = command else {
                    return Ok(());
                };
                apply_control_command(schedulable, command);
                send_control_register(&mut write, client, slot_id, *schedulable).await?;
            }
            message = read.next() => {
                let Some(message) = message else {
                    break;
                };
                match message? {
                    Message::Text(text) => handle_control_message(&text, tx),
                    Message::Ping(data) => write.send(Message::Pong(data)).await?,
                    Message::Close(_) => break,
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }

    Ok(())
}

async fn send_control_register(
    write: &mut ClientWsSink,
    client: &RefsMcpClient,
    slot_id: &str,
    schedulable: bool,
) -> Result<()> {
    let register = json!({
        "type": "refs-ptyt.register",
        "sessionID": client.session.as_str(),
        "slotID": slot_id,
        "schedulable": schedulable
    });
    write
        .send(Message::Text(serde_json::to_string(&register)?.into()))
        .await
        .map_err(Into::into)
}

fn drain_control_commands(
    schedulable: &mut bool,
    cmd_rx: &mut mpsc::UnboundedReceiver<ControlCommand>,
) {
    while let Ok(command) = cmd_rx.try_recv() {
        apply_control_command(schedulable, command);
    }
}

fn apply_control_command(schedulable: &mut bool, command: ControlCommand) {
    match command {
        ControlCommand::SetSchedulable(value) => *schedulable = value,
    }
}

fn toggle_auto_schedule(
    auto_schedule: &mut bool,
    control_tx: &mpsc::UnboundedSender<ControlCommand>,
) {
    *auto_schedule = !*auto_schedule;
    let _ = control_tx.send(ControlCommand::SetSchedulable(*auto_schedule));
}

fn handle_control_message(input: &str, tx: &mpsc::UnboundedSender<LocalEvent>) {
    let Ok(message) = serde_json::from_str::<ControlMessage>(input) else {
        return;
    };
    match message {
        ControlMessage::Registered { slot_id } => {
            drop(slot_id);
        }
        ControlMessage::Assign { task } => {
            let _ = tx.send(LocalEvent::Assign(task));
        }
        ControlMessage::Unassign { async_id, executor } => {
            let _ = tx.send(LocalEvent::Unassign { async_id, executor });
        }
        ControlMessage::Message { message } => {
            let _ = tx.send(LocalEvent::Notice(message));
        }
    }
}

async fn load_task_list(client: &RefsMcpClient) -> Result<TaskList> {
    let local = client.exbash_list("local").await?;
    let workspace = client.exbash_list("workspace").await?;
    let local_meta = list_metadata(&local)?;
    let workspace_meta = list_metadata(&workspace)?;
    let local_tasks = tasks_from_metadata(local_meta, "local")?;
    let workspace_tasks = tasks_from_metadata(workspace_meta, "workspace")?;
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
    Ok(TaskList { tasks })
}

async fn resolve_attach_target(client: &RefsMcpClient, task: ExbashTask) -> Result<AttachTarget> {
    let _ = client
        .call_tool(
            "exbash",
            json!({
                "mode": "attach",
                "asyncID": task.async_id.as_str(),
                "executor": task.executor.as_str(),
                "scope": task.scope.as_str(),
                "refsPtytResolve": true,
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

#[derive(Clone, Debug)]
enum AttachAction {
    List,
    Quit,
    Switch(Box<ExbashTask>),
}

async fn attach_loop(
    client: &RefsMcpClient,
    task: ExbashTask,
    rx: &mut mpsc::UnboundedReceiver<LocalEvent>,
    size: TerminalSize,
    auto_schedule: &mut bool,
    control_tx: &mpsc::UnboundedSender<ControlCommand>,
) -> AttachAction {
    let mut task = task;
    loop {
        match resolve_attach_target(client, task).await {
            Ok(target) => match run_attach(target, rx, size, auto_schedule, control_tx).await {
                AttachAction::Switch(next) => task = *next,
                action => return action,
            },
            Err(err) => {
                let mut out = stdout();
                let _ = draw_error_message(&mut out, &err.to_string(), *auto_schedule);
                time::sleep(Duration::from_millis(1200)).await;
                return AttachAction::List;
            }
        }
    }
}

async fn run_attach(
    target: AttachTarget,
    rx: &mut mpsc::UnboundedReceiver<LocalEvent>,
    size: TerminalSize,
    auto_schedule: &mut bool,
    control_tx: &mpsc::UnboundedSender<ControlCommand>,
) -> AttachAction {
    match run_attach_inner(target, rx, size, auto_schedule, control_tx).await {
        Ok(action) => action,
        Err(err) => {
            let mut out = stdout();
            let _ = draw_error_message(&mut out, &err.to_string(), *auto_schedule);
            time::sleep(Duration::from_millis(1200)).await;
            AttachAction::List
        }
    }
}

async fn run_attach_inner(
    target: AttachTarget,
    rx: &mut mpsc::UnboundedReceiver<LocalEvent>,
    size: TerminalSize,
    auto_schedule: &mut bool,
    control_tx: &mpsc::UnboundedSender<ControlCommand>,
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
        mode: Mode::Input,
        command_target: CommandTarget::Input,
        ctrl_c_count: 0,
        message: None,
        mouse_tracking: false,
    };
    let mut metrics = Metrics {
        next_ping_seq: 1,
        ..Metrics::default()
    };
    render_attach(
        &mut out,
        &parser,
        &view,
        &target.task,
        &metrics,
        *auto_schedule,
    )?;

    let mut ping_tick = time::interval_at(
        time::Instant::now() + Duration::from_secs(3),
        Duration::from_secs(3),
    );
    ping_tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut spinner_tick = time::interval(Duration::from_millis(250));
    spinner_tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
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
                        render_attach(
                            &mut out,
                            &parser,
                            &view,
                            &target.task,
                            &metrics,
                            *auto_schedule,
                        )?;
                    }
                    LocalEvent::Assign(task) => {
                        if !*auto_schedule {
                            continue;
                        }
                        if same_task(&target.task, &task) {
                            view.message = Some("assigned here".to_string());
                            render_attach(
                                &mut out,
                                &parser,
                                &view,
                                &target.task,
                                &metrics,
                                *auto_schedule,
                            )?;
                            continue;
                        }
                        return Ok(AttachAction::Switch(task));
                    }
                    LocalEvent::Unassign { async_id, executor } => {
                        if target.task.async_id == async_id && target.task.executor == executor {
                            return Ok(AttachAction::List);
                        }
                    }
                    LocalEvent::Notice(message) => {
                        view.message = Some(message);
                        render_attach(
                            &mut out,
                            &parser,
                            &view,
                            &target.task,
                            &metrics,
                            *auto_schedule,
                        )?;
                    }
                    LocalEvent::Key(key) => {
                        match process_attach_key(
                            key,
                            AttachKeyContext {
                                parser: &parser,
                                view: &mut view,
                                task: &target.task,
                                metrics: &mut metrics,
                                out: &mut out,
                                ws_write: &mut ws_write,
                                ctrl_c_streak: &mut ctrl_c_streak,
                                auto_schedule: *auto_schedule,
                            },
                        ).await? {
                            AttachActionRequest::Continue => {}
                            AttachActionRequest::List => return Ok(AttachAction::List),
                            AttachActionRequest::Quit => return Ok(AttachAction::Quit),
                        }
                    }
                    LocalEvent::Mouse(mouse) => {
                        match process_attach_mouse(
                            mouse,
                            AttachMouseContext {
                                parser: &parser,
                                view: &mut view,
                                task: &target.task,
                                metrics: &mut metrics,
                                out: &mut out,
                                ws_write: &mut ws_write,
                                auto_schedule,
                                control_tx,
                            },
                        ).await? {
                            AttachActionRequest::Continue => {}
                            AttachActionRequest::List => return Ok(AttachAction::List),
                            AttachActionRequest::Quit => return Ok(AttachAction::Quit),
                        }
                    }
                }
            }
            _ = spinner_tick.tick(), if should_animate_attach(&view, &target.task) => {
                render_attach(
                    &mut out,
                    &parser,
                    &view,
                    &target.task,
                    &metrics,
                    *auto_schedule,
                )?;
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
                        update_mouse_tracking(&mut view, &data);
                        parser.process(&data);
                        render_attach(
                            &mut out,
                            &parser,
                            &view,
                            &target.task,
                            &metrics,
                            *auto_schedule,
                        )?;
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
                                render_attach(
                                    &mut out,
                                    &parser,
                                    &view,
                                    &target.task,
                                    &metrics,
                                    *auto_schedule,
                                )?;
                            }
                            Ok(ServerText::Error { message }) | Ok(ServerText::Info { message }) => {
                                view.message = Some(message);
                                render_attach(
                                    &mut out,
                                    &parser,
                                    &view,
                                    &target.task,
                                    &metrics,
                                    *auto_schedule,
                                )?;
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
                            render_attach(
                                &mut out,
                                &parser,
                                &view,
                                &target.task,
                                &metrics,
                                *auto_schedule,
                            )?;
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

struct AttachKeyContext<'a> {
    parser: &'a vt100::Parser,
    view: &'a mut AttachView,
    task: &'a ExbashTask,
    metrics: &'a mut Metrics,
    out: &'a mut Stdout,
    ws_write: &'a mut ClientWsSink,
    ctrl_c_streak: &'a mut u8,
    auto_schedule: bool,
}

struct AttachMouseContext<'a> {
    parser: &'a vt100::Parser,
    view: &'a mut AttachView,
    task: &'a ExbashTask,
    metrics: &'a mut Metrics,
    out: &'a mut Stdout,
    ws_write: &'a mut ClientWsSink,
    auto_schedule: &'a mut bool,
    control_tx: &'a mpsc::UnboundedSender<ControlCommand>,
}

async fn process_attach_mouse(
    mouse: MouseEvent,
    context: AttachMouseContext<'_>,
) -> Result<AttachActionRequest> {
    let AttachMouseContext {
        parser,
        view,
        task,
        metrics,
        out,
        ws_write,
        auto_schedule,
        control_tx,
    } = context;

    if mouse.row == view.local_rows.saturating_sub(1) {
        if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left))
            && status_auto_hit(
                *auto_schedule,
                view.local_cols,
                view.local_rows,
                mouse.column,
            )
        {
            toggle_auto_schedule(auto_schedule, control_tx);
            render_attach(out, parser, view, task, metrics, *auto_schedule)?;
        } else if view.message.is_none()
            && matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left))
        {
            match view.mode {
                Mode::Input | Mode::Link => {
                    if status_identity_hit(view, mouse.column) {
                        request_control(ws_write, metrics).await?;
                    } else {
                        view.mode = Mode::Command;
                        view.command_target = CommandTarget::Input;
                        view.ctrl_c_count = 0;
                    }
                    render_attach(out, parser, view, task, metrics, *auto_schedule)?;
                }
                Mode::Command => {
                    if let Some(target) =
                        command_target_at_column(view.command_target, mouse.column)
                    {
                        view.command_target = target;
                        match target {
                            CommandTarget::Input => view.mode = Mode::Input,
                            CommandTarget::List => return Ok(AttachActionRequest::List),
                            CommandTarget::Quit => return Ok(AttachActionRequest::Quit),
                        }
                    }
                    render_attach(out, parser, view, task, metrics, *auto_schedule)?;
                }
            }
        } else if view.mode == Mode::Command
            && matches!(
                mouse.kind,
                MouseEventKind::Moved | MouseEventKind::Drag(MouseButton::Left)
            )
        {
            if let Some(target) = command_target_at_column(view.command_target, mouse.column) {
                view.command_target = target;
                render_attach(out, parser, view, task, metrics, *auto_schedule)?;
            }
        }
        return Ok(AttachActionRequest::Continue);
    }

    if view.mode == Mode::Command || !view.mouse_tracking {
        return Ok(AttachActionRequest::Continue);
    }
    if mouse.row >= view.pty_rows || mouse.column >= view.pty_cols {
        return Ok(AttachActionRequest::Continue);
    }

    if let Some(bytes) = mouse_to_sgr_bytes(mouse) {
        metrics.record_tx(bytes.len());
        ws_write.send(Message::Binary(bytes.into())).await?;
    }
    Ok(AttachActionRequest::Continue)
}

fn command_target_at_column(selected: CommandTarget, column: u16) -> Option<CommandTarget> {
    let mut start = UnicodeWidthStr::width("[:] ");
    let column = column as usize;
    for (target, label) in command_targets() {
        let display = if target == selected {
            format!("[{label}]")
        } else {
            label.to_string()
        };
        let end = start + UnicodeWidthStr::width(display.as_str());
        if column >= start && column < end {
            return Some(target);
        }
        start = end + 2;
    }
    None
}

async fn request_control(ws_write: &mut ClientWsSink, metrics: &mut Metrics) -> Result<()> {
    let msg = serde_json::to_string(&ClientText::RequestControl)?;
    metrics.record_tx(msg.len());
    ws_write.send(Message::Text(msg.into())).await?;
    Ok(())
}

async fn process_attach_key(
    key: KeyEvent,
    context: AttachKeyContext<'_>,
) -> Result<AttachActionRequest> {
    let AttachKeyContext {
        parser,
        view,
        task,
        metrics,
        out,
        ws_write,
        ctrl_c_streak,
        auto_schedule,
    } = context;
    match view.mode {
        Mode::Input | Mode::Link => {
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
                    view.mode = Mode::Command;
                    view.command_target = CommandTarget::Input;
                }
                render_attach(out, parser, view, task, metrics, auto_schedule)?;
                return Ok(AttachActionRequest::Continue);
            }

            let had_ctrl_c_hint = *ctrl_c_streak > 0;
            *ctrl_c_streak = 0;
            view.ctrl_c_count = 0;

            if matches!(key.code, KeyCode::Tab) {
                view.mode = if view.mode == Mode::Input {
                    Mode::Link
                } else {
                    Mode::Input
                };
                let bytes = b"\t".to_vec();
                metrics.record_tx(bytes.len());
                ws_write.send(Message::Binary(bytes.into())).await?;
                render_attach(out, parser, view, task, metrics, auto_schedule)?;
                return Ok(AttachActionRequest::Continue);
            }

            if let Some(bytes) = key_to_bytes(key) {
                metrics.record_tx(bytes.len());
                ws_write.send(Message::Binary(bytes.into())).await?;
            }
            if had_ctrl_c_hint {
                render_attach(out, parser, view, task, metrics, auto_schedule)?;
            }
            Ok(AttachActionRequest::Continue)
        }
        Mode::Command => {
            *ctrl_c_streak = 0;
            view.ctrl_c_count = 0;
            match key.code {
                KeyCode::Esc => {
                    view.mode = Mode::Input;
                    view.command_target = CommandTarget::Input;
                }
                KeyCode::Enter => match view.command_target {
                    CommandTarget::Input => view.mode = Mode::Input,
                    CommandTarget::List => return Ok(AttachActionRequest::List),
                    CommandTarget::Quit => return Ok(AttachActionRequest::Quit),
                },
                KeyCode::Left => view.command_target = previous_command(view.command_target),
                KeyCode::Right | KeyCode::Tab => {
                    view.command_target = next_command(view.command_target)
                }
                _ => {}
            }
            render_attach(out, parser, view, task, metrics, auto_schedule)?;
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

fn draw_list(
    out: &mut Stdout,
    list: &TaskList,
    selected: usize,
    scroll: usize,
    message: Option<&str>,
    auto_schedule: bool,
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
        draw_empty_list(out, cols, body_rows as u16)?;
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
    draw_list_status(out, list.tasks.get(selected), cols, rows, auto_schedule)?;
    out.flush()?;
    Ok(())
}

const EMPTY_LOGO: [&str; 5] = [
    " ____  _____ _____ ____  ",
    "|  _ \\| ____|  ___/ ___| ",
    "| |_) |  _| | |_  \\___ \\ ",
    "|  _ <| |___|  _|  ___) |",
    "|_| \\_\\_____|_|   |____/ ",
];

fn draw_empty_list(out: &mut Stdout, cols: u16, body_rows: u16) -> Result<()> {
    let lines = EMPTY_LOGO
        .into_iter()
        .chain(["", "No exbash tasks"])
        .collect::<Vec<_>>();
    let start = body_rows.saturating_sub(lines.len() as u16) / 2;
    for (index, line) in lines.into_iter().enumerate() {
        let row = start + index as u16;
        if row >= body_rows {
            break;
        }
        queue!(
            out,
            cursor::MoveTo(0, row),
            terminal::Clear(ClearType::CurrentLine)
        )?;
        write!(out, "{}", center_line(line, cols as usize))?;
    }
    Ok(())
}

fn draw_list_status(
    out: &mut Stdout,
    task: Option<&ExbashTask>,
    cols: u16,
    rows: u16,
    auto_schedule: bool,
) -> Result<()> {
    let left = list_status_body(task);
    let marker = status_marker(auto_schedule, cols, rows);
    draw_status_with_marker(out, rows.saturating_sub(1), cols, &left, &marker)
}

fn list_status_body(task: Option<&ExbashTask>) -> String {
    let icon = task.map(task_state_icon).unwrap_or("[?]");
    format!("[:]{icon}SW: INPUT LIST QUIT")
}

fn render_attach(
    out: &mut Stdout,
    parser: &vt100::Parser,
    view: &AttachView,
    task: &ExbashTask,
    metrics: &Metrics,
    auto_schedule: bool,
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
            &status_marker(auto_schedule, view.local_cols, view.local_rows),
        )?;
    } else {
        draw_attach_status(out, view, task, metrics, auto_schedule)?;
    }
    if view.mode == Mode::Command {
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
    auto_schedule: bool,
) -> Result<()> {
    let body = attach_status_body(view, task, metrics);
    draw_status_with_marker(
        out,
        view.local_rows.saturating_sub(1),
        view.local_cols,
        &body,
        &status_marker(auto_schedule, view.local_cols, view.local_rows),
    )
}

fn attach_status_body(view: &AttachView, task: &ExbashTask, metrics: &Metrics) -> String {
    let role = role_symbol(&view.role);
    let state = attach_state_icon(view, task);
    match view.mode {
        Mode::Input if view.ctrl_c_count > 0 => {
            format!("[ctrl c x{}] x3 to command mode", view.ctrl_c_count)
        }
        Mode::Input => format!("[>] [{role}:{}]  {state} {}", view.id, task_title(task)),
        Mode::Link => format!(
            "[~] [{role}:{}]  rtt={} rx={} tx={} idle={}  {state} {}:{}",
            view.id,
            metrics.latency_text(),
            format_bytes(metrics.rx_bytes),
            format_bytes(metrics.tx_bytes),
            metrics.idle_text(),
            task.executor,
            task.async_id
        ),
        Mode::Command => format!("[:] {}  {state}", command_targets_text(view.command_target)),
    }
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
    write!(out, "{}", status_line(left, marker, cols as usize))?;
    queue!(out, SetAttribute(Attribute::Reset))?;
    Ok(())
}

fn status_line(left: &str, marker: &str, width: usize) -> String {
    let marker_len = UnicodeWidthStr::width(marker);
    let max_left = width.saturating_sub(marker_len + 1);
    let left = trim_to_width(left, max_left);
    let left_len = UnicodeWidthStr::width(left.as_str());
    if left_len + 1 + marker_len <= width {
        return format!(
            "{left}{}{marker}",
            " ".repeat(width.saturating_sub(left_len + marker_len))
        );
    }
    trim_to_width(marker, width)
}

fn size_marker(cols: u16, rows: u16) -> String {
    format!("[{cols}x{rows}]")
}

fn status_marker(auto_schedule: bool, cols: u16, rows: u16) -> String {
    format!("{} {}", auto_marker(auto_schedule), size_marker(cols, rows))
}

fn auto_marker(auto_schedule: bool) -> &'static str {
    if auto_schedule {
        "[Auto]"
    } else {
        "[    ]"
    }
}

fn status_auto_hit(auto_schedule: bool, cols: u16, rows: u16, column: u16) -> bool {
    let marker = status_marker(auto_schedule, cols, rows);
    let marker_width = UnicodeWidthStr::width(marker.as_str());
    let width = cols as usize;
    if marker_width > width {
        return false;
    }
    let start = width - marker_width;
    let end = start + UnicodeWidthStr::width(auto_marker(auto_schedule));
    let column = column as usize;
    column >= start && column < end
}

fn list_auto_hit(auto_schedule: bool, mouse: MouseEvent) -> bool {
    if !matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
        return false;
    }
    let Ok((cols, rows)) = terminal::size() else {
        return false;
    };
    mouse.row == rows.saturating_sub(1) && status_auto_hit(auto_schedule, cols, rows, mouse.column)
}

fn draw_error_message(out: &mut Stdout, message: &str, auto_schedule: bool) -> Result<()> {
    let (cols, rows) = terminal::size()?;
    let marker = status_marker(auto_schedule, cols, rows);
    draw_status_with_marker(
        out,
        rows.saturating_sub(1),
        cols,
        &format!("error: {message}"),
        &marker,
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
                Event::Mouse(mouse) => {
                    let _ = tx.send(LocalEvent::Mouse(mouse));
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

fn mouse_to_sgr_bytes(mouse: MouseEvent) -> Option<Vec<u8>> {
    let (base, final_byte) = match mouse.kind {
        MouseEventKind::Down(button) => (mouse_button_code(button)?, b'M'),
        MouseEventKind::Up(button) => (mouse_button_code(button)?, b'm'),
        MouseEventKind::Drag(button) => (mouse_button_code(button)? + 32, b'M'),
        MouseEventKind::Moved => (35, b'M'),
        MouseEventKind::ScrollUp => (64, b'M'),
        MouseEventKind::ScrollDown => (65, b'M'),
        MouseEventKind::ScrollLeft => (66, b'M'),
        MouseEventKind::ScrollRight => (67, b'M'),
    };
    let code = base + mouse_modifier_code(mouse.modifiers);
    let col = mouse.column.saturating_add(1);
    let row = mouse.row.saturating_add(1);
    Some(format!("\x1b[<{code};{col};{row}{}", final_byte as char).into_bytes())
}

fn mouse_button_code(button: MouseButton) -> Option<u16> {
    match button {
        MouseButton::Left => Some(0),
        MouseButton::Middle => Some(1),
        MouseButton::Right => Some(2),
    }
}

fn mouse_modifier_code(modifiers: KeyModifiers) -> u16 {
    let mut code = 0;
    if modifiers.contains(KeyModifiers::SHIFT) {
        code += 4;
    }
    if modifiers.contains(KeyModifiers::ALT) {
        code += 8;
    }
    if modifiers.contains(KeyModifiers::CONTROL) {
        code += 16;
    }
    code
}

fn status_identity_hit(view: &AttachView, column: u16) -> bool {
    if !matches!(view.mode, Mode::Input | Mode::Link) {
        return false;
    }
    let prefix = match view.mode {
        Mode::Input => "[>] ",
        Mode::Link => "[~] ",
        Mode::Command => return false,
    };
    let role = role_symbol(&view.role);
    let identity = format!("[{role}:{}]", view.id);
    let start = UnicodeWidthStr::width(prefix);
    let end = start + UnicodeWidthStr::width(identity.as_str());
    let column = column as usize;
    column >= start && column < end
}

fn update_mouse_tracking(view: &mut AttachView, data: &[u8]) {
    let mut index = 0;
    while index + 3 <= data.len() {
        let Some(relative) = data[index..]
            .windows(3)
            .position(|window| window == b"\x1b[?")
        else {
            break;
        };
        let params_start = index + relative + 3;
        let mut params_end = params_start;
        while params_end < data.len()
            && (data[params_end].is_ascii_digit() || data[params_end] == b';')
        {
            params_end += 1;
        }
        if params_end < data.len()
            && matches!(data[params_end], b'h' | b'l')
            && private_modes_include_mouse(&data[params_start..params_end])
        {
            view.mouse_tracking = data[params_end] == b'h';
        }
        index = params_end.saturating_add(1);
    }
}

fn private_modes_include_mouse(params: &[u8]) -> bool {
    params
        .split(|byte| *byte == b';')
        .filter_map(|part| std::str::from_utf8(part).ok())
        .filter_map(|part| part.parse::<u16>().ok())
        .any(|mode| matches!(mode, 1000 | 1002 | 1003 | 1005 | 1006 | 1015))
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
    let command = clipped_command_summary(&task.command);
    format!(
        "{} · {} [{}/{}] command={}",
        task.async_id, task.cwd, task.scope, task.executor, command
    )
}

const RUNNING_ICONS: [&str; 10] = [
    "[⠋]", "[⠙]", "[⠹]", "[⠸]", "[⠼]", "[⠴]", "[⠦]", "[⠧]", "[⠇]", "[⠏]",
];

fn task_state_icon(task: &ExbashTask) -> &'static str {
    match task_state(task).as_str() {
        "running" => running_state_icon(running_icon_frame()),
        "exit:0" => "[✓]",
        "timeout" | "stop" => "[!]",
        "unknown" => "[?]",
        state if state.starts_with("exit:") => "[E]",
        _ => "[?]",
    }
}

fn running_state_icon(frame: usize) -> &'static str {
    RUNNING_ICONS[frame % RUNNING_ICONS.len()]
}

fn running_icon_frame() -> usize {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    ((millis / 250) % RUNNING_ICONS.len() as u128) as usize
}

fn has_running_task(list: &TaskList) -> bool {
    list.tasks.iter().any(is_running)
}

fn should_animate_attach(view: &AttachView, task: &ExbashTask) -> bool {
    view.exit_code.is_none() && is_running(task)
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

fn same_task(left: &ExbashTask, right: &ExbashTask) -> bool {
    left.async_id == right.async_id && left.executor == right.executor && left.scope == right.scope
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
    command_targets()
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

fn command_targets() -> impl Iterator<Item = (CommandTarget, &'static str)> {
    [
        (CommandTarget::Input, "INPUT"),
        (CommandTarget::List, "LIST"),
        (CommandTarget::Quit, "QUIT"),
    ]
    .into_iter()
}

fn next_command(command: CommandTarget) -> CommandTarget {
    match command {
        CommandTarget::Input => CommandTarget::List,
        CommandTarget::List => CommandTarget::Quit,
        CommandTarget::Quit => CommandTarget::Input,
    }
}

fn previous_command(command: CommandTarget) -> CommandTarget {
    match command {
        CommandTarget::Input => CommandTarget::Quit,
        CommandTarget::List => CommandTarget::Input,
        CommandTarget::Quit => CommandTarget::List,
    }
}

fn trim_to_width(text: &str, width: usize) -> String {
    let mut used = 0usize;
    let mut out = String::new();
    for ch in text.chars() {
        let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
        if used + ch_width > width {
            break;
        }
        used += ch_width;
        out.push(ch);
    }
    out
}

fn center_line(line: &str, width: usize) -> String {
    let line = trim_to_width(line, width);
    let line_width = UnicodeWidthStr::width(line.as_str());
    if line_width >= width {
        return line;
    }
    format!("{}{}", " ".repeat((width - line_width) / 2), line)
}

fn clipped_command_summary(command: &str) -> String {
    let first_line = command.split(['\r', '\n']).next().unwrap_or("");
    first_line.chars().take(100).collect()
}

fn single_line(text: &str) -> String {
    text.replace("\r\n", "\\n").replace(['\n', '\r'], "\\n")
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_attach_view() -> AttachView {
        AttachView {
            id: "client-123".to_string(),
            role: "Viewer".to_string(),
            pty_cols: 80,
            pty_rows: 23,
            exit_code: None,
            local_cols: 80,
            local_rows: 24,
            mode: Mode::Input,
            command_target: CommandTarget::Input,
            ctrl_c_count: 0,
            message: None,
            mouse_tracking: false,
        }
    }

    fn test_task() -> ExbashTask {
        ExbashTask {
            async_id: "rex-1".to_string(),
            scope: "workspace".to_string(),
            executor: "remote".to_string(),
            description: "build firmware".to_string(),
            command: "make".to_string(),
            cwd: "/workspace/project".to_string(),
            state: Some("running".to_string()),
            exit_code: None,
            started_at: Some(1),
        }
    }

    #[test]
    fn status_line_keeps_marker_on_same_row_with_wide_symbols() {
        let line = status_line(
            "[>] [◆:client-123]  [⠋] local:rex-1781162394566-96  long title",
            "[#]",
            48,
        );
        assert_eq!(UnicodeWidthStr::width(line.as_str()), 48);
        assert!(line.ends_with("[#]"));
    }

    #[test]
    fn status_marker_shows_auto_or_blank_control_mode() {
        assert_eq!(status_marker(true, 80, 24), "[Auto] [80x24]");
        assert_eq!(status_marker(false, 80, 24), "[    ] [80x24]");
    }

    #[test]
    fn status_auto_hit_targets_only_auto_segment() {
        let cols = 80;
        let rows = 24;
        let start =
            cols as usize - UnicodeWidthStr::width(status_marker(true, cols, rows).as_str());
        let end = start + UnicodeWidthStr::width(auto_marker(true));

        assert!(!status_auto_hit(true, cols, rows, (start - 1) as u16));
        assert!(status_auto_hit(true, cols, rows, start as u16));
        assert!(status_auto_hit(true, cols, rows, (end - 1) as u16));
        assert!(!status_auto_hit(true, cols, rows, end as u16));
    }

    #[test]
    fn trim_to_width_uses_display_columns() {
        let text = trim_to_width("[▶] abc", 4);
        assert_eq!(UnicodeWidthStr::width(text.as_str()), 4);
        assert_eq!(text, "[▶] ");
    }

    #[test]
    fn attach_status_hides_task_identity_except_link_mode() {
        let mut view = test_attach_view();
        let task = test_task();
        let metrics = Metrics::default();

        let input = attach_status_body(&view, &task, &metrics);
        assert!(input.contains("build firmware"));
        assert!(!input.contains("remote:rex-1"));

        view.mode = Mode::Link;
        let link = attach_status_body(&view, &task, &metrics);
        assert!(link.contains("remote:rex-1"));

        view.mode = Mode::Command;
        let command = attach_status_body(&view, &task, &metrics);
        assert!(command.contains("INPUT"));
        assert!(!command.contains("remote:rex-1"));
    }

    #[test]
    fn list_status_uses_switch_menu_with_selected_task_icon() {
        let task = test_task();

        assert!(list_status_body(Some(&task)).starts_with("[:]["));
        assert!(list_status_body(Some(&task)).contains("]SW: INPUT LIST QUIT"));
        assert_eq!(list_status_body(None), "[:][?]SW: INPUT LIST QUIT");
    }

    #[test]
    fn running_icon_cycles_through_geometric_frames() {
        assert_eq!(running_state_icon(0), "[⠋]");
        assert_eq!(running_state_icon(1), "[⠙]");
        assert_eq!(running_state_icon(2), "[⠹]");
        assert_eq!(running_state_icon(3), "[⠸]");
        assert_eq!(running_state_icon(4), "[⠼]");
        assert_eq!(running_state_icon(5), "[⠴]");
        assert_eq!(running_state_icon(6), "[⠦]");
        assert_eq!(running_state_icon(7), "[⠧]");
        assert_eq!(running_state_icon(8), "[⠇]");
        assert_eq!(running_state_icon(9), "[⠏]");
        assert_eq!(running_state_icon(10), "[⠋]");
    }

    #[test]
    fn mouse_sgr_encodes_button_scroll_and_modifiers() {
        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 2,
            row: 3,
            modifiers: KeyModifiers::CONTROL,
        };
        assert_eq!(mouse_to_sgr_bytes(click).unwrap(), b"\x1b[<16;3;4M");

        let release = MouseEvent {
            kind: MouseEventKind::Up(MouseButton::Left),
            column: 2,
            row: 3,
            modifiers: KeyModifiers::empty(),
        };
        assert_eq!(mouse_to_sgr_bytes(release).unwrap(), b"\x1b[<0;3;4m");

        let scroll = MouseEvent {
            kind: MouseEventKind::ScrollDown,
            column: 0,
            row: 0,
            modifiers: KeyModifiers::SHIFT,
        };
        assert_eq!(mouse_to_sgr_bytes(scroll).unwrap(), b"\x1b[<69;1;1M");
    }

    #[test]
    fn mouse_tracking_updates_from_xterm_private_modes() {
        let mut view = test_attach_view();

        update_mouse_tracking(&mut view, b"\x1b[?1000;1006h");
        assert!(view.mouse_tracking);

        update_mouse_tracking(&mut view, b"\x1b[?1000;1006l");
        assert!(!view.mouse_tracking);
    }

    #[test]
    fn status_identity_hit_targets_visible_role_segment() {
        let view = test_attach_view();
        let start = UnicodeWidthStr::width("[>] ");
        let end = start + UnicodeWidthStr::width("[◇:client-123]");

        assert!(!status_identity_hit(&view, (start - 1) as u16));
        assert!(status_identity_hit(&view, start as u16));
        assert!(status_identity_hit(&view, (end - 1) as u16));
        assert!(!status_identity_hit(&view, end as u16));
    }

    #[test]
    fn command_menu_hit_test_matches_rendered_targets() {
        assert_eq!(
            command_target_at_column(CommandTarget::Input, 4),
            Some(CommandTarget::Input)
        );
        assert_eq!(
            command_target_at_column(CommandTarget::Input, 10),
            Some(CommandTarget::Input)
        );
        assert_eq!(command_target_at_column(CommandTarget::Input, 11), None);
        assert_eq!(
            command_target_at_column(CommandTarget::Input, 13),
            Some(CommandTarget::List)
        );
        assert_eq!(
            command_target_at_column(CommandTarget::Input, 19),
            Some(CommandTarget::Quit)
        );
        assert_eq!(command_target_at_column(CommandTarget::Input, 3), None);
    }

    #[test]
    fn command_menu_excludes_task_controls() {
        let text = command_targets_text(CommandTarget::Input);

        assert!(!text.contains("STOP"));
        assert!(!text.contains("REMOVE"));
        assert!(!text.contains("IDENTITY"));
        assert_eq!(next_command(CommandTarget::Input), CommandTarget::List);
        assert_eq!(previous_command(CommandTarget::Quit), CommandTarget::List);
    }

    #[test]
    fn command_summary_uses_first_line_and_one_hundred_chars() {
        let text = clipped_command_summary(&format!("{}EXTRA\nnext", "0123456789".repeat(10)));

        assert_eq!(text, "0123456789".repeat(10));
    }

    #[test]
    fn control_assign_message_parses_task() {
        let message = r#"{
            "type": "refs-ptyt.assign",
            "task": {
                "asyncID": "rex-1",
                "scope": "workspace",
                "executor": "remote",
                "description": "build",
                "command": "make",
                "cwd": "/workspace/project",
                "state": "running",
                "startedAt": 10
            }
        }"#;

        let ControlMessage::Assign { task } = serde_json::from_str(message).unwrap() else {
            panic!("expected assign message");
        };
        assert_eq!(task.async_id, "rex-1");
        assert_eq!(task.scope, "workspace");
        assert_eq!(task.executor, "remote");
    }

    #[test]
    fn same_task_compares_scope_executor_and_async_id() {
        let left = ExbashTask {
            async_id: "rex-1".to_string(),
            scope: "local".to_string(),
            executor: "remote".to_string(),
            description: String::new(),
            command: String::new(),
            cwd: String::new(),
            state: Some("running".to_string()),
            exit_code: None,
            started_at: Some(1),
        };
        let mut right = left.clone();
        assert!(same_task(&left, &right));

        right.scope = "workspace".to_string();
        assert!(!same_task(&left, &right));
    }
}
