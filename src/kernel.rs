use std::collections::HashMap;
use std::fs;
use std::io;
use std::sync::mpsc;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};

use serde::Deserialize;
use serde_json::{Value, json};
use sysinfo::{Pid, System};

use crate::protocol::{
    IMPLEMENTATION, JUPYTER_PROTOCOL_VERSION, JupyterMessage, LANGUAGE, MessageHeader,
    MessageSigner, ProtocolError,
};
use crate::worker::{
    CommOutcome, ExecutionOutcome, PythonWorker, WorkerCommEvent, WorkerInterruptHandle,
    WorkerKernelInfo,
};

const CHANNEL_POLL_INTERVAL_MS: i64 = 100;
const HEARTBEAT_POLL_INTERVAL_MS: i32 = 100;

#[derive(Deserialize)]
struct ConnectionConfig {
    transport: String,
    ip: String,
    shell_port: u16,
    iopub_port: u16,
    stdin_port: u16,
    control_port: u16,
    hb_port: u16,
    signature_scheme: String,
    key: String,
    #[serde(default)]
    kernel_name: Option<String>,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all, frozen))]
pub struct ConnectionInfo {
    pub transport: String,
    pub ip: String,
    pub shell_port: u16,
    pub iopub_port: u16,
    pub stdin_port: u16,
    pub control_port: u16,
    pub hb_port: u16,
    pub signature_scheme: String,
    pub key: String,
    pub kernel_name: Option<String>,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all, frozen))]
pub struct ChannelEndpoints {
    pub shell: String,
    pub iopub: String,
    pub stdin: String,
    pub control: String,
    pub hb: String,
}

#[derive(Debug)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all, frozen))]
pub struct KernelRuntimeInfo {
    pub implementation: String,
    pub implementation_version: String,
    pub language: String,
    pub protocol_version: String,
}

#[derive(Debug)]
pub enum KernelError {
    Io(io::Error),
    InvalidConnectionConfig(&'static str),
    InvalidConnectionFile(serde_json::Error),
    Worker(String),
    Protocol(ProtocolError),
    Zmq(zmq::Error),
    HeartbeatThreadPanicked,
    MessageLoopThreadPanicked,
}

impl std::fmt::Display for KernelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::InvalidConnectionConfig(message) => f.write_str(message),
            Self::InvalidConnectionFile(error) => {
                write!(f, "invalid connection file JSON: {error}")
            }
            Self::Worker(message) => f.write_str(message),
            Self::Protocol(error) => write!(f, "{error}"),
            Self::Zmq(error) => write!(f, "{error}"),
            Self::HeartbeatThreadPanicked => f.write_str("heartbeat thread panicked"),
            Self::MessageLoopThreadPanicked => f.write_str("message loop thread panicked"),
        }
    }
}

impl std::error::Error for KernelError {}

impl From<ProtocolError> for KernelError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<zmq::Error> for KernelError {
    fn from(error: zmq::Error) -> Self {
        Self::Zmq(error)
    }
}

impl ConnectionInfo {
    fn from_config(config: ConnectionConfig) -> Result<Self, KernelError> {
        if config.transport.trim().is_empty() {
            return Err(KernelError::InvalidConnectionConfig(
                "connection file transport must not be empty",
            ));
        }
        if config.ip.trim().is_empty() {
            return Err(KernelError::InvalidConnectionConfig(
                "connection file ip must not be empty",
            ));
        }
        if config.signature_scheme.trim().is_empty() {
            return Err(KernelError::InvalidConnectionConfig(
                "connection file signature_scheme must not be empty",
            ));
        }

        Ok(Self {
            transport: config.transport,
            ip: config.ip,
            shell_port: config.shell_port,
            iopub_port: config.iopub_port,
            stdin_port: config.stdin_port,
            control_port: config.control_port,
            hb_port: config.hb_port,
            signature_scheme: config.signature_scheme,
            key: config.key,
            kernel_name: config.kernel_name,
        })
    }

    pub fn channel_endpoints(&self) -> ChannelEndpoints {
        ChannelEndpoints {
            shell: self.endpoint(self.shell_port),
            iopub: self.endpoint(self.iopub_port),
            stdin: self.endpoint(self.stdin_port),
            control: self.endpoint(self.control_port),
            hb: self.endpoint(self.hb_port),
        }
    }

    fn endpoint(&self, port: u16) -> String {
        format!("{}://{}:{port}", self.transport, self.ip)
    }
}

struct ShutdownSignal {
    is_stopped: AtomicBool,
    lock: Mutex<bool>,
    cvar: Condvar,
}

impl ShutdownSignal {
    fn new() -> Self {
        Self {
            is_stopped: AtomicBool::new(false),
            lock: Mutex::new(false),
            cvar: Condvar::new(),
        }
    }

    fn request_stop(&self) {
        self.is_stopped.store(true, Ordering::SeqCst);
        if let Ok(mut stopped) = self.lock.lock() {
            *stopped = true;
            self.cvar.notify_all();
        }
    }

    fn is_stopped(&self) -> bool {
        self.is_stopped.load(Ordering::SeqCst)
    }

    fn wait(&self) {
        let mut stopped = self.lock.lock().expect("shutdown mutex poisoned");
        while !*stopped {
            stopped = self
                .cvar
                .wait(stopped)
                .expect("shutdown condvar wait poisoned");
        }
    }
}

struct MessageLoopState {
    connection: ConnectionInfo,
    signer: MessageSigner,
    kernel_session: String,
    execution_count: u32,
    history: HistoryStore,
    comms: CommStore,
    worker: Arc<Mutex<PythonWorker>>,
    worker_interrupt: WorkerInterruptHandle,
    worker_kernel_info: WorkerKernelInfo,
    pending_execute: Option<PendingExecute>,
}

struct PendingExecute;

struct ExecuteCompletion {
    request: JupyterMessage,
    code: String,
    execution_count: u32,
    silent: bool,
    store_history: bool,
    outcome: Result<ExecutionOutcome, KernelError>,
}

struct HistoryStore {
    sessions: Vec<HistorySession>,
}

struct HistorySession {
    id: u32,
    entries: Vec<HistoryEntry>,
}

struct HistoryEntry {
    line: u32,
    input: String,
    output: Option<String>,
}

struct HistoryReplyEntry {
    session: u32,
    line: u32,
    input: String,
    output: Option<String>,
}

struct CommStore {
    entries: HashMap<String, CommEntry>,
}

struct CommEntry {
    target_name: String,
}

impl MessageLoopState {
    fn new(connection: &ConnectionInfo) -> Result<Self, KernelError> {
        let mut worker = PythonWorker::start()?;
        let worker_kernel_info = worker.kernel_info()?;
        let worker_interrupt = worker.interrupt_handle();
        Ok(Self {
            connection: connection.clone(),
            signer: MessageSigner::new(&connection.signature_scheme, &connection.key)?,
            kernel_session: connection
                .kernel_name
                .clone()
                .unwrap_or_else(|| IMPLEMENTATION.to_owned()),
            execution_count: 0,
            history: HistoryStore::new(),
            comms: CommStore::new(),
            worker: Arc::new(Mutex::new(worker)),
            worker_interrupt,
            worker_kernel_info,
            pending_execute: None,
        })
    }

    fn lock_worker(&self) -> Result<std::sync::MutexGuard<'_, PythonWorker>, KernelError> {
        self.worker
            .lock()
            .map_err(|_| KernelError::Worker("python worker mutex poisoned".to_owned()))
    }

    fn next_execution_count(&mut self, content: &Value) -> u32 {
        let silent = content
            .get("silent")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let store_history = content
            .get("store_history")
            .and_then(Value::as_bool)
            .unwrap_or(!silent);

        if silent || !store_history {
            self.execution_count
        } else {
            self.execution_count += 1;
            self.execution_count
        }
    }

    fn new_header(&self, msg_type: &str) -> MessageHeader {
        MessageHeader::new(msg_type, &self.kernel_session)
    }

    fn connect_reply_content(&self) -> Value {
        json!({
            "status": "ok",
            "shell_port": self.connection.shell_port,
            "iopub_port": self.connection.iopub_port,
            "stdin_port": self.connection.stdin_port,
            "control_port": self.connection.control_port,
            "hb_port": self.connection.hb_port,
        })
    }

    fn usage_reply_content(&self) -> Value {
        let mut system = System::new_all();
        system.refresh_all();

        let kernel_pid = Pid::from_u32(std::process::id());
        let kernel_processes = system
            .processes()
            .keys()
            .copied()
            .filter(|pid| process_is_kernel_descendant(&system, *pid, kernel_pid));

        let kernel_cpu = kernel_processes
            .clone()
            .filter_map(|pid| system.process(pid))
            .map(|process| f64::from(process.cpu_usage()))
            .sum::<f64>();
        let kernel_memory = kernel_processes
            .filter_map(|pid| system.process(pid))
            .map(|process| process.memory())
            .sum::<u64>();

        let mut content = json!({
            "hostname": System::host_name().unwrap_or_default(),
            "pid": std::process::id(),
            "kernel_cpu": kernel_cpu,
            "kernel_memory": kernel_memory,
            "cpu_count": system.cpus().len(),
            "host_virtual_memory": {
                "total": system.total_memory(),
                "available": system.available_memory(),
                "used": system.used_memory(),
                "free": system.free_memory(),
                "total_swap": system.total_swap(),
                "used_swap": system.used_swap(),
                "free_swap": system.free_swap(),
            },
        });

        let host_cpu_percent = f64::from(system.global_cpu_usage());
        if host_cpu_percent > 0.0 {
            content["host_cpu_percent"] = json!(host_cpu_percent);
        }

        content
    }

    fn record_history(&mut self, line: u32, input: &str, outcome: &ExecutionOutcome) {
        self.history.record(line, input, outcome);
    }

    fn history_reply_content(&self, request: &Value) -> Value {
        self.history.reply_content(request)
    }

    fn register_comm(&mut self, content: &Value) {
        self.comms.register(content);
    }

    fn close_comm(&mut self, content: &Value) {
        self.comms.close(content);
    }

    fn comm_info_reply_content(&self, request: &Value) -> Value {
        self.comms.reply_content(request)
    }

    fn kernel_info_content(&self) -> Value {
        let language_version = &self.worker_kernel_info.language_version;
        let language_version_major = self.worker_kernel_info.language_version_major;
        let language_version_minor = self.worker_kernel_info.language_version_minor;

        json!({
            "status": "ok",
            "protocol_version": JUPYTER_PROTOCOL_VERSION,
            "implementation": IMPLEMENTATION,
            "implementation_version": env!("CARGO_PKG_VERSION"),
            "banner": format!(
                "{IMPLEMENTATION} {} (Python {language_version})",
                env!("CARGO_PKG_VERSION"),
            ),
            // Some clients still probe the pre-v5 aliases before falling back to
            // language_info, so keep them in sync to reduce frontend branching.
            "language": LANGUAGE,
            "language_version": language_version,
            "debugger": false,
            "help_links": [{
                "text": "Python Reference",
                "url": format!("https://docs.python.org/{language_version_major}.{language_version_minor}"),
            }],
            "supported_features": [],
            "language_info": {
                "name": LANGUAGE,
                "version": language_version,
                "mimetype": "text/x-python",
                "file_extension": ".py",
                "pygments_lexer": format!("ipython{language_version_major}"),
                "nbconvert_exporter": "python",
                "codemirror_mode": {
                    "name": "ipython",
                    "version": language_version_major,
                },
            },
        })
    }

    fn restart(&mut self) -> Result<(), KernelError> {
        let worker_kernel_info = {
            let mut worker = self.lock_worker()?;
            worker.restart()?;
            worker.kernel_info()?
        };
        self.worker_kernel_info = worker_kernel_info;
        self.execution_count = 0;
        self.history.start_new_session();
        self.comms.clear();
        Ok(())
    }

    fn interrupt(&self) -> Result<(), KernelError> {
        self.worker_interrupt.interrupt()
    }

    fn is_executing(&self) -> bool {
        self.pending_execute.is_some()
    }
}

fn process_is_kernel_descendant(system: &System, pid: Pid, kernel_pid: Pid) -> bool {
    let mut current_pid = Some(pid);
    while let Some(candidate) = current_pid {
        if candidate == kernel_pid {
            return true;
        }

        current_pid = system
            .process(candidate)
            .and_then(|process| process.parent());
    }

    false
}

impl HistoryStore {
    fn new() -> Self {
        Self {
            sessions: vec![HistorySession::new(1)],
        }
    }

    fn record(&mut self, line: u32, input: &str, outcome: &ExecutionOutcome) {
        self.current_session_mut().entries.push(HistoryEntry {
            line,
            input: input.to_owned(),
            output: history_output(outcome),
        });
    }

    fn reply_content(&self, request: &Value) -> Value {
        let hist_access_type = request
            .get("hist_access_type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let output = request
            .get("output")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let session = request
            .get("session")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(0);
        let start = request
            .get("start")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(0);
        let stop = request
            .get("stop")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok());
        let n = request
            .get("n")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok());
        let pattern = request
            .get("pattern")
            .and_then(Value::as_str)
            .unwrap_or("*");
        let unique = request
            .get("unique")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let history = match hist_access_type {
            "tail" => self
                .tail_entries(n.unwrap_or_else(|| self.entry_count()))
                .into_iter()
                .map(|entry| entry.as_value(output))
                .collect::<Vec<_>>(),
            "range" => self
                .range_entries(session, start, stop)
                .into_iter()
                .map(|entry| entry.as_value(output))
                .collect::<Vec<_>>(),
            "search" => self
                .search_entries(pattern, n, unique)
                .into_iter()
                .map(|entry| entry.as_value(output))
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        };

        json!({
            "status": "ok",
            "history": history,
        })
    }

    fn start_new_session(&mut self) {
        let next_id = self.current_session_id().saturating_add(1);
        self.sessions.push(HistorySession::new(next_id));
    }

    fn entry_count(&self) -> usize {
        self.sessions
            .iter()
            .map(|session| session.entries.len())
            .sum()
    }

    fn current_session(&self) -> &HistorySession {
        self.sessions
            .last()
            .expect("history store must keep a current session")
    }

    fn current_session_mut(&mut self) -> &mut HistorySession {
        self.sessions
            .last_mut()
            .expect("history store must keep a current session")
    }

    fn current_session_id(&self) -> u32 {
        self.current_session().id
    }

    fn resolve_session(&self, requested_session: i32) -> Option<&HistorySession> {
        let current_session = i32::try_from(self.current_session_id()).ok()?;
        let target_session = if requested_session <= 0 {
            current_session + requested_session
        } else {
            requested_session
        };

        if target_session <= 0 {
            return None;
        }

        let target_session = u32::try_from(target_session).ok()?;
        self.sessions
            .iter()
            .find(|session| session.id == target_session)
    }

    fn tail_entries(&self, n: usize) -> Vec<HistoryReplyEntry> {
        let mut entries = self.all_entries();
        keep_last_entries(&mut entries, n);
        entries
    }

    fn range_entries(
        &self,
        requested_session: i32,
        start: i32,
        stop: Option<i32>,
    ) -> Vec<HistoryReplyEntry> {
        let Some(session) = self.resolve_session(requested_session) else {
            return Vec::new();
        };

        if session.id == self.current_session_id() {
            return self.current_session_range_entries(start, stop);
        }

        session
            .entries
            .iter()
            .filter(|entry| {
                i32::try_from(entry.line).ok().is_some_and(|line| {
                    line >= start && stop.is_none_or(|stop_line| line < stop_line)
                })
            })
            .map(|entry| HistoryReplyEntry::from_entry(session.id, entry))
            .collect()
    }

    fn current_session_range_entries(
        &self,
        start: i32,
        stop: Option<i32>,
    ) -> Vec<HistoryReplyEntry> {
        let session = self.current_session();
        let line_count = i32::try_from(session.entries.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .unwrap_or(i32::MAX);
        let start = normalize_history_index(start, line_count);
        let stop = stop
            .map(|stop_line| normalize_history_index(stop_line, line_count))
            .unwrap_or(line_count);

        if start >= stop {
            return Vec::new();
        }

        let mut entries = Vec::new();
        if start == 0 {
            entries.push(HistoryReplyEntry {
                session: 0,
                line: 0,
                input: String::new(),
                output: None,
            });
        }

        let first_line = start.max(1);
        for line in first_line..stop {
            let Some(index) = usize::try_from(line - 1).ok() else {
                continue;
            };
            let Some(entry) = session.entries.get(index) else {
                continue;
            };
            entries.push(HistoryReplyEntry::from_entry(0, entry));
        }

        entries
    }

    fn search_entries(
        &self,
        pattern: &str,
        n: Option<usize>,
        unique: bool,
    ) -> Vec<HistoryReplyEntry> {
        let mut matches = self
            .all_entries()
            .into_iter()
            .filter(|entry| matches_history_pattern(&entry.input, pattern))
            .collect::<Vec<_>>();

        if unique {
            let mut latest_indices = HashMap::new();
            for (index, entry) in matches.iter().enumerate() {
                latest_indices.insert(entry.input.clone(), index);
            }
            matches = matches
                .into_iter()
                .enumerate()
                .filter(|(index, entry)| latest_indices.get(&entry.input) == Some(index))
                .map(|(_, entry)| entry)
                .collect();
        }

        if let Some(limit) = n {
            keep_last_entries(&mut matches, limit);
        }

        matches
    }

    fn all_entries(&self) -> Vec<HistoryReplyEntry> {
        self.sessions
            .iter()
            .flat_map(|session| {
                session
                    .entries
                    .iter()
                    .map(move |entry| HistoryReplyEntry::from_entry(session.id, entry))
            })
            .collect()
    }
}

impl HistorySession {
    fn new(id: u32) -> Self {
        Self {
            id,
            entries: Vec::new(),
        }
    }
}

impl HistoryReplyEntry {
    fn from_entry(session: u32, entry: &HistoryEntry) -> Self {
        Self {
            session,
            line: entry.line,
            input: entry.input.clone(),
            output: entry.output.clone(),
        }
    }

    fn as_value(&self, include_output: bool) -> Value {
        if include_output {
            json!([self.session, self.line, [self.input, self.output]])
        } else {
            json!([self.session, self.line, self.input])
        }
    }
}

impl CommStore {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    fn register(&mut self, content: &Value) {
        let Some(comm_id) = content.get("comm_id").and_then(Value::as_str) else {
            return;
        };
        let Some(target_name) = content.get("target_name").and_then(Value::as_str) else {
            return;
        };

        self.entries.insert(
            comm_id.to_owned(),
            CommEntry {
                target_name: target_name.to_owned(),
            },
        );
    }

    fn close(&mut self, content: &Value) {
        let Some(comm_id) = content.get("comm_id").and_then(Value::as_str) else {
            return;
        };
        self.entries.remove(comm_id);
    }

    fn reply_content(&self, request: &Value) -> Value {
        let target_name = request.get("target_name").and_then(Value::as_str);
        let comms = self
            .entries
            .iter()
            .filter(|(_, entry)| target_name.is_none_or(|target| entry.target_name == target))
            .map(|(comm_id, entry)| {
                (
                    comm_id.clone(),
                    json!({
                        "target_name": entry.target_name,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();

        json!({
            "status": "ok",
            "comms": comms,
        })
    }

    fn clear(&mut self) {
        self.entries.clear();
    }

    fn apply_event(&mut self, event: &WorkerCommEvent) {
        match event.msg_type.as_str() {
            "comm_open" => self.register(&event.content),
            "comm_close" => self.close(&event.content),
            _ => {}
        }
    }
}

fn history_output(outcome: &ExecutionOutcome) -> Option<String> {
    outcome
        .result
        .as_ref()
        .and_then(|result| result.data.get("text/plain"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn matches_history_pattern(input: &str, pattern: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let input = input.chars().collect::<Vec<_>>();
    let mut states = vec![vec![false; input.len() + 1]; pattern.len() + 1];
    states[0][0] = true;

    for pattern_index in 0..pattern.len() {
        if pattern[pattern_index] == '*' {
            states[pattern_index + 1][0] = states[pattern_index][0];
        }
    }

    for pattern_index in 0..pattern.len() {
        for input_index in 0..input.len() {
            states[pattern_index + 1][input_index + 1] = match pattern[pattern_index] {
                '*' => {
                    states[pattern_index][input_index + 1] || states[pattern_index + 1][input_index]
                }
                '?' => states[pattern_index][input_index],
                literal => states[pattern_index][input_index] && literal == input[input_index],
            };
        }
    }

    states[pattern.len()][input.len()]
}

fn keep_last_entries<T>(entries: &mut Vec<T>, limit: usize) {
    if entries.len() > limit {
        let drop_count = entries.len() - limit;
        entries.drain(0..drop_count);
    }
}

fn normalize_history_index(index: i32, line_count: i32) -> i32 {
    let adjusted = if index < 0 {
        index.saturating_add(line_count)
    } else {
        index
    };
    adjusted.clamp(0, line_count)
}

enum ChannelKind {
    Shell,
    Control,
}

enum RequestDisposition {
    Complete { should_stop: bool },
    Deferred,
}

pub struct KernelRuntime {
    connection: ConnectionInfo,
    endpoints: ChannelEndpoints,
    _context: zmq::Context,
    shutdown: Arc<ShutdownSignal>,
    hb_thread: Option<JoinHandle<Result<(), zmq::Error>>>,
    message_loop_thread: Option<JoinHandle<Result<(), KernelError>>>,
}

impl KernelRuntime {
    pub fn start(connection: ConnectionInfo) -> Result<Self, KernelError> {
        let endpoints = connection.channel_endpoints();
        let context = zmq::Context::new();
        let shutdown = Arc::new(ShutdownSignal::new());

        let (hb_thread, hb_ready) =
            spawn_heartbeat_thread(context.clone(), endpoints.hb.clone(), Arc::clone(&shutdown));
        match hb_ready.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = hb_thread.join();
                return Err(KernelError::Zmq(error));
            }
            Err(_) => return Err(KernelError::HeartbeatThreadPanicked),
        }

        let (message_loop_thread, message_loop_ready) = spawn_message_loop_thread(
            context.clone(),
            connection.clone(),
            endpoints.clone(),
            Arc::clone(&shutdown),
        );
        match message_loop_ready.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                shutdown.request_stop();
                let _ = message_loop_thread.join();
                match hb_thread.join() {
                    Ok(Ok(())) | Ok(Err(_)) | Err(_) => {}
                }
                return Err(error);
            }
            Err(_) => {
                shutdown.request_stop();
                let _ = message_loop_thread.join();
                match hb_thread.join() {
                    Ok(Ok(())) | Ok(Err(_)) | Err(_) => {}
                }
                return Err(KernelError::MessageLoopThreadPanicked);
            }
        }

        Ok(Self {
            connection,
            endpoints,
            _context: context,
            shutdown,
            hb_thread: Some(hb_thread),
            message_loop_thread: Some(message_loop_thread),
        })
    }

    pub fn connection_info(&self) -> &ConnectionInfo {
        &self.connection
    }

    pub fn channel_endpoints(&self) -> &ChannelEndpoints {
        &self.endpoints
    }

    pub fn is_running(&self) -> bool {
        !self.shutdown.is_stopped()
    }

    pub fn wait_for_shutdown(&self) {
        self.shutdown.wait();
    }

    pub fn stop(&mut self) -> Result<(), KernelError> {
        if self.shutdown.is_stopped() {
            return self.finish_shutdown();
        }

        self.shutdown.request_stop();
        self.finish_shutdown()
    }

    fn finish_shutdown(&mut self) -> Result<(), KernelError> {
        if let Some(handle) = self.message_loop_thread.take() {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => return Err(KernelError::MessageLoopThreadPanicked),
            }
        }

        if let Some(handle) = self.hb_thread.take() {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(KernelError::Zmq(error)),
                Err(_) => return Err(KernelError::HeartbeatThreadPanicked),
            }
        }

        Ok(())
    }
}

impl Drop for KernelRuntime {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn bind_socket(
    context: &zmq::Context,
    socket_type: zmq::SocketType,
    endpoint: &str,
) -> Result<zmq::Socket, KernelError> {
    let socket = context.socket(socket_type)?;
    socket.set_linger(0)?;
    socket.bind(endpoint)?;
    Ok(socket)
}

fn spawn_message_loop_thread(
    context: zmq::Context,
    connection: ConnectionInfo,
    endpoints: ChannelEndpoints,
    shutdown: Arc<ShutdownSignal>,
) -> (
    JoinHandle<Result<(), KernelError>>,
    mpsc::Receiver<Result<(), KernelError>>,
) {
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let handle = thread::spawn(move || {
        let shell_socket = bind_socket(&context, zmq::ROUTER, &endpoints.shell)?;
        let iopub_socket = bind_socket(&context, zmq::PUB, &endpoints.iopub)?;
        let stdin_socket = bind_socket(&context, zmq::ROUTER, &endpoints.stdin)?;
        let control_socket = bind_socket(&context, zmq::ROUTER, &endpoints.control)?;
        let mut state = MessageLoopState::new(&connection)?;
        let (execute_tx, execute_rx) = mpsc::channel();

        publish_status(&iopub_socket, &state, json!({}), "starting")?;
        let _ = ready_tx.send(Ok(()));

        loop {
            if shutdown.is_stopped() {
                return Ok(());
            }

            while let Ok(completion) = execute_rx.try_recv() {
                finalize_execute_completion(&shell_socket, &iopub_socket, &mut state, completion)?;
            }

            let poll_shell = !state.is_executing();
            let mut poll_items = if poll_shell {
                vec![
                    shell_socket.as_poll_item(zmq::POLLIN),
                    control_socket.as_poll_item(zmq::POLLIN),
                    stdin_socket.as_poll_item(zmq::POLLIN),
                ]
            } else {
                vec![
                    control_socket.as_poll_item(zmq::POLLIN),
                    stdin_socket.as_poll_item(zmq::POLLIN),
                ]
            };
            match zmq::poll(&mut poll_items, CHANNEL_POLL_INTERVAL_MS) {
                Ok(_) => {}
                Err(zmq::Error::ETERM) if shutdown.is_stopped() => return Ok(()),
                Err(error) => return Err(KernelError::Zmq(error)),
            }

            if poll_shell && poll_items[0].is_readable() {
                let frames = shell_socket.recv_multipart(0)?;
                handle_request(
                    ChannelKind::Shell,
                    frames,
                    &shell_socket,
                    &stdin_socket,
                    &iopub_socket,
                    &mut state,
                    &execute_tx,
                    shutdown.as_ref(),
                )?;
            }

            let control_index = if poll_shell { 1 } else { 0 };
            if poll_items[control_index].is_readable() {
                let frames = control_socket.recv_multipart(0)?;
                handle_request(
                    ChannelKind::Control,
                    frames,
                    &control_socket,
                    &stdin_socket,
                    &iopub_socket,
                    &mut state,
                    &execute_tx,
                    shutdown.as_ref(),
                )?;
            }

            let stdin_index = if poll_shell { 2 } else { 1 };
            if poll_items[stdin_index].is_readable() {
                let frames = stdin_socket.recv_multipart(0)?;
                handle_stdin_message(frames, &state)?;
            }
        }
    });

    (handle, ready_rx)
}

fn handle_request(
    channel: ChannelKind,
    frames: Vec<Vec<u8>>,
    reply_socket: &zmq::Socket,
    stdin_socket: &zmq::Socket,
    iopub_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    execute_tx: &mpsc::Sender<ExecuteCompletion>,
    shutdown: &ShutdownSignal,
) -> Result<(), KernelError> {
    let request = match state.signer.decode(frames) {
        Ok(request) => request,
        Err(_) => return Ok(()),
    };

    let parent_header = request.header_value.clone();
    publish_status(iopub_socket, state, parent_header.clone(), "busy")?;

    let disposition = match channel {
        ChannelKind::Shell => handle_shell_request(
            reply_socket,
            stdin_socket,
            iopub_socket,
            state,
            execute_tx,
            &request,
        )?,
        ChannelKind::Control => {
            handle_control_request(reply_socket, iopub_socket, state, &request)?
        }
    };

    match disposition {
        RequestDisposition::Complete { should_stop } => {
            publish_status(iopub_socket, state, parent_header, "idle")?;

            if should_stop {
                shutdown.request_stop();
            }
        }
        RequestDisposition::Deferred => {}
    }

    Ok(())
}

fn spawn_execute_request(
    execute_tx: &mpsc::Sender<ExecuteCompletion>,
    worker: Arc<Mutex<PythonWorker>>,
    request: JupyterMessage,
    code: String,
    user_expressions: Value,
    execution_count: u32,
    silent: bool,
    store_history: bool,
) {
    let execute_tx = execute_tx.clone();
    thread::spawn(move || {
        let outcome = worker
            .lock()
            .map_err(|_| KernelError::Worker("python worker mutex poisoned".to_owned()))
            .and_then(|mut worker| {
                worker.execute(
                    &code,
                    &user_expressions,
                    execution_count,
                    silent,
                    store_history,
                    |_prompt, _password| {
                        Err("stdin is not enabled for this execute_request".to_owned())
                    },
                )
            });
        let _ = execute_tx.send(ExecuteCompletion {
            request,
            code,
            execution_count,
            silent,
            store_history,
            outcome,
        });
    });
}

fn finalize_execute_completion(
    reply_socket: &zmq::Socket,
    iopub_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    completion: ExecuteCompletion,
) -> Result<(), KernelError> {
    state.pending_execute = None;

    let ExecuteCompletion {
        request,
        code,
        execution_count,
        silent,
        store_history,
        outcome,
    } = completion;
    let outcome = outcome?;

    if !silent && !outcome.stdout.is_empty() {
        publish_stream(
            iopub_socket,
            state,
            request.header_value.clone(),
            "stdout",
            &outcome.stdout,
        )?;
    }
    if !silent && !outcome.stderr.is_empty() {
        publish_stream(
            iopub_socket,
            state,
            request.header_value.clone(),
            "stderr",
            &outcome.stderr,
        )?;
    }
    if !silent {
        publish_comm_events(
            iopub_socket,
            state,
            request.header_value.clone(),
            &outcome.comm_events,
        )?;
    }

    if !silent && store_history {
        state.record_history(execution_count, &code, &outcome);
    }

    if outcome.status == "ok" {
        if !silent {
            for display in outcome.displays {
                if !display.content.is_null() {
                    publish_iopub_message(
                        iopub_socket,
                        state,
                        request.header_value.clone(),
                        &display.msg_type,
                        display.content,
                    )?;
                } else {
                    publish_display_event(
                        iopub_socket,
                        state,
                        request.header_value.clone(),
                        &display.msg_type,
                        display.data,
                        display.metadata,
                        display.transient,
                    )?;
                }
            }

            if let Some(result) = outcome.result {
                publish_iopub_message(
                    iopub_socket,
                    state,
                    request.header_value.clone(),
                    "execute_result",
                    json!({
                        "execution_count": execution_count,
                        "data": result.data,
                        "metadata": result.metadata,
                    }),
                )?;
            }
        }

        send_reply(
            reply_socket,
            state,
            &request,
            "execute_reply",
            json!({
                "status": "ok",
                "execution_count": execution_count,
                "user_expressions": outcome.user_expressions,
                "payload": outcome.payload,
            }),
        )?;
    } else {
        let ename = outcome.ename.unwrap_or_else(|| "ExecutionError".to_owned());
        let evalue = outcome.evalue.unwrap_or_default();
        let user_expressions = outcome.user_expressions;
        let payload = outcome.payload;
        let traceback = outcome.traceback;

        if !silent {
            publish_iopub_message(
                iopub_socket,
                state,
                request.header_value.clone(),
                "error",
                json!({
                    "ename": ename,
                    "evalue": evalue,
                    "traceback": traceback,
                }),
            )?;
        }

        send_reply(
            reply_socket,
            state,
            &request,
            "execute_reply",
            json!({
                "status": "error",
                "execution_count": execution_count,
                "ename": ename,
                "evalue": evalue,
                "user_expressions": user_expressions,
                "payload": payload,
                "traceback": traceback,
            }),
        )?;
    }

    publish_status(iopub_socket, state, request.header_value.clone(), "idle")?;
    Ok(())
}

fn handle_shell_request(
    reply_socket: &zmq::Socket,
    stdin_socket: &zmq::Socket,
    iopub_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    execute_tx: &mpsc::Sender<ExecuteCompletion>,
    request: &JupyterMessage,
) -> Result<RequestDisposition, KernelError> {
    match request.header.msg_type.as_str() {
        "kernel_info_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "kernel_info_reply",
                state.kernel_info_content(),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "connect_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "connect_reply",
                state.connect_reply_content(),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "execute_request" => {
            let silent = request
                .content
                .get("silent")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let user_expressions = request
                .content
                .get("user_expressions")
                .unwrap_or(&Value::Null);
            let store_history = request
                .content
                .get("store_history")
                .and_then(Value::as_bool)
                .unwrap_or(!silent);
            let allow_stdin = request
                .content
                .get("allow_stdin")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let code = request
                .content
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let execution_count = state.next_execution_count(&request.content);
            let code_owned = code.to_owned();

            if !silent {
                publish_iopub_message(
                    iopub_socket,
                    state,
                    request.header_value.clone(),
                    "execute_input",
                    json!({
                        "code": code,
                        "execution_count": execution_count,
                    }),
                )?;
            }

            if allow_stdin {
                let signer = state.signer.clone();
                let kernel_session = state.kernel_session.clone();
                let request_identities = request.identities.clone();
                let parent_header = request.header_value.clone();
                let outcome = state.lock_worker()?.execute(
                    code,
                    user_expressions,
                    execution_count,
                    silent,
                    store_history,
                    |prompt, password| {
                        request_stdin_input(
                            stdin_socket,
                            &signer,
                            &kernel_session,
                            &request_identities,
                            &parent_header,
                            prompt,
                            password,
                            allow_stdin,
                        )
                    },
                )?;

                finalize_execute_completion(
                    reply_socket,
                    iopub_socket,
                    state,
                    ExecuteCompletion {
                        request: request.clone(),
                        code: code_owned,
                        execution_count,
                        silent,
                        store_history,
                        outcome: Ok(outcome),
                    },
                )?;
                Ok(RequestDisposition::Deferred)
            } else {
                state.pending_execute = Some(PendingExecute);
                spawn_execute_request(
                    execute_tx,
                    Arc::clone(&state.worker),
                    request.clone(),
                    code_owned,
                    user_expressions.clone(),
                    execution_count,
                    silent,
                    store_history,
                );
                Ok(RequestDisposition::Deferred)
            }
        }
        "is_complete_request" => {
            let code = request
                .content
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let outcome = state.lock_worker()?.is_complete(code)?;
            send_reply(
                reply_socket,
                state,
                request,
                "is_complete_reply",
                json!({
                    "status": outcome.status,
                    "indent": outcome.indent,
                }),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "complete_request" => {
            let code = request
                .content
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let cursor_pos = request
                .content
                .get("cursor_pos")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let completion = state
                .lock_worker()?
                .complete(code, cursor_pos.max(0) as usize)?;
            send_reply(
                reply_socket,
                state,
                request,
                "complete_reply",
                json!({
                    "status": completion.status,
                    "matches": completion.matches,
                    "cursor_start": completion.cursor_start,
                    "cursor_end": completion.cursor_end,
                    "metadata": completion.metadata,
                }),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "inspect_request" => {
            let code = request
                .content
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let cursor_pos = request
                .content
                .get("cursor_pos")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let detail_level = request
                .content
                .get("detail_level")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let inspection = state.lock_worker()?.inspect(
                code,
                cursor_pos.max(0) as usize,
                detail_level as u8,
            )?;
            send_reply(
                reply_socket,
                state,
                request,
                "inspect_reply",
                json!({
                    "status": inspection.status,
                    "found": inspection.found,
                    "data": inspection.data,
                    "metadata": inspection.metadata,
                }),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "history_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "history_reply",
                state.history_reply_content(&request.content),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "comm_open" => {
            let comm_id = request
                .content
                .get("comm_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let target_name = request
                .content
                .get("target_name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let data = request.content.get("data").unwrap_or(&Value::Null);
            let metadata = request.content.get("metadata").unwrap_or(&Value::Null);
            let outcome = state
                .lock_worker()?
                .comm_open(comm_id, target_name, data, metadata)?;
            handle_comm_outcome(iopub_socket, state, request, &outcome)?;
            if outcome.registered {
                state.register_comm(&request.content);
            } else {
                state.close_comm(&request.content);
            }
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "comm_msg" => {
            let comm_id = request
                .content
                .get("comm_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let data = request.content.get("data").unwrap_or(&Value::Null);
            let metadata = request.content.get("metadata").unwrap_or(&Value::Null);
            let outcome = state.lock_worker()?.comm_msg(comm_id, data, metadata)?;
            handle_comm_outcome(iopub_socket, state, request, &outcome)?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "comm_close" => {
            let comm_id = request
                .content
                .get("comm_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let data = request.content.get("data").unwrap_or(&Value::Null);
            let metadata = request.content.get("metadata").unwrap_or(&Value::Null);
            let outcome = state.lock_worker()?.comm_close(comm_id, data, metadata)?;
            handle_comm_outcome(iopub_socket, state, request, &outcome)?;
            state.close_comm(&request.content);
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "comm_info_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "comm_info_reply",
                state.comm_info_reply_content(&request.content),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "shutdown_request" => handle_shutdown_request(reply_socket, iopub_socket, state, request),
        _ => send_unsupported_reply(reply_socket, state, request),
    }
}

fn handle_control_request(
    reply_socket: &zmq::Socket,
    iopub_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<RequestDisposition, KernelError> {
    match request.header.msg_type.as_str() {
        "kernel_info_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "kernel_info_reply",
                state.kernel_info_content(),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "connect_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "connect_reply",
                state.connect_reply_content(),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "shutdown_request" => handle_shutdown_request(reply_socket, iopub_socket, state, request),
        "interrupt_request" => {
            if state.is_executing() {
                state.interrupt()?;
            }
            send_reply(
                reply_socket,
                state,
                request,
                "interrupt_reply",
                json!({ "status": "ok" }),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "usage_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "usage_reply",
                state.usage_reply_content(),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        _ => send_unsupported_reply(reply_socket, state, request),
    }
}

fn handle_shutdown_request(
    reply_socket: &zmq::Socket,
    iopub_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<RequestDisposition, KernelError> {
    let restart = request
        .content
        .get("restart")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if restart {
        state.restart()?;
    }

    send_reply(
        reply_socket,
        state,
        request,
        "shutdown_reply",
        json!({
            "status": "ok",
            "restart": restart,
        }),
    )?;
    publish_iopub_message(
        iopub_socket,
        state,
        request.header_value.clone(),
        "shutdown_reply",
        json!({
            "status": "ok",
            "restart": restart,
        }),
    )?;
    Ok(RequestDisposition::Complete {
        should_stop: !restart,
    })
}

fn send_unsupported_reply(
    reply_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<RequestDisposition, KernelError> {
    let reply_type = reply_type_for(&request.header.msg_type);
    send_reply(
        reply_socket,
        state,
        request,
        &reply_type,
        json!({
            "status": "error",
            "ename": "NotImplemented",
            "evalue": format!("unsupported message type: {}", request.header.msg_type),
            "traceback": [],
        }),
    )?;
    Ok(RequestDisposition::Complete { should_stop: false })
}

fn request_stdin_input(
    stdin_socket: &zmq::Socket,
    signer: &MessageSigner,
    kernel_session: &str,
    identities: &[Vec<u8>],
    parent_header: &Value,
    prompt: &str,
    password: bool,
    allow_stdin: bool,
) -> Result<String, String> {
    if !allow_stdin {
        return Err("stdin is not enabled for this execute_request".to_owned());
    }

    let input_request = JupyterMessage::new(
        identities.to_vec(),
        MessageHeader::new("input_request", kernel_session),
        parent_header.clone(),
        json!({}),
        json!({
            "prompt": prompt,
            "password": password,
        }),
    );

    stdin_socket
        .send_multipart(
            signer
                .encode(&input_request)
                .map_err(|error| error.to_string())?,
            0,
        )
        .map_err(|error| error.to_string())?;

    loop {
        let frames = stdin_socket
            .recv_multipart(0)
            .map_err(|error| error.to_string())?;
        let message = match signer.decode(frames) {
            Ok(message) => message,
            Err(_) => continue,
        };

        if message.header.msg_type != "input_reply" {
            continue;
        }

        if let Some(value) = message.content.get("value").and_then(Value::as_str) {
            return Ok(value.to_owned());
        }

        return Ok(String::new());
    }
}

fn handle_stdin_message(frames: Vec<Vec<u8>>, state: &MessageLoopState) -> Result<(), KernelError> {
    let message = match state.signer.decode(frames) {
        Ok(message) => message,
        Err(_) => return Ok(()),
    };

    if message.header.msg_type == "input_reply" {
        return Ok(());
    }

    Ok(())
}

fn send_reply(
    socket: &zmq::Socket,
    state: &MessageLoopState,
    request: &JupyterMessage,
    msg_type: &str,
    content: Value,
) -> Result<(), KernelError> {
    let reply = JupyterMessage::new(
        request.identities.clone(),
        state.new_header(msg_type),
        request.header_value.clone(),
        json!({}),
        content,
    );
    socket.send_multipart(state.signer.encode(&reply)?, 0)?;
    Ok(())
}

fn publish_status(
    socket: &zmq::Socket,
    state: &MessageLoopState,
    parent_header: Value,
    execution_state: &str,
) -> Result<(), KernelError> {
    publish_iopub_message(
        socket,
        state,
        parent_header,
        "status",
        json!({ "execution_state": execution_state }),
    )
}

fn publish_iopub_message(
    socket: &zmq::Socket,
    state: &MessageLoopState,
    parent_header: Value,
    msg_type: &str,
    content: Value,
) -> Result<(), KernelError> {
    let message = JupyterMessage::new(
        vec![msg_type.as_bytes().to_vec()],
        state.new_header(msg_type),
        parent_header,
        json!({}),
        content,
    );
    socket.send_multipart(state.signer.encode(&message)?, 0)?;
    Ok(())
}

fn publish_display_event(
    socket: &zmq::Socket,
    state: &MessageLoopState,
    parent_header: Value,
    msg_type: &str,
    data: Value,
    metadata: Value,
    transient: Value,
) -> Result<(), KernelError> {
    publish_iopub_message(
        socket,
        state,
        parent_header,
        msg_type,
        json!({
            "data": data,
            "metadata": metadata,
            "transient": transient,
        }),
    )
}

fn publish_comm_events(
    socket: &zmq::Socket,
    state: &mut MessageLoopState,
    parent_header: Value,
    events: &[WorkerCommEvent],
) -> Result<(), KernelError> {
    for event in events {
        publish_iopub_message(
            socket,
            state,
            parent_header.clone(),
            &event.msg_type,
            event.content.clone(),
        )?;
        state.comms.apply_event(event);
    }
    Ok(())
}

fn publish_stream(
    socket: &zmq::Socket,
    state: &MessageLoopState,
    parent_header: Value,
    name: &str,
    text: &str,
) -> Result<(), KernelError> {
    publish_iopub_message(
        socket,
        state,
        parent_header,
        "stream",
        json!({
            "name": name,
            "text": text,
        }),
    )
}

fn handle_comm_outcome(
    iopub_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
    outcome: &CommOutcome,
) -> Result<(), KernelError> {
    if !outcome.stdout.is_empty() {
        publish_stream(
            iopub_socket,
            state,
            request.header_value.clone(),
            "stdout",
            &outcome.stdout,
        )?;
    }
    if !outcome.stderr.is_empty() {
        publish_stream(
            iopub_socket,
            state,
            request.header_value.clone(),
            "stderr",
            &outcome.stderr,
        )?;
    }
    publish_comm_events(
        iopub_socket,
        state,
        request.header_value.clone(),
        &outcome.events,
    )?;
    Ok(())
}

fn reply_type_for(msg_type: &str) -> String {
    if let Some(prefix) = msg_type.strip_suffix("_request") {
        format!("{prefix}_reply")
    } else {
        "error".to_owned()
    }
}

fn spawn_heartbeat_thread(
    context: zmq::Context,
    endpoint: String,
    shutdown: Arc<ShutdownSignal>,
) -> (
    JoinHandle<Result<(), zmq::Error>>,
    mpsc::Receiver<Result<(), zmq::Error>>,
) {
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let handle = thread::spawn(move || {
        let socket = context.socket(zmq::REP)?;
        socket.set_linger(0)?;
        socket.set_rcvtimeo(HEARTBEAT_POLL_INTERVAL_MS)?;
        if let Err(error) = socket.bind(&endpoint) {
            let _ = ready_tx.send(Err(error));
            return Err(error);
        }
        let _ = ready_tx.send(Ok(()));

        loop {
            if shutdown.is_stopped() {
                return Ok(());
            }

            match socket.recv_multipart(0) {
                Ok(message) => socket.send_multipart(message, 0)?,
                Err(zmq::Error::EAGAIN) => {}
                Err(zmq::Error::ETERM) if shutdown.is_stopped() => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    });

    (handle, ready_rx)
}

pub fn healthcheck() -> &'static str {
    "ok"
}

pub fn parse_connection_file(path: &str) -> Result<ConnectionInfo, KernelError> {
    let contents = fs::read_to_string(path).map_err(KernelError::Io)?;
    let config: ConnectionConfig =
        serde_json::from_str(&contents).map_err(KernelError::InvalidConnectionFile)?;

    ConnectionInfo::from_config(config)
}

pub fn start_kernel(connection: ConnectionInfo) -> Result<KernelRuntime, KernelError> {
    KernelRuntime::start(connection)
}

pub fn start_kernel_from_connection_file(path: &str) -> Result<KernelRuntime, KernelError> {
    let connection = parse_connection_file(path)?;
    start_kernel(connection)
}

pub fn runtime_info() -> KernelRuntimeInfo {
    KernelRuntimeInfo {
        implementation: IMPLEMENTATION.to_owned(),
        implementation_version: env!("CARGO_PKG_VERSION").to_owned(),
        language: LANGUAGE.to_owned(),
        protocol_version: JUPYTER_PROTOCOL_VERSION.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::thread;
    use std::time::{Duration, Instant};

    use serde_json::{Value, json};

    use super::{ConnectionConfig, ConnectionInfo, KernelError, start_kernel};
    use crate::protocol::{JupyterMessage, MessageHeader, MessageSigner};

    fn test_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        match LOCK.get_or_init(|| Mutex::new(())).lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[test]
    fn accepts_valid_connection_config() {
        let _guard = test_lock();
        let config: ConnectionConfig = serde_json::from_str(
            r#"{
                "transport": "tcp",
                "ip": "127.0.0.1",
                "shell_port": 57555,
                "iopub_port": 57556,
                "stdin_port": 57557,
                "control_port": 57558,
                "hb_port": 57559,
                "signature_scheme": "hmac-sha256",
                "key": "secret"
            }"#,
        )
        .unwrap();

        let parsed = ConnectionInfo::from_config(config).unwrap();
        assert_eq!(parsed.transport, "tcp");
        assert_eq!(parsed.ip, "127.0.0.1");
        assert_eq!(parsed.shell_port, 57555);
        assert_eq!(parsed.signature_scheme, "hmac-sha256");
        assert_eq!(parsed.key, "secret");
    }

    #[test]
    fn rejects_empty_transport() {
        let _guard = test_lock();
        let config: ConnectionConfig = serde_json::from_str(
            r#"{
                "transport": "",
                "ip": "127.0.0.1",
                "shell_port": 57555,
                "iopub_port": 57556,
                "stdin_port": 57557,
                "control_port": 57558,
                "hb_port": 57559,
                "signature_scheme": "hmac-sha256",
                "key": ""
            }"#,
        )
        .unwrap();

        let error = ConnectionInfo::from_config(config).unwrap_err();
        assert!(matches!(
            error,
            KernelError::InvalidConnectionConfig("connection file transport must not be empty")
        ));
    }

    #[test]
    fn builds_channel_endpoints_from_connection() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let endpoints = connection.channel_endpoints();

        assert_eq!(
            endpoints.shell,
            format!("tcp://127.0.0.1:{}", connection.shell_port)
        );
        assert_eq!(
            endpoints.iopub,
            format!("tcp://127.0.0.1:{}", connection.iopub_port)
        );
        assert_eq!(
            endpoints.stdin,
            format!("tcp://127.0.0.1:{}", connection.stdin_port)
        );
        assert_eq!(
            endpoints.control,
            format!("tcp://127.0.0.1:{}", connection.control_port)
        );
        assert_eq!(
            endpoints.hb,
            format!("tcp://127.0.0.1:{}", connection.hb_port)
        );
    }

    #[test]
    fn heartbeat_channel_echoes_messages() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection).unwrap();

        let context = zmq::Context::new();
        let socket = context.socket(zmq::REQ).unwrap();
        socket.set_rcvtimeo(2_000).unwrap();
        socket.set_sndtimeo(2_000).unwrap();
        socket.connect(&runtime.channel_endpoints().hb).unwrap();

        thread::sleep(Duration::from_millis(100));

        socket.send("ping", 0).unwrap();
        let reply = socket.recv_bytes(0).unwrap();
        assert_eq!(reply, b"ping");

        runtime.stop().unwrap();
        assert!(!runtime.is_running());
    }

    #[test]
    fn shell_channel_replies_to_kernel_info_and_publishes_status() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let request = client_request("client-session", "kernel_info_request", json!({}));
        send_client_message(&shell, &signer, &request);

        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "kernel_info_reply");
        assert_eq!(reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(
            reply.content.get("implementation"),
            Some(&json!("rustykernel"))
        );
        assert_eq!(reply.content.get("protocol_version"), Some(&json!("5.3")));
        assert_eq!(reply.content.get("language"), Some(&json!("python")));
        assert_eq!(
            reply.content.get("language_version"),
            reply.content.pointer("/language_info/version")
        );
        assert_eq!(
            reply.content.pointer("/language_info/name"),
            Some(&json!("python"))
        );
        assert_eq!(
            reply.content.pointer("/language_info/mimetype"),
            Some(&json!("text/x-python"))
        );
        assert_eq!(
            reply.content.pointer("/language_info/file_extension"),
            Some(&json!(".py"))
        );
        assert_eq!(
            reply.content.pointer("/language_info/nbconvert_exporter"),
            Some(&json!("python"))
        );
        assert_eq!(
            reply.content.pointer("/language_info/codemirror_mode/name"),
            Some(&json!("ipython"))
        );
        assert_eq!(
            reply.content.pointer("/help_links/0/text"),
            Some(&json!("Python Reference"))
        );
        assert_eq!(reply.content.get("debugger"), Some(&json!(false)));
        assert_eq!(reply.content.get("supported_features"), Some(&json!([])));

        let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 2);
        assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

        runtime.stop().unwrap();
    }

    #[test]
    fn shell_channel_replies_to_connect_request() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let request = client_request("client-session", "connect_request", json!({}));
        send_client_message(&shell, &signer, &request);

        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "connect_reply");
        assert_eq!(reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(
            reply.content.get("shell_port"),
            Some(&json!(connection.shell_port))
        );
        assert_eq!(
            reply.content.get("control_port"),
            Some(&json!(connection.control_port))
        );
        assert_eq!(
            reply.content.get("hb_port"),
            Some(&json!(connection.hb_port))
        );

        let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 2);
        assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

        runtime.stop().unwrap();
    }

    #[test]
    fn execute_request_publishes_execute_input_and_reply() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let request = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "print('hello')",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &request);

        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "execute_reply");
        assert_eq!(reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(reply.content.get("execution_count"), Some(&json!(1)));

        let published = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 4);
        let msg_types: Vec<_> = published
            .iter()
            .map(|message| message.header.msg_type.as_str())
            .collect();
        assert_eq!(
            msg_types,
            vec!["status", "execute_input", "stream", "status"]
        );
        assert_eq!(
            published[1].content.get("code"),
            Some(&json!("print('hello')"))
        );
        assert_eq!(published[1].content.get("execution_count"), Some(&json!(1)));
        assert_eq!(published[2].content.get("name"), Some(&json!("stdout")));
        assert_eq!(published[2].content.get("text"), Some(&json!("hello\n")));
        assert_eq!(status_message_states(&published), vec!["busy", "idle"]);

        runtime.stop().unwrap();
    }

    #[test]
    fn execute_request_publishes_execute_result() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let request = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "1 + 2",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &request);

        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "execute_reply");
        assert_eq!(reply.content.get("status"), Some(&json!("ok")));

        let published = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 4);
        let msg_types: Vec<_> = published
            .iter()
            .map(|message| message.header.msg_type.as_str())
            .collect();
        assert_eq!(
            msg_types,
            vec!["status", "execute_input", "execute_result", "status"]
        );
        assert_eq!(
            published[2]
                .content
                .get("data")
                .and_then(|data| data.get("text/plain")),
            Some(&json!("3"))
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn execute_reply_includes_user_expressions() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let request = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value = 21",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {
                    "double": "value * 2",
                    "missing": "unknown_name",
                },
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &request);

        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "execute_reply");
        assert_eq!(reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(
            reply.content.pointer("/user_expressions/double/status"),
            Some(&json!("ok"))
        );
        assert_eq!(
            reply
                .content
                .pointer("/user_expressions/double/data/text~1plain"),
            Some(&json!("42"))
        );
        assert_eq!(
            reply.content.pointer("/user_expressions/missing/status"),
            Some(&json!("error"))
        );
        assert_eq!(
            reply.content.pointer("/user_expressions/missing/ename"),
            Some(&json!("NameError"))
        );

        let published = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 3);
        assert_eq!(
            published
                .iter()
                .map(|message| message.header.msg_type.as_str())
                .collect::<Vec<_>>(),
            vec!["status", "execute_input", "status"]
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn execute_error_reply_includes_empty_user_expressions_and_payload() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let request = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "1 / 0",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &request);

        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "execute_reply");
        assert_eq!(reply.content.get("status"), Some(&json!("error")));
        assert_eq!(
            reply.content.get("ename"),
            Some(&json!("ZeroDivisionError"))
        );
        assert_eq!(reply.content.get("user_expressions"), Some(&json!({})));
        assert_eq!(reply.content.get("payload"), Some(&json!([])));
        assert!(
            reply
                .content
                .get("traceback")
                .and_then(|value| value.as_array())
                .is_some_and(|traceback| !traceback.is_empty())
        );

        let published = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 4);
        assert_eq!(
            published
                .iter()
                .map(|message| message.header.msg_type.as_str())
                .collect::<Vec<_>>(),
            vec!["status", "execute_input", "error", "status"]
        );
        assert_eq!(
            published[2].content.get("ename"),
            Some(&json!("ZeroDivisionError"))
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn execute_request_preserves_state_across_cells() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let first = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value = 40",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &first);
        let first_reply = recv_message(&shell, &signer);
        assert_eq!(first_reply.content.get("status"), Some(&json!("ok")));
        let first_published =
            recv_iopub_messages_for_parent(&iopub, &signer, &first.header.msg_id, 3);
        let first_types: Vec<_> = first_published
            .iter()
            .map(|message| message.header.msg_type.as_str())
            .collect();
        assert_eq!(first_types, vec!["status", "execute_input", "status"]);

        let second = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value + 2",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &second);
        let second_reply = recv_message(&shell, &signer);
        assert_eq!(second_reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(second_reply.content.get("execution_count"), Some(&json!(2)));

        let second_published =
            recv_iopub_messages_for_parent(&iopub, &signer, &second.header.msg_id, 4);
        assert_eq!(
            second_published[2]
                .content
                .get("data")
                .and_then(|data| data.get("text/plain")),
            Some(&json!("42"))
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn history_request_returns_recorded_entries() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let first = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value = 40",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &first);
        let _ = recv_message(&shell, &signer);
        let _ = recv_iopub_messages_for_parent(&iopub, &signer, &first.header.msg_id, 3);

        let second = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value + 2",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &second);
        let _ = recv_message(&shell, &signer);
        let _ = recv_iopub_messages_for_parent(&iopub, &signer, &second.header.msg_id, 4);

        let range = client_request(
            "client-session",
            "history_request",
            json!({
                "hist_access_type": "range",
                "output": true,
                "raw": true,
                "session": 0,
                "start": 0,
                "stop": 3,
            }),
        );
        send_client_message(&shell, &signer, &range);
        let range_reply = recv_message(&shell, &signer);
        assert_eq!(range_reply.header.msg_type, "history_reply");
        assert_eq!(range_reply.content.get("status"), Some(&json!("ok")));
        let history = range_reply.content["history"].as_array().unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0], json!([0, 0, ["", null]]));
        assert_eq!(history[1], json!([0, 1, ["value = 40", null]]));
        assert_eq!(history[2], json!([0, 2, ["value + 2", "42"]]));

        let current_session = client_request(
            "client-session",
            "history_request",
            json!({
                "hist_access_type": "range",
                "output": false,
                "raw": true,
                "session": 1,
                "start": -1,
                "stop": null,
            }),
        );
        send_client_message(&shell, &signer, &current_session);
        let current_session_reply = recv_message(&shell, &signer);
        assert_eq!(
            current_session_reply.content.get("history"),
            Some(&json!([[0, 2, "value + 2"]]))
        );

        let search = client_request(
            "client-session",
            "history_request",
            json!({
                "hist_access_type": "search",
                "output": false,
                "raw": true,
                "session": 0,
                "pattern": "value*",
                "n": 1,
            }),
        );
        send_client_message(&shell, &signer, &search);
        let search_reply = recv_message(&shell, &signer);
        let matches = search_reply.content["history"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], json!([1, 2, "value + 2"]));

        runtime.stop().unwrap();
    }

    #[test]
    fn comm_info_request_reports_registered_comms() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let setup = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "from rustykernel.comm import register_target\n\ndef target(comm, msg):\n    pass\n\nregister_target('jupyter.widget', target)",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &setup);
        let setup_reply = recv_message(&shell, &signer);
        assert_eq!(setup_reply.content.get("status"), Some(&json!("ok")));
        let _ = recv_iopub_messages_for_parent(&iopub, &signer, &setup.header.msg_id, 3);

        let open = client_request(
            "client-session",
            "comm_open",
            json!({
                "comm_id": "comm-1",
                "target_name": "jupyter.widget",
                "data": {},
            }),
        );
        send_client_message(&shell, &signer, &open);
        let open_statuses = recv_iopub_messages_for_parent(&iopub, &signer, &open.header.msg_id, 2);
        assert_eq!(status_message_states(&open_statuses), vec!["busy", "idle"]);

        let list_all = client_request("client-session", "comm_info_request", json!({}));
        send_client_message(&shell, &signer, &list_all);
        let list_all_reply = recv_message(&shell, &signer);
        assert_eq!(list_all_reply.header.msg_type, "comm_info_reply");
        assert_eq!(list_all_reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(
            list_all_reply.content.get("comms"),
            Some(&json!({
                "comm-1": {
                    "target_name": "jupyter.widget",
                },
            }))
        );
        let list_all_statuses =
            recv_iopub_messages_for_parent(&iopub, &signer, &list_all.header.msg_id, 2);
        assert_eq!(
            status_message_states(&list_all_statuses),
            vec!["busy", "idle"]
        );

        let filtered = client_request(
            "client-session",
            "comm_info_request",
            json!({
                "target_name": "other.target",
            }),
        );
        send_client_message(&shell, &signer, &filtered);
        let filtered_reply = recv_message(&shell, &signer);
        assert_eq!(filtered_reply.content.get("comms"), Some(&json!({})));
        let filtered_statuses =
            recv_iopub_messages_for_parent(&iopub, &signer, &filtered.header.msg_id, 2);
        assert_eq!(
            status_message_states(&filtered_statuses),
            vec!["busy", "idle"]
        );

        let close = client_request(
            "client-session",
            "comm_close",
            json!({
                "comm_id": "comm-1",
                "data": {},
            }),
        );
        send_client_message(&shell, &signer, &close);
        let close_statuses =
            recv_iopub_messages_for_parent(&iopub, &signer, &close.header.msg_id, 2);
        assert_eq!(status_message_states(&close_statuses), vec!["busy", "idle"]);

        let after_close = client_request("client-session", "comm_info_request", json!({}));
        send_client_message(&shell, &signer, &after_close);
        let after_close_reply = recv_message(&shell, &signer);
        assert_eq!(after_close_reply.content.get("comms"), Some(&json!({})));
        let after_close_statuses =
            recv_iopub_messages_for_parent(&iopub, &signer, &after_close.header.msg_id, 2);
        assert_eq!(
            status_message_states(&after_close_statuses),
            vec!["busy", "idle"]
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn complete_request_returns_matches_from_worker_state() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let define = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value = 40",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &define);
        let _ = recv_message(&shell, &signer);
        let _ = recv_iopub_messages_for_parent(&iopub, &signer, &define.header.msg_id, 3);

        let complete = client_request(
            "client-session",
            "complete_request",
            json!({
                "code": "val",
                "cursor_pos": 3,
            }),
        );
        send_client_message(&shell, &signer, &complete);

        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "complete_reply");
        assert_eq!(reply.content.get("status"), Some(&json!("ok")));
        assert!(
            reply.content["matches"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "value")
        );
        assert_eq!(reply.content.get("cursor_start"), Some(&json!(0)));
        assert_eq!(reply.content.get("cursor_end"), Some(&json!(3)));
        assert!(
            reply.content["metadata"]
                .get("backend")
                .and_then(Value::as_str)
                .is_some()
        );
        let metadata_matches = reply.content["metadata"]["_jupyter_types_experimental"]
            .as_array()
            .unwrap();
        assert!(
            metadata_matches
                .iter()
                .any(|item| item.get("text") == Some(&json!("value")))
        );

        let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &complete.header.msg_id, 2);
        assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

        runtime.stop().unwrap();
    }

    #[test]
    fn inspect_request_returns_details_from_worker_state() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let define = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value = 40",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &define);
        let _ = recv_message(&shell, &signer);
        let _ = recv_iopub_messages_for_parent(&iopub, &signer, &define.header.msg_id, 3);

        let inspect = client_request(
            "client-session",
            "inspect_request",
            json!({
                "code": "value",
                "cursor_pos": 5,
                "detail_level": 1,
            }),
        );
        send_client_message(&shell, &signer, &inspect);

        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "inspect_reply");
        assert_eq!(reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(reply.content.get("found"), Some(&json!(true)));
        let text = reply.content["data"]
            .get("text/plain")
            .and_then(Value::as_str)
            .unwrap();
        assert!(text.starts_with("40\ntype: int"));
        assert_eq!(
            reply.content["metadata"].get("type_name"),
            Some(&json!("int"))
        );
        assert_eq!(
            reply.content["metadata"].get("doc_summary"),
            Some(&json!("int([x]) -> integer"))
        );
        assert!(
            reply.content["data"]
                .get("text/markdown")
                .and_then(Value::as_str)
                .unwrap()
                .contains("**type:** `int`")
        );

        let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &inspect.header.msg_id, 2);
        assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

        runtime.stop().unwrap();
    }

    #[test]
    fn complete_request_strips_call_parens_and_reports_completion_types() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let define = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "text = 'hello'",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &define);
        let _ = recv_message(&shell, &signer);
        let _ = recv_iopub_messages_for_parent(&iopub, &signer, &define.header.msg_id, 3);

        let complete = client_request(
            "client-session",
            "complete_request",
            json!({
                "code": "text.st",
                "cursor_pos": 7,
            }),
        );
        send_client_message(&shell, &signer, &complete);
        let reply = recv_message(&shell, &signer);
        let matches = reply.content["matches"].as_array().unwrap();
        assert!(matches.iter().any(|value| value == "text.startswith"));
        assert!(
            matches
                .iter()
                .all(|value| { value.as_str().is_some_and(|text| !text.ends_with('(')) })
        );
        assert!(
            reply.content["metadata"]["_jupyter_types_experimental"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| {
                    item.get("text") == Some(&json!("text.startswith"))
                        && item.get("type") == Some(&json!("function"))
                        && item.get("signature").is_some()
                })
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn inspect_request_prioritizes_callable_context_like_ipykernel() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let define = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value = 40",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &define);
        let _ = recv_message(&shell, &signer);
        let _ = recv_iopub_messages_for_parent(&iopub, &signer, &define.header.msg_id, 3);

        let inspect = client_request(
            "client-session",
            "inspect_request",
            json!({
                "code": "str(value)",
                "cursor_pos": 8,
                "detail_level": 0,
            }),
        );
        send_client_message(&shell, &signer, &inspect);

        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "inspect_reply");
        assert_eq!(reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(reply.content.get("found"), Some(&json!(true)));
        assert_eq!(
            reply.content["metadata"].get("signature"),
            Some(&json!("str(object='') -> str"))
        );
        assert!(
            reply.content["data"]
                .get("text/plain")
                .and_then(Value::as_str)
                .unwrap()
                .contains("signature: str(object='') -> str")
        );
        assert!(
            reply.content["data"]
                .get("text/html")
                .and_then(Value::as_str)
                .unwrap()
                .contains("type:")
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn is_complete_request_reports_complete_incomplete_and_invalid_code() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let complete = client_request(
            "client-session",
            "is_complete_request",
            json!({ "code": "value = 1" }),
        );
        send_client_message(&shell, &signer, &complete);
        let complete_reply = recv_message(&shell, &signer);
        assert_eq!(complete_reply.header.msg_type, "is_complete_reply");
        assert_eq!(
            complete_reply.content.get("status"),
            Some(&json!("complete"))
        );
        assert_eq!(complete_reply.content.get("indent"), Some(&json!("")));
        let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &complete.header.msg_id, 2);
        assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

        let incomplete = client_request(
            "client-session",
            "is_complete_request",
            json!({ "code": "for i in range(3):" }),
        );
        send_client_message(&shell, &signer, &incomplete);
        let incomplete_reply = recv_message(&shell, &signer);
        assert_eq!(
            incomplete_reply.content.get("status"),
            Some(&json!("incomplete"))
        );
        assert_eq!(incomplete_reply.content.get("indent"), Some(&json!("    ")));
        let statuses =
            recv_iopub_messages_for_parent(&iopub, &signer, &incomplete.header.msg_id, 2);
        assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

        let invalid = client_request(
            "client-session",
            "is_complete_request",
            json!({ "code": "1+" }),
        );
        send_client_message(&shell, &signer, &invalid);
        let invalid_reply = recv_message(&shell, &signer);
        assert_eq!(invalid_reply.content.get("status"), Some(&json!("invalid")));
        assert_eq!(invalid_reply.content.get("indent"), Some(&json!("")));
        let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &invalid.header.msg_id, 2);
        assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

        runtime.stop().unwrap();
    }

    #[test]
    fn execute_request_supports_input_via_stdin_channel() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let client_identity = b"stdin-client";
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            client_identity,
        );
        let stdin_socket = connect_dealer(
            &context,
            &runtime.channel_endpoints().stdin,
            client_identity,
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let request = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "name = input('Name: ')\nname",
                "silent": false,
                "store_history": true,
                "allow_stdin": true,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &request);

        let input_request = recv_message(&stdin_socket, &signer);
        assert_eq!(input_request.header.msg_type, "input_request");
        assert_eq!(input_request.content.get("prompt"), Some(&json!("Name: ")));
        assert_eq!(input_request.content.get("password"), Some(&json!(false)));

        let input_reply = client_request(
            "client-session",
            "input_reply",
            json!({
                "value": "Ada",
            }),
        );
        send_client_message(&stdin_socket, &signer, &input_reply);

        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "execute_reply");
        assert_eq!(reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(reply.content.get("execution_count"), Some(&json!(1)));

        let published = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 4);
        let msg_types: Vec<_> = published
            .iter()
            .map(|message| message.header.msg_type.as_str())
            .collect();
        assert_eq!(
            msg_types,
            vec!["status", "execute_input", "execute_result", "status"]
        );
        assert_eq!(
            published[2]
                .content
                .get("data")
                .and_then(|data| data.get("text/plain")),
            Some(&json!("'Ada'"))
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn stdin_channel_accepts_input_reply_without_stopping_kernel() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let stdin_socket = connect_dealer(
            &context,
            &runtime.channel_endpoints().stdin,
            b"stdin-client",
        );

        let stdin_message = client_request(
            "client-session",
            "input_reply",
            json!({
                "value": "test input",
            }),
        );
        send_client_message(&stdin_socket, &signer, &stdin_message);

        let request = client_request("client-session", "kernel_info_request", json!({}));
        send_client_message(&shell, &signer, &request);
        let reply = recv_message(&shell, &signer);
        assert_eq!(reply.header.msg_type, "kernel_info_reply");

        runtime.stop().unwrap();
    }

    #[test]
    fn control_channel_interrupt_request_preserves_worker_state() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let control = connect_dealer(
            &context,
            &runtime.channel_endpoints().control,
            b"control-client",
        );

        let define = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value = 99",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &define);
        let _ = recv_message(&shell, &signer);

        let interrupt = client_request("client-session", "interrupt_request", json!({}));
        send_client_message(&control, &signer, &interrupt);
        let interrupt_reply = recv_message(&control, &signer);
        assert_eq!(interrupt_reply.header.msg_type, "interrupt_reply");
        assert_eq!(interrupt_reply.content.get("status"), Some(&json!("ok")));

        let probe = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &probe);
        let probe_reply = recv_message(&shell, &signer);
        assert_eq!(probe_reply.header.msg_type, "execute_reply");
        assert_eq!(probe_reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(probe_reply.content.get("execution_count"), Some(&json!(2)));

        let history = client_request(
            "client-session",
            "history_request",
            json!({
                "hist_access_type": "tail",
                "output": false,
                "raw": true,
                "session": 0,
                "n": 10,
            }),
        );
        send_client_message(&shell, &signer, &history);
        let history_reply = recv_message(&shell, &signer);
        assert_eq!(
            history_reply.content.get("history"),
            Some(&json!([[1, 1, "value = 99"], [1, 2, "value"]]))
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn control_channel_usage_request_reports_kernel_metrics() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let control = connect_dealer(
            &context,
            &runtime.channel_endpoints().control,
            b"control-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let request = client_request("client-session", "usage_request", json!({}));
        send_client_message(&control, &signer, &request);

        let reply = recv_message(&control, &signer);
        assert_eq!(reply.header.msg_type, "usage_reply");
        assert!(
            reply
                .content
                .get("hostname")
                .and_then(Value::as_str)
                .is_some_and(|hostname| !hostname.is_empty())
        );
        assert!(
            reply
                .content
                .get("pid")
                .and_then(Value::as_u64)
                .is_some_and(|pid| pid > 0)
        );
        assert!(reply.content.get("kernel_cpu").is_some());
        assert!(
            reply
                .content
                .get("kernel_memory")
                .and_then(Value::as_u64)
                .is_some_and(|memory| memory > 0)
        );
        assert!(
            reply
                .content
                .get("cpu_count")
                .and_then(Value::as_u64)
                .is_some_and(|count| count > 0)
        );
        assert!(
            reply
                .content
                .pointer("/host_virtual_memory/total")
                .and_then(Value::as_u64)
                .is_some_and(|total| total > 0)
        );

        let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 2);
        assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

        runtime.stop().unwrap();
    }

    #[test]
    fn control_channel_interrupt_request_interrupts_running_execution() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let control = connect_dealer(
            &context,
            &runtime.channel_endpoints().control,
            b"control-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let define = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value = 99",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &define);
        let define_reply = recv_message(&shell, &signer);
        assert_eq!(define_reply.content.get("status"), Some(&json!("ok")));
        let define_statuses =
            recv_iopub_messages_for_parent(&iopub, &signer, &define.header.msg_id, 3);
        assert_eq!(
            status_message_states(&define_statuses),
            vec!["busy", "idle"]
        );

        let long_running = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "import time\ntime.sleep(30)",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &long_running);
        thread::sleep(std::time::Duration::from_millis(200));

        let interrupt = client_request("client-session", "interrupt_request", json!({}));
        send_client_message(&control, &signer, &interrupt);
        let interrupt_reply = recv_message(&control, &signer);
        assert_eq!(interrupt_reply.header.msg_type, "interrupt_reply");
        assert_eq!(interrupt_reply.content.get("status"), Some(&json!("ok")));
        let interrupt_statuses =
            recv_iopub_messages_for_parent(&iopub, &signer, &interrupt.header.msg_id, 2);
        assert_eq!(
            status_message_states(&interrupt_statuses),
            vec!["busy", "idle"]
        );

        let long_running_reply = recv_message(&shell, &signer);
        assert_eq!(long_running_reply.header.msg_type, "execute_reply");
        assert_eq!(
            long_running_reply.content.get("status"),
            Some(&json!("error"))
        );
        assert_eq!(
            long_running_reply.content.get("ename"),
            Some(&json!("KeyboardInterrupt"))
        );
        assert_eq!(
            long_running_reply.content.get("execution_count"),
            Some(&json!(2))
        );
        let long_running_messages =
            recv_iopub_messages_for_parent(&iopub, &signer, &long_running.header.msg_id, 2);
        assert_eq!(
            long_running_messages
                .iter()
                .map(|message| message.header.msg_type.as_str())
                .collect::<Vec<_>>(),
            vec!["error", "status"]
        );

        let probe = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &probe);
        let probe_reply = recv_message(&shell, &signer);
        assert_eq!(probe_reply.content.get("status"), Some(&json!("ok")));
        assert_eq!(probe_reply.content.get("execution_count"), Some(&json!(3)));

        let history = client_request(
            "client-session",
            "history_request",
            json!({
                "hist_access_type": "tail",
                "output": false,
                "raw": true,
                "session": 0,
                "n": 10,
            }),
        );
        send_client_message(&shell, &signer, &history);
        let history_reply = recv_message(&shell, &signer);
        assert_eq!(
            history_reply.content.get("history"),
            Some(&json!([
                [1, 1, "value = 99"],
                [1, 2, "import time\ntime.sleep(30)"],
                [1, 3, "value"]
            ]))
        );

        runtime.stop().unwrap();
    }

    #[test]
    fn control_channel_shutdown_request_stops_kernel() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let control = connect_dealer(
            &context,
            &runtime.channel_endpoints().control,
            b"control-client",
        );
        let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

        let request = client_request(
            "client-session",
            "shutdown_request",
            json!({
                "restart": false,
            }),
        );
        send_client_message(&control, &signer, &request);

        let reply = recv_message(&control, &signer);
        assert_eq!(reply.header.msg_type, "shutdown_reply");
        assert_eq!(reply.content.get("restart"), Some(&json!(false)));
        let published = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 3);
        assert_eq!(
            published
                .iter()
                .map(|message| message.header.msg_type.as_str())
                .collect::<Vec<_>>(),
            vec!["status", "shutdown_reply", "status"]
        );

        runtime.wait_for_shutdown();
        assert!(!runtime.is_running());
        runtime.stop().unwrap();
    }

    #[test]
    fn control_channel_shutdown_request_with_restart_keeps_kernel_running() {
        let _guard = test_lock();
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let shell = connect_dealer(
            &context,
            &runtime.channel_endpoints().shell,
            b"shell-client",
        );
        let control = connect_dealer(
            &context,
            &runtime.channel_endpoints().control,
            b"control-client",
        );

        let define = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value = 7",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &define);
        let _ = recv_message(&shell, &signer);

        let restart = client_request(
            "client-session",
            "shutdown_request",
            json!({
                "restart": true,
            }),
        );
        send_client_message(&control, &signer, &restart);
        let restart_reply = recv_message(&control, &signer);
        assert_eq!(restart_reply.header.msg_type, "shutdown_reply");
        assert_eq!(restart_reply.content.get("restart"), Some(&json!(true)));
        assert!(runtime.is_running());

        let probe = client_request("client-session", "kernel_info_request", json!({}));
        send_client_message(&shell, &signer, &probe);
        let probe_reply = recv_message(&shell, &signer);
        assert_eq!(probe_reply.header.msg_type, "kernel_info_reply");

        let state_probe = client_request(
            "client-session",
            "execute_request",
            json!({
                "code": "value",
                "silent": false,
                "store_history": true,
                "allow_stdin": false,
                "user_expressions": {},
                "stop_on_error": true,
            }),
        );
        send_client_message(&shell, &signer, &state_probe);
        let state_reply = recv_message(&shell, &signer);
        assert_eq!(state_reply.content.get("status"), Some(&json!("error")));
        assert_eq!(state_reply.content.get("ename"), Some(&json!("NameError")));

        let previous_session = client_request(
            "client-session",
            "history_request",
            json!({
                "hist_access_type": "range",
                "output": false,
                "raw": true,
                "session": -1,
                "start": 1,
                "stop": 2,
            }),
        );
        send_client_message(&shell, &signer, &previous_session);
        let previous_session_reply = recv_message(&shell, &signer);
        assert_eq!(
            previous_session_reply.content.get("history"),
            Some(&json!([[1, 1, "value = 7"]]))
        );

        let search = client_request(
            "client-session",
            "history_request",
            json!({
                "hist_access_type": "search",
                "output": false,
                "raw": true,
                "session": 0,
                "pattern": "value*",
                "n": 10,
            }),
        );
        send_client_message(&shell, &signer, &search);
        let search_reply = recv_message(&shell, &signer);
        assert_eq!(
            search_reply.content.get("history"),
            Some(&json!([[1, 1, "value = 7"], [2, 1, "value"]]))
        );

        runtime.stop().unwrap();
    }

    fn client_request(session: &str, msg_type: &str, content: Value) -> JupyterMessage {
        let mut header = MessageHeader::new(msg_type, session);
        header.username = "test-client".to_owned();
        JupyterMessage::new(Vec::new(), header, json!({}), json!({}), content)
    }

    fn connect_dealer(context: &zmq::Context, endpoint: &str, identity: &[u8]) -> zmq::Socket {
        let socket = context.socket(zmq::DEALER).unwrap();
        socket.set_identity(identity).unwrap();
        socket.set_rcvtimeo(2_000).unwrap();
        socket.set_sndtimeo(2_000).unwrap();
        socket.connect(endpoint).unwrap();
        thread::sleep(Duration::from_millis(100));
        socket
    }

    fn connect_subscriber(context: &zmq::Context, endpoint: &str) -> zmq::Socket {
        let socket = context.socket(zmq::SUB).unwrap();
        socket.set_rcvtimeo(2_000).unwrap();
        socket.set_subscribe(b"").unwrap();
        socket.connect(endpoint).unwrap();
        thread::sleep(Duration::from_millis(100));
        socket
    }

    fn send_client_message(socket: &zmq::Socket, signer: &MessageSigner, message: &JupyterMessage) {
        socket
            .send_multipart(signer.encode(message).unwrap(), 0)
            .unwrap();
    }

    fn recv_message(socket: &zmq::Socket, signer: &MessageSigner) -> JupyterMessage {
        let frames = socket.recv_multipart(0).unwrap();
        signer.decode(frames).unwrap()
    }

    fn recv_iopub_messages_for_parent(
        socket: &zmq::Socket,
        signer: &MessageSigner,
        parent_msg_id: &str,
        expected_count: usize,
    ) -> Vec<JupyterMessage> {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut messages = Vec::new();

        while messages.len() < expected_count {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for iopub messages"
            );
            let message = recv_message(socket, signer);
            let parent = message
                .parent_header
                .get("msg_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if parent == parent_msg_id {
                messages.push(message);
            }
        }

        messages
    }

    fn status_message_states(messages: &[JupyterMessage]) -> Vec<&str> {
        messages
            .iter()
            .filter_map(|message| {
                message
                    .content
                    .get("execution_state")
                    .and_then(Value::as_str)
            })
            .collect()
    }

    fn test_connection_info() -> ConnectionInfo {
        let mut ports = reserve_tcp_ports(5);
        ConnectionInfo {
            transport: "tcp".to_owned(),
            ip: "127.0.0.1".to_owned(),
            shell_port: ports.remove(0),
            iopub_port: ports.remove(0),
            stdin_port: ports.remove(0),
            control_port: ports.remove(0),
            hb_port: ports.remove(0),
            signature_scheme: "hmac-sha256".to_owned(),
            key: "secret".to_owned(),
            kernel_name: Some("rustykernel".to_owned()),
        }
    }

    fn reserve_tcp_ports(count: usize) -> Vec<u16> {
        let listeners: Vec<_> = (0..count)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().port())
            .collect();
        drop(listeners);
        ports
    }
}
