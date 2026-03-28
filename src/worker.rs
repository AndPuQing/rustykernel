use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU32, Ordering},
    mpsc,
};
use std::thread;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::kernel::KernelError;

const PYTHON_WORKER_SCRIPT: &str = include_str!("../python/rustykernel/worker_main.py");
#[cfg(unix)]
const WORKER_PROTOCOL_ENV: &str = "RUSTYKERNEL_PROTOCOL_FD";
#[cfg(unix)]
const WORKER_PROTOCOL_FD: libc::c_int = 3;

#[derive(Debug, Deserialize)]
pub struct ExecutionDisplay {
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionDisplayEvent {
    pub msg_type: String,
    #[serde(default)]
    pub content: Value,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub transient: Value,
}

#[derive(Debug, Deserialize)]
pub struct WorkerCommEvent {
    pub msg_type: String,
    #[serde(default)]
    pub content: Value,
}

#[derive(Debug, Deserialize)]
pub struct WorkerDebugEvent {
    #[serde(default = "debug_event_msg_type")]
    pub msg_type: String,
    #[serde(default)]
    pub content: Value,
}

fn debug_event_msg_type() -> String {
    "debug_event".to_owned()
}

#[derive(Debug, Deserialize)]
pub struct ExecutionOutcome {
    pub status: String,
    #[serde(default)]
    pub displays: Vec<ExecutionDisplayEvent>,
    #[serde(default)]
    pub comm_events: Vec<WorkerCommEvent>,
    #[serde(default)]
    pub debug_events: Vec<WorkerDebugEvent>,
    #[serde(default)]
    pub payload: Vec<Value>,
    #[serde(default)]
    pub result: Option<ExecutionDisplay>,
    #[serde(default)]
    pub user_expressions: Map<String, Value>,
    #[serde(default)]
    pub ename: Option<String>,
    #[serde(default)]
    pub evalue: Option<String>,
    #[serde(default)]
    pub traceback: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CompletionOutcome {
    pub matches: Vec<String>,
    pub cursor_start: usize,
    pub cursor_end: usize,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct InspectionOutcome {
    pub found: bool,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct IsCompleteOutcome {
    pub status: String,
    #[serde(default)]
    pub indent: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WorkerKernelInfo {
    pub language_version: String,
    pub language_version_major: u8,
    pub language_version_minor: u8,
}

#[derive(Debug, Deserialize)]
pub struct CommOutcome {
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub events: Vec<WorkerCommEvent>,
    #[serde(default)]
    pub registered: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct WorkerDebugListen {
    pub available: bool,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
}

#[derive(Serialize)]
#[serde(tag = "kind")]
enum WorkerRequest<'a> {
    #[serde(rename = "execute")]
    Execute {
        id: u64,
        code: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        subshell_id: Option<&'a str>,
        user_expressions: &'a Value,
        execution_count: u32,
        silent: bool,
        store_history: bool,
    },
    #[serde(rename = "input_reply")]
    InputReply {
        id: u64,
        value: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    #[serde(rename = "complete")]
    Complete {
        id: u64,
        code: &'a str,
        cursor_pos: usize,
    },
    #[serde(rename = "is_complete")]
    IsComplete { id: u64, code: &'a str },
    #[serde(rename = "inspect")]
    Inspect {
        id: u64,
        code: &'a str,
        cursor_pos: usize,
        detail_level: u8,
    },
    #[serde(rename = "comm_open")]
    CommOpen {
        id: u64,
        comm_id: &'a str,
        target_name: &'a str,
        data: &'a Value,
        metadata: &'a Value,
    },
    #[serde(rename = "comm_msg")]
    CommMsg {
        id: u64,
        comm_id: &'a str,
        data: &'a Value,
        metadata: &'a Value,
    },
    #[serde(rename = "comm_close")]
    CommClose {
        id: u64,
        comm_id: &'a str,
        data: &'a Value,
        metadata: &'a Value,
    },
    #[serde(rename = "kernel_info")]
    KernelInfo { id: u64 },
    #[serde(rename = "debug_listen")]
    DebugListen { id: u64 },
    #[serde(rename = "debug")]
    Debug { id: u64, message: &'a Value },
    #[serde(rename = "interrupt")]
    Interrupt { id: u64 },
    #[serde(rename = "create_subshell")]
    CreateSubshell { id: u64 },
    #[serde(rename = "delete_subshell")]
    DeleteSubshell { id: u64, subshell_id: &'a str },
    #[serde(rename = "list_subshell")]
    ListSubshell { id: u64 },
}

#[derive(Debug, Deserialize)]
struct WorkerInputRequest {
    #[serde(default)]
    prompt: String,
    #[serde(default)]
    password: bool,
}

#[derive(Debug, Deserialize)]
struct WorkerStreamEvent {
    name: String,
    #[serde(default)]
    text: String,
}

#[derive(Debug, Deserialize)]
struct WorkerDebugProtocolEvent {
    #[serde(default = "debug_event_msg_type")]
    msg_type: String,
    #[serde(default)]
    content: Value,
}

#[derive(Deserialize)]
struct WorkerEnvelope<T> {
    #[serde(flatten)]
    payload: T,
}

#[derive(Debug, Deserialize)]
struct WorkerStatusReply {
    #[serde(default)]
    status: String,
    #[serde(default)]
    evalue: String,
}

#[derive(Debug, Deserialize)]
struct WorkerCreateSubshellReply {
    #[serde(default)]
    status: String,
    #[serde(default)]
    subshell_id: String,
    #[serde(default)]
    evalue: String,
}

#[derive(Debug, Deserialize)]
struct WorkerListSubshellReply {
    #[serde(default)]
    status: String,
    #[serde(default)]
    subshell_id: Vec<String>,
    #[serde(default)]
    evalue: String,
}

#[derive(Debug)]
pub enum WorkerExecutionMessage {
    InputRequest { prompt: String, password: bool },
    Stream { name: String, text: String },
    DebugEvent(WorkerDebugEvent),
    Completion(ExecutionOutcome),
    Failure(String),
}

pub struct WorkerExecutionHandle {
    pub request_id: u64,
    rx: mpsc::Receiver<WorkerExecutionMessage>,
}

impl WorkerExecutionHandle {
    pub fn recv(&self) -> Result<WorkerExecutionMessage, KernelError> {
        self.rx.recv().map_err(|error| {
            KernelError::Worker(format!(
                "worker execution channel closed unexpectedly for request {}: {error}",
                self.request_id
            ))
        })
    }
}

enum PendingRequest {
    Execute(mpsc::Sender<WorkerExecutionMessage>),
    Response(mpsc::Sender<Value>),
}

#[derive(Clone)]
pub struct WorkerInterruptHandle {
    pid: Arc<AtomicU32>,
}

impl WorkerInterruptHandle {
    fn new(pid: u32) -> Self {
        Self {
            pid: Arc::new(AtomicU32::new(pid)),
        }
    }

    fn clear(&self) {
        self.pid.store(0, Ordering::SeqCst);
    }

    pub fn interrupt(&self) -> Result<(), KernelError> {
        let pid = self.pid.load(Ordering::SeqCst);
        if pid == 0 {
            return Ok(());
        }

        #[cfg(unix)]
        {
            let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGINT) };
            if rc == 0 {
                return Ok(());
            }

            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::NotFound {
                return Ok(());
            }

            return Err(KernelError::Worker(format!(
                "failed to interrupt python worker {pid}: {error}"
            )));
        }

        #[cfg(not(unix))]
        {
            Err(KernelError::Worker(
                "interrupt is not supported on this platform".to_owned(),
            ))
        }
    }
}

pub struct PythonWorker {
    child: Child,
    stdin: ChildStdin,
    next_id: u64,
    interrupt_handle: WorkerInterruptHandle,
    pending: Arc<Mutex<HashMap<u64, PendingRequest>>>,
    protocol_thread: Option<thread::JoinHandle<()>>,
}

impl PythonWorker {
    pub fn start() -> Result<Self, KernelError> {
        #[cfg(not(unix))]
        {
            return Err(KernelError::Worker(
                "python worker side-channel IPC requires unix support".to_owned(),
            ));
        }

        #[cfg(unix)]
        let (protocol_read, protocol_write) = create_worker_protocol_pipe()?;

        let interpreter = if python_supports_ipython("python3", &[]) {
            ("python3", Vec::<&str>::new())
        } else if python_supports_ipython("uv", &["run", "python"]) {
            ("uv", vec!["run", "python"])
        } else {
            ("python3", Vec::<&str>::new())
        };

        let mut command = Command::new(interpreter.0);
        #[cfg(unix)]
        {
            let protocol_write_fd = protocol_write.as_raw_fd();
            command.env(WORKER_PROTOCOL_ENV, WORKER_PROTOCOL_FD.to_string());
            unsafe {
                command.pre_exec(move || install_worker_protocol_fd(protocol_write_fd));
            }
        }
        command
            .args(interpreter.1)
            .arg("-u")
            .arg("-c")
            .arg(PYTHON_WORKER_SCRIPT)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let mut child = command.spawn().map_err(KernelError::Io)?;
        #[cfg(unix)]
        drop(protocol_write);

        let stdin = child.stdin.take().ok_or_else(|| {
            KernelError::Worker("python worker stdin pipe was not available".to_owned())
        })?;
        let interrupt_handle = WorkerInterruptHandle::new(child.id());
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let protocol_thread = Some(spawn_protocol_reader(protocol_read, Arc::clone(&pending)));

        Ok(Self {
            child,
            stdin,
            next_id: 1,
            interrupt_handle,
            pending,
            protocol_thread,
        })
    }

    pub fn interrupt_handle(&self) -> WorkerInterruptHandle {
        self.interrupt_handle.clone()
    }

    pub fn execute_async(
        &mut self,
        code: &str,
        subshell_id: Option<&str>,
        user_expressions: &Value,
        execution_count: u32,
        silent: bool,
        store_history: bool,
    ) -> Result<WorkerExecutionHandle, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let (tx, rx) = mpsc::channel();
        self.pending
            .lock()
            .map_err(|_| KernelError::Worker("worker pending map mutex poisoned".to_owned()))?
            .insert(request_id, PendingRequest::Execute(tx));

        let request = WorkerRequest::Execute {
            id: request_id,
            code,
            subshell_id,
            user_expressions,
            execution_count,
            silent,
            store_history,
        };
        if let Err(error) = self.write_request(&request) {
            let _ = self
                .pending
                .lock()
                .map(|mut pending| pending.remove(&request_id));
            return Err(error);
        }

        Ok(WorkerExecutionHandle { request_id, rx })
    }

    pub fn execute<F, G, H>(
        &mut self,
        code: &str,
        subshell_id: Option<&str>,
        user_expressions: &Value,
        execution_count: u32,
        silent: bool,
        store_history: bool,
        mut on_input: F,
        mut on_stream: G,
        mut on_debug_event: H,
    ) -> Result<ExecutionOutcome, KernelError>
    where
        F: FnMut(&str, bool) -> Result<String, String>,
        G: FnMut(&str, &str) -> Result<(), KernelError>,
        H: FnMut(&WorkerDebugEvent) -> Result<(), KernelError>,
    {
        let handle = self.execute_async(
            code,
            subshell_id,
            user_expressions,
            execution_count,
            silent,
            store_history,
        )?;
        loop {
            match handle.recv()? {
                WorkerExecutionMessage::InputRequest { prompt, password } => {
                    match on_input(&prompt, password) {
                        Ok(value) => self.send_input_reply(handle.request_id, value, None)?,
                        Err(error) => {
                            self.send_input_reply(handle.request_id, String::new(), Some(error))?
                        }
                    }
                }
                WorkerExecutionMessage::Stream { name, text } => on_stream(&name, &text)?,
                WorkerExecutionMessage::DebugEvent(event) => on_debug_event(&event)?,
                WorkerExecutionMessage::Completion(outcome) => return Ok(outcome),
                WorkerExecutionMessage::Failure(message) => {
                    return Err(KernelError::Worker(message));
                }
            }
        }
    }

    pub fn complete(
        &mut self,
        code: &str,
        cursor_pos: usize,
    ) -> Result<CompletionOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::Complete {
            id: request_id,
            code,
            cursor_pos,
        };
        let response: WorkerEnvelope<CompletionOutcome> =
            self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn inspect(
        &mut self,
        code: &str,
        cursor_pos: usize,
        detail_level: u8,
    ) -> Result<InspectionOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::Inspect {
            id: request_id,
            code,
            cursor_pos,
            detail_level,
        };
        let response: WorkerEnvelope<InspectionOutcome> =
            self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn is_complete(&mut self, code: &str) -> Result<IsCompleteOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::IsComplete {
            id: request_id,
            code,
        };
        let response: WorkerEnvelope<IsCompleteOutcome> =
            self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn kernel_info(&mut self) -> Result<WorkerKernelInfo, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::KernelInfo { id: request_id };
        let response: WorkerEnvelope<WorkerKernelInfo> = self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn debug_listen(&mut self) -> Result<WorkerDebugListen, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::DebugListen { id: request_id };
        let response: WorkerEnvelope<WorkerDebugListen> =
            self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn debug_request(&mut self, message: &Value) -> Result<Value, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::Debug {
            id: request_id,
            message,
        };
        let response: WorkerEnvelope<Value> = self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn interrupt_subshells(&mut self) -> Result<(), KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::Interrupt { id: request_id };
        let response: WorkerEnvelope<WorkerStatusReply> =
            self.send_request(&request, request_id)?;
        if response.payload.status == "ok" {
            Ok(())
        } else {
            Err(KernelError::Worker(response.payload.evalue))
        }
    }

    pub fn comm_open(
        &mut self,
        comm_id: &str,
        target_name: &str,
        data: &Value,
        metadata: &Value,
    ) -> Result<CommOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::CommOpen {
            id: request_id,
            comm_id,
            target_name,
            data,
            metadata,
        };
        let response: WorkerEnvelope<CommOutcome> = self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn comm_msg(
        &mut self,
        comm_id: &str,
        data: &Value,
        metadata: &Value,
    ) -> Result<CommOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::CommMsg {
            id: request_id,
            comm_id,
            data,
            metadata,
        };
        let response: WorkerEnvelope<CommOutcome> = self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn comm_close(
        &mut self,
        comm_id: &str,
        data: &Value,
        metadata: &Value,
    ) -> Result<CommOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::CommClose {
            id: request_id,
            comm_id,
            data,
            metadata,
        };
        let response: WorkerEnvelope<CommOutcome> = self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn create_subshell(&mut self) -> Result<String, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::CreateSubshell { id: request_id };
        let response: WorkerEnvelope<WorkerCreateSubshellReply> =
            self.send_request(&request, request_id)?;
        if response.payload.status == "ok" {
            if response.payload.subshell_id.is_empty() {
                Err(KernelError::Worker(
                    "worker create_subshell response did not include subshell id".to_owned(),
                ))
            } else {
                Ok(response.payload.subshell_id)
            }
        } else {
            Err(KernelError::Worker(response.payload.evalue))
        }
    }

    pub fn delete_subshell(&mut self, subshell_id: &str) -> Result<(), KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::DeleteSubshell {
            id: request_id,
            subshell_id,
        };
        let response: WorkerEnvelope<WorkerStatusReply> =
            self.send_request(&request, request_id)?;
        if response.payload.status == "ok" {
            Ok(())
        } else {
            Err(KernelError::Worker(response.payload.evalue))
        }
    }

    pub fn list_subshell(&mut self) -> Result<Vec<String>, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::ListSubshell { id: request_id };
        let response: WorkerEnvelope<WorkerListSubshellReply> =
            self.send_request(&request, request_id)?;
        if response.payload.status == "ok" {
            Ok(response.payload.subshell_id)
        } else {
            Err(KernelError::Worker(response.payload.evalue))
        }
    }

    pub fn restart(&mut self) -> Result<(), KernelError> {
        self.terminate();
        let replacement = Self::start()?;
        *self = replacement;
        Ok(())
    }

    fn terminate(&mut self) {
        let _ = self.stdin.flush();
        self.interrupt_handle.clear();
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
            Err(_) => {}
        }
        let _ = self.protocol_thread.take();
    }

    fn send_request<T>(
        &mut self,
        request: &WorkerRequest<'_>,
        request_id: u64,
    ) -> Result<WorkerEnvelope<T>, KernelError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let (tx, rx) = mpsc::channel();
        self.pending
            .lock()
            .map_err(|_| KernelError::Worker("worker pending map mutex poisoned".to_owned()))?
            .insert(request_id, PendingRequest::Response(tx));
        if let Err(error) = self.write_request(request) {
            let _ = self
                .pending
                .lock()
                .map(|mut pending| pending.remove(&request_id));
            return Err(error);
        }
        let raw = rx.recv().map_err(|error| {
            KernelError::Worker(format!(
                "worker response channel closed unexpectedly for request {request_id}: {error}"
            ))
        })?;
        let response: WorkerEnvelope<T> = serde_json::from_value(raw).map_err(|error| {
            KernelError::Worker(format!("failed to decode worker response: {error}"))
        })?;
        Ok(response)
    }

    fn send_input_reply(
        &mut self,
        request_id: u64,
        value: String,
        error: Option<String>,
    ) -> Result<(), KernelError> {
        self.write_request(&WorkerRequest::InputReply {
            id: request_id,
            value,
            error,
        })
    }

    fn write_request(&mut self, request: &WorkerRequest<'_>) -> Result<(), KernelError> {
        let payload = serde_json::to_vec(request).map_err(|error| {
            KernelError::Worker(format!("failed to encode worker request: {error}"))
        })?;
        self.stdin.write_all(&payload).map_err(KernelError::Io)?;
        self.stdin.write_all(b"\n").map_err(KernelError::Io)?;
        self.stdin.flush().map_err(KernelError::Io)?;
        Ok(())
    }
}

fn spawn_protocol_reader(
    protocol_read: OwnedFd,
    pending: Arc<Mutex<HashMap<u64, PendingRequest>>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut protocol = BufReader::new(File::from(protocol_read));
        loop {
            let mut line = String::new();
            let bytes_read = match protocol.read_line(&mut line) {
                Ok(bytes_read) => bytes_read,
                Err(error) => {
                    fail_all_pending(
                        &pending,
                        format!("failed to read worker protocol message: {error}"),
                    );
                    return;
                }
            };
            if bytes_read == 0 {
                fail_all_pending(
                    &pending,
                    "python worker closed its protocol channel unexpectedly".to_owned(),
                );
                return;
            }

            let raw: Value = match serde_json::from_str(&line) {
                Ok(raw) => raw,
                Err(error) => {
                    fail_all_pending(
                        &pending,
                        format!("failed to decode worker protocol message: {error}"),
                    );
                    return;
                }
            };

            let Some(request_id) = raw.get("id").and_then(Value::as_u64) else {
                continue;
            };
            let is_event = raw.get("event").is_some();
            let pending_request = if is_event {
                pending
                    .lock()
                    .ok()
                    .and_then(|pending_map| match pending_map.get(&request_id) {
                        Some(PendingRequest::Execute(sender)) => {
                            Some(PendingRequest::Execute(sender.clone()))
                        }
                        _ => None,
                    })
            } else {
                pending
                    .lock()
                    .ok()
                    .and_then(|mut pending_map| pending_map.remove(&request_id))
            };

            let Some(pending_request) = pending_request else {
                continue;
            };

            match pending_request {
                PendingRequest::Execute(sender) => {
                    let message = if let Some(event_name) = raw.get("event").and_then(Value::as_str)
                    {
                        match event_name {
                            "input_request" => {
                                match serde_json::from_value::<WorkerInputRequest>(raw) {
                                    Ok(event) => WorkerExecutionMessage::InputRequest {
                                        prompt: event.prompt,
                                        password: event.password,
                                    },
                                    Err(error) => WorkerExecutionMessage::Failure(format!(
                                        "failed to decode worker input request: {error}"
                                    )),
                                }
                            }
                            "stream" => match serde_json::from_value::<WorkerStreamEvent>(raw) {
                                Ok(event) => WorkerExecutionMessage::Stream {
                                    name: event.name,
                                    text: event.text,
                                },
                                Err(error) => WorkerExecutionMessage::Failure(format!(
                                    "failed to decode worker stream event: {error}"
                                )),
                            },
                            "debug_event" => {
                                match serde_json::from_value::<WorkerDebugProtocolEvent>(raw) {
                                    Ok(event) => {
                                        WorkerExecutionMessage::DebugEvent(WorkerDebugEvent {
                                            msg_type: event.msg_type,
                                            content: event.content,
                                        })
                                    }
                                    Err(error) => WorkerExecutionMessage::Failure(format!(
                                        "failed to decode worker debug event: {error}"
                                    )),
                                }
                            }
                            other => WorkerExecutionMessage::Failure(format!(
                                "unexpected worker event kind {other:?} for request {request_id}"
                            )),
                        }
                    } else {
                        match serde_json::from_value::<WorkerEnvelope<ExecutionOutcome>>(raw) {
                            Ok(response) => WorkerExecutionMessage::Completion(response.payload),
                            Err(error) => WorkerExecutionMessage::Failure(format!(
                                "failed to decode worker execution response: {error}"
                            )),
                        }
                    };
                    let _ = sender.send(message);
                }
                PendingRequest::Response(sender) => {
                    let _ = sender.send(raw);
                }
            }
        }
    })
}

fn fail_all_pending(pending: &Arc<Mutex<HashMap<u64, PendingRequest>>>, message: String) {
    let pending_requests = pending
        .lock()
        .map(|mut pending_map| pending_map.drain().collect::<Vec<_>>())
        .unwrap_or_default();
    for (_, request) in pending_requests {
        match request {
            PendingRequest::Execute(sender) => {
                let _ = sender.send(WorkerExecutionMessage::Failure(message.clone()));
            }
            PendingRequest::Response(sender) => {
                let _ = sender.send(serde_json::json!({
                    "id": 0,
                    "status": "error",
                    "evalue": message,
                }));
            }
        }
    }
}

fn python_supports_ipython(program: &str, prefix_args: &[&str]) -> bool {
    Command::new(program)
        .args(prefix_args)
        .arg("-c")
        .arg("import IPython")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

impl Drop for PythonWorker {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[cfg(unix)]
fn create_worker_protocol_pipe() -> Result<(OwnedFd, OwnedFd), KernelError> {
    let mut fds = [0; 2];
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if rc == -1 {
        return Err(KernelError::Io(std::io::Error::last_os_error()));
    }

    set_cloexec(fds[0])?;
    set_cloexec(fds[1])?;

    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

#[cfg(unix)]
fn set_cloexec(fd: libc::c_int) -> Result<(), KernelError> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags == -1 {
        return Err(KernelError::Io(std::io::Error::last_os_error()));
    }

    let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
    if rc == -1 {
        return Err(KernelError::Io(std::io::Error::last_os_error()));
    }

    Ok(())
}

#[cfg(unix)]
unsafe fn install_worker_protocol_fd(protocol_write_fd: libc::c_int) -> std::io::Result<()> {
    if unsafe { libc::dup2(protocol_write_fd, WORKER_PROTOCOL_FD) } == -1 {
        return Err(std::io::Error::last_os_error());
    }

    if protocol_write_fd != WORKER_PROTOCOL_FD && unsafe { libc::close(protocol_write_fd) } == -1 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::PythonWorker;

    #[test]
    fn python_worker_can_report_debug_listener_endpoint() {
        let mut worker = PythonWorker::start().expect("worker should start");
        let listen = worker.debug_listen().expect("debug_listen should succeed");
        if listen.available {
            assert_eq!(listen.host, "127.0.0.1");
            assert!(listen.port > 0);
        } else {
            assert_eq!(listen.host, "");
            assert_eq!(listen.port, 0);
        }
    }
}
