use std::fs::File;
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

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
pub struct ExecutionOutcome {
    pub status: String,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub displays: Vec<ExecutionDisplayEvent>,
    #[serde(default)]
    pub comm_events: Vec<WorkerCommEvent>,
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

#[derive(Serialize)]
#[serde(tag = "kind")]
enum WorkerRequest<'a> {
    #[serde(rename = "execute")]
    Execute {
        id: u64,
        code: &'a str,
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
}

#[derive(Debug, Deserialize)]
struct WorkerInputRequest {
    id: u64,
    event: String,
    #[serde(default)]
    prompt: String,
    #[serde(default)]
    password: bool,
}

#[derive(Debug, Deserialize)]
struct WorkerStreamEvent {
    id: u64,
    event: String,
    name: String,
    #[serde(default)]
    text: String,
}

#[derive(Deserialize)]
struct WorkerEnvelope<T> {
    id: u64,
    #[serde(flatten)]
    payload: T,
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
    protocol: BufReader<File>,
    next_id: u64,
    interrupt_handle: WorkerInterruptHandle,
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

        Ok(Self {
            child,
            stdin,
            protocol: BufReader::new(File::from(protocol_read)),
            next_id: 1,
            interrupt_handle,
        })
    }

    pub fn interrupt_handle(&self) -> WorkerInterruptHandle {
        self.interrupt_handle.clone()
    }

    pub fn execute<F, G>(
        &mut self,
        code: &str,
        user_expressions: &Value,
        execution_count: u32,
        silent: bool,
        store_history: bool,
        mut on_input: F,
        mut on_stream: G,
    ) -> Result<ExecutionOutcome, KernelError>
    where
        F: FnMut(&str, bool) -> Result<String, String>,
        G: FnMut(&str, &str) -> Result<(), KernelError>,
    {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::Execute {
            id: request_id,
            code,
            user_expressions,
            execution_count,
            silent,
            store_history,
        };

        let payload = serde_json::to_vec(&request).map_err(|error| {
            KernelError::Worker(format!("failed to encode worker request: {error}"))
        })?;
        self.stdin.write_all(&payload).map_err(KernelError::Io)?;
        self.stdin
            .write_all(
                b"
",
            )
            .map_err(KernelError::Io)?;
        self.stdin.flush().map_err(KernelError::Io)?;

        loop {
            let mut line = String::new();
            let bytes_read = self
                .protocol
                .read_line(&mut line)
                .map_err(KernelError::Io)?;
            if bytes_read == 0 {
                return Err(KernelError::Worker(self.stderr_summary()));
            }

            let raw: Value = serde_json::from_str(&line).map_err(|error| {
                KernelError::Worker(format!("failed to decode worker response: {error}"))
            })?;

            if raw.get("event").is_some() {
                match raw.get("event").and_then(Value::as_str).unwrap_or_default() {
                    "input_request" => {
                        let event: WorkerInputRequest =
                            serde_json::from_value(raw).map_err(|error| {
                                KernelError::Worker(format!(
                                    "failed to decode worker input request: {error}"
                                ))
                            })?;
                        if event.id != request_id || event.event != "input_request" {
                            return Err(KernelError::Worker(format!(
                                "unexpected worker event for request {}",
                                request_id
                            )));
                        }
                        let response = match on_input(&event.prompt, event.password) {
                            Ok(value) => WorkerRequest::InputReply {
                                id: request_id,
                                value,
                                error: None,
                            },
                            Err(error) => WorkerRequest::InputReply {
                                id: request_id,
                                value: String::new(),
                                error: Some(error),
                            },
                        };
                        let payload = serde_json::to_vec(&response).map_err(|error| {
                            KernelError::Worker(format!(
                                "failed to encode worker input reply: {error}"
                            ))
                        })?;
                        self.stdin.write_all(&payload).map_err(KernelError::Io)?;
                        self.stdin.write_all(b"\n").map_err(KernelError::Io)?;
                        self.stdin.flush().map_err(KernelError::Io)?;
                    }
                    "stream" => {
                        let event: WorkerStreamEvent =
                            serde_json::from_value(raw).map_err(|error| {
                                KernelError::Worker(format!(
                                    "failed to decode worker stream event: {error}"
                                ))
                            })?;
                        if event.id != request_id || event.event != "stream" {
                            return Err(KernelError::Worker(format!(
                                "unexpected worker stream event for request {}",
                                request_id
                            )));
                        }
                        on_stream(&event.name, &event.text)?;
                    }
                    event_name => {
                        return Err(KernelError::Worker(format!(
                            "unexpected worker event kind {event_name:?} for request {}",
                            request_id
                        )));
                    }
                }
                continue;
            }

            let response: WorkerEnvelope<ExecutionOutcome> =
                serde_json::from_value(raw).map_err(|error| {
                    KernelError::Worker(format!("failed to decode worker response: {error}"))
                })?;

            if response.id != request_id {
                return Err(KernelError::Worker(format!(
                    "worker response id mismatch: expected {}, got {}",
                    request_id, response.id
                )));
            }

            return Ok(response.payload);
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

    pub fn restart(&mut self) -> Result<(), KernelError> {
        self.terminate();
        let replacement = Self::start()?;
        *self = replacement;
        Ok(())
    }

    fn stderr_summary(&mut self) -> String {
        match self.child.try_wait() {
            Ok(Some(status)) => format!("python worker exited unexpectedly with status {status}"),
            Ok(None) => "python worker closed its protocol channel unexpectedly".to_owned(),
            Err(error) => format!("python worker became unavailable: {error}"),
        }
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
    }

    fn send_request<T>(
        &mut self,
        request: &WorkerRequest<'_>,
        request_id: u64,
    ) -> Result<WorkerEnvelope<T>, KernelError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let payload = serde_json::to_vec(request).map_err(|error| {
            KernelError::Worker(format!("failed to encode worker request: {error}"))
        })?;
        self.stdin.write_all(&payload).map_err(KernelError::Io)?;
        self.stdin.write_all(b"\n").map_err(KernelError::Io)?;
        self.stdin.flush().map_err(KernelError::Io)?;

        let mut line = String::new();
        let bytes_read = self
            .protocol
            .read_line(&mut line)
            .map_err(KernelError::Io)?;
        if bytes_read == 0 {
            return Err(KernelError::Worker(self.stderr_summary()));
        }

        let response: WorkerEnvelope<T> = serde_json::from_str(&line).map_err(|error| {
            KernelError::Worker(format!("failed to decode worker response: {error}"))
        })?;

        if response.id != request_id {
            return Err(KernelError::Worker(format!(
                "worker response id mismatch: expected {}, got {}",
                request_id, response.id
            )));
        }

        Ok(response)
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
    if libc::dup2(protocol_write_fd, WORKER_PROTOCOL_FD) == -1 {
        return Err(std::io::Error::last_os_error());
    }

    if protocol_write_fd != WORKER_PROTOCOL_FD && libc::close(protocol_write_fd) == -1 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(())
}
