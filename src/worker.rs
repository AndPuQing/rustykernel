use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Write};
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

use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use serde_json::Value;

use crate::kernel::KernelError;
use crate::kernel::execute::{KernelEvent, KernelEventSender, WorkerUpdateEvent};
pub use crate::worker_protocol::{
    CommOutcome, CompletionOutcome, ExecutionDisplayEvent, ExecutionOutcome, InspectionOutcome,
    IsCompleteOutcome, WorkerCommEvent, WorkerDebugEvent, WorkerDebugListen, WorkerKernelInfo,
};
use crate::worker_protocol::{
    FrameType, RawWorkerEnvelope, WorkerEnvelope, WorkerEvent, WorkerRequest, WorkerResponse,
};

const PYTHON_WORKER_SCRIPT: &str = include_str!("../python/rustykernel/worker_main.py");
pub const WORKER_PYTHON_EXECUTABLE_ENV: &str = "RUSTYKERNEL_PYTHON_EXECUTABLE";
#[cfg(unix)]
const WORKER_PROTOCOL_ENV: &str = "RUSTYKERNEL_PROTOCOL_FD";
#[cfg(unix)]
const WORKER_PROTOCOL_FD: libc::c_int = 3;
const WORKER_PROTOCOL_COMPRESSED_FLAG: u8 = 1 << 0;
const WORKER_PROTOCOL_HEADER_LEN: usize = 9;
const WORKER_PROTOCOL_COMPRESSION_THRESHOLD: usize = 128 * 1024;
const WORKER_PROTOCOL_MIN_COMPRESSION_SAVINGS: usize = 256;

enum PendingRequest {
    Execute,
    Response(mpsc::Sender<Result<WorkerEnvelope<WorkerResponse>, String>>),
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

            Err(KernelError::Worker(format!(
                "failed to interrupt python worker {pid}: {error}"
            )))
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
    pub fn start(kernel_events: KernelEventSender, worker_epoch: u64) -> Result<Self, KernelError> {
        #[cfg(not(unix))]
        {
            Err(KernelError::Worker(
                "python worker side-channel IPC requires unix support".to_owned(),
            ))
        }

        #[cfg(unix)]
        {
            let (protocol_read, protocol_write) = create_worker_protocol_pipe()?;

            let mut command =
                if let Some(executable) = std::env::var_os(WORKER_PYTHON_EXECUTABLE_ENV) {
                    Command::new(executable)
                } else {
                    let interpreter = if python_supports_ipython("python3", &[]) {
                        ("python3", Vec::<&str>::new())
                    } else if python_supports_ipython("uv", &["run", "python"]) {
                        ("uv", vec!["run", "python"])
                    } else {
                        ("python3", Vec::<&str>::new())
                    };
                    let mut command = Command::new(interpreter.0);
                    command.args(interpreter.1);
                    command
                };
            {
                let protocol_write_fd = protocol_write.as_raw_fd();
                command.env(WORKER_PROTOCOL_ENV, WORKER_PROTOCOL_FD.to_string());
                unsafe {
                    command.pre_exec(move || install_worker_protocol_fd(protocol_write_fd));
                }
            }
            command
                .arg("-u")
                .arg("-c")
                .arg(PYTHON_WORKER_SCRIPT)
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null());

            let mut child = command.spawn().map_err(KernelError::Io)?;
            drop(protocol_write);

            let stdin = child.stdin.take().ok_or_else(|| {
                KernelError::Worker("python worker stdin pipe was not available".to_owned())
            })?;
            let interrupt_handle = WorkerInterruptHandle::new(child.id());
            let pending = Arc::new(Mutex::new(HashMap::new()));
            let protocol_thread = Some(spawn_protocol_reader(
                protocol_read,
                Arc::clone(&pending),
                kernel_events,
                worker_epoch,
            ));

            Ok(Self {
                child,
                stdin,
                next_id: 1,
                interrupt_handle,
                pending,
                protocol_thread,
            })
        }
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
    ) -> Result<u64, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        self.pending
            .lock()
            .map_err(|_| KernelError::Worker("worker pending map mutex poisoned".to_owned()))?
            .insert(request_id, PendingRequest::Execute);

        let request = WorkerRequest::Execute {
            code: code.to_owned(),
            subshell_id: subshell_id.map(ToOwned::to_owned),
            user_expressions: user_expressions.clone(),
            execution_count,
            silent,
            store_history,
        };
        if let Err(error) = self.write_request(request_id, &request) {
            let _ = self
                .pending
                .lock()
                .map(|mut pending| pending.remove(&request_id));
            return Err(error);
        }

        Ok(request_id)
    }

    pub fn complete(
        &mut self,
        code: &str,
        cursor_pos: usize,
    ) -> Result<CompletionOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::Complete {
            code: code.to_owned(),
            cursor_pos,
        };
        match self.send_request(request_id, request)? {
            WorkerResponse::Complete(outcome) => Ok(outcome),
            other => Err(unexpected_response("complete", other)),
        }
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
            code: code.to_owned(),
            cursor_pos,
            detail_level,
        };
        match self.send_request(request_id, request)? {
            WorkerResponse::Inspect(outcome) => Ok(outcome),
            other => Err(unexpected_response("inspect", other)),
        }
    }

    pub fn is_complete(&mut self, code: &str) -> Result<IsCompleteOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::IsComplete {
            code: code.to_owned(),
        };
        match self.send_request(request_id, request)? {
            WorkerResponse::IsComplete(outcome) => Ok(outcome),
            other => Err(unexpected_response("is_complete", other)),
        }
    }

    pub fn kernel_info(&mut self) -> Result<WorkerKernelInfo, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        match self.send_request(request_id, WorkerRequest::KernelInfo)? {
            WorkerResponse::KernelInfo(info) => Ok(info),
            other => Err(unexpected_response("kernel_info", other)),
        }
    }

    pub fn debug_listen(&mut self) -> Result<WorkerDebugListen, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        match self.send_request(request_id, WorkerRequest::DebugListen)? {
            WorkerResponse::DebugListen(info) => Ok(info),
            other => Err(unexpected_response("debug_listen", other)),
        }
    }

    pub fn debug_request(&mut self, message: &Value) -> Result<Value, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::Debug {
            message: message.clone(),
        };
        match self.send_request(request_id, request)? {
            WorkerResponse::Debug(value) => Ok(value),
            other => Err(unexpected_response("debug", other)),
        }
    }

    pub fn interrupt_subshells(&mut self) -> Result<(), KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        match self.send_request(request_id, WorkerRequest::Interrupt)? {
            WorkerResponse::Interrupt(response) if response.status == "ok" => Ok(()),
            WorkerResponse::Interrupt(response) => Err(KernelError::Worker(response.evalue)),
            other => Err(unexpected_response("interrupt", other)),
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
            comm_id: comm_id.to_owned(),
            target_name: target_name.to_owned(),
            data: data.clone(),
            metadata: metadata.clone(),
        };
        match self.send_request(request_id, request)? {
            WorkerResponse::Comm(outcome) => Ok(outcome),
            other => Err(unexpected_response("comm_open", other)),
        }
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
            comm_id: comm_id.to_owned(),
            data: data.clone(),
            metadata: metadata.clone(),
        };
        match self.send_request(request_id, request)? {
            WorkerResponse::Comm(outcome) => Ok(outcome),
            other => Err(unexpected_response("comm_msg", other)),
        }
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
            comm_id: comm_id.to_owned(),
            data: data.clone(),
            metadata: metadata.clone(),
        };
        match self.send_request(request_id, request)? {
            WorkerResponse::Comm(outcome) => Ok(outcome),
            other => Err(unexpected_response("comm_close", other)),
        }
    }

    pub fn create_subshell(&mut self) -> Result<String, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        match self.send_request(request_id, WorkerRequest::CreateSubshell)? {
            WorkerResponse::CreateSubshell(response) if response.status == "ok" => {
                if response.subshell_id.is_empty() {
                    Err(KernelError::Worker(
                        "worker create_subshell response did not include subshell id".to_owned(),
                    ))
                } else {
                    Ok(response.subshell_id)
                }
            }
            WorkerResponse::CreateSubshell(response) => Err(KernelError::Worker(response.evalue)),
            other => Err(unexpected_response("create_subshell", other)),
        }
    }

    pub fn delete_subshell(&mut self, subshell_id: &str) -> Result<(), KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::DeleteSubshell {
            subshell_id: subshell_id.to_owned(),
        };
        match self.send_request(request_id, request)? {
            WorkerResponse::DeleteSubshell(response) if response.status == "ok" => Ok(()),
            WorkerResponse::DeleteSubshell(response) => Err(KernelError::Worker(response.evalue)),
            other => Err(unexpected_response("delete_subshell", other)),
        }
    }

    pub fn list_subshell(&mut self) -> Result<Vec<String>, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        match self.send_request(request_id, WorkerRequest::ListSubshell)? {
            WorkerResponse::ListSubshell(response) if response.status == "ok" => {
                Ok(response.subshell_id)
            }
            WorkerResponse::ListSubshell(response) => Err(KernelError::Worker(response.evalue)),
            other => Err(unexpected_response("list_subshell", other)),
        }
    }

    pub fn restart(
        &mut self,
        kernel_events: KernelEventSender,
        worker_epoch: u64,
    ) -> Result<(), KernelError> {
        self.terminate();
        let replacement = Self::start(kernel_events, worker_epoch)?;
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

    fn send_request(
        &mut self,
        request_id: u64,
        request: WorkerRequest,
    ) -> Result<WorkerResponse, KernelError> {
        let (tx, rx) = mpsc::channel();
        self.pending
            .lock()
            .map_err(|_| KernelError::Worker("worker pending map mutex poisoned".to_owned()))?
            .insert(request_id, PendingRequest::Response(tx));
        if let Err(error) = self.write_request(request_id, &request) {
            let _ = self
                .pending
                .lock()
                .map(|mut pending| pending.remove(&request_id));
            return Err(error);
        }
        let response = rx.recv().map_err(|error| {
            KernelError::Worker(format!(
                "worker response channel closed unexpectedly for request {request_id}: {error}"
            ))
        })?;
        let response = response.map_err(KernelError::Worker)?;
        Ok(response.body)
    }

    pub(crate) fn send_input_reply(
        &mut self,
        request_id: u64,
        value: String,
        error: Option<String>,
    ) -> Result<(), KernelError> {
        self.write_request(request_id, &WorkerRequest::InputReply { value, error })
    }

    fn write_request(
        &mut self,
        request_id: u64,
        request: &WorkerRequest,
    ) -> Result<(), KernelError> {
        let payload = encode_protocol_frame(&WorkerEnvelope::request(request_id, request))
            .map_err(|error| {
                KernelError::Worker(format!("failed to encode worker request: {error}"))
            })?;
        self.stdin.write_all(&payload).map_err(KernelError::Io)?;
        self.stdin.flush().map_err(KernelError::Io)?;
        Ok(())
    }
}

fn unexpected_response(expected: &str, response: WorkerResponse) -> KernelError {
    KernelError::Worker(format!(
        "unexpected worker response for {expected}: {response:?}"
    ))
}

#[cfg(unix)]
fn spawn_protocol_reader(
    protocol_read: OwnedFd,
    pending: Arc<Mutex<HashMap<u64, PendingRequest>>>,
    kernel_events: KernelEventSender,
    worker_epoch: u64,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut protocol = BufReader::new(File::from(protocol_read));
        loop {
            let payload = match read_protocol_frame(&mut protocol) {
                Ok(Some(payload)) => payload,
                Ok(None) => {
                    fail_all_pending(
                        &pending,
                        "python worker closed its protocol channel unexpectedly".to_owned(),
                    );
                    return;
                }
                Err(error) => {
                    fail_all_pending(
                        &pending,
                        format!("failed to read worker protocol message: {error}"),
                    );
                    return;
                }
            };

            let envelope = match serde_json::from_slice::<RawWorkerEnvelope>(&payload) {
                Ok(envelope) => envelope,
                Err(error) => {
                    fail_all_pending(
                        &pending,
                        format!("failed to decode worker protocol envelope: {error}"),
                    );
                    return;
                }
            };

            if envelope.protocol_version != crate::worker_protocol::WORKER_PROTOCOL_VERSION {
                fail_all_pending(
                    &pending,
                    format!(
                        "unsupported worker protocol version {}",
                        envelope.protocol_version
                    ),
                );
                return;
            }

            let request_id = envelope.request_id;
            if envelope.frame_type == FrameType::Event && envelope.seq.is_none() {
                fail_all_pending(
                    &pending,
                    format!("worker event missing seq for request {request_id}"),
                );
                return;
            }

            let pending_request = if envelope.frame_type == FrameType::Event {
                pending.lock().ok().and_then(|pending_map| {
                    pending_map.get(&request_id).map(|request| match request {
                        PendingRequest::Execute => PendingRequest::Execute,
                        PendingRequest::Response(sender) => {
                            PendingRequest::Response(sender.clone())
                        }
                    })
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
                PendingRequest::Execute => {
                    let update = match envelope.frame_type {
                        FrameType::Event => {
                            match serde_json::from_value::<WorkerEvent>(envelope.body) {
                                Ok(WorkerEvent::InputRequest { prompt, password }) => {
                                    WorkerUpdateEvent::InputRequest {
                                        request_id,
                                        prompt,
                                        password,
                                    }
                                }
                                Ok(WorkerEvent::Stream { name, text, source }) => {
                                    WorkerUpdateEvent::Stream {
                                        request_id,
                                        name,
                                        source,
                                        text,
                                    }
                                }
                                Ok(WorkerEvent::Display {
                                    msg_type,
                                    content,
                                    data,
                                    metadata,
                                    transient,
                                }) => WorkerUpdateEvent::DisplayEvent {
                                    request_id,
                                    event: ExecutionDisplayEvent {
                                        msg_type,
                                        content,
                                        data,
                                        metadata,
                                        transient,
                                    },
                                },
                                Ok(WorkerEvent::Comm { msg_type, content }) => {
                                    WorkerUpdateEvent::CommEvent {
                                        request_id,
                                        event: WorkerCommEvent { msg_type, content },
                                    }
                                }
                                Ok(WorkerEvent::Debug { msg_type, content }) => {
                                    WorkerUpdateEvent::DebugEvent {
                                        request_id,
                                        event: WorkerDebugEvent { msg_type, content },
                                    }
                                }
                                Err(error) => WorkerUpdateEvent::Completion {
                                    request_id,
                                    outcome: Err(format!("failed to decode worker event: {error}")),
                                },
                            }
                        }
                        FrameType::Response => {
                            match serde_json::from_value::<WorkerResponse>(envelope.body) {
                                Ok(WorkerResponse::Execute(outcome)) => {
                                    WorkerUpdateEvent::Completion {
                                        request_id,
                                        outcome: Ok(outcome),
                                    }
                                }
                                Ok(other) => WorkerUpdateEvent::Completion {
                                    request_id,
                                    outcome: Err(format!(
                                        "unexpected worker response for execute request {request_id}: {other:?}"
                                    )),
                                },
                                Err(error) => WorkerUpdateEvent::Completion {
                                    request_id,
                                    outcome: Err(format!(
                                        "failed to decode worker execution response: {error}"
                                    )),
                                },
                            }
                        }
                        FrameType::Request => WorkerUpdateEvent::Completion {
                            request_id,
                            outcome: Err(format!(
                                "unexpected worker request frame for request {request_id}"
                            )),
                        },
                    };
                    let _ = kernel_events.send(KernelEvent::WorkerUpdate {
                        worker_epoch,
                        update,
                    });
                }
                PendingRequest::Response(sender) => {
                    let response = match envelope.frame_type {
                        FrameType::Response => {
                            match serde_json::from_value::<WorkerResponse>(envelope.body) {
                                Ok(body) => Ok(WorkerEnvelope::response(request_id, body)),
                                Err(error) => {
                                    Err(format!("failed to decode worker response: {error}"))
                                }
                            }
                        }
                        other => Err(format!(
                            "unexpected worker frame type {other:?} for request {request_id}"
                        )),
                    };
                    let _ = sender.send(response);
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
            PendingRequest::Execute => {}
            PendingRequest::Response(sender) => {
                let _ = sender.send(Err(message.clone()));
            }
        }
    }
}

fn encode_protocol_frame<T: serde::Serialize>(message: &T) -> Result<Vec<u8>, serde_json::Error> {
    let payload = serde_json::to_vec(message)?;
    let (flags, payload) = maybe_compress_protocol_payload(payload);

    let mut frame = Vec::with_capacity(WORKER_PROTOCOL_HEADER_LEN + payload.len());
    frame.push(flags);
    frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn maybe_compress_protocol_payload(payload: Vec<u8>) -> (u8, Vec<u8>) {
    if payload.len() < WORKER_PROTOCOL_COMPRESSION_THRESHOLD {
        return (0, payload);
    }

    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    if encoder.write_all(&payload).is_err() {
        return (0, payload);
    }

    let Ok(compressed) = encoder.finish() else {
        return (0, payload);
    };

    let savings = payload.len().saturating_sub(compressed.len());
    if savings < WORKER_PROTOCOL_MIN_COMPRESSION_SAVINGS
        || compressed.len() * 10 > payload.len() * 7
    {
        return (0, payload);
    }

    (WORKER_PROTOCOL_COMPRESSED_FLAG, compressed)
}

fn read_protocol_frame(reader: &mut impl Read) -> Result<Option<Vec<u8>>, std::io::Error> {
    let mut header = [0_u8; WORKER_PROTOCOL_HEADER_LEN];
    let mut read = 0;
    while read < header.len() {
        let bytes_read = reader.read(&mut header[read..])?;
        if bytes_read == 0 {
            if read == 0 {
                return Ok(None);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated worker protocol header",
            ));
        }
        read += bytes_read;
    }

    let flags = header[0];
    if flags & !WORKER_PROTOCOL_COMPRESSED_FLAG != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown worker protocol flags: {flags}"),
        ));
    }

    let payload_len = u64::from_be_bytes(header[1..].try_into().unwrap());
    let payload_len = usize::try_from(payload_len).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "worker protocol payload length exceeded usize",
        )
    })?;
    let mut payload = vec![0_u8; payload_len];
    reader.read_exact(&mut payload)?;

    if flags & WORKER_PROTOCOL_COMPRESSED_FLAG == 0 {
        return Ok(Some(payload));
    }

    let mut decoder = ZlibDecoder::new(payload.as_slice());
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(Some(decompressed))
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
    use std::io::Cursor;
    use std::sync::mpsc;

    use serde_json::json;

    use crate::kernel::execute::KernelEventSender;
    use crate::worker_protocol::{FrameType, RawWorkerEnvelope, WorkerEnvelope, WorkerRequest};

    use super::{PythonWorker, encode_protocol_frame, read_protocol_frame};

    #[test]
    fn python_worker_can_report_debug_listener_endpoint() {
        let (tx, _rx) = mpsc::channel();
        let mut worker =
            PythonWorker::start(KernelEventSender::new(tx), 1).expect("worker should start");
        let listen = worker.debug_listen().expect("debug_listen should succeed");
        if listen.available {
            assert_eq!(listen.host, "127.0.0.1");
            assert!(listen.port > 0);
        } else {
            assert_eq!(listen.host, "");
            assert_eq!(listen.port, 0);
        }
    }

    #[test]
    fn worker_protocol_frame_roundtrips_small_payload() {
        let frame =
            encode_protocol_frame(&WorkerEnvelope::request(7, WorkerRequest::KernelInfo)).unwrap();
        let decoded = read_protocol_frame(&mut Cursor::new(frame))
            .unwrap()
            .expect("frame should be present");
        let envelope: RawWorkerEnvelope = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(
            envelope.protocol_version,
            crate::worker_protocol::WORKER_PROTOCOL_VERSION
        );
        assert_eq!(envelope.frame_type, FrameType::Request);
        assert_eq!(envelope.request_id, 7);
        assert_eq!(envelope.body, json!({"request_type": "kernel_info"}));
    }

    #[test]
    fn worker_protocol_frame_roundtrips_compressed_payload() {
        let code = format!(
            "display({{\"image/png\": \"{}\"}}, raw=True)",
            "A".repeat(400_000)
        );
        let frame = encode_protocol_frame(&WorkerEnvelope::request(
            9,
            WorkerRequest::Execute {
                code: code.clone(),
                subshell_id: None,
                user_expressions: json!({}),
                execution_count: 1,
                silent: false,
                store_history: true,
            },
        ))
        .unwrap();
        assert_ne!(frame[0], 0, "large payload should be compressed");

        let decoded = read_protocol_frame(&mut Cursor::new(frame))
            .unwrap()
            .expect("frame should be present");
        let envelope: RawWorkerEnvelope = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(envelope.frame_type, FrameType::Request);
        assert_eq!(envelope.request_id, 9);
        assert_eq!(envelope.body["request_type"], json!("execute"),);
        assert_eq!(envelope.body["code"], json!(code));
    }
}
