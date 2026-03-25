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

use crate::protocol::{
    IMPLEMENTATION, JUPYTER_PROTOCOL_VERSION, JupyterMessage, LANGUAGE, MessageHeader,
    MessageSigner, ProtocolError,
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
    signer: MessageSigner,
    kernel_session: String,
    execution_count: u32,
}

impl MessageLoopState {
    fn new(connection: &ConnectionInfo) -> Result<Self, KernelError> {
        Ok(Self {
            signer: MessageSigner::new(&connection.signature_scheme, &connection.key)?,
            kernel_session: connection
                .kernel_name
                .clone()
                .unwrap_or_else(|| IMPLEMENTATION.to_owned()),
            execution_count: 0,
        })
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
}

enum ChannelKind {
    Shell,
    Control,
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

        publish_status(&iopub_socket, &state, json!({}), "starting")?;
        let _ = ready_tx.send(Ok(()));

        loop {
            if shutdown.is_stopped() {
                return Ok(());
            }

            let mut poll_items = [
                shell_socket.as_poll_item(zmq::POLLIN),
                control_socket.as_poll_item(zmq::POLLIN),
                stdin_socket.as_poll_item(zmq::POLLIN),
            ];
            match zmq::poll(&mut poll_items, CHANNEL_POLL_INTERVAL_MS) {
                Ok(_) => {}
                Err(zmq::Error::ETERM) if shutdown.is_stopped() => return Ok(()),
                Err(error) => return Err(KernelError::Zmq(error)),
            }

            if poll_items[0].is_readable() {
                let frames = shell_socket.recv_multipart(0)?;
                handle_request(
                    ChannelKind::Shell,
                    frames,
                    &shell_socket,
                    &iopub_socket,
                    &mut state,
                    shutdown.as_ref(),
                )?;
            }

            if poll_items[1].is_readable() {
                let frames = control_socket.recv_multipart(0)?;
                handle_request(
                    ChannelKind::Control,
                    frames,
                    &control_socket,
                    &iopub_socket,
                    &mut state,
                    shutdown.as_ref(),
                )?;
            }

            if poll_items[2].is_readable() {
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
    iopub_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    shutdown: &ShutdownSignal,
) -> Result<(), KernelError> {
    let request = match state.signer.decode(frames) {
        Ok(request) => request,
        Err(_) => return Ok(()),
    };

    let parent_header = request.header_value.clone();
    publish_status(iopub_socket, state, parent_header.clone(), "busy")?;

    let should_stop = match channel {
        ChannelKind::Shell => handle_shell_request(reply_socket, iopub_socket, state, &request)?,
        ChannelKind::Control => {
            handle_control_request(reply_socket, iopub_socket, state, &request)?
        }
    };

    publish_status(iopub_socket, state, parent_header, "idle")?;

    if should_stop {
        shutdown.request_stop();
    }

    Ok(())
}

fn handle_shell_request(
    reply_socket: &zmq::Socket,
    iopub_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<bool, KernelError> {
    match request.header.msg_type.as_str() {
        "kernel_info_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "kernel_info_reply",
                kernel_info_content(),
            )?;
            Ok(false)
        }
        "execute_request" => {
            let silent = request
                .content
                .get("silent")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let code = request
                .content
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let execution_count = state.next_execution_count(&request.content);

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

            send_reply(
                reply_socket,
                state,
                request,
                "execute_reply",
                json!({
                    "status": "ok",
                    "execution_count": execution_count,
                    "user_expressions": {},
                    "payload": [],
                }),
            )?;
            Ok(false)
        }
        "is_complete_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "is_complete_reply",
                json!({
                    "status": "complete",
                    "indent": "",
                }),
            )?;
            Ok(false)
        }
        "complete_request" => {
            let cursor_pos = request
                .content
                .get("cursor_pos")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            send_reply(
                reply_socket,
                state,
                request,
                "complete_reply",
                json!({
                    "status": "ok",
                    "matches": [],
                    "cursor_start": cursor_pos,
                    "cursor_end": cursor_pos,
                    "metadata": {},
                }),
            )?;
            Ok(false)
        }
        "inspect_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "inspect_reply",
                json!({
                    "status": "ok",
                    "found": false,
                    "data": {},
                    "metadata": {},
                }),
            )?;
            Ok(false)
        }
        "history_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "history_reply",
                json!({
                    "status": "ok",
                    "history": [],
                }),
            )?;
            Ok(false)
        }
        "comm_info_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "comm_info_reply",
                json!({
                    "status": "ok",
                    "comms": {},
                }),
            )?;
            Ok(false)
        }
        "shutdown_request" => handle_shutdown_request(reply_socket, state, request),
        _ => send_unsupported_reply(reply_socket, state, request),
    }
}

fn handle_control_request(
    reply_socket: &zmq::Socket,
    _iopub_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<bool, KernelError> {
    match request.header.msg_type.as_str() {
        "kernel_info_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "kernel_info_reply",
                kernel_info_content(),
            )?;
            Ok(false)
        }
        "shutdown_request" => handle_shutdown_request(reply_socket, state, request),
        "interrupt_request" => {
            send_reply(
                reply_socket,
                state,
                request,
                "interrupt_reply",
                json!({ "status": "ok" }),
            )?;
            Ok(false)
        }
        _ => send_unsupported_reply(reply_socket, state, request),
    }
}

fn handle_shutdown_request(
    reply_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<bool, KernelError> {
    let restart = request
        .content
        .get("restart")
        .and_then(Value::as_bool)
        .unwrap_or(false);
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
    Ok(true)
}

fn send_unsupported_reply(
    reply_socket: &zmq::Socket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<bool, KernelError> {
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
    Ok(false)
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

fn reply_type_for(msg_type: &str) -> String {
    if let Some(prefix) = msg_type.strip_suffix("_request") {
        format!("{prefix}_reply")
    } else {
        "error".to_owned()
    }
}

fn kernel_info_content() -> Value {
    json!({
        "status": "ok",
        "protocol_version": JUPYTER_PROTOCOL_VERSION,
        "implementation": IMPLEMENTATION,
        "implementation_version": env!("CARGO_PKG_VERSION"),
        "banner": format!("{IMPLEMENTATION} {}", env!("CARGO_PKG_VERSION")),
        "debugger": false,
        "help_links": [],
        "language_info": {
            "name": LANGUAGE,
            "version": "3",
            "mimetype": "text/x-python",
            "file_extension": ".py",
            "pygments_lexer": "python",
            "nbconvert_exporter": "python",
            "codemirror_mode": {
                "name": "ipython",
                "version": 3,
            },
        },
    })
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
    use std::thread;
    use std::time::{Duration, Instant};

    use serde_json::{Value, json};

    use super::{ConnectionConfig, ConnectionInfo, KernelError, start_kernel};
    use crate::protocol::{JupyterMessage, MessageHeader, MessageSigner};

    #[test]
    fn accepts_valid_connection_config() {
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

        let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 2);
        assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

        runtime.stop().unwrap();
    }

    #[test]
    fn execute_request_publishes_execute_input_and_reply() {
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

        let published = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 3);
        let msg_types: Vec<_> = published
            .iter()
            .map(|message| message.header.msg_type.as_str())
            .collect();
        assert_eq!(msg_types, vec!["status", "execute_input", "status"]);
        assert_eq!(
            published[1].content.get("code"),
            Some(&json!("print('hello')"))
        );
        assert_eq!(published[1].content.get("execution_count"), Some(&json!(1)));
        assert_eq!(status_message_states(&published), vec!["busy", "idle"]);

        runtime.stop().unwrap();
    }

    #[test]
    fn stdin_channel_accepts_input_reply_without_stopping_kernel() {
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
    fn control_channel_shutdown_request_stops_kernel() {
        let connection = test_connection_info();
        let mut runtime = start_kernel(connection.clone()).unwrap();
        let signer = MessageSigner::new(&connection.signature_scheme, &connection.key).unwrap();
        let context = zmq::Context::new();
        let control = connect_dealer(
            &context,
            &runtime.channel_endpoints().control,
            b"control-client",
        );

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

        runtime.wait_for_shutdown();
        assert!(!runtime.is_running());
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
