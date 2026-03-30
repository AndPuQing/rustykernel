use std::fs;
use std::io::Error as IoError;
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};

use serde::Deserialize;
use zeromq::ZmqError;

use crate::protocol::{IMPLEMENTATION, JUPYTER_PROTOCOL_VERSION, LANGUAGE, ProtocolError};

mod comms;
mod debug;
mod dispatch;
pub(crate) mod execute;
mod history;
mod io;
mod runtime;
mod state;

pub use runtime::KernelRuntime;
#[cfg(test)]
use runtime::{build_transport_runtime, multipart_message, recv_frames};

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
    Io(IoError),
    InvalidConnectionConfig(&'static str),
    InvalidConnectionFile(serde_json::Error),
    Worker(String),
    Protocol(ProtocolError),
    Zmq(ZmqError),
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

impl From<ZmqError> for KernelError {
    fn from(error: ZmqError) -> Self {
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

pub(crate) struct ShutdownSignal {
    is_stopped: AtomicBool,
    lock: Mutex<bool>,
    cvar: Condvar,
    pub(crate) wake: tokio::sync::Notify,
}

impl ShutdownSignal {
    fn new() -> Self {
        Self {
            is_stopped: AtomicBool::new(false),
            lock: Mutex::new(false),
            cvar: Condvar::new(),
            wake: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn request_stop(&self) {
        self.is_stopped.store(true, Ordering::SeqCst);
        self.wake.notify_waiters();
        if let Ok(mut stopped) = self.lock.lock() {
            *stopped = true;
            self.cvar.notify_all();
        }
    }

    pub(crate) fn is_stopped(&self) -> bool {
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

pub(crate) struct InterruptSignal {
    pending: AtomicBool,
    pub(crate) wake: tokio::sync::Notify,
}

impl InterruptSignal {
    fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            wake: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn request_interrupt(&self) {
        self.pending.store(true, Ordering::SeqCst);
        self.wake.notify_waiters();
    }

    pub(crate) fn take_pending(&self) -> bool {
        self.pending.swap(false, Ordering::SeqCst)
    }
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

pub(crate) fn process_is_kernel_descendant(
    system: &sysinfo::System,
    pid: sysinfo::Pid,
    kernel_pid: sysinfo::Pid,
) -> bool {
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

#[cfg(test)]
mod tests;
