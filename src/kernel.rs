use std::fs;
use std::io;

use serde::Deserialize;

use crate::protocol::{IMPLEMENTATION, JUPYTER_PROTOCOL_VERSION, LANGUAGE};

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

#[derive(Debug)]
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
}

impl std::fmt::Display for KernelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::InvalidConnectionConfig(message) => f.write_str(message),
            Self::InvalidConnectionFile(error) => {
                write!(f, "invalid connection file JSON: {error}")
            }
        }
    }
}

impl std::error::Error for KernelError {}

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
    use super::{ConnectionConfig, ConnectionInfo, KernelError};

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
}
