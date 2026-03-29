
use std::net::TcpListener;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use zeromq::Socket as _;
use zeromq::SocketSend as _;
use zeromq::{DealerSocket, ReqSocket, SocketOptions, SubSocket, ZmqError};

use super::{
    ConnectionConfig, ConnectionInfo, KernelError, build_transport_runtime, multipart_message,
    recv_frames, start_kernel,
};
use crate::protocol::{JupyterMessage, MessageHeader, MessageSigner};

mod zmq {
    use super::*;

    #[derive(Clone, Copy)]
    pub enum SocketType {
        Dealer,
        Sub,
        Req,
    }

    #[derive(Clone)]
    pub struct Context {
        runtime: Arc<tokio::runtime::Runtime>,
    }

    impl Context {
        pub fn new() -> Self {
            Self {
                runtime: Arc::new(build_transport_runtime()),
            }
        }

        pub fn socket(&self, socket_type: SocketType) -> Result<Socket, ZmqError> {
            Ok(Socket::new(Arc::clone(&self.runtime), socket_type))
        }
    }

    pub struct Socket {
        runtime: Arc<tokio::runtime::Runtime>,
        inner: Mutex<InnerSocket>,
        rcvtimeo: Mutex<Option<Duration>>,
        sndtimeo: Mutex<Option<Duration>>,
    }

    enum InnerSocket {
        Dealer(DealerSocket),
        Sub(SubSocket),
        Req(ReqSocket),
    }

    impl Socket {
        fn new(runtime: Arc<tokio::runtime::Runtime>, socket_type: SocketType) -> Self {
            let inner = match socket_type {
                SocketType::Dealer => InnerSocket::Dealer(DealerSocket::new()),
                SocketType::Sub => InnerSocket::Sub(SubSocket::new()),
                SocketType::Req => InnerSocket::Req(ReqSocket::new()),
            };
            Self {
                runtime,
                inner: Mutex::new(inner),
                rcvtimeo: Mutex::new(None),
                sndtimeo: Mutex::new(None),
            }
        }

        pub fn set_rcvtimeo(&self, timeout_ms: i32) -> Result<(), ZmqError> {
            *self.rcvtimeo.lock().expect("test rcvtimeo mutex poisoned") = if timeout_ms < 0 {
                None
            } else {
                Some(Duration::from_millis(timeout_ms as u64))
            };
            Ok(())
        }

        pub fn set_sndtimeo(&self, timeout_ms: i32) -> Result<(), ZmqError> {
            *self.sndtimeo.lock().expect("test sndtimeo mutex poisoned") = if timeout_ms < 0 {
                None
            } else {
                Some(Duration::from_millis(timeout_ms as u64))
            };
            Ok(())
        }

        pub fn set_identity(&self, identity: &[u8]) -> Result<(), ZmqError> {
            let mut options = SocketOptions::default();
            options.peer_identity(
                zeromq::util::PeerIdentity::try_from(identity.to_vec())
                    .map_err(|_| ZmqError::PeerIdentity)?,
            );

            let mut inner = self.inner.lock().expect("test socket mutex poisoned");
            *inner = match &*inner {
                InnerSocket::Dealer(_) => InnerSocket::Dealer(DealerSocket::with_options(options)),
                InnerSocket::Req(_) => InnerSocket::Req(ReqSocket::with_options(options)),
                InnerSocket::Sub(_) => {
                    return Err(ZmqError::Socket("identity is unsupported for SUB socket"));
                }
            };
            Ok(())
        }

        pub fn set_subscribe(&self, subscription: &[u8]) -> Result<(), ZmqError> {
            let mut inner = self.inner.lock().expect("test socket mutex poisoned");
            match &mut *inner {
                InnerSocket::Sub(socket) => self
                    .runtime
                    .block_on(socket.subscribe(&String::from_utf8_lossy(subscription))),
                _ => Err(ZmqError::Socket(
                    "subscriptions are only supported for SUB sockets",
                )),
            }
        }

        pub fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
            let mut inner = self.inner.lock().expect("test socket mutex poisoned");
            match &mut *inner {
                InnerSocket::Dealer(socket) => self.runtime.block_on(socket.connect(endpoint)),
                InnerSocket::Sub(socket) => self.runtime.block_on(socket.connect(endpoint)),
                InnerSocket::Req(socket) => self.runtime.block_on(socket.connect(endpoint)),
            }
        }

        pub fn send_multipart(&self, frames: Vec<Vec<u8>>, _flags: i32) -> Result<(), ZmqError> {
            let _timeout = *self.sndtimeo.lock().expect("test sndtimeo mutex poisoned");
            let message = multipart_message(frames)?;
            let mut inner = self.inner.lock().expect("test socket mutex poisoned");
            match &mut *inner {
                InnerSocket::Dealer(socket) => self.runtime.block_on(socket.send(message)),
                InnerSocket::Req(socket) => self.runtime.block_on(socket.send(message)),
                InnerSocket::Sub(_) => Err(ZmqError::Socket("cannot send on SUB socket")),
            }
        }

        pub fn recv_multipart(&self, _flags: i32) -> Result<Vec<Vec<u8>>, ZmqError> {
            let timeout = *self.rcvtimeo.lock().expect("test rcvtimeo mutex poisoned");
            let mut inner = self.inner.lock().expect("test socket mutex poisoned");
            let message = match &mut *inner {
                InnerSocket::Dealer(socket) => recv_frames(&self.runtime, socket, timeout)?,
                InnerSocket::Sub(socket) => recv_frames(&self.runtime, socket, timeout)?,
                InnerSocket::Req(socket) => recv_frames(&self.runtime, socket, timeout)?,
            };
            message.ok_or(ZmqError::NoMessage)
        }
    }
}

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
    let socket = context.socket(zmq::SocketType::Req).unwrap();
    socket.set_rcvtimeo(2_000).unwrap();
    socket.set_sndtimeo(2_000).unwrap();
    socket.connect(&runtime.channel_endpoints().hb).unwrap();

    thread::sleep(Duration::from_millis(100));

    socket.send_multipart(vec![b"ping".to_vec()], 0).unwrap();
    let reply = socket.recv_multipart(0).unwrap();
    assert_eq!(reply, vec![b"ping".to_vec()]);

    runtime.stop().unwrap();
    assert!(!runtime.is_running());
}

#[test]
fn heartbeat_stop_returns_promptly_while_idle() {
    let _guard = test_lock();
    let connection = test_connection_info();
    let mut runtime = start_kernel(connection).unwrap();

    let started = std::time::Instant::now();
    runtime.stop().unwrap();

    assert!(
        started.elapsed() < Duration::from_millis(200),
        "idle heartbeat shutdown took {:?}",
        started.elapsed()
    );
    assert!(!runtime.is_running());
}

#[test]
fn wait_for_shutdown_returns_after_control_shutdown_request() {
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

    let request = client_request(
        "client-session",
        "shutdown_request",
        json!({
            "restart": false,
        }),
    );
    send_client_message(&control, &signer, &request);

    let _reply = recv_message(&control, &signer);

    let started = std::time::Instant::now();
    runtime.wait_for_shutdown();

    assert!(
        started.elapsed() < Duration::from_millis(200),
        "wait_for_shutdown took {:?}",
        started.elapsed()
    );
    assert!(!runtime.is_running());
    runtime.stop().unwrap();
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
    assert_eq!(reply.content.get("debugger"), Some(&json!(true)));
    assert_eq!(
        reply.content.get("supported_features"),
        Some(&json!(["debugger", "kernel subshells"]))
    );

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
    let first_published = recv_iopub_messages_for_parent(&iopub, &signer, &first.header.msg_id, 3);
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
    let close_statuses = recv_iopub_messages_for_parent(&iopub, &signer, &close.header.msg_id, 2);
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
    let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &incomplete.header.msg_id, 2);
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
    shell.set_rcvtimeo(10_000).unwrap();
    let stdin_socket = connect_dealer(
        &context,
        &runtime.channel_endpoints().stdin,
        client_identity,
    );
    stdin_socket.set_rcvtimeo(10_000).unwrap();
    let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);
    iopub.set_rcvtimeo(10_000).unwrap();

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

    let deadline = Instant::now() + Duration::from_secs(10);
    let input_request = loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for stdin input_request"
        );
        if let Some(message) = try_recv_message(&stdin_socket, &signer).unwrap() {
            break message;
        }
    };
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

    drop(stdin_socket);
    drop(shell);
    drop(iopub);
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

    drop(stdin_socket);
    drop(shell);
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
    control.set_rcvtimeo(10_000).unwrap();
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
fn control_channel_subshell_requests_round_trip() {
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
    control.set_rcvtimeo(10_000).unwrap();
    let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

    let create_request = client_request("client-session", "create_subshell_request", json!({}));
    send_client_message(&control, &signer, &create_request);
    let create_reply = recv_message(&control, &signer);
    assert_eq!(create_reply.header.msg_type, "create_subshell_reply");
    let subshell_id = create_reply
        .content
        .get("subshell_id")
        .and_then(Value::as_str)
        .unwrap()
        .to_owned();
    let statuses =
        recv_iopub_messages_for_parent(&iopub, &signer, &create_request.header.msg_id, 2);
    assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

    let list_request = client_request("client-session", "list_subshell_request", json!({}));
    send_client_message(&control, &signer, &list_request);
    let list_reply = recv_message(&control, &signer);
    assert_eq!(list_reply.header.msg_type, "list_subshell_reply");
    assert_eq!(
        list_reply.content.get("subshell_id"),
        Some(&json!([subshell_id.clone()]))
    );
    let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &list_request.header.msg_id, 2);
    assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

    let delete_request = client_request(
        "client-session",
        "delete_subshell_request",
        json!({"subshell_id": subshell_id}),
    );
    send_client_message(&control, &signer, &delete_request);
    let delete_reply = recv_message(&control, &signer);
    assert_eq!(delete_reply.header.msg_type, "delete_subshell_reply");
    assert_eq!(delete_reply.content.get("status"), Some(&json!("ok")));
    let statuses =
        recv_iopub_messages_for_parent(&iopub, &signer, &delete_request.header.msg_id, 2);
    assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

    runtime.stop().unwrap();
}

#[test]
fn control_channel_debug_request_returns_minimal_debug_info_reply() {
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
    control.set_rcvtimeo(10_000).unwrap();
    let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

    let request = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 1,
            "type": "request",
            "command": "debugInfo",
        }),
    );
    send_client_message(&control, &signer, &request);

    let reply = recv_message(&control, &signer);
    assert_eq!(reply.header.msg_type, "debug_reply");
    assert_eq!(reply.content.get("type"), Some(&json!("response")));
    assert_eq!(reply.content.get("request_seq"), Some(&json!(1)));
    assert_eq!(reply.content.get("success"), Some(&json!(true)));
    assert_eq!(reply.content.get("command"), Some(&json!("debugInfo")));
    assert_eq!(
        reply.content.pointer("/body/isStarted"),
        Some(&json!(false))
    );
    assert_eq!(reply.content.pointer("/body/breakpoints"), Some(&json!([])));
    assert_eq!(
        reply.content.pointer("/body/stoppedThreads"),
        Some(&json!([]))
    );
    assert_eq!(
        reply.content.pointer("/body/tmpFileSuffix"),
        Some(&json!(".py"))
    );

    let statuses = recv_iopub_messages_for_parent(&iopub, &signer, &request.header.msg_id, 2);
    assert_eq!(status_message_states(&statuses), vec!["busy", "idle"]);

    runtime.stop().unwrap();
}

#[test]
fn control_channel_debug_request_tracks_initialize_attach_and_breakpoints() {
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
    control.set_rcvtimeo(10_000).unwrap();

    let initialize = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 1,
            "type": "request",
            "command": "initialize",
            "arguments": {
                "clientID": "test-client",
                "clientName": "test-client",
                "adapterID": "",
            },
        }),
    );
    send_client_message(&control, &signer, &initialize);
    let initialize_reply = recv_message(&control, &signer);
    assert_eq!(initialize_reply.header.msg_type, "debug_reply");
    assert_eq!(initialize_reply.content.get("success"), Some(&json!(true)));
    assert_eq!(
        initialize_reply.content.get("command"),
        Some(&json!("initialize"))
    );

    let attach = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 2,
            "type": "request",
            "command": "attach",
            "arguments": {},
        }),
    );
    send_client_message(&control, &signer, &attach);
    let attach_reply = recv_message(&control, &signer);
    assert_eq!(attach_reply.content.get("success"), Some(&json!(true)));

    let dump_cell = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 3,
            "type": "request",
            "command": "dumpCell",
            "arguments": {
                "code": "def f():\n    return 42\nf()\n",
            },
        }),
    );
    send_client_message(&control, &signer, &dump_cell);
    let dump_cell_reply = recv_message(&control, &signer);
    let source_path = dump_cell_reply
        .content
        .pointer("/body/sourcePath")
        .and_then(Value::as_str)
        .expect("dumpCell sourcePath missing")
        .to_owned();
    assert!(source_path.ends_with(".py"));

    let set_breakpoints = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 4,
            "type": "request",
            "command": "setBreakpoints",
            "arguments": {
                "source": {"path": source_path},
                "breakpoints": [{"line": 2}],
                "sourceModified": false,
            },
        }),
    );
    send_client_message(&control, &signer, &set_breakpoints);
    let set_breakpoints_reply = recv_message(&control, &signer);
    assert_eq!(
        set_breakpoints_reply
            .content
            .pointer("/body/breakpoints/0/verified"),
        Some(&json!(true))
    );

    let debug_info = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 5,
            "type": "request",
            "command": "debugInfo",
        }),
    );
    send_client_message(&control, &signer, &debug_info);
    let debug_info_reply = recv_message(&control, &signer);
    assert_eq!(
        debug_info_reply.content.pointer("/body/isStarted"),
        Some(&json!(true))
    );
    assert_eq!(
        debug_info_reply
            .content
            .pointer("/body/breakpoints/0/source"),
        Some(&json!(source_path))
    );

    let disconnect = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 6,
            "type": "request",
            "command": "disconnect",
            "arguments": {
                "restart": false,
                "terminateDebuggee": true,
            },
        }),
    );
    send_client_message(&control, &signer, &disconnect);
    let disconnect_reply = recv_message(&control, &signer);
    assert_eq!(disconnect_reply.content.get("success"), Some(&json!(true)));

    runtime.stop().unwrap();
}

#[test]
fn execute_request_publishes_debug_event_and_exposes_stacktrace_variables() {
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
    control.set_rcvtimeo(10_000).unwrap();
    let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

    let code = "x = 1\ny = x + 1\nz = y + 1\n";

    let dump_cell = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 1,
            "type": "request",
            "command": "dumpCell",
            "arguments": {"code": code},
        }),
    );
    send_client_message(&control, &signer, &dump_cell);
    let dump_cell_reply = recv_message(&control, &signer);
    let source_path = dump_cell_reply
        .content
        .pointer("/body/sourcePath")
        .and_then(Value::as_str)
        .expect("dumpCell sourcePath missing")
        .to_owned();

    for request in [client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 2,
            "type": "request",
            "command": "setBreakpoints",
            "arguments": {
                "source": {"path": source_path},
                "breakpoints": [{"line": 2}],
                "sourceModified": false,
            },
        }),
    )] {
        send_client_message(&control, &signer, &request);
        let reply = recv_message(&control, &signer);
        assert_eq!(reply.header.msg_type, "debug_reply");
        assert_eq!(
            reply.content.get("success"),
            Some(&json!(true)),
            "unexpected debug reply: {:?}",
            reply.content
        );
    }

    let execute = client_request(
        "client-session",
        "execute_request",
        json!({
            "code": code,
            "silent": false,
            "store_history": true,
            "allow_stdin": false,
            "user_expressions": {},
            "stop_on_error": true,
        }),
    );
    send_client_message(&shell, &signer, &execute);
    let execute_reply = recv_message(&shell, &signer);
    assert_eq!(execute_reply.header.msg_type, "execute_reply");
    assert_eq!(execute_reply.content.get("status"), Some(&json!("ok")));

    let published =
        recv_iopub_messages_until_idle_for_parent(&iopub, &signer, &execute.header.msg_id);
    assert!(
        published.iter().any(|message| {
            message.header.msg_type == "debug_event"
                && message.content.get("event") == Some(&json!("stopped"))
        }),
        "expected stopped debug_event, got {:?}",
        published
            .iter()
            .map(|message| (
                message.header.msg_type.clone(),
                message.content.get("event").cloned(),
                message.content.get("execution_state").cloned(),
            ))
            .collect::<Vec<_>>()
    );
    let stopped_thread_id = published
        .iter()
        .find(|message| {
            message.header.msg_type == "debug_event"
                && message.content.get("event") == Some(&json!("stopped"))
        })
        .and_then(|message| message.content.pointer("/body/threadId"))
        .and_then(Value::as_i64)
        .expect("stopped debug_event threadId missing");

    let debug_info = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 30,
            "type": "request",
            "command": "debugInfo",
        }),
    );
    send_client_message(&control, &signer, &debug_info);
    let debug_info_reply = recv_message(&control, &signer);
    assert!(
        debug_info_reply
            .content
            .pointer("/body/stoppedThreads")
            .and_then(Value::as_array)
            .is_some_and(|threads| threads
                .iter()
                .any(|thread| thread == &json!(stopped_thread_id))),
        "debugInfo should reflect Rust-owned stoppedThreads: {:?}",
        debug_info_reply.content
    );

    let stack_trace = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 3,
            "type": "request",
            "command": "stackTrace",
            "arguments": {"threadId": 1},
        }),
    );
    send_client_message(&control, &signer, &stack_trace);
    let stack_trace_reply = recv_message(&control, &signer);
    let maybe_frame_id = stack_trace_reply
        .content
        .pointer("/body/stackFrames/0/id")
        .and_then(Value::as_i64);
    let frame_id = maybe_frame_id.expect("stackTrace frame id missing");
    assert_eq!(
        stack_trace_reply
            .content
            .pointer("/body/stackFrames/0/source/path"),
        Some(&json!(source_path))
    );
    assert_eq!(
        stack_trace_reply
            .content
            .pointer("/body/stackFrames/0/line"),
        Some(&json!(2))
    );

    let scopes = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 4,
            "type": "request",
            "command": "scopes",
            "arguments": {"frameId": frame_id},
        }),
    );
    send_client_message(&control, &signer, &scopes);
    let scopes_reply = recv_message(&control, &signer);
    let locals_ref = scopes_reply
        .content
        .pointer("/body/scopes/0/variablesReference")
        .and_then(Value::as_i64)
        .expect("locals variablesReference missing");

    let variables = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 5,
            "type": "request",
            "command": "variables",
            "arguments": {"variablesReference": locals_ref},
        }),
    );
    send_client_message(&control, &signer, &variables);
    let variables_reply = recv_message(&control, &signer);
    assert!(
        variables_reply
            .content
            .pointer("/body/variables")
            .and_then(Value::as_array)
            .is_some_and(|items| items.iter().any(|item| {
                item.get("name") == Some(&json!("x")) && item.get("value") == Some(&json!("1"))
            }))
    );

    runtime.stop().unwrap();
}

#[test]
fn live_debugpy_continue_next_stepin_stepout_work_end_to_end() {
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
    control.set_rcvtimeo(10_000).unwrap();
    let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

    let code = concat!(
        "def inner():\n",
        "    value = 1\n",
        "    value += 1\n",
        "    return value\n",
        "\n",
        "result = inner()\n",
        "result += 10\n",
        "result\n",
    );

    let dump_cell_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        1,
        "dumpCell",
        json!({ "code": code }),
    );
    let source_path = dump_cell_reply
        .content
        .pointer("/body/sourcePath")
        .and_then(Value::as_str)
        .expect("dumpCell sourcePath missing")
        .to_owned();

    for (seq, command, arguments) in [
        (
            2,
            "initialize",
            json!({
                "clientID": "rustykernel-test",
                "clientName": "rustykernel-test",
                "adapterID": "python",
            }),
        ),
        (3, "attach", json!({})),
        (
            4,
            "setBreakpoints",
            json!({
                "source": {"path": source_path},
                "breakpoints": [{"line": 6}],
                "sourceModified": false,
            }),
        ),
        (5, "configurationDone", json!({})),
    ] {
        let reply =
            send_debug_request_and_drain_iopub(&control, &iopub, &signer, seq, command, arguments);
        assert_eq!(
            reply.content.get("success"),
            Some(&json!(true)),
            "unexpected {command} reply: {:?}",
            reply.content
        );
    }

    let first_execute = client_request(
        "client-session",
        "execute_request",
        json!({
            "code": code,
            "silent": false,
            "store_history": true,
            "allow_stdin": false,
            "user_expressions": {},
            "stop_on_error": true,
        }),
    );
    send_client_message(&shell, &signer, &first_execute);
    let first_stop = recv_iopub_messages_until_parent_predicate(
        &iopub,
        &signer,
        &first_execute.header.msg_id,
        |message| {
            message.header.msg_type == "debug_event"
                && message.content.get("event") == Some(&json!("stopped"))
        },
    );
    let stopped_thread_id = debug_event_thread_id(&first_stop, "stopped");
    assert_stack_line(
        &control,
        &iopub,
        &signer,
        6,
        stopped_thread_id,
        &source_path,
        6,
    );

    let continue_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        7,
        "continue",
        json!({ "threadId": stopped_thread_id }),
    );
    assert_eq!(continue_reply.content.get("success"), Some(&json!(true)));
    let first_execute_reply = recv_message(&shell, &signer);
    assert_eq!(first_execute_reply.header.msg_type, "execute_reply");
    assert_eq!(
        first_execute_reply.content.get("status"),
        Some(&json!("ok"))
    );
    let first_execute_tail =
        recv_iopub_messages_until_idle_for_parent(&iopub, &signer, &first_execute.header.msg_id);
    assert!(
        first_execute_tail
            .iter()
            .any(|message| { message.header.msg_type == "execute_result" }),
        "expected execute_result after continue, got {:?}",
        first_execute_tail
            .iter()
            .map(|message| (
                message.header.msg_type.clone(),
                message.content.get("event").cloned(),
                message.content.get("execution_state").cloned(),
            ))
            .collect::<Vec<_>>()
    );

    let second_execute = client_request(
        "client-session",
        "execute_request",
        json!({
            "code": code,
            "silent": false,
            "store_history": true,
            "allow_stdin": false,
            "user_expressions": {},
            "stop_on_error": true,
        }),
    );
    send_client_message(&shell, &signer, &second_execute);
    let second_breakpoint_stop = recv_iopub_messages_until_parent_predicate(
        &iopub,
        &signer,
        &second_execute.header.msg_id,
        |message| {
            message.header.msg_type == "debug_event"
                && message.content.get("event") == Some(&json!("stopped"))
        },
    );
    let breakpoint_thread_id = debug_event_thread_id(&second_breakpoint_stop, "stopped");
    assert_stack_line(
        &control,
        &iopub,
        &signer,
        8,
        breakpoint_thread_id,
        &source_path,
        6,
    );
    assert_thread_visible(&control, &iopub, &signer, 9, breakpoint_thread_id);
    let _breakpoint_locals = top_frame_locals(&control, &iopub, &signer, 10, breakpoint_thread_id);

    let step_in_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        11,
        "stepIn",
        json!({ "threadId": breakpoint_thread_id }),
    );
    assert_eq!(step_in_reply.content.get("success"), Some(&json!(true)));
    let step_in_stop = recv_iopub_messages_until_parent_predicate(
        &iopub,
        &signer,
        &second_execute.header.msg_id,
        |message| {
            message.header.msg_type == "debug_event"
                && message.content.get("event") == Some(&json!("stopped"))
        },
    );
    let step_in_thread_id = debug_event_thread_id(&step_in_stop, "stopped");
    assert_stack_line(
        &control,
        &iopub,
        &signer,
        14,
        step_in_thread_id,
        &source_path,
        2,
    );
    assert_thread_visible(&control, &iopub, &signer, 15, step_in_thread_id);

    let next_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        16,
        "next",
        json!({ "threadId": step_in_thread_id }),
    );
    assert_eq!(next_reply.content.get("success"), Some(&json!(true)));
    let next_stop = recv_iopub_messages_until_parent_predicate(
        &iopub,
        &signer,
        &second_execute.header.msg_id,
        |message| {
            message.header.msg_type == "debug_event"
                && message.content.get("event") == Some(&json!("stopped"))
        },
    );
    let next_thread_id = debug_event_thread_id(&next_stop, "stopped");
    assert_stack_line(
        &control,
        &iopub,
        &signer,
        17,
        next_thread_id,
        &source_path,
        3,
    );
    let next_locals = top_frame_locals(&control, &iopub, &signer, 18, next_thread_id);
    assert_eq!(next_locals.get("value"), Some(&json!("1")));

    let step_out_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        19,
        "stepOut",
        json!({ "threadId": next_thread_id }),
    );
    assert_eq!(step_out_reply.content.get("success"), Some(&json!(true)));
    let step_out_stop = recv_iopub_messages_until_parent_predicate(
        &iopub,
        &signer,
        &second_execute.header.msg_id,
        |message| {
            message.header.msg_type == "debug_event"
                && message.content.get("event") == Some(&json!("stopped"))
        },
    );
    let step_out_thread_id = debug_event_thread_id(&step_out_stop, "stopped");
    assert_stack_line(
        &control,
        &iopub,
        &signer,
        20,
        step_out_thread_id,
        &source_path,
        6,
    );

    let second_continue_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        21,
        "continue",
        json!({ "threadId": step_out_thread_id }),
    );
    assert_eq!(
        second_continue_reply.content.get("success"),
        Some(&json!(true))
    );
    let second_execute_reply = recv_message(&shell, &signer);
    assert_eq!(second_execute_reply.header.msg_type, "execute_reply");
    assert_eq!(
        second_execute_reply.content.get("status"),
        Some(&json!("ok"))
    );
    let second_execute_tail =
        recv_iopub_messages_until_idle_for_parent(&iopub, &signer, &second_execute.header.msg_id);
    assert!(
        second_execute_tail
            .iter()
            .any(|message| { message.header.msg_type == "execute_result" }),
        "expected final execute_result, got {:?}",
        second_execute_tail
            .iter()
            .map(|message| (message.header.msg_type.clone(), message.content.clone()))
            .collect::<Vec<_>>()
    );

    let disconnect_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        22,
        "disconnect",
        json!({
            "restart": false,
            "terminateDebuggee": true,
        }),
    );
    assert_eq!(disconnect_reply.content.get("success"), Some(&json!(true)));

    runtime.stop().unwrap();
}

#[test]
fn live_debugpy_pause_request_stops_and_resumes_running_code() {
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
    control.set_rcvtimeo(10_000).unwrap();
    let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

    let code = concat!(
        "def runner():\n",
        "    import time\n",
        "    total = 0\n",
        "    for _ in range(200):\n",
        "        total += 1\n",
        "        time.sleep(0.01)\n",
        "    return total\n",
        "\n",
        "result = runner()\n",
        "result\n",
    );

    let dump_cell_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        1,
        "dumpCell",
        json!({ "code": code }),
    );
    let source_path = dump_cell_reply
        .content
        .pointer("/body/sourcePath")
        .and_then(Value::as_str)
        .expect("dumpCell sourcePath missing")
        .to_owned();

    for (seq, command, arguments) in [
        (
            2,
            "initialize",
            json!({
                "clientID": "rustykernel-test",
                "clientName": "rustykernel-test",
                "adapterID": "python",
            }),
        ),
        (3, "attach", json!({})),
        (
            4,
            "setBreakpoints",
            json!({
                "source": {"path": source_path},
                "breakpoints": [{"line": 2}],
                "sourceModified": false,
            }),
        ),
        (5, "configurationDone", json!({})),
    ] {
        let reply =
            send_debug_request_and_drain_iopub(&control, &iopub, &signer, seq, command, arguments);
        assert_eq!(
            reply.content.get("success"),
            Some(&json!(true)),
            "unexpected {command} reply: {:?}",
            reply.content
        );
    }

    let execute = client_request(
        "client-session",
        "execute_request",
        json!({
            "code": code,
            "silent": false,
            "store_history": true,
            "allow_stdin": false,
            "user_expressions": {},
            "stop_on_error": true,
        }),
    );
    send_client_message(&shell, &signer, &execute);
    let breakpoint_stop = recv_iopub_messages_until_parent_predicate(
        &iopub,
        &signer,
        &execute.header.msg_id,
        |message| {
            message.header.msg_type == "debug_event"
                && message.content.get("event") == Some(&json!("stopped"))
        },
    );
    let thread_id = debug_event_thread_id(&breakpoint_stop, "stopped");
    assert_stack_line(&control, &iopub, &signer, 6, thread_id, &source_path, 2);

    let continue_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        7,
        "continue",
        json!({ "threadId": thread_id }),
    );
    assert_eq!(continue_reply.content.get("success"), Some(&json!(true)));

    std::thread::sleep(Duration::from_millis(150));

    let pause_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        8,
        "pause",
        json!({ "threadId": thread_id }),
    );
    assert_eq!(pause_reply.content.get("success"), Some(&json!(true)));
    let pause_stop = recv_iopub_messages_until_parent_predicate(
        &iopub,
        &signer,
        &execute.header.msg_id,
        |message| {
            message.header.msg_type == "debug_event"
                && message.content.get("event") == Some(&json!("stopped"))
                && message.content.pointer("/body/reason") == Some(&json!("pause"))
        },
    );
    let paused_thread_id = debug_event_thread_id(&pause_stop, "stopped");
    assert_eq!(paused_thread_id, thread_id);
    let pause_stack_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        9,
        "stackTrace",
        json!({ "threadId": paused_thread_id }),
    );
    assert_eq!(pause_stack_reply.content.get("success"), Some(&json!(true)));
    assert_eq!(
        pause_stack_reply
            .content
            .pointer("/body/stackFrames/0/source/path"),
        Some(&json!(source_path))
    );
    assert!(
        pause_stack_reply
            .content
            .pointer("/body/stackFrames/0/line")
            .and_then(Value::as_i64)
            .is_some_and(|line| (4..=6).contains(&line)),
        "unexpected pause stack: {:?}",
        pause_stack_reply.content
    );

    let resume_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        10,
        "continue",
        json!({ "threadId": paused_thread_id }),
    );
    assert_eq!(resume_reply.content.get("success"), Some(&json!(true)));
    let execute_reply = recv_message(&shell, &signer);
    assert_eq!(execute_reply.header.msg_type, "execute_reply");
    assert_eq!(execute_reply.content.get("status"), Some(&json!("ok")));
    let execute_tail =
        recv_iopub_messages_until_idle_for_parent(&iopub, &signer, &execute.header.msg_id);
    assert!(
        execute_tail
            .iter()
            .any(|message| message.header.msg_type == "execute_result"),
        "expected execute_result after pause/resume, got {:?}",
        execute_tail
            .iter()
            .map(|message| (message.header.msg_type.clone(), message.content.clone()))
            .collect::<Vec<_>>()
    );

    let disconnect_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        11,
        "disconnect",
        json!({
            "restart": false,
            "terminateDebuggee": true,
        }),
    );
    assert_eq!(disconnect_reply.content.get("success"), Some(&json!(true)));

    runtime.stop().unwrap();
}

#[test]
fn interrupt_request_stops_running_debug_session_in_subshell_without_leaking_debug_state() {
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
    control.set_rcvtimeo(10_000).unwrap();
    let iopub = connect_subscriber(&context, &runtime.channel_endpoints().iopub);

    let create_subshell = client_request("client-session", "create_subshell_request", json!({}));
    send_client_message(&control, &signer, &create_subshell);
    let create_reply = recv_message(&control, &signer);
    assert_eq!(create_reply.header.msg_type, "create_subshell_reply");
    let subshell_id = create_reply
        .content
        .get("subshell_id")
        .and_then(Value::as_str)
        .expect("subshell id missing")
        .to_owned();
    let create_statuses =
        recv_iopub_messages_for_parent(&iopub, &signer, &create_subshell.header.msg_id, 2);
    assert_eq!(
        status_message_states(&create_statuses),
        vec!["busy", "idle"]
    );

    let code = concat!(
        "def runner():\n",
        "    import time\n",
        "    total = 0\n",
        "    for _ in range(300):\n",
        "        total += 1\n",
        "        time.sleep(0.01)\n",
        "    return total\n",
        "\n",
        "runner()\n",
    );

    let dump_cell_reply = send_debug_request_and_drain_iopub(
        &control,
        &iopub,
        &signer,
        1,
        "dumpCell",
        json!({ "code": code }),
    );
    assert_eq!(dump_cell_reply.content.get("success"), Some(&json!(true)));

    initialize_live_debug_session(&control, &iopub, &signer, 2);

    let execute = subshell_execute_request(
        &subshell_id,
        json!({
            "code": code,
            "silent": false,
            "store_history": true,
            "allow_stdin": false,
            "user_expressions": {},
            "stop_on_error": true,
        }),
    );
    send_client_message(&shell, &signer, &execute);
    thread::sleep(Duration::from_millis(200));

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

    let execute_reply = recv_message(&shell, &signer);
    assert_eq!(execute_reply.header.msg_type, "execute_reply");
    assert_eq!(execute_reply.content.get("status"), Some(&json!("error")));
    assert_eq!(
        execute_reply.content.get("ename"),
        Some(&json!("KeyboardInterrupt"))
    );
    assert_eq!(
        execute_reply.parent_header.get("subshell_id"),
        Some(&json!(subshell_id.clone()))
    );
    let execute_tail =
        recv_iopub_messages_until_idle_for_parent(&iopub, &signer, &execute.header.msg_id);
    assert!(
        execute_tail.iter().any(|message| {
            message.header.msg_type == "error"
                && message.content.get("ename") == Some(&json!("KeyboardInterrupt"))
        }),
        "expected KeyboardInterrupt on iopub, got {:?}",
        execute_tail
            .iter()
            .map(|message| (message.header.msg_type.clone(), message.content.clone()))
            .collect::<Vec<_>>()
    );

    let probe = subshell_execute_request(
        &subshell_id,
        json!({
            "code": "40 + 2",
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
    assert_eq!(
        probe_reply.parent_header.get("subshell_id"),
        Some(&json!(subshell_id))
    );
    let probe_tail =
        recv_iopub_messages_until_idle_for_parent(&iopub, &signer, &probe.header.msg_id);
    assert!(
        probe_tail
            .iter()
            .any(|message| message.header.msg_type == "execute_result"),
        "expected execute_result after interrupt recovery, got {:?}",
        probe_tail
            .iter()
            .map(|message| (message.header.msg_type.clone(), message.content.clone()))
            .collect::<Vec<_>>()
    );
    assert!(
        !probe_tail
            .iter()
            .any(|message| message.header.msg_type == "debug_event"),
        "did not expect leaked debug_event on probe execute, got {:?}",
        probe_tail
            .iter()
            .map(|message| (message.header.msg_type.clone(), message.content.clone()))
            .collect::<Vec<_>>()
    );

    runtime.stop().unwrap();
}

#[test]
fn continue_request_without_live_debug_session_returns_error_instead_of_fallback_success() {
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
    control.set_rcvtimeo(10_000).unwrap();

    let continue_request = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 1,
            "type": "request",
            "command": "continue",
            "arguments": {"threadId": 1},
        }),
    );
    send_client_message(&control, &signer, &continue_request);
    let continue_reply = recv_message(&control, &signer);
    assert_eq!(continue_reply.header.msg_type, "debug_reply");
    assert_eq!(
        continue_reply.content.get("command"),
        Some(&json!("continue"))
    );
    assert_eq!(continue_reply.content.get("success"), Some(&json!(false)));

    runtime.stop().unwrap();
}

#[test]
fn pause_request_without_live_debug_session_returns_error_instead_of_fallback_success() {
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
    control.set_rcvtimeo(10_000).unwrap();

    let pause_request = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": 1,
            "type": "request",
            "command": "pause",
            "arguments": {"threadId": 1},
        }),
    );
    send_client_message(&control, &signer, &pause_request);
    let pause_reply = recv_message(&control, &signer);
    assert_eq!(pause_reply.header.msg_type, "debug_reply");
    assert_eq!(pause_reply.content.get("command"), Some(&json!("pause")));
    assert_eq!(pause_reply.content.get("success"), Some(&json!(false)));

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
    let define_statuses = recv_iopub_messages_for_parent(&iopub, &signer, &define.header.msg_id, 3);
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

fn subshell_execute_request(subshell_id: &str, content: Value) -> JupyterMessage {
    let mut request = client_request("client-session", "execute_request", content);
    request.header.subshell_id = Some(subshell_id.to_owned());
    request.header_value = serde_json::to_value(&request.header).unwrap();
    request
}

fn connect_dealer(context: &zmq::Context, endpoint: &str, identity: &[u8]) -> zmq::Socket {
    let socket = context.socket(zmq::SocketType::Dealer).unwrap();
    socket.set_identity(identity).unwrap();
    socket.set_rcvtimeo(2_000).unwrap();
    socket.set_sndtimeo(2_000).unwrap();
    socket.connect(endpoint).unwrap();
    thread::sleep(Duration::from_millis(100));
    socket
}

fn connect_subscriber(context: &zmq::Context, endpoint: &str) -> zmq::Socket {
    let socket = context.socket(zmq::SocketType::Sub).unwrap();
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

fn try_recv_message(
    socket: &zmq::Socket,
    signer: &MessageSigner,
) -> Result<Option<JupyterMessage>, ZmqError> {
    match socket.recv_multipart(0) {
        Ok(frames) => Ok(Some(signer.decode(frames).unwrap())),
        Err(ZmqError::NoMessage) => Ok(None),
        Err(error) => Err(error),
    }
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
        let Some(message) = try_recv_message(socket, signer).unwrap() else {
            continue;
        };
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

fn recv_iopub_messages_until_idle_for_parent(
    socket: &zmq::Socket,
    signer: &MessageSigner,
    parent_msg_id: &str,
) -> Vec<JupyterMessage> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut messages = Vec::new();

    loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for idle iopub message"
        );
        let Some(message) = try_recv_message(socket, signer).unwrap() else {
            continue;
        };
        let parent = message
            .parent_header
            .get("msg_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if parent != parent_msg_id {
            continue;
        }

        let is_idle = message.header.msg_type == "status"
            && message.content.get("execution_state") == Some(&json!("idle"));
        messages.push(message);
        if is_idle {
            return messages;
        }
    }
}

fn recv_iopub_messages_until_parent_predicate(
    socket: &zmq::Socket,
    signer: &MessageSigner,
    parent_msg_id: &str,
    predicate: impl Fn(&JupyterMessage) -> bool,
) -> Vec<JupyterMessage> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut messages = Vec::new();

    loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for matching iopub message"
        );
        let Some(message) = try_recv_message(socket, signer).unwrap() else {
            continue;
        };
        let parent = message
            .parent_header
            .get("msg_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if parent != parent_msg_id {
            continue;
        }

        let matched = predicate(&message);
        messages.push(message);
        if matched {
            return messages;
        }
    }
}

fn send_debug_request_and_drain_iopub(
    control: &zmq::Socket,
    iopub: &zmq::Socket,
    signer: &MessageSigner,
    seq: i64,
    command: &str,
    arguments: Value,
) -> JupyterMessage {
    let request = client_request(
        "client-session",
        "debug_request",
        json!({
            "seq": seq,
            "type": "request",
            "command": command,
            "arguments": arguments,
        }),
    );
    send_client_message(control, signer, &request);
    let reply = recv_message(control, signer);
    let _ = recv_iopub_messages_until_idle_for_parent(iopub, signer, &request.header.msg_id);
    reply
}

fn initialize_live_debug_session(
    control: &zmq::Socket,
    iopub: &zmq::Socket,
    signer: &MessageSigner,
    initial_seq: i64,
) {
    for (offset, command, arguments) in [
        (
            0,
            "initialize",
            json!({
                "clientID": "rustykernel-test",
                "clientName": "rustykernel-test",
                "adapterID": "python",
            }),
        ),
        (1, "attach", json!({})),
        (2, "configurationDone", json!({})),
    ] {
        let reply = send_debug_request_and_drain_iopub(
            control,
            iopub,
            signer,
            initial_seq + offset,
            command,
            arguments,
        );
        assert_eq!(
            reply.content.get("success"),
            Some(&json!(true)),
            "unexpected {command} reply: {:?}",
            reply.content
        );
    }
}

fn debug_event_thread_id(messages: &[JupyterMessage], event_name: &str) -> i64 {
    messages
        .iter()
        .find(|message| {
            message.header.msg_type == "debug_event"
                && message.content.get("event") == Some(&json!(event_name))
        })
        .and_then(|message| message.content.pointer("/body/threadId"))
        .and_then(Value::as_i64)
        .unwrap_or(1)
}

fn assert_stack_line(
    control: &zmq::Socket,
    iopub: &zmq::Socket,
    signer: &MessageSigner,
    seq: i64,
    thread_id: i64,
    source_path: &str,
    expected_line: i64,
) {
    let stack_trace_reply = send_debug_request_and_drain_iopub(
        control,
        iopub,
        signer,
        seq,
        "stackTrace",
        json!({ "threadId": thread_id }),
    );
    assert_eq!(stack_trace_reply.content.get("success"), Some(&json!(true)));
    assert_eq!(
        stack_trace_reply
            .content
            .pointer("/body/stackFrames/0/source/path"),
        Some(&json!(source_path))
    );
    assert_eq!(
        stack_trace_reply
            .content
            .pointer("/body/stackFrames/0/line"),
        Some(&json!(expected_line))
    );
}

fn assert_thread_visible(
    control: &zmq::Socket,
    iopub: &zmq::Socket,
    signer: &MessageSigner,
    seq: i64,
    thread_id: i64,
) {
    let threads_reply =
        send_debug_request_and_drain_iopub(control, iopub, signer, seq, "threads", json!({}));
    assert_eq!(threads_reply.content.get("success"), Some(&json!(true)));
    assert!(
        threads_reply
            .content
            .pointer("/body/threads")
            .and_then(Value::as_array)
            .is_some_and(|threads| threads
                .iter()
                .any(|thread| thread.get("id") == Some(&json!(thread_id)))),
        "expected thread {thread_id} in threads reply: {:?}",
        threads_reply.content
    );
}

fn top_frame_locals(
    control: &zmq::Socket,
    iopub: &zmq::Socket,
    signer: &MessageSigner,
    seq: i64,
    thread_id: i64,
) -> serde_json::Map<String, Value> {
    let stack_trace_reply = send_debug_request_and_drain_iopub(
        control,
        iopub,
        signer,
        seq,
        "stackTrace",
        json!({ "threadId": thread_id }),
    );
    let frame_id = stack_trace_reply
        .content
        .pointer("/body/stackFrames/0/id")
        .and_then(Value::as_i64)
        .expect("stackTrace frame id missing");

    let scopes_reply = send_debug_request_and_drain_iopub(
        control,
        iopub,
        signer,
        seq + 1,
        "scopes",
        json!({ "frameId": frame_id }),
    );
    let locals_ref = scopes_reply
        .content
        .pointer("/body/scopes/0/variablesReference")
        .and_then(Value::as_i64)
        .expect("locals variablesReference missing");

    let variables_reply = send_debug_request_and_drain_iopub(
        control,
        iopub,
        signer,
        seq + 2,
        "variables",
        json!({ "variablesReference": locals_ref }),
    );
    variables_reply
        .content
        .pointer("/body/variables")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| {
            Some((
                item.get("name")?.as_str()?.to_owned(),
                item.get("value")?.clone(),
            ))
        })
        .collect()
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
