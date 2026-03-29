#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI64, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Notify;

use crate::kernel::KernelError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DebugListenEndpoint {
    pub host: String,
    pub port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DebugTransport {
    Inactive,
    Listening(DebugListenEndpoint),
    Connected(DebugListenEndpoint),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DebugEventEnvelope {
    pub event: Value,
    pub parent_header: Option<Value>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DebugStateCache {
    pub initialized: bool,
    pub attached: bool,
    pub configured: bool,
    pub stopped_threads: HashSet<i64>,
    pub last_threads: Vec<Value>,
    pub last_stop_reason: Option<String>,
    pub all_threads_stopped: bool,
    pub capabilities: Option<Value>,
    pub source_parents: HashMap<String, Value>,
    pub synthetic_stack_frames: Vec<Value>,
    pub synthetic_scopes: HashMap<i64, Vec<Value>>,
    pub synthetic_variables: HashMap<i64, Vec<Value>>,
}

impl DebugStateCache {
    pub fn update_from_event(&mut self, event: &Value) {
        let event_name = event
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let body = event.get("body").and_then(Value::as_object);

        match event_name {
            "initialized" => {
                self.initialized = true;
                self.attached = true;
            }
            "stopped" => {
                let Some(body) = body else {
                    return;
                };
                let thread_id = body.get("threadId").and_then(Value::as_i64);
                let all_threads_stopped = body
                    .get("allThreadsStopped")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if all_threads_stopped {
                    self.stopped_threads.clear();
                }
                if let Some(thread_id) = thread_id {
                    self.stopped_threads.insert(thread_id);
                }
                self.last_stop_reason = body
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                self.all_threads_stopped = all_threads_stopped;
                self.clear_synthetic_snapshot();
                self.load_synthetic_snapshot(body);
            }
            "continued" => {
                let Some(body) = body else {
                    return;
                };
                let all_threads_continued = body
                    .get("allThreadsContinued")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if all_threads_continued {
                    self.stopped_threads.clear();
                } else if let Some(thread_id) = body.get("threadId").and_then(Value::as_i64) {
                    self.stopped_threads.remove(&thread_id);
                }
                self.last_stop_reason = None;
                self.all_threads_stopped = false;
                self.clear_synthetic_snapshot();
            }
            "terminated" | "exited" => {
                self.attached = false;
                self.configured = false;
                self.stopped_threads.clear();
                self.last_stop_reason = None;
                self.all_threads_stopped = false;
                self.clear_synthetic_snapshot();
            }
            "thread" => {
                if let Some(body) = body {
                    self.last_threads.push(Value::Object(body.clone()));
                }
            }
            _ => {}
        }
    }

    fn load_synthetic_snapshot(&mut self, body: &serde_json::Map<String, Value>) {
        let Some(snapshot) = body.get("rustykernel").and_then(Value::as_object) else {
            return;
        };

        self.synthetic_stack_frames = snapshot
            .get("stackFrames")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        self.synthetic_scopes = snapshot
            .get("scopesByFrame")
            .and_then(Value::as_object)
            .map(|frames| {
                frames
                    .iter()
                    .filter_map(|(frame_id, scopes)| {
                        Some((
                            frame_id.parse::<i64>().ok()?,
                            scopes.as_array().cloned().unwrap_or_default(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();

        self.synthetic_variables = snapshot
            .get("variablesByRef")
            .and_then(Value::as_object)
            .map(|refs| {
                refs.iter()
                    .filter_map(|(reference, variables)| {
                        Some((
                            reference.parse::<i64>().ok()?,
                            variables.as_array().cloned().unwrap_or_default(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
    }

    fn clear_synthetic_snapshot(&mut self) {
        self.synthetic_stack_frames.clear();
        self.synthetic_scopes.clear();
        self.synthetic_variables.clear();
    }
}

#[derive(Debug)]
pub struct DebugSession {
    transport: Mutex<DebugTransport>,
    state: Arc<Mutex<DebugStateCache>>,
    responses: Arc<Mutex<HashMap<i64, mpsc::Sender<Value>>>>,
    event_tx: mpsc::Sender<DebugEventEnvelope>,
    event_rx: Mutex<mpsc::Receiver<DebugEventEnvelope>>,
    notifier: Arc<Notify>,
    writer: Mutex<Option<TcpStream>>,
    next_seq: AtomicI64,
}

impl DebugSession {
    pub fn new() -> Self {
        let (event_tx, event_rx) = mpsc::channel();
        Self {
            transport: Mutex::new(DebugTransport::Inactive),
            state: Arc::new(Mutex::new(DebugStateCache::default())),
            responses: Arc::new(Mutex::new(HashMap::new())),
            event_tx,
            event_rx: Mutex::new(event_rx),
            notifier: Arc::new(Notify::new()),
            writer: Mutex::new(None),
            next_seq: AtomicI64::new(1),
        }
    }

    pub fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.notifier)
    }

    pub fn next_seq(&self) -> i64 {
        self.next_seq.fetch_add(1, Ordering::SeqCst)
    }

    pub fn transport(&self) -> Result<DebugTransport, KernelError> {
        self.transport
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| KernelError::Worker("debug session transport mutex poisoned".to_owned()))
    }

    pub fn endpoint(&self) -> Result<Option<DebugListenEndpoint>, KernelError> {
        Ok(match self.transport()? {
            DebugTransport::Inactive => None,
            DebugTransport::Listening(endpoint) | DebugTransport::Connected(endpoint) => {
                Some(endpoint)
            }
        })
    }

    pub fn set_listening(&self, endpoint: DebugListenEndpoint) -> Result<(), KernelError> {
        let mut transport = self.transport.lock().map_err(|_| {
            KernelError::Worker("debug session transport mutex poisoned".to_owned())
        })?;
        *transport = DebugTransport::Listening(endpoint);
        Ok(())
    }

    pub fn set_connected(&self) -> Result<(), KernelError> {
        let mut transport = self.transport.lock().map_err(|_| {
            KernelError::Worker("debug session transport mutex poisoned".to_owned())
        })?;
        *transport = match &*transport {
            DebugTransport::Listening(endpoint) | DebugTransport::Connected(endpoint) => {
                DebugTransport::Connected(endpoint.clone())
            }
            DebugTransport::Inactive => DebugTransport::Inactive,
        };
        Ok(())
    }

    pub fn connect(&self, endpoint: DebugListenEndpoint) -> Result<(), KernelError> {
        let stream =
            TcpStream::connect((endpoint.host.as_str(), endpoint.port)).map_err(KernelError::Io)?;
        stream.set_nodelay(true).map_err(KernelError::Io)?;
        let reader = stream.try_clone().map_err(KernelError::Io)?;
        {
            let mut writer = self.writer.lock().map_err(|_| {
                KernelError::Worker("debug session writer mutex poisoned".to_owned())
            })?;
            *writer = Some(stream);
        }
        self.set_listening(endpoint)?;
        self.set_connected()?;
        self.spawn_recv_loop(reader);
        Ok(())
    }

    pub fn reset(&self) -> Result<(), KernelError> {
        if let Ok(mut writer) = self.writer.lock() {
            *writer = None;
        }
        if let Ok(mut transport) = self.transport.lock() {
            *transport = DebugTransport::Inactive;
        }
        if let Ok(mut state) = self.state.lock() {
            *state = DebugStateCache::default();
        }
        if let Ok(mut responses) = self.responses.lock() {
            responses.clear();
        }
        Ok(())
    }

    fn spawn_recv_loop(&self, mut reader: TcpStream) {
        let responses = Arc::clone(&self.responses);
        let state = Arc::clone(&self.state);
        let event_tx = self.event_tx.clone();
        let notifier = Arc::clone(&self.notifier);
        thread::spawn(move || {
            let mut buffer = Vec::<u8>::new();
            let mut chunk = [0_u8; 4096];
            loop {
                let bytes_read = match reader.read(&mut chunk) {
                    Ok(0) => return,
                    Ok(bytes_read) => bytes_read,
                    Err(_) => return,
                };
                buffer.extend_from_slice(&chunk[..bytes_read]);
                while let Some(message) = try_parse_dap_message(&mut buffer) {
                    match message
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                    {
                        "response" => {
                            if let Some(request_seq) =
                                message.get("request_seq").and_then(Value::as_i64)
                            {
                                if let Ok(mut response_map) = responses.lock() {
                                    if let Some(sender) = response_map.remove(&request_seq) {
                                        let _ = sender.send(message);
                                    }
                                }
                            }
                        }
                        "event" => {
                            if let Ok(mut cache) = state.lock() {
                                cache.update_from_event(&message);
                            }
                            let _ = event_tx.send(DebugEventEnvelope {
                                event: message,
                                parent_header: None,
                            });
                            notifier.notify_one();
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    pub fn state_snapshot(&self) -> Result<DebugStateCache, KernelError> {
        self.state
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| KernelError::Worker("debug session state mutex poisoned".to_owned()))
    }

    pub fn record_capabilities(&self, capabilities: Value) -> Result<(), KernelError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| KernelError::Worker("debug session state mutex poisoned".to_owned()))?;
        state.capabilities = Some(capabilities);
        Ok(())
    }

    pub fn update_from_response(&self, command: &str, response: &Value) -> Result<(), KernelError> {
        if !response
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Ok(());
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| KernelError::Worker("debug session state mutex poisoned".to_owned()))?;
        match command {
            "attach" => {
                state.attached = true;
            }
            "configurationDone" => {
                state.configured = true;
            }
            "threads" => {
                if let Some(threads) = response.pointer("/body/threads").and_then(Value::as_array) {
                    state.last_threads = threads.clone();
                }
            }
            "continue" | "next" | "stepIn" | "stepOut" => {
                state.stopped_threads.clear();
                state.last_stop_reason = None;
                state.all_threads_stopped = false;
            }
            _ => {}
        }
        Ok(())
    }

    pub fn record_source_parent(
        &self,
        source_path: impl Into<String>,
        parent_header: Value,
    ) -> Result<(), KernelError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| KernelError::Worker("debug session state mutex poisoned".to_owned()))?;
        state
            .source_parents
            .insert(source_path.into(), parent_header);
        Ok(())
    }

    pub fn parent_for_source(&self, source_path: &str) -> Result<Option<Value>, KernelError> {
        let state = self
            .state
            .lock()
            .map_err(|_| KernelError::Worker("debug session state mutex poisoned".to_owned()))?;
        Ok(state.source_parents.get(source_path).cloned())
    }

    pub fn apply_event_state(&self, event: &Value) -> Result<(), KernelError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| KernelError::Worker("debug session state mutex poisoned".to_owned()))?;
        state.update_from_event(event);
        Ok(())
    }

    pub fn register_pending_response(
        &self,
        seq: i64,
    ) -> Result<mpsc::Receiver<Value>, KernelError> {
        let (tx, rx) = mpsc::channel();
        self.responses
            .lock()
            .map_err(|_| KernelError::Worker("debug session response mutex poisoned".to_owned()))?
            .insert(seq, tx);
        Ok(rx)
    }

    pub fn send_request(
        &self,
        command: &str,
        arguments: Value,
        timeout: Duration,
    ) -> Result<Value, KernelError> {
        let seq = self.next_seq();
        let rx = self.register_pending_response(seq)?;
        let message = serde_json::json!({
            "seq": seq,
            "type": "request",
            "command": command,
            "arguments": arguments,
        });
        self.send_raw_message(&message)?;
        rx.recv_timeout(timeout).map_err(|error| {
            KernelError::Worker(format!(
                "timed out waiting for debug session response to {command}: {error}"
            ))
        })
    }

    pub fn send_attach_request(
        &self,
        arguments: Value,
        timeout: Duration,
    ) -> Result<Value, KernelError> {
        let seq = self.next_seq();
        let message = serde_json::json!({
            "seq": seq,
            "type": "request",
            "command": "attach",
            "arguments": arguments,
        });
        self.send_raw_message(&message)?;
        self.wait_for_initialized(timeout)?;
        Ok(serde_json::json!({
            "type": "response",
            "request_seq": seq,
            "success": true,
            "command": "attach",
            "body": {},
        }))
    }

    pub fn send_raw_message(&self, message: &Value) -> Result<(), KernelError> {
        let payload = serde_json::to_vec(message).map_err(|error| {
            KernelError::Worker(format!("failed to encode debug session request: {error}"))
        })?;
        let frame = format!("Content-Length: {}\r\n\r\n", payload.len()).into_bytes();
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| KernelError::Worker("debug session writer mutex poisoned".to_owned()))?;
        let writer = writer
            .as_mut()
            .ok_or_else(|| KernelError::Worker("debug session is not connected".to_owned()))?;
        writer.write_all(&frame).map_err(KernelError::Io)?;
        writer.write_all(&payload).map_err(KernelError::Io)?;
        writer.flush().map_err(KernelError::Io)?;
        Ok(())
    }

    pub fn wait_for_initialized(&self, timeout: Duration) -> Result<(), KernelError> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.state_snapshot()?.initialized {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(KernelError::Worker(
                    "timed out waiting for debug session initialized event".to_owned(),
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn resolve_response(&self, request_seq: i64, response: Value) -> Result<bool, KernelError> {
        let sender = self
            .responses
            .lock()
            .map_err(|_| KernelError::Worker("debug session response mutex poisoned".to_owned()))?
            .remove(&request_seq);
        if let Some(sender) = sender {
            let _ = sender.send(response);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn push_event(
        &self,
        event: Value,
        parent_header: Option<Value>,
    ) -> Result<(), KernelError> {
        {
            let mut state = self.state.lock().map_err(|_| {
                KernelError::Worker("debug session state mutex poisoned".to_owned())
            })?;
            state.update_from_event(&event);
        }
        self.event_tx
            .send(DebugEventEnvelope {
                event,
                parent_header,
            })
            .map_err(|error| KernelError::Worker(format!("failed to queue debug event: {error}")))?;
        self.notifier.notify_one();
        Ok(())
    }

    pub fn try_recv_event(&self) -> Result<Option<DebugEventEnvelope>, KernelError> {
        let receiver = self
            .event_rx
            .lock()
            .map_err(|_| KernelError::Worker("debug session event mutex poisoned".to_owned()))?;
        match receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Err(KernelError::Worker(
                "debug session event channel disconnected".to_owned(),
            )),
        }
    }
}

impl Default for DebugSession {
    fn default() -> Self {
        Self::new()
    }
}

fn try_parse_dap_message(buffer: &mut Vec<u8>) -> Option<Value> {
    let marker = buffer.windows(4).position(|window| window == b"\r\n\r\n")?;
    let header = String::from_utf8_lossy(&buffer[..marker]);
    let content_length = header.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("Content-Length") {
            value.trim().parse::<usize>().ok()
        } else {
            None
        }
    })?;
    let body_start = marker + 4;
    if buffer.len() < body_start + content_length {
        return None;
    }
    let body = buffer[body_start..body_start + content_length].to_vec();
    buffer.drain(..body_start + content_length);
    serde_json::from_slice(&body).ok()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::{DebugListenEndpoint, DebugSession, DebugTransport};
    use crate::worker::PythonWorker;

    #[test]
    fn debug_session_updates_stop_state_from_events() {
        let session = DebugSession::new();
        session
            .push_event(
                json!({
                    "type": "event",
                    "event": "stopped",
                    "body": {"threadId": 7, "reason": "breakpoint", "allThreadsStopped": true}
                }),
                None,
            )
            .unwrap();

        let state = session.state_snapshot().unwrap();
        assert!(state.stopped_threads.contains(&7));
        assert_eq!(state.last_stop_reason.as_deref(), Some("breakpoint"));
        assert!(state.all_threads_stopped);

        session
            .push_event(
                json!({
                    "type": "event",
                    "event": "continued",
                    "body": {"threadId": 7, "allThreadsContinued": false}
                }),
                None,
            )
            .unwrap();
        let state = session.state_snapshot().unwrap();
        assert!(state.stopped_threads.is_empty());
        assert_eq!(state.last_stop_reason, None);
    }

    #[test]
    fn debug_session_loads_synthetic_snapshot_from_stopped_event() {
        let session = DebugSession::new();
        session
            .push_event(
                json!({
                    "type": "event",
                    "event": "stopped",
                    "body": {
                        "threadId": 1,
                        "reason": "breakpoint",
                        "allThreadsStopped": true,
                        "rustykernel": {
                            "stackFrames": [
                                {"id": 1, "name": "<module>", "line": 2, "column": 1}
                            ],
                            "scopesByFrame": {
                                "1": [
                                    {"name": "Locals", "variablesReference": 1000}
                                ]
                            },
                            "variablesByRef": {
                                "1000": [
                                    {"name": "x", "value": "1", "variablesReference": 0}
                                ]
                            }
                        }
                    }
                }),
                None,
            )
            .unwrap();

        let state = session.state_snapshot().unwrap();
        assert_eq!(state.synthetic_stack_frames.len(), 1);
        assert_eq!(state.synthetic_scopes.get(&1).map(Vec::len), Some(1));
        assert_eq!(state.synthetic_variables.get(&1000).map(Vec::len), Some(1));
    }

    #[test]
    fn debug_session_clears_synthetic_snapshot_when_new_stop_has_no_snapshot() {
        let session = DebugSession::new();
        session
            .push_event(
                json!({
                    "type": "event",
                    "event": "stopped",
                    "body": {
                        "threadId": 1,
                        "reason": "breakpoint",
                        "allThreadsStopped": true,
                        "rustykernel": {
                            "stackFrames": [
                                {"id": 1, "name": "<module>", "line": 6, "column": 1}
                            ],
                            "scopesByFrame": {
                                "1": [
                                    {"name": "Locals", "variablesReference": 1000}
                                ]
                            },
                            "variablesByRef": {
                                "1000": [
                                    {"name": "x", "value": "1", "variablesReference": 0}
                                ]
                            }
                        }
                    }
                }),
                None,
            )
            .unwrap();

        session
            .push_event(
                json!({
                    "type": "event",
                    "event": "stopped",
                    "body": {"threadId": 1, "reason": "step", "allThreadsStopped": true}
                }),
                None,
            )
            .unwrap();

        let state = session.state_snapshot().unwrap();
        assert!(state.synthetic_stack_frames.is_empty());
        assert!(state.synthetic_scopes.is_empty());
        assert!(state.synthetic_variables.is_empty());
    }

    #[test]
    fn debug_session_continue_and_step_responses_clear_stop_state() {
        let session = DebugSession::new();
        session
            .push_event(
                json!({
                    "type": "event",
                    "event": "stopped",
                    "body": {"threadId": 7, "reason": "breakpoint", "allThreadsStopped": true}
                }),
                None,
            )
            .unwrap();

        session
            .update_from_response(
                "continue",
                &json!({
                    "type": "response",
                    "success": true,
                    "body": {"allThreadsContinued": true}
                }),
            )
            .unwrap();
        let state = session.state_snapshot().unwrap();
        assert!(state.stopped_threads.is_empty());

        session
            .push_event(
                json!({
                    "type": "event",
                    "event": "stopped",
                    "body": {"threadId": 8, "reason": "step", "allThreadsStopped": false}
                }),
                None,
            )
            .unwrap();
        session
            .update_from_response(
                "stepIn",
                &json!({
                    "type": "response",
                    "success": true,
                    "body": {}
                }),
            )
            .unwrap();
        let state = session.state_snapshot().unwrap();
        assert!(state.stopped_threads.is_empty());
    }

    #[test]
    fn debug_session_tracks_transport_and_response_slots() {
        let session = DebugSession::new();
        session
            .set_listening(DebugListenEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 5678,
            })
            .unwrap();
        assert_eq!(
            session.transport().unwrap(),
            DebugTransport::Listening(DebugListenEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 5678,
            })
        );

        let rx = session.register_pending_response(42).unwrap();
        assert!(
            session
                .resolve_response(42, json!({"success": true}))
                .unwrap()
        );
        assert_eq!(rx.recv().unwrap(), json!({"success": true}));
    }

    #[test]
    fn debug_session_can_connect_and_initialize_against_worker_debugpy() {
        let mut worker = PythonWorker::start().expect("worker should start");
        let endpoint = worker.debug_listen().expect("debug_listen should succeed");
        if !endpoint.available {
            return;
        }

        let session = DebugSession::new();
        session
            .connect(DebugListenEndpoint {
                host: endpoint.host,
                port: endpoint.port,
            })
            .expect("debug session should connect");

        let reply = session
            .send_request(
                "initialize",
                json!({
                    "clientID": "rustykernel-test",
                    "clientName": "rustykernel-test",
                    "adapterID": "",
                }),
                Duration::from_secs(5),
            )
            .expect("initialize should succeed");
        assert_eq!(reply.get("type"), Some(&json!("response")));
        assert_eq!(reply.get("success"), Some(&json!(true)));
        assert_eq!(reply.get("command"), Some(&json!("initialize")));
    }

    #[test]
    fn debug_session_can_attach_and_configure_after_initialize_against_worker_debugpy() {
        let mut worker = PythonWorker::start().expect("worker should start");
        let endpoint = worker.debug_listen().expect("debug_listen should succeed");
        if !endpoint.available {
            return;
        }

        let session = DebugSession::new();
        session
            .connect(DebugListenEndpoint {
                host: endpoint.host.clone(),
                port: endpoint.port,
            })
            .expect("debug session should connect");

        session
            .send_request(
                "initialize",
                json!({
                    "clientID": "rustykernel-test",
                    "clientName": "rustykernel-test",
                    "adapterID": "",
                }),
                Duration::from_secs(5),
            )
            .expect("initialize should succeed");

        let attach = session
            .send_attach_request(
                json!({
                    "connect": {
                        "host": endpoint.host,
                        "port": endpoint.port,
                    },
                    "logToFile": false,
                }),
                Duration::from_secs(5),
            )
            .expect("attach should succeed");
        assert_eq!(attach.get("type"), Some(&json!("response")));
        assert_eq!(attach.get("success"), Some(&json!(true)));
        assert_eq!(attach.get("command"), Some(&json!("attach")));

        let configuration_done = session
            .send_request("configurationDone", json!({}), Duration::from_secs(5))
            .expect("configurationDone should succeed");
        assert_eq!(configuration_done.get("type"), Some(&json!("response")));
        assert_eq!(configuration_done.get("success"), Some(&json!(true)));
        assert_eq!(
            configuration_done.get("command"),
            Some(&json!("configurationDone"))
        );
    }
}
