use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicI64, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Notify;

use crate::kernel::KernelError;

use super::types::{DebugEventEnvelope, DebugListenEndpoint, DebugTransport};

#[derive(Debug)]
pub struct DapClient {
    transport: Mutex<DebugTransport>,
    responses: Arc<Mutex<HashMap<i64, mpsc::Sender<Value>>>>,
    event_tx: mpsc::Sender<DebugEventEnvelope>,
    event_rx: Mutex<mpsc::Receiver<DebugEventEnvelope>>,
    notifier: Arc<Notify>,
    writer: Mutex<Option<TcpStream>>,
    initialized_seen: Arc<AtomicBool>,
    next_seq: AtomicI64,
}

impl DapClient {
    pub fn new() -> Self {
        let (event_tx, event_rx) = mpsc::channel();
        Self {
            transport: Mutex::new(DebugTransport::Inactive),
            responses: Arc::new(Mutex::new(HashMap::new())),
            event_tx,
            event_rx: Mutex::new(event_rx),
            notifier: Arc::new(Notify::new()),
            writer: Mutex::new(None),
            initialized_seen: Arc::new(AtomicBool::new(false)),
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

    pub fn connect(
        &self,
        endpoint: DebugListenEndpoint,
        debug_epoch: u64,
    ) -> Result<(), KernelError> {
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
        self.initialized_seen.store(false, Ordering::SeqCst);
        self.set_listening(endpoint)?;
        self.set_connected()?;
        self.spawn_recv_loop(reader, debug_epoch);
        Ok(())
    }

    pub fn reset(&self) -> Result<(), KernelError> {
        if let Ok(mut writer) = self.writer.lock() {
            *writer = None;
        }
        if let Ok(mut transport) = self.transport.lock() {
            *transport = DebugTransport::Inactive;
        }
        if let Ok(mut responses) = self.responses.lock() {
            responses.clear();
        }
        self.initialized_seen.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn spawn_recv_loop(&self, mut reader: TcpStream, debug_epoch: u64) {
        let responses = Arc::clone(&self.responses);
        let event_tx = self.event_tx.clone();
        let notifier = Arc::clone(&self.notifier);
        let initialized_seen = Arc::clone(&self.initialized_seen);
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
                            if message.get("event").and_then(Value::as_str) == Some("initialized") {
                                initialized_seen.store(true, Ordering::SeqCst);
                            }
                            let _ = event_tx.send(DebugEventEnvelope {
                                debug_epoch,
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
            if self.initialized_seen.load(Ordering::SeqCst) {
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn push_event(
        &self,
        event: Value,
        parent_header: Option<Value>,
    ) -> Result<(), KernelError> {
        self.event_tx
            .send(DebugEventEnvelope {
                debug_epoch: 0,
                event,
                parent_header,
            })
            .map_err(|error| {
                KernelError::Worker(format!("failed to queue debug event: {error}"))
            })?;
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

impl Default for DapClient {
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
    use std::sync::mpsc;
    use std::time::Duration;

    use serde_json::json;

    use super::DapClient;
    use crate::debug::{DebugListenEndpoint, DebugTransport};
    use crate::kernel::execute::KernelEventSender;
    use crate::worker::PythonWorker;

    #[test]
    fn dap_client_tracks_transport_and_response_slots() {
        let session = DapClient::new();
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
    fn dap_client_can_connect_and_initialize_against_worker_debugpy() {
        let (tx, _rx) = mpsc::channel();
        let mut worker =
            PythonWorker::start(KernelEventSender::new(tx), 1).expect("worker should start");
        let endpoint = worker.debug_listen().expect("debug_listen should succeed");
        if !endpoint.available {
            return;
        }

        let session = DapClient::new();
        session
            .connect(
                DebugListenEndpoint {
                    host: endpoint.host,
                    port: endpoint.port,
                },
                1,
            )
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
    fn dap_client_can_attach_and_configure_after_initialize_against_worker_debugpy() {
        let (tx, _rx) = mpsc::channel();
        let mut worker =
            PythonWorker::start(KernelEventSender::new(tx), 1).expect("worker should start");
        let endpoint = worker.debug_listen().expect("debug_listen should succeed");
        if !endpoint.available {
            return;
        }

        let session = DapClient::new();
        session
            .connect(
                DebugListenEndpoint {
                    host: endpoint.host.clone(),
                    port: endpoint.port,
                },
                1,
            )
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
