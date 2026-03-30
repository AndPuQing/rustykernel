use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use crate::kernel::KernelError;
use crate::worker::{PythonWorker, WorkerDebugListen};
use crate::worker_protocol::WorkerDebugEvent;

use super::{DapClient, DebugCache, DebugEventEnvelope, DebugListenEndpoint, DebugTransport};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DebugCommandKind {
    Bootstrap,
    Breakpoints,
    Introspection,
    Execution,
    Info,
    Passthrough,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FallbackKind {
    Snapshot,
    Compat,
}

pub struct DebugBridge {
    epoch: u64,
    dap: DapClient,
    cache: DebugCache,
}

impl DebugBridge {
    pub fn new() -> Self {
        Self {
            epoch: 1,
            dap: DapClient::new(),
            cache: DebugCache::default(),
        }
    }

    pub fn notifier(&self) -> Arc<tokio::sync::Notify> {
        self.dap.notifier()
    }

    pub fn is_connected(&self) -> Result<bool, KernelError> {
        Ok(matches!(
            self.dap.transport()?,
            DebugTransport::Connected(_)
        ))
    }

    pub fn handle_request(
        &mut self,
        worker: &mut PythonWorker,
        request: &Value,
    ) -> Result<Value, KernelError> {
        let command = request
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let arguments = request
            .get("arguments")
            .and_then(Value::as_object)
            .cloned()
            .map(Value::Object)
            .unwrap_or_else(|| json!({}));
        let request_seq = request.get("seq").and_then(Value::as_i64).unwrap_or(0);

        match Self::classify_command(command) {
            DebugCommandKind::Bootstrap => match command {
                "initialize" => self.handle_initialize(worker, request_seq, arguments.clone()),
                "attach" => self.handle_attach(worker, request_seq, arguments.clone()),
                "configurationDone" => {
                    self.handle_configuration_done(worker, request_seq, arguments)
                }
                "disconnect" => self.handle_disconnect(worker, request_seq, arguments.clone()),
                _ => Ok(self.debug_error_reply(
                    command,
                    request_seq,
                    format!("unsupported bootstrap debug command: {command}"),
                )),
            },
            DebugCommandKind::Breakpoints => {
                self.handle_set_breakpoints(worker, request_seq, request, arguments)
            }
            DebugCommandKind::Introspection | DebugCommandKind::Execution => {
                self.handle_passthrough(worker, command, request_seq, arguments)
            }
            DebugCommandKind::Info => {
                let reply = worker.debug_request(request)?;
                self.overlay_debug_info(reply)
            }
            DebugCommandKind::Passthrough => worker.debug_request(request),
        }
    }

    pub fn next_epoch(&mut self) -> Result<(), KernelError> {
        self.epoch += 1;
        self.dap.reset()?;
        self.cache = DebugCache::default();
        Ok(())
    }

    pub fn on_restart(&mut self) -> Result<(), KernelError> {
        self.next_epoch()
    }

    pub fn on_subshell_interrupt(&mut self) -> Result<(), KernelError> {
        self.next_epoch()
    }

    pub fn drain_event(&mut self) -> Result<Option<DebugEventEnvelope>, KernelError> {
        while let Some(event) = self.dap.try_recv_event()? {
            if let Some(event) = self.accept_session_event(event)? {
                return Ok(Some(event));
            }
        }
        Ok(None)
    }

    pub fn accept_worker_event(
        &mut self,
        event: &WorkerDebugEvent,
    ) -> Result<Option<Value>, KernelError> {
        let rust_session_connected = self.is_connected()?;
        let is_synthetic_stop = Self::is_synthetic_stop_event(&event.content);

        if rust_session_connected && !is_synthetic_stop {
            return Ok(None);
        }
        if rust_session_connected || is_synthetic_stop {
            self.cache.update_from_event(&event.content);
        }
        Ok(Some(event.content.clone()))
    }

    fn classify_command(command: &str) -> DebugCommandKind {
        match command {
            "initialize" | "attach" | "configurationDone" | "disconnect" => {
                DebugCommandKind::Bootstrap
            }
            "setBreakpoints" => DebugCommandKind::Breakpoints,
            "threads" | "stackTrace" | "scopes" | "variables" => DebugCommandKind::Introspection,
            "continue" | "next" | "stepIn" | "stepOut" | "pause" => DebugCommandKind::Execution,
            "debugInfo" => DebugCommandKind::Info,
            _ => DebugCommandKind::Passthrough,
        }
    }

    fn fallback_kind(command: &str) -> Option<FallbackKind> {
        match command {
            "threads" | "stackTrace" | "scopes" | "variables" => Some(FallbackKind::Snapshot),
            _ => None,
        }
    }

    fn is_synthetic_stop_event(event: &Value) -> bool {
        event.get("event") == Some(&json!("stopped")) && event.get("seq") == Some(&json!(0))
    }

    fn accept_session_event(
        &mut self,
        event: DebugEventEnvelope,
    ) -> Result<Option<DebugEventEnvelope>, KernelError> {
        if event.debug_epoch != 0 && event.debug_epoch != self.epoch {
            return Ok(None);
        }
        self.cache.update_from_event(&event.event);
        if matches!(self.dap.transport()?, DebugTransport::Inactive)
            && event.parent_header.is_none()
        {
            return Ok(None);
        }
        Ok(Some(event))
    }

    fn ensure_session_endpoint(
        &mut self,
        worker: &mut PythonWorker,
    ) -> Result<DebugListenEndpoint, KernelError> {
        if let Some(endpoint) = self.dap.endpoint()? {
            return Ok(endpoint);
        }

        let WorkerDebugListen {
            available,
            host,
            port,
        } = worker.debug_listen()?;
        if !available {
            return Err(KernelError::Worker(
                "debugpy listener is not available in python worker".to_owned(),
            ));
        }
        let endpoint = DebugListenEndpoint { host, port };
        self.dap.set_listening(endpoint.clone())?;
        Ok(endpoint)
    }

    fn ensure_session_connected(
        &mut self,
        worker: &mut PythonWorker,
    ) -> Result<DebugListenEndpoint, KernelError> {
        let endpoint = self.ensure_session_endpoint(worker)?;
        if !self.is_connected()? {
            self.dap.connect(endpoint.clone(), self.epoch)?;
        }
        Ok(endpoint)
    }

    fn handle_initialize(
        &mut self,
        worker: &mut PythonWorker,
        request_seq: i64,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        self.ensure_session_connected(worker)?;
        let reply = match self
            .dap
            .send_request("initialize", arguments, Duration::from_secs(5))
        {
            Ok(reply) => reply,
            Err(error) => {
                return Ok(self.debug_error_reply("initialize", request_seq, error.to_string()));
            }
        };
        if let Some(body) = reply.get("body").cloned() {
            self.cache.record_capabilities(body);
        }
        Ok(self.rewrite_debug_command(reply, "initialize", request_seq))
    }

    fn handle_attach(
        &mut self,
        worker: &mut PythonWorker,
        request_seq: i64,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        let endpoint = self.ensure_session_connected(worker)?;
        let mut arguments_obj = arguments.as_object().cloned().unwrap_or_default();
        arguments_obj.insert(
            "connect".to_owned(),
            json!({
                "host": endpoint.host,
                "port": endpoint.port,
            }),
        );
        arguments_obj
            .entry("logToFile".to_owned())
            .or_insert_with(|| json!(false));
        let reply = match self
            .dap
            .send_attach_request(Value::Object(arguments_obj), Duration::from_secs(5))
        {
            Ok(reply) => reply,
            Err(error) => {
                return Ok(self.debug_error_reply("attach", request_seq, error.to_string()));
            }
        };
        self.cache.update_from_response("attach", &reply);
        Ok(self.rewrite_debug_command(reply, "attach", request_seq))
    }

    fn handle_configuration_done(
        &mut self,
        worker: &mut PythonWorker,
        request_seq: i64,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        self.ensure_session_connected(worker)?;
        let reply =
            match self
                .dap
                .send_request("configurationDone", arguments, Duration::from_secs(5))
            {
                Ok(reply) => reply,
                Err(error) => {
                    return Ok(self.debug_error_reply(
                        "configurationDone",
                        request_seq,
                        error.to_string(),
                    ));
                }
            };
        self.cache.update_from_response("configurationDone", &reply);
        Ok(self.rewrite_debug_command(reply, "configurationDone", request_seq))
    }

    fn handle_disconnect(
        &mut self,
        worker: &mut PythonWorker,
        request_seq: i64,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        self.ensure_session_connected(worker)?;
        let reply = self
            .dap
            .send_request("disconnect", arguments, Duration::from_secs(5))
            .map(|reply| self.rewrite_debug_command(reply, "disconnect", request_seq))
            .or_else(|error| {
                Ok(self.debug_error_reply("disconnect", request_seq, error.to_string()))
            });
        self.dap.reset()?;
        reply
    }

    fn handle_set_breakpoints(
        &mut self,
        worker: &mut PythonWorker,
        request_seq: i64,
        request: &Value,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        let worker_reply = worker.debug_request(request)?;
        let state = self.cache.clone();
        if !state.initialized && !state.attached {
            return Ok(worker_reply);
        }
        self.ensure_session_connected(worker)?;
        let reply = match self
            .dap
            .send_request("setBreakpoints", arguments, Duration::from_secs(5))
        {
            Ok(reply) => reply,
            Err(error) => {
                return Ok(self.debug_error_reply(
                    "setBreakpoints",
                    request_seq,
                    error.to_string(),
                ));
            }
        };
        Ok(self.rewrite_debug_command(reply, "setBreakpoints", request_seq))
    }

    fn handle_passthrough(
        &mut self,
        worker: &mut PythonWorker,
        command: &str,
        request_seq: i64,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        self.ensure_session_connected(worker)?;
        let reply = match self
            .dap
            .send_request(command, arguments.clone(), Duration::from_secs(5))
        {
            Ok(reply) => reply,
            Err(error) => {
                return self.synthesize_or_error_reply(
                    command,
                    request_seq,
                    &arguments,
                    error.to_string(),
                );
            }
        };
        self.cache.update_from_response(command, &reply);
        let reply = self.rewrite_debug_command(reply, command, request_seq);
        if !reply
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return self.synthesize_or_error_reply(
                command,
                request_seq,
                &arguments,
                format!("debug session command {command} was not successful"),
            );
        }
        if self.reply_needs_snapshot_fallback(command, &reply) {
            return self.synthesize_or_error_reply(
                command,
                request_seq,
                &arguments,
                format!("debug session {command} returned an empty body"),
            );
        }
        Ok(reply)
    }

    fn reply_needs_snapshot_fallback(&self, command: &str, reply: &Value) -> bool {
        match command {
            "threads" => reply
                .pointer("/body/threads")
                .and_then(Value::as_array)
                .is_none_or(|threads| threads.is_empty()),
            "stackTrace" => reply
                .pointer("/body/stackFrames")
                .and_then(Value::as_array)
                .is_none_or(|frames| frames.is_empty()),
            "scopes" => reply
                .pointer("/body/scopes")
                .and_then(Value::as_array)
                .is_none_or(|scopes| scopes.is_empty()),
            "variables" => reply
                .pointer("/body/variables")
                .and_then(Value::as_array)
                .is_none_or(|variables| variables.is_empty()),
            _ => false,
        }
    }

    fn synthesize_fallback_reply(
        &self,
        kind: FallbackKind,
        command: &str,
        request_seq: i64,
        arguments: &Value,
        message: String,
    ) -> Result<Value, KernelError> {
        match kind {
            FallbackKind::Snapshot => self.snapshot_fallback_reply(command, request_seq, arguments),
            FallbackKind::Compat => Ok(self.debug_error_reply(command, request_seq, message)),
        }
    }

    fn snapshot_fallback_reply(
        &self,
        command: &str,
        request_seq: i64,
        arguments: &Value,
    ) -> Result<Value, KernelError> {
        match command {
            "threads" => self.synthesize_threads_reply(command, request_seq),
            "stackTrace" => self.synthesize_stack_trace_reply(command, request_seq),
            "scopes" => {
                let frame_id = arguments
                    .get("frameId")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                self.synthesize_scopes_reply(command, request_seq, frame_id)
            }
            "variables" => {
                let variables_reference = arguments
                    .get("variablesReference")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                self.synthesize_variables_reply(command, request_seq, variables_reference)
            }
            _ => Ok(self.debug_error_reply(
                command,
                request_seq,
                format!("snapshot fallback is not available for {command}"),
            )),
        }
    }

    fn synthesize_or_error_reply(
        &self,
        command: &str,
        request_seq: i64,
        arguments: &Value,
        message: String,
    ) -> Result<Value, KernelError> {
        match Self::fallback_kind(command) {
            Some(kind) => {
                self.synthesize_fallback_reply(kind, command, request_seq, arguments, message)
            }
            None => self.synthesize_fallback_reply(
                FallbackKind::Compat,
                command,
                request_seq,
                arguments,
                message,
            ),
        }
    }

    fn debug_error_reply(&self, command: &str, request_seq: i64, message: String) -> Value {
        json!({
            "type": "response",
            "request_seq": request_seq,
            "success": false,
            "command": command,
            "message": message,
            "body": {},
        })
    }

    fn rewrite_debug_command(&self, mut reply: Value, command: &str, request_seq: i64) -> Value {
        if let Some(object) = reply.as_object_mut() {
            object.insert("command".to_owned(), json!(command));
            object.insert("request_seq".to_owned(), json!(request_seq));
        }
        reply
    }

    fn overlay_debug_info(&self, mut reply: Value) -> Result<Value, KernelError> {
        let DebugCache {
            attached,
            stopped_threads,
            ..
        } = self.cache.clone();
        let rust_is_authoritative = !matches!(self.dap.transport()?, DebugTransport::Inactive);
        if let Some(body) = reply.get_mut("body").and_then(Value::as_object_mut) {
            if rust_is_authoritative {
                body.insert("isStarted".to_owned(), json!(attached));
            }
            body.insert(
                "stoppedThreads".to_owned(),
                Value::Array(stopped_threads.into_iter().map(Value::from).collect()),
            );
        }
        Ok(reply)
    }

    fn synthesize_threads_reply(
        &self,
        command: &str,
        request_seq: i64,
    ) -> Result<Value, KernelError> {
        let state = self.cache.clone();
        let mut threads = if !state.last_threads.is_empty() {
            state.last_threads
        } else {
            state
                .stopped_threads
                .into_iter()
                .map(|thread_id| {
                    json!({
                        "id": thread_id,
                        "name": format!("Thread {thread_id}"),
                    })
                })
                .collect()
        };
        threads.sort_by_key(|item| item.get("id").and_then(Value::as_i64).unwrap_or_default());
        Ok(json!({
            "type": "response",
            "request_seq": request_seq,
            "success": true,
            "command": command,
            "body": {
                "threads": threads,
            },
        }))
    }

    fn synthesize_stack_trace_reply(
        &self,
        command: &str,
        request_seq: i64,
    ) -> Result<Value, KernelError> {
        let state = self.cache.clone();
        let stack_frames = state.synthetic_stack_frames;
        Ok(json!({
            "type": "response",
            "request_seq": request_seq,
            "success": true,
            "command": command,
            "body": {
                "stackFrames": stack_frames.clone(),
                "totalFrames": stack_frames.len(),
            },
        }))
    }

    fn synthesize_scopes_reply(
        &self,
        command: &str,
        request_seq: i64,
        frame_id: i64,
    ) -> Result<Value, KernelError> {
        let state = self.cache.clone();
        let scopes = state
            .synthetic_scopes
            .get(&frame_id)
            .cloned()
            .unwrap_or_default();
        Ok(json!({
            "type": "response",
            "request_seq": request_seq,
            "success": true,
            "command": command,
            "body": {
                "scopes": scopes,
            },
        }))
    }

    fn synthesize_variables_reply(
        &self,
        command: &str,
        request_seq: i64,
        variables_reference: i64,
    ) -> Result<Value, KernelError> {
        let state = self.cache.clone();
        let variables = state
            .synthetic_variables
            .get(&variables_reference)
            .cloned()
            .unwrap_or_default();
        Ok(json!({
            "type": "response",
            "request_seq": request_seq,
            "success": true,
            "command": command,
            "body": {
                "variables": variables,
            },
        }))
    }
}

impl Default for DebugBridge {
    fn default() -> Self {
        Self::new()
    }
}
