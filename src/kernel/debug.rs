use std::time::Duration;

use serde_json::{Value, json};

use crate::debug_session::{DebugListenEndpoint, DebugStateCache};
use crate::worker::WorkerDebugListen;

use super::{KernelError, state::MessageLoopState};

impl MessageLoopState {
    pub(crate) fn debug_reply_content(&mut self, request: &Value) -> Result<Value, KernelError> {
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

        match command {
            "initialize" => self.handle_debug_initialize(request_seq, arguments.clone()),
            "attach" => self.handle_debug_attach(request_seq, arguments.clone()),
            "configurationDone" => self.handle_debug_configuration_done(request_seq, arguments),
            "disconnect" => self.handle_debug_disconnect(request_seq, arguments.clone()),
            "setBreakpoints" => self.handle_debug_set_breakpoints(request_seq, request, arguments),
            "threads" | "stackTrace" | "scopes" | "variables" | "continue" | "next" | "stepIn"
            | "stepOut" | "pause" => self.handle_debug_passthrough(command, request_seq, arguments),
            "debugInfo" => {
                let reply = self.with_worker(|worker| worker.debug_request(request))?;
                self.overlay_debug_info(reply)
            }
            _ => self.with_worker(|worker| worker.debug_request(request)),
        }
    }

    fn ensure_debug_session_endpoint(&mut self) -> Result<DebugListenEndpoint, KernelError> {
        if let Some(endpoint) = self.debug_session.endpoint()? {
            return Ok(endpoint);
        }

        let WorkerDebugListen {
            available,
            host,
            port,
        } = self.with_worker(|worker| worker.debug_listen())?;
        if !available {
            return Err(KernelError::Worker(
                "debugpy listener is not available in python worker".to_owned(),
            ));
        }
        let endpoint = DebugListenEndpoint { host, port };
        self.debug_session.set_listening(endpoint.clone())?;
        Ok(endpoint)
    }

    fn ensure_debug_session_connected(&mut self) -> Result<DebugListenEndpoint, KernelError> {
        let endpoint = self.ensure_debug_session_endpoint()?;
        if !matches!(
            self.debug_session.transport()?,
            crate::debug_session::DebugTransport::Connected(_)
        ) {
            self.debug_session.connect(endpoint.clone())?;
        }
        Ok(endpoint)
    }

    fn handle_debug_initialize(
        &mut self,
        request_seq: i64,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        self.ensure_debug_session_connected()?;
        let reply =
            match self
                .debug_session
                .send_request("initialize", arguments, Duration::from_secs(5))
            {
                Ok(reply) => reply,
                Err(error) => {
                    return Ok(self.debug_error_reply(
                        "initialize",
                        request_seq,
                        error.to_string(),
                    ));
                }
            };
        if let Some(body) = reply.get("body").cloned() {
            self.debug_session.record_capabilities(body)?;
        }
        Ok(self.rewrite_debug_command(reply, "initialize", request_seq))
    }

    fn handle_debug_attach(
        &mut self,
        request_seq: i64,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        let endpoint = self.ensure_debug_session_connected()?;
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
            .debug_session
            .send_attach_request(Value::Object(arguments_obj), Duration::from_secs(5))
        {
            Ok(reply) => reply,
            Err(error) => {
                return Ok(self.debug_error_reply("attach", request_seq, error.to_string()));
            }
        };
        self.debug_session.update_from_response("attach", &reply)?;
        Ok(self.rewrite_debug_command(reply, "attach", request_seq))
    }

    fn handle_debug_configuration_done(
        &mut self,
        request_seq: i64,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        self.ensure_debug_session_connected()?;
        let reply = match self.debug_session.send_request(
            "configurationDone",
            arguments,
            Duration::from_secs(5),
        ) {
            Ok(reply) => reply,
            Err(error) => {
                return Ok(self.debug_error_reply(
                    "configurationDone",
                    request_seq,
                    error.to_string(),
                ));
            }
        };
        self.debug_session
            .update_from_response("configurationDone", &reply)?;
        Ok(self.rewrite_debug_command(reply, "configurationDone", request_seq))
    }

    fn handle_debug_disconnect(
        &mut self,
        request_seq: i64,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        self.ensure_debug_session_connected()?;
        let reply = self
            .debug_session
            .send_request("disconnect", arguments, Duration::from_secs(5))
            .map(|reply| self.rewrite_debug_command(reply, "disconnect", request_seq))
            .or_else(|error| {
                Ok(self.debug_error_reply("disconnect", request_seq, error.to_string()))
            });
        self.debug_session.reset()?;
        reply
    }

    fn handle_debug_set_breakpoints(
        &mut self,
        request_seq: i64,
        request: &Value,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        let _ = self.with_worker(|worker| worker.debug_request(request))?;
        let state = self.debug_session.state_snapshot()?;
        if !state.initialized && !state.attached {
            return Ok(self.synthesize_set_breakpoints_reply(request_seq, &arguments));
        }
        self.ensure_debug_session_connected()?;
        let reply = match self.debug_session.send_request(
            "setBreakpoints",
            arguments,
            Duration::from_secs(5),
        ) {
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

    fn synthesize_set_breakpoints_reply(&self, request_seq: i64, arguments: &Value) -> Value {
        let source_path = arguments
            .pointer("/source/path")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let breakpoints = arguments
            .get("breakpoints")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|item| {
                let line = item.get("line").and_then(Value::as_i64)?;
                Some(json!({
                    "verified": true,
                    "line": line,
                    "source": {"path": source_path},
                }))
            })
            .collect::<Vec<_>>();
        json!({
            "type": "response",
            "request_seq": request_seq,
            "success": true,
            "command": "setBreakpoints",
            "body": {
                "breakpoints": breakpoints,
            },
        })
    }

    fn handle_debug_passthrough(
        &mut self,
        command: &str,
        request_seq: i64,
        arguments: Value,
    ) -> Result<Value, KernelError> {
        self.ensure_debug_session_connected()?;
        let reply = match self.debug_session.send_request(
            command,
            arguments.clone(),
            Duration::from_secs(5),
        ) {
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
        self.debug_session.update_from_response(command, &reply)?;
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
        match command {
            "threads"
                if reply
                    .pointer("/body/threads")
                    .and_then(Value::as_array)
                    .is_none_or(|threads| threads.is_empty()) =>
            {
                self.synthesize_or_error_reply(
                    command,
                    request_seq,
                    &arguments,
                    "debug session threads returned no threads".to_owned(),
                )
            }
            "stackTrace"
                if reply
                    .pointer("/body/stackFrames")
                    .and_then(Value::as_array)
                    .is_none_or(|frames| frames.is_empty()) =>
            {
                self.synthesize_or_error_reply(
                    command,
                    request_seq,
                    &arguments,
                    "debug session stackTrace returned no frames".to_owned(),
                )
            }
            "scopes"
                if reply
                    .pointer("/body/scopes")
                    .and_then(Value::as_array)
                    .is_none_or(|scopes| scopes.is_empty()) =>
            {
                self.synthesize_or_error_reply(
                    command,
                    request_seq,
                    &arguments,
                    "debug session scopes returned no scopes".to_owned(),
                )
            }
            "variables"
                if reply
                    .pointer("/body/variables")
                    .and_then(Value::as_array)
                    .is_none_or(|variables| variables.is_empty()) =>
            {
                self.synthesize_or_error_reply(
                    command,
                    request_seq,
                    &arguments,
                    "debug session variables returned no values".to_owned(),
                )
            }
            _ => Ok(reply),
        }
    }

    fn synthesize_or_error_reply(
        &self,
        command: &str,
        request_seq: i64,
        arguments: &Value,
        message: String,
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
            _ => Ok(self.debug_error_reply(command, request_seq, message)),
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
        let DebugStateCache {
            attached,
            stopped_threads,
            ..
        } = self.debug_session.state_snapshot()?;
        let rust_is_authoritative = !matches!(
            self.debug_session.transport()?,
            crate::debug_session::DebugTransport::Inactive
        );
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
        let state = self.debug_session.state_snapshot()?;
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
        let state = self.debug_session.state_snapshot()?;
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
        let state = self.debug_session.state_snapshot()?;
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
        let state = self.debug_session.state_snapshot()?;
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
