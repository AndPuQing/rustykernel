use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use std::thread;

use serde_json::{Value, json};
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use zeromq::{PubSocket, RouterSocket};

use crate::protocol::JupyterMessage;
use crate::worker::{
    ExecutionOutcome, WorkerDebugEvent, WorkerExecutionHandle, WorkerExecutionMessage,
};

use super::KernelError;
use super::debug::filter_worker_debug_events_for_publish;
use super::io::{
    publish_comm_events, publish_debug_events, publish_display_event, publish_iopub_message,
    publish_status, publish_stream, send_reply,
};
use super::state::MessageLoopState;

pub(crate) struct PendingExecute {
    pub(crate) parent_header: Value,
    pub(crate) silent: bool,
    pub(crate) subshell_id: Option<String>,
}

pub(crate) struct StreamBatch {
    segments: Vec<StreamSegment>,
}

struct StreamSegment {
    name: String,
    source: String,
    text: String,
}

impl StreamBatch {
    pub(crate) fn push(&mut self, name: String, source: String, text: String) {
        if let Some(segment) = self.segments.last_mut() {
            if segment.name == name && segment.source == source {
                segment.text.push_str(&text);
                return;
            }
        }

        self.segments.push(StreamSegment { name, source, text });
    }
}

impl Default for StreamBatch {
    fn default() -> Self {
        Self {
            segments: Vec::new(),
        }
    }
}

pub(crate) struct ExecuteCompletion {
    pub(crate) request: JupyterMessage,
    pub(crate) code: String,
    pub(crate) execution_count: u32,
    pub(crate) silent: bool,
    pub(crate) store_history: bool,
    pub(crate) outcome: Result<ExecutionOutcome, KernelError>,
}

pub(crate) enum ExecuteUpdate {
    Stream {
        request_id: u64,
        name: String,
        source: String,
        text: String,
    },
    DebugEvent {
        request_id: u64,
        event: WorkerDebugEvent,
    },
    Completion {
        request_id: u64,
        completion: ExecuteCompletion,
    },
}

#[derive(Clone)]
pub(crate) struct ExecuteUpdateSender {
    tx: mpsc::Sender<ExecuteUpdate>,
    wake: Arc<Notify>,
}

impl ExecuteUpdateSender {
    pub(crate) fn new(tx: mpsc::Sender<ExecuteUpdate>) -> Self {
        Self {
            tx,
            wake: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn send(&self, update: ExecuteUpdate) -> Result<(), mpsc::SendError<ExecuteUpdate>> {
        self.tx.send(update)?;
        self.wake.notify_one();
        Ok(())
    }

    pub(crate) fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.wake)
    }
}

pub(crate) fn spawn_execute_request_from_handle(
    execute_tx: &ExecuteUpdateSender,
    handle: WorkerExecutionHandle,
    request: JupyterMessage,
    code: String,
    execution_count: u32,
    silent: bool,
    store_history: bool,
) {
    let execute_tx = execute_tx.clone();
    thread::spawn(move || {
        let request_id = handle.request_id;
        let outcome = loop {
            match handle.recv() {
                Ok(WorkerExecutionMessage::InputRequest { .. }) => {
                    break Err(KernelError::Worker(
                        "stdin is not enabled for this execute_request".to_owned(),
                    ));
                }
                Ok(WorkerExecutionMessage::Stream { name, text, source }) => {
                    if execute_tx
                        .send(ExecuteUpdate::Stream {
                            request_id,
                            name,
                            source,
                            text,
                        })
                        .is_err()
                    {
                        break Err(KernelError::Worker(
                            "failed to send execute stream update".to_owned(),
                        ));
                    }
                }
                Ok(WorkerExecutionMessage::DebugEvent(event)) => {
                    if execute_tx
                        .send(ExecuteUpdate::DebugEvent { request_id, event })
                        .is_err()
                    {
                        break Err(KernelError::Worker(
                            "failed to send execute debug event update".to_owned(),
                        ));
                    }
                }
                Ok(WorkerExecutionMessage::Completion(outcome)) => break Ok(outcome),
                Ok(WorkerExecutionMessage::Failure(message)) => {
                    break Err(KernelError::Worker(message));
                }
                Err(error) => break Err(error),
            }
        };
        let _ = execute_tx.send(ExecuteUpdate::Completion {
            request_id,
            completion: ExecuteCompletion {
                request,
                code,
                execution_count,
                silent,
                store_history,
                outcome,
            },
        });
    });
}

pub(crate) fn finalize_execute_completion(
    runtime: &Runtime,
    reply_socket: &mut RouterSocket,
    iopub_socket: &mut PubSocket,
    state: &mut MessageLoopState,
    request_id: u64,
    completion: ExecuteCompletion,
) -> Result<(), KernelError> {
    state.pending_executes.remove(&request_id);

    let ExecuteCompletion {
        request,
        code,
        execution_count,
        silent,
        store_history,
        outcome,
    } = completion;
    let outcome = outcome?;

    if !silent {
        publish_comm_events(
            runtime,
            iopub_socket,
            state,
            request.header_value.clone(),
            &outcome.comm_events,
        )?;
        let debug_events = filter_worker_debug_events_for_publish(state, &outcome.debug_events)?;
        publish_debug_events(
            runtime,
            iopub_socket,
            state,
            request.header_value.clone(),
            &debug_events,
        )?;
    }

    if !silent && store_history {
        state.record_history(execution_count, &code, &outcome);
    }

    if outcome.status == "ok" {
        if !silent {
            for display in outcome.displays {
                if !display.content.is_null() {
                    publish_iopub_message(
                        runtime,
                        iopub_socket,
                        state,
                        request.header_value.clone(),
                        &display.msg_type,
                        display.content,
                    )?;
                } else {
                    publish_display_event(
                        runtime,
                        iopub_socket,
                        state,
                        request.header_value.clone(),
                        &display.msg_type,
                        display.data,
                        display.metadata,
                        display.transient,
                    )?;
                }
            }

            if let Some(result) = outcome.result {
                publish_iopub_message(
                    runtime,
                    iopub_socket,
                    state,
                    request.header_value.clone(),
                    "execute_result",
                    json!({
                        "execution_count": execution_count,
                        "data": result.data,
                        "metadata": result.metadata,
                    }),
                )?;
            }
        }

        send_reply(
            runtime,
            reply_socket,
            state,
            &request,
            "execute_reply",
            json!({
                "status": "ok",
                "execution_count": execution_count,
                "user_expressions": outcome.user_expressions,
                "payload": outcome.payload,
            }),
        )?;
    } else {
        let ename = outcome.ename.unwrap_or_else(|| "ExecutionError".to_owned());
        let evalue = outcome.evalue.unwrap_or_default();
        let user_expressions = outcome.user_expressions;
        let payload = outcome.payload;
        let traceback = outcome.traceback;

        if !silent {
            publish_iopub_message(
                runtime,
                iopub_socket,
                state,
                request.header_value.clone(),
                "error",
                json!({
                    "ename": ename,
                    "evalue": evalue,
                    "traceback": traceback,
                }),
            )?;
        }

        send_reply(
            runtime,
            reply_socket,
            state,
            &request,
            "execute_reply",
            json!({
                "status": "error",
                "execution_count": execution_count,
                "ename": ename,
                "evalue": evalue,
                "user_expressions": user_expressions,
                "payload": payload,
                "traceback": traceback,
            }),
        )?;
    }

    publish_status(
        runtime,
        iopub_socket,
        state,
        request.header_value.clone(),
        "idle",
    )?;
    Ok(())
}

pub(crate) fn publish_execute_stream(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &MessageLoopState,
    request_id: u64,
    name: &str,
    text: &str,
) -> Result<(), KernelError> {
    let Some(pending) = state.pending_executes.get(&request_id) else {
        return Err(KernelError::Worker(
            "received execute stream update without a pending execute".to_owned(),
        ));
    };

    if pending.silent {
        return Ok(());
    }

    publish_stream(
        runtime,
        socket,
        state,
        pending.parent_header.clone(),
        name,
        text,
    )
}

pub(crate) fn flush_execute_stream_batches(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &MessageLoopState,
    stream_batches: HashMap<u64, StreamBatch>,
) -> Result<(), KernelError> {
    for (request_id, batch) in stream_batches {
        flush_execute_stream_batch(runtime, socket, state, request_id, Some(batch))?;
    }
    Ok(())
}

pub(crate) fn flush_execute_stream_batch(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &MessageLoopState,
    request_id: u64,
    batch: Option<StreamBatch>,
) -> Result<(), KernelError> {
    let Some(batch) = batch else {
        return Ok(());
    };

    for segment in batch.segments {
        publish_execute_stream(
            runtime,
            socket,
            state,
            request_id,
            &segment.name,
            &segment.text,
        )?;
    }

    Ok(())
}

pub(crate) fn publish_execute_debug_event(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &MessageLoopState,
    request_id: u64,
    event: &WorkerDebugEvent,
) -> Result<(), KernelError> {
    let rust_session_connected = matches!(
        state.debug_session.transport()?,
        crate::debug_session::DebugTransport::Connected(_)
    );
    if rust_session_connected
        && !(event.content.get("event") == Some(&json!("stopped"))
            && event.content.get("seq") == Some(&json!(0)))
    {
        return Ok(());
    }
    if rust_session_connected
        || (event.content.get("event") == Some(&json!("stopped"))
            && event.content.get("seq") == Some(&json!(0)))
    {
        state.debug_session.apply_event_state(&event.content)?;
    }

    let Some(pending) = state.pending_executes.get(&request_id) else {
        return Err(KernelError::Worker(
            "received execute debug event update without a pending execute".to_owned(),
        ));
    };

    if pending.silent {
        return Ok(());
    }

    publish_iopub_message(
        runtime,
        socket,
        state,
        pending.parent_header.clone(),
        &event.msg_type,
        event.content.clone(),
    )
}
