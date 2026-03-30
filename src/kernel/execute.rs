use std::collections::HashMap;
use std::sync::{Arc, mpsc};

use serde_json::json;
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use zeromq::{PubSocket, RouterSocket};

use crate::protocol::JupyterMessage;
use crate::worker::{ExecutionDisplayEvent, ExecutionOutcome, WorkerCommEvent, WorkerDebugEvent};

use super::KernelError;
use super::io::{
    publish_display_event, publish_iopub_message, publish_status, publish_stream, send_reply,
};
use super::state::MessageLoopState;

#[allow(dead_code)]
pub(crate) enum ExecutePhase {
    Running,
    WaitingInput { input_request_msg_id: String },
}

pub(crate) struct PendingExecute {
    pub(crate) request: JupyterMessage,
    pub(crate) code: String,
    pub(crate) execution_count: u32,
    pub(crate) silent: bool,
    pub(crate) store_history: bool,
    pub(crate) subshell_id: Option<String>,
    pub(crate) allow_stdin: bool,
    pub(crate) phase: ExecutePhase,
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

pub(crate) enum WorkerUpdateEvent {
    InputRequest {
        request_id: u64,
        prompt: String,
        password: bool,
    },
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
    DisplayEvent {
        request_id: u64,
        event: ExecutionDisplayEvent,
    },
    CommEvent {
        request_id: u64,
        event: WorkerCommEvent,
    },
    Completion {
        request_id: u64,
        outcome: Result<ExecutionOutcome, String>,
    },
}

pub(crate) enum KernelEvent {
    WorkerUpdate {
        worker_epoch: u64,
        update: WorkerUpdateEvent,
    },
}

#[derive(Clone)]
pub(crate) struct KernelEventSender {
    tx: mpsc::Sender<KernelEvent>,
    wake: Arc<Notify>,
}

impl KernelEventSender {
    pub(crate) fn new(tx: mpsc::Sender<KernelEvent>) -> Self {
        Self {
            tx,
            wake: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn send(&self, event: KernelEvent) -> Result<(), mpsc::SendError<KernelEvent>> {
        self.tx.send(event)?;
        self.wake.notify_one();
        Ok(())
    }

    pub(crate) fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.wake)
    }
}

pub(crate) fn finalize_execute_completion(
    runtime: &Runtime,
    reply_socket: &mut RouterSocket,
    iopub_socket: &mut PubSocket,
    state: &mut MessageLoopState,
    request_id: u64,
    outcome: Result<ExecutionOutcome, KernelError>,
) -> Result<(), KernelError> {
    let pending = state.pending_executes.remove(&request_id).ok_or_else(|| {
        KernelError::Worker("received execute completion without a pending execute".to_owned())
    })?;
    state.on_execute_finished()?;

    let PendingExecute {
        request,
        code,
        execution_count,
        silent,
        store_history,
        ..
    } = pending;
    let outcome = outcome?;

    if !silent && store_history {
        state.record_history(execution_count, &code, &outcome);
    }

    if outcome.status == "ok" {
        if !silent {
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
        pending.request.header_value.clone(),
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
    state: &mut MessageLoopState,
    request_id: u64,
    event: &WorkerDebugEvent,
) -> Result<(), KernelError> {
    let Some(content) = state.debug.accept_worker_event(event)? else {
        return Ok(());
    };

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
        pending.request.header_value.clone(),
        &event.msg_type,
        content,
    )
}

pub(crate) fn publish_execute_display_event(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &MessageLoopState,
    request_id: u64,
    event: &ExecutionDisplayEvent,
) -> Result<(), KernelError> {
    let Some(pending) = state.pending_executes.get(&request_id) else {
        return Err(KernelError::Worker(
            "received execute display event update without a pending execute".to_owned(),
        ));
    };

    if pending.silent {
        return Ok(());
    }

    if !event.content.is_null() {
        publish_iopub_message(
            runtime,
            socket,
            state,
            pending.request.header_value.clone(),
            &event.msg_type,
            event.content.clone(),
        )
    } else {
        publish_display_event(
            runtime,
            socket,
            state,
            pending.request.header_value.clone(),
            &event.msg_type,
            event.data.clone(),
            event.metadata.clone(),
            event.transient.clone(),
        )
    }
}

pub(crate) fn publish_execute_comm_event(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &mut MessageLoopState,
    request_id: u64,
    event: &WorkerCommEvent,
) -> Result<(), KernelError> {
    let Some(pending) = state.pending_executes.get(&request_id) else {
        return Err(KernelError::Worker(
            "received execute comm event update without a pending execute".to_owned(),
        ));
    };

    if pending.silent {
        return Ok(());
    }

    publish_iopub_message(
        runtime,
        socket,
        state,
        pending.request.header_value.clone(),
        &event.msg_type,
        event.content.clone(),
    )?;
    state.comms.apply_event(event);
    Ok(())
}
