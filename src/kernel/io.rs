use serde_json::{Value, json};
use tokio::runtime::Runtime;
use tracing::warn;
use zeromq::{PubSocket, RouterSocket};

use crate::protocol::{JupyterMessage, MessageHeader, MessageSigner};
use crate::worker::{CommOutcome, WorkerCommEvent};

use super::KernelError;
use super::runtime::send_frames;
use super::state::MessageLoopState;

pub(crate) struct StdinRequestContext<'a> {
    pub(crate) signer: &'a MessageSigner,
    pub(crate) kernel_session: &'a str,
    pub(crate) identities: &'a [Vec<u8>],
    pub(crate) parent_header: &'a Value,
    pub(crate) allow_stdin: bool,
}

pub(crate) struct DisplayMessage<'a> {
    pub(crate) msg_type: &'a str,
    pub(crate) data: Value,
    pub(crate) metadata: Value,
    pub(crate) transient: Value,
}

pub(crate) fn send_stdin_input_request(
    runtime: &Runtime,
    stdin_socket: &mut RouterSocket,
    request: StdinRequestContext<'_>,
    prompt: &str,
    password: bool,
) -> Result<String, KernelError> {
    if !request.allow_stdin {
        return Err(KernelError::Worker(
            "stdin is not enabled for this execute_request".to_owned(),
        ));
    }

    let input_request = JupyterMessage::new(
        request.identities.to_vec(),
        MessageHeader::new("input_request", request.kernel_session),
        request.parent_header.clone(),
        json!({}),
        json!({
            "prompt": prompt,
            "password": password,
        }),
    );
    let input_request_id = input_request.header.msg_id.clone();

    send_frames(
        runtime,
        stdin_socket,
        request.signer.encode(&input_request)?,
    )?;
    Ok(input_request_id)
}

pub(crate) fn handle_stdin_message(
    frames: Vec<Vec<u8>>,
    state: &mut MessageLoopState,
) -> Result<(), KernelError> {
    let message = match state.signer.decode(frames) {
        Ok(message) => message,
        Err(err) => {
            warn!(%err, "failed to decode stdin message, dropping");
            return Ok(());
        }
    };

    if message.header.msg_type != "input_reply" {
        return Ok(());
    }

    let input_request_msg_id = message
        .parent_header
        .get("msg_id")
        .and_then(Value::as_str)
        .filter(|msg_id| !msg_id.is_empty());

    let Some((&request_id, _)) = state
        .pending_executes
        .iter()
        .find(|(_, pending)| match &pending.phase {
            super::execute::ExecutePhase::WaitingInput {
                input_request_msg_id: waiting_for,
            } => {
                if let Some(msg_id) = input_request_msg_id {
                    waiting_for == msg_id
                } else {
                    pending.request.identities == message.identities
                }
            }
            super::execute::ExecutePhase::Running => false,
        })
    else {
        return Ok(());
    };

    let value = message
        .content
        .get("value")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();

    state.with_worker(|worker| worker.send_input_reply(request_id, value, None))?;
    if let Some(pending) = state.pending_executes.get_mut(&request_id) {
        pending.phase = super::execute::ExecutePhase::Running;
    }
    Ok(())
}

pub(crate) fn send_reply(
    runtime: &Runtime,
    socket: &mut impl zeromq::SocketSend,
    state: &mut MessageLoopState,
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
    send_frames(runtime, socket, state.signer.encode(&reply)?)?;
    Ok(())
}

pub(crate) fn publish_status(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &MessageLoopState,
    parent_header: Value,
    execution_state: &str,
) -> Result<(), KernelError> {
    publish_iopub_message(
        runtime,
        socket,
        state,
        parent_header,
        "status",
        json!({ "execution_state": execution_state }),
    )
}

pub(crate) fn publish_iopub_message(
    runtime: &Runtime,
    socket: &mut PubSocket,
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
    send_frames(runtime, socket, state.signer.encode(&message)?)?;
    Ok(())
}

pub(crate) fn publish_display_event(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &MessageLoopState,
    parent_header: Value,
    message: DisplayMessage<'_>,
) -> Result<(), KernelError> {
    publish_iopub_message(
        runtime,
        socket,
        state,
        parent_header,
        message.msg_type,
        json!({
            "data": message.data,
            "metadata": message.metadata,
            "transient": message.transient,
        }),
    )
}

pub(crate) fn publish_comm_events(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &mut MessageLoopState,
    parent_header: Value,
    events: &[WorkerCommEvent],
) -> Result<(), KernelError> {
    for event in events {
        publish_iopub_message(
            runtime,
            socket,
            state,
            parent_header.clone(),
            &event.msg_type,
            event.content.clone(),
        )?;
        state.comms.apply_event(event);
    }
    Ok(())
}

pub(crate) fn publish_stream(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &MessageLoopState,
    parent_header: Value,
    name: &str,
    text: &str,
) -> Result<(), KernelError> {
    publish_iopub_message(
        runtime,
        socket,
        state,
        parent_header,
        "stream",
        json!({
            "name": name,
            "text": text,
        }),
    )
}

pub(crate) fn drain_debug_session_events(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &mut MessageLoopState,
) -> Result<(), KernelError> {
    while let Some(event) = state.debug.drain_event()? {
        publish_debug_session_event(runtime, socket, state, event)?;
    }
    Ok(())
}

pub(crate) fn publish_debug_session_event(
    runtime: &Runtime,
    socket: &mut PubSocket,
    state: &mut MessageLoopState,
    event: crate::debug::DebugEventEnvelope,
) -> Result<(), KernelError> {
    let parent_header = if let Some(parent_header) = event.parent_header {
        parent_header
    } else if let Some(pending) = state
        .pending_executes
        .values()
        .find(|pending| !pending.silent)
    {
        if pending.silent {
            return Ok(());
        }
        pending.request.header_value.clone()
    } else {
        return Ok(());
    };

    publish_iopub_message(
        runtime,
        socket,
        state,
        parent_header,
        "debug_event",
        event.event,
    )
}

pub(crate) fn handle_comm_outcome(
    runtime: &Runtime,
    iopub_socket: &mut PubSocket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
    outcome: &CommOutcome,
) -> Result<(), KernelError> {
    if !outcome.stdout.is_empty() {
        publish_stream(
            runtime,
            iopub_socket,
            state,
            request.header_value.clone(),
            "stdout",
            &outcome.stdout,
        )?;
    }
    if !outcome.stderr.is_empty() {
        publish_stream(
            runtime,
            iopub_socket,
            state,
            request.header_value.clone(),
            "stderr",
            &outcome.stderr,
        )?;
    }
    publish_comm_events(
        runtime,
        iopub_socket,
        state,
        request.header_value.clone(),
        &outcome.events,
    )?;
    Ok(())
}
