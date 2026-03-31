use serde_json::{Value, json};
use tokio::runtime::Runtime;
use tracing::warn;
use zeromq::RouterSocket;

use crate::protocol::JupyterMessage;

use super::execute::{ExecutePhase, PendingExecute};
use super::io::{handle_comm_outcome, publish_iopub_message, publish_status, send_reply};
use super::runtime::IopubSocket;
use super::state::MessageLoopState;
use super::{KernelError, ShutdownSignal};

#[derive(Clone, Copy)]
pub(crate) enum ChannelKind {
    Shell,
    Control,
    Stdin,
}

pub(crate) enum RequestDisposition {
    Complete { should_stop: bool },
    Deferred,
}

pub(crate) struct RequestSockets<'a> {
    pub(crate) reply: &'a mut RouterSocket,
    pub(crate) iopub: &'a mut IopubSocket,
}

pub(crate) fn handle_request(
    runtime: &Runtime,
    channel: ChannelKind,
    frames: Vec<Vec<u8>>,
    sockets: &mut RequestSockets<'_>,
    state: &mut MessageLoopState,
    shutdown: &ShutdownSignal,
) -> Result<(), KernelError> {
    let request = match state.signer.decode(frames) {
        Ok(request) => request,
        Err(err) => {
            warn!(%err, "failed to decode Jupyter message, dropping");
            return Ok(());
        }
    };

    let parent_header = request.header_value.clone();
    publish_status(runtime, sockets.iopub, state, parent_header.clone(), "busy")?;

    let disposition = match channel {
        ChannelKind::Shell => handle_shell_request(runtime, sockets, state, &request)?,
        ChannelKind::Control => {
            handle_control_request(runtime, sockets.reply, sockets.iopub, state, &request)?
        }
        ChannelKind::Stdin => unreachable!("stdin messages are handled before request dispatch"),
    };

    match disposition {
        RequestDisposition::Complete { should_stop } => {
            publish_status(runtime, sockets.iopub, state, parent_header, "idle")?;
            if should_stop {
                shutdown.request_stop();
            }
        }
        RequestDisposition::Deferred => {}
    }

    Ok(())
}

pub(crate) fn handle_shell_request(
    runtime: &Runtime,
    sockets: &mut RequestSockets<'_>,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<RequestDisposition, KernelError> {
    match request.header.msg_type.as_str() {
        "kernel_info_request" => {
            send_reply(
                runtime,
                sockets.reply,
                state,
                request,
                "kernel_info_reply",
                state.kernel_info_content(),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "connect_request" => {
            send_reply(
                runtime,
                sockets.reply,
                state,
                request,
                "connect_reply",
                state.connect_reply_content(),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "execute_request" => {
            let subshell_id = request.header.subshell_id.clone();
            let silent = request
                .content
                .get("silent")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let user_expressions = request
                .content
                .get("user_expressions")
                .unwrap_or(&Value::Null);
            let store_history = request
                .content
                .get("store_history")
                .and_then(Value::as_bool)
                .unwrap_or(!silent);
            let allow_stdin = request
                .content
                .get("allow_stdin")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let code = request
                .content
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let execution_count = state.next_execution_count(&request.content);

            if !silent {
                publish_iopub_message(
                    runtime,
                    sockets.iopub,
                    state,
                    request.header_value.clone(),
                    "execute_input",
                    json!({
                        "code": code,
                        "execution_count": execution_count,
                    }),
                )?;
            }

            let worker_request_id = state.with_worker(|worker| {
                worker.execute_async(
                    code,
                    subshell_id.as_deref(),
                    user_expressions,
                    execution_count,
                    silent,
                    store_history,
                )
            })?;

            state.pending_executes.insert(
                worker_request_id,
                PendingExecute {
                    request: request.clone(),
                    code: code.to_owned(),
                    execution_count,
                    silent,
                    store_history,
                    subshell_id,
                    allow_stdin,
                    phase: ExecutePhase::Running,
                },
            );
            state.on_execute_started();
            Ok(RequestDisposition::Deferred)
        }
        "is_complete_request" => {
            let code = request
                .content
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let outcome = state.with_worker(|worker| worker.is_complete(code))?;
            send_reply(
                runtime,
                sockets.reply,
                state,
                request,
                "is_complete_reply",
                json!({
                    "status": outcome.status,
                    "indent": outcome.indent,
                }),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "complete_request" => {
            let code = request
                .content
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let cursor_pos = request
                .content
                .get("cursor_pos")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let completion =
                state.with_worker(|worker| worker.complete(code, cursor_pos.max(0) as usize))?;
            send_reply(
                runtime,
                sockets.reply,
                state,
                request,
                "complete_reply",
                json!({
                    "status": completion.status,
                    "matches": completion.matches,
                    "cursor_start": completion.cursor_start,
                    "cursor_end": completion.cursor_end,
                    "metadata": completion.metadata,
                }),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "inspect_request" => {
            let code = request
                .content
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let cursor_pos = request
                .content
                .get("cursor_pos")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let detail_level = request
                .content
                .get("detail_level")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let inspection = state.with_worker(|worker| {
                worker.inspect(code, cursor_pos.max(0) as usize, detail_level as u8)
            })?;
            send_reply(
                runtime,
                sockets.reply,
                state,
                request,
                "inspect_reply",
                json!({
                    "status": inspection.status,
                    "found": inspection.found,
                    "data": inspection.data,
                    "metadata": inspection.metadata,
                }),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "history_request" => {
            send_reply(
                runtime,
                sockets.reply,
                state,
                request,
                "history_reply",
                state.history_reply_content(&request.content),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "comm_open" => {
            let comm_id = request
                .content
                .get("comm_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let target_name = request
                .content
                .get("target_name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let data = request.content.get("data").unwrap_or(&Value::Null);
            let metadata = request.content.get("metadata").unwrap_or(&Value::Null);
            let outcome = state
                .with_worker(|worker| worker.comm_open(comm_id, target_name, data, metadata))?;
            handle_comm_outcome(runtime, sockets.iopub, state, request, &outcome)?;
            if outcome.registered {
                state.register_comm(&request.content);
            } else {
                state.close_comm(&request.content);
            }
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "comm_msg" => {
            let comm_id = request
                .content
                .get("comm_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let data = request.content.get("data").unwrap_or(&Value::Null);
            let metadata = request.content.get("metadata").unwrap_or(&Value::Null);
            let outcome = state.with_worker(|worker| worker.comm_msg(comm_id, data, metadata))?;
            handle_comm_outcome(runtime, sockets.iopub, state, request, &outcome)?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "comm_close" => {
            let comm_id = request
                .content
                .get("comm_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let data = request.content.get("data").unwrap_or(&Value::Null);
            let metadata = request.content.get("metadata").unwrap_or(&Value::Null);
            let outcome = state.with_worker(|worker| worker.comm_close(comm_id, data, metadata))?;
            handle_comm_outcome(runtime, sockets.iopub, state, request, &outcome)?;
            state.close_comm(&request.content);
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "comm_info_request" => {
            send_reply(
                runtime,
                sockets.reply,
                state,
                request,
                "comm_info_reply",
                state.comm_info_reply_content(&request.content),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "shutdown_request" => {
            handle_shutdown_request(runtime, sockets.reply, sockets.iopub, state, request)
        }
        _ => send_unsupported_reply(runtime, sockets.reply, state, request),
    }
}

pub(crate) fn handle_control_request(
    runtime: &Runtime,
    reply_socket: &mut RouterSocket,
    iopub_socket: &mut IopubSocket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<RequestDisposition, KernelError> {
    match request.header.msg_type.as_str() {
        "kernel_info_request" => {
            send_reply(
                runtime,
                reply_socket,
                state,
                request,
                "kernel_info_reply",
                state.kernel_info_content(),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "connect_request" => {
            send_reply(
                runtime,
                reply_socket,
                state,
                request,
                "connect_reply",
                state.connect_reply_content(),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "shutdown_request" => {
            handle_shutdown_request(runtime, reply_socket, iopub_socket, state, request)
        }
        "interrupt_request" => {
            if state.is_executing() {
                state.interrupt()?;
            }
            send_reply(
                runtime,
                reply_socket,
                state,
                request,
                "interrupt_reply",
                json!({ "status": "ok" }),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "usage_request" => {
            send_reply(
                runtime,
                reply_socket,
                state,
                request,
                "usage_reply",
                state.usage_reply_content(),
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "debug_request" => {
            let content = state.debug_reply_content(&request.content)?;
            send_reply(
                runtime,
                reply_socket,
                state,
                request,
                "debug_reply",
                content,
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "create_subshell_request" => {
            let content = state.create_subshell_reply_content()?;
            send_reply(
                runtime,
                reply_socket,
                state,
                request,
                "create_subshell_reply",
                content,
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "delete_subshell_request" => {
            let content = state.delete_subshell_reply_content(&request.content)?;
            send_reply(
                runtime,
                reply_socket,
                state,
                request,
                "delete_subshell_reply",
                content,
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        "list_subshell_request" => {
            let content = state.list_subshell_reply_content()?;
            send_reply(
                runtime,
                reply_socket,
                state,
                request,
                "list_subshell_reply",
                content,
            )?;
            Ok(RequestDisposition::Complete { should_stop: false })
        }
        _ => send_unsupported_reply(runtime, reply_socket, state, request),
    }
}

pub(crate) fn handle_shutdown_request(
    runtime: &Runtime,
    reply_socket: &mut RouterSocket,
    iopub_socket: &mut IopubSocket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<RequestDisposition, KernelError> {
    let restart = request
        .content
        .get("restart")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if restart {
        state.restart()?;
    }

    send_reply(
        runtime,
        reply_socket,
        state,
        request,
        "shutdown_reply",
        json!({
            "status": "ok",
            "restart": restart,
        }),
    )?;
    publish_iopub_message(
        runtime,
        iopub_socket,
        state,
        request.header_value.clone(),
        "shutdown_reply",
        json!({
            "status": "ok",
            "restart": restart,
        }),
    )?;
    Ok(RequestDisposition::Complete {
        should_stop: !restart,
    })
}

pub(crate) fn send_unsupported_reply(
    runtime: &Runtime,
    reply_socket: &mut RouterSocket,
    state: &mut MessageLoopState,
    request: &JupyterMessage,
) -> Result<RequestDisposition, KernelError> {
    let reply_type = reply_type_for(&request.header.msg_type);
    send_reply(
        runtime,
        reply_socket,
        state,
        request,
        &reply_type,
        json!({
            "status": "error",
            "ename": "NotImplemented",
            "evalue": format!("unsupported message type: {}", request.header.msg_type),
            "traceback": [],
        }),
    )?;
    Ok(RequestDisposition::Complete { should_stop: false })
}

pub(crate) fn reply_type_for(msg_type: &str) -> String {
    if let Some(prefix) = msg_type.strip_suffix("_request") {
        format!("{prefix}_reply")
    } else {
        "error".to_owned()
    }
}
