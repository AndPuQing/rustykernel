use std::collections::HashMap;
use std::io;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::json;
use tokio::runtime::{Builder, Runtime};
use tokio::time;
use zeromq::Socket as _;
use zeromq::SocketRecv as _;
use zeromq::SocketSend as _;
use zeromq::{PubSocket, RepSocket, RouterSocket, ZmqError, ZmqMessage};

use super::dispatch::{ChannelKind, handle_request};
use super::execute::{
    ExecuteUpdate, ExecuteUpdateSender, StreamBatch, finalize_execute_completion,
    flush_execute_stream_batch, flush_execute_stream_batches, publish_execute_debug_event,
};
use super::io::{drain_debug_session_events, handle_stdin_message, publish_status};
use super::state::MessageLoopState;
use super::{ChannelEndpoints, ConnectionInfo, KernelError, ShutdownSignal};

pub struct KernelRuntime {
    connection: ConnectionInfo,
    endpoints: ChannelEndpoints,
    shutdown: Arc<ShutdownSignal>,
    hb_thread: Option<JoinHandle<Result<(), ZmqError>>>,
    message_loop_thread: Option<JoinHandle<Result<(), KernelError>>>,
}

impl KernelRuntime {
    pub fn start(connection: ConnectionInfo) -> Result<Self, KernelError> {
        let endpoints = connection.channel_endpoints();
        let shutdown = Arc::new(ShutdownSignal::new());

        let (hb_thread, hb_ready) =
            spawn_heartbeat_thread(endpoints.hb.clone(), Arc::clone(&shutdown));
        match hb_ready.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = hb_thread.join();
                return Err(KernelError::Zmq(error));
            }
            Err(_) => return Err(KernelError::HeartbeatThreadPanicked),
        }

        let (message_loop_thread, message_loop_ready) =
            spawn_message_loop_thread(connection.clone(), endpoints.clone(), Arc::clone(&shutdown));
        match message_loop_ready.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                shutdown.request_stop();
                let _ = message_loop_thread.join();
                match hb_thread.join() {
                    Ok(Ok(())) | Ok(Err(_)) | Err(_) => {}
                }
                return Err(error);
            }
            Err(_) => {
                shutdown.request_stop();
                let _ = message_loop_thread.join();
                match hb_thread.join() {
                    Ok(Ok(())) | Ok(Err(_)) | Err(_) => {}
                }
                return Err(KernelError::MessageLoopThreadPanicked);
            }
        }

        Ok(Self {
            connection,
            endpoints,
            shutdown,
            hb_thread: Some(hb_thread),
            message_loop_thread: Some(message_loop_thread),
        })
    }

    pub fn connection_info(&self) -> &ConnectionInfo {
        &self.connection
    }

    pub fn channel_endpoints(&self) -> &ChannelEndpoints {
        &self.endpoints
    }

    pub fn is_running(&self) -> bool {
        !self.shutdown.is_stopped()
    }

    pub fn wait_for_shutdown(&self) {
        self.shutdown.wait();
    }

    pub fn stop(&mut self) -> Result<(), KernelError> {
        if self.shutdown.is_stopped() {
            return self.finish_shutdown();
        }

        self.shutdown.request_stop();
        self.finish_shutdown()
    }

    fn finish_shutdown(&mut self) -> Result<(), KernelError> {
        if let Some(handle) = self.message_loop_thread.take() {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => return Err(KernelError::MessageLoopThreadPanicked),
            }
        }

        if let Some(handle) = self.hb_thread.take() {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(KernelError::Zmq(error)),
                Err(_) => return Err(KernelError::HeartbeatThreadPanicked),
            }
        }

        Ok(())
    }
}

impl Drop for KernelRuntime {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub(crate) fn build_transport_runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime for zeromq")
}

pub(crate) fn multipart_message(frames: Vec<Vec<u8>>) -> Result<ZmqMessage, ZmqError> {
    let frames = frames
        .into_iter()
        .map(bytes::Bytes::from)
        .collect::<Vec<_>>();
    ZmqMessage::try_from(frames)
        .map_err(|_| ZmqError::Other("empty multipart messages are not supported"))
}

fn message_to_frames(message: ZmqMessage) -> Vec<Vec<u8>> {
    message
        .into_vec()
        .into_iter()
        .map(|frame| frame.to_vec())
        .collect()
}

fn bind_socket<S: zeromq::Socket>(
    runtime: &Runtime,
    socket: &mut S,
    endpoint: &str,
) -> Result<(), KernelError> {
    runtime.block_on(socket.bind(endpoint))?;
    Ok(())
}

pub(crate) fn send_frames<S: zeromq::SocketSend>(
    runtime: &Runtime,
    socket: &mut S,
    frames: Vec<Vec<u8>>,
) -> Result<(), KernelError> {
    runtime.block_on(socket.send(multipart_message(frames)?))?;
    Ok(())
}

pub(crate) fn recv_frames<S: zeromq::SocketRecv>(
    runtime: &Runtime,
    socket: &mut S,
    timeout: Option<Duration>,
) -> Result<Option<Vec<Vec<u8>>>, ZmqError> {
    runtime.block_on(async {
        match timeout {
            Some(timeout) => match time::timeout(timeout, socket.recv()).await {
                Ok(result) => result.map(message_to_frames).map(Some),
                Err(_) => Ok(None),
            },
            None => socket.recv().await.map(message_to_frames).map(Some),
        }
    })
}

async fn next_channel_message(
    shell_socket: &mut RouterSocket,
    control_socket: &mut RouterSocket,
    stdin_socket: &mut RouterSocket,
) -> Result<Option<(ChannelKind, Vec<Vec<u8>>)>, ZmqError> {
    tokio::select! {
        biased;
        result = shell_socket.recv() => result.map(message_to_frames).map(|frames| Some((ChannelKind::Shell, frames))),
        result = control_socket.recv() => result.map(message_to_frames).map(|frames| Some((ChannelKind::Control, frames))),
        result = stdin_socket.recv() => result.map(message_to_frames).map(|frames| Some((ChannelKind::Stdin, frames))),
    }
}

pub(crate) fn spawn_message_loop_thread(
    connection: ConnectionInfo,
    endpoints: ChannelEndpoints,
    shutdown: Arc<ShutdownSignal>,
) -> (
    JoinHandle<Result<(), KernelError>>,
    mpsc::Receiver<Result<(), KernelError>>,
) {
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let handle = thread::spawn(move || {
        let runtime = build_transport_runtime();
        let mut shell_socket = RouterSocket::new();
        bind_socket(&runtime, &mut shell_socket, &endpoints.shell)?;
        let mut iopub_socket = PubSocket::new();
        bind_socket(&runtime, &mut iopub_socket, &endpoints.iopub)?;
        let mut stdin_socket = RouterSocket::new();
        bind_socket(&runtime, &mut stdin_socket, &endpoints.stdin)?;
        let mut control_socket = RouterSocket::new();
        bind_socket(&runtime, &mut control_socket, &endpoints.control)?;
        let mut state = MessageLoopState::new(&connection)?;
        let (execute_tx, execute_rx) = mpsc::channel();
        let execute_tx = ExecuteUpdateSender::new(execute_tx);
        let execute_wake = execute_tx.notifier();
        let debug_wake = state.debug_session.notifier();

        publish_status(&runtime, &mut iopub_socket, &state, json!({}), "starting")?;
        let _ = ready_tx.send(Ok(()));

        loop {
            if shutdown.is_stopped() {
                return Ok(());
            }

            drain_debug_session_events(&runtime, &mut iopub_socket, &state)?;

            let mut stream_batches: HashMap<u64, StreamBatch> = HashMap::new();
            while let Ok(update) = execute_rx.try_recv() {
                match update {
                    ExecuteUpdate::Stream {
                        request_id,
                        name,
                        source,
                        text,
                    } => {
                        let batch = stream_batches.entry(request_id).or_default();
                        batch.push(name, source, text);
                    }
                    ExecuteUpdate::DebugEvent { request_id, event } => {
                        flush_execute_stream_batch(
                            &runtime,
                            &mut iopub_socket,
                            &state,
                            request_id,
                            stream_batches.remove(&request_id),
                        )?;
                        publish_execute_debug_event(
                            &runtime,
                            &mut iopub_socket,
                            &state,
                            request_id,
                            &event,
                        )?;
                    }
                    ExecuteUpdate::Completion {
                        request_id,
                        completion,
                    } => {
                        flush_execute_stream_batch(
                            &runtime,
                            &mut iopub_socket,
                            &state,
                            request_id,
                            stream_batches.remove(&request_id),
                        )?;
                        finalize_execute_completion(
                            &runtime,
                            &mut shell_socket,
                            &mut iopub_socket,
                            &mut state,
                            request_id,
                            completion,
                        )?;
                    }
                }
            }
            flush_execute_stream_batches(&runtime, &mut iopub_socket, &state, stream_batches)?;

            let execute_ready = execute_wake.notified();
            let debug_ready = debug_wake.notified();
            let shutdown_ready = shutdown.wake.notified();
            tokio::pin!(execute_ready);
            tokio::pin!(debug_ready);
            tokio::pin!(shutdown_ready);
            let next_message = match runtime.block_on(async {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_ready => Ok(None),
                    _ = &mut execute_ready => Ok(None),
                    _ = &mut debug_ready => Ok(None),
                    next_message = next_channel_message(
                        &mut shell_socket,
                        &mut control_socket,
                        &mut stdin_socket,
                    ) => next_message,
                }
            }) {
                Ok(next_message) => next_message,
                Err(error) => return Err(KernelError::Zmq(error)),
            };

            let Some((channel, frames)) = next_message else {
                continue;
            };

            match channel {
                ChannelKind::Shell | ChannelKind::Control => {
                    let reply_socket = if matches!(channel, ChannelKind::Shell) {
                        &mut shell_socket
                    } else {
                        &mut control_socket
                    };
                    handle_request(
                        &runtime,
                        channel,
                        frames,
                        reply_socket,
                        &mut stdin_socket,
                        &mut iopub_socket,
                        &mut state,
                        &execute_tx,
                        shutdown.as_ref(),
                    )?;
                }
                ChannelKind::Stdin => {
                    handle_stdin_message(frames, &state)?;
                }
            }
        }
    });

    (handle, ready_rx)
}

pub(crate) fn spawn_heartbeat_thread(
    endpoint: String,
    shutdown: Arc<ShutdownSignal>,
) -> (
    JoinHandle<Result<(), ZmqError>>,
    mpsc::Receiver<Result<(), ZmqError>>,
) {
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let handle = thread::spawn(move || {
        let runtime = build_transport_runtime();
        let mut socket = RepSocket::new();
        if let Err(error) = runtime.block_on(socket.bind(&endpoint)) {
            let _ = ready_tx.send(Err(ZmqError::Network(io::Error::other(error.to_string()))));
            return Err(error);
        }
        let _ = ready_tx.send(Ok(()));

        loop {
            if shutdown.is_stopped() {
                return Ok(());
            }

            let shutdown_ready = shutdown.wake.notified();
            tokio::pin!(shutdown_ready);
            let next_message = runtime.block_on(async {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_ready => Ok(None),
                    result = socket.recv() => result.map(message_to_frames).map(Some),
                }
            });

            match next_message {
                Ok(Some(message)) => {
                    runtime.block_on(socket.send(multipart_message(message)?))?;
                }
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }
    });

    (handle, ready_rx)
}
