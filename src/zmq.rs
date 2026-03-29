#![allow(dead_code)]

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc as tokio_mpsc;
use zeromq::Socket as _;
use zeromq::SocketRecv as _;
use zeromq::SocketSend as _;

pub const POLLIN: i16 = 1;
pub const ROUTER: SocketType = SocketType::Router;
pub const PUB: SocketType = SocketType::Pub;
pub const SUB: SocketType = SocketType::Sub;
pub const DEALER: SocketType = SocketType::Dealer;
pub const REQ: SocketType = SocketType::Req;
pub const REP: SocketType = SocketType::Rep;

#[derive(Clone, Copy, Debug)]
pub enum SocketType {
    Router,
    Pub,
    Sub,
    Dealer,
    Req,
    Rep,
}

#[derive(Debug, Clone)]
pub enum Error {
    EAGAIN,
    ETERM,
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EAGAIN => f.write_str("resource temporarily unavailable"),
            Self::ETERM => f.write_str("context terminated"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

impl From<zeromq::ZmqError> for Error {
    fn from(error: zeromq::ZmqError) -> Self {
        Self::Other(error.to_string())
    }
}

#[derive(Clone)]
pub struct Context {
    runtime: Arc<Runtime>,
}

impl Context {
    pub fn new() -> Self {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for zeromq");
        Self {
            runtime: Arc::new(runtime),
        }
    }

    pub fn socket(&self, socket_type: SocketType) -> Result<Socket, Error> {
        Socket::new(self.runtime.clone(), socket_type)
    }
}

pub struct PollItem<'a> {
    socket: &'a Socket,
    readable: bool,
}

impl<'a> PollItem<'a> {
    pub fn is_readable(&self) -> bool {
        self.readable
    }
}

pub fn poll(items: &mut [PollItem<'_>], timeout_ms: i64) -> Result<i32, Error> {
    let timeout = if timeout_ms < 0 {
        None
    } else {
        Some(Duration::from_millis(timeout_ms as u64))
    };
    let deadline = timeout.map(|duration| Instant::now() + duration);

    loop {
        let mut readable_count = 0;
        for item in items.iter_mut() {
            item.readable = item.socket.has_pending_events();
            if item.readable {
                readable_count += 1;
            }
        }

        if readable_count > 0 {
            return Ok(readable_count);
        }

        if let Some(deadline) = deadline {
            if Instant::now() >= deadline {
                return Ok(0);
            }
        }

        std::thread::sleep(Duration::from_millis(1));
    }
}

pub struct Socket {
    command_tx: tokio_mpsc::UnboundedSender<Command>,
    incoming: Arc<IncomingQueue>,
    rcvtimeo: Mutex<Option<Duration>>,
    sndtimeo: Mutex<Option<Duration>>,
}

impl Socket {
    fn new(runtime: Arc<Runtime>, socket_type: SocketType) -> Result<Self, Error> {
        let (command_tx, command_rx) = tokio_mpsc::unbounded_channel();
        let incoming = Arc::new(IncomingQueue::default());
        let actor_incoming = Arc::clone(&incoming);

        runtime.spawn(async move {
            socket_actor(socket_type, command_rx, actor_incoming).await;
        });

        Ok(Self {
            command_tx,
            incoming,
            rcvtimeo: Mutex::new(None),
            sndtimeo: Mutex::new(None),
        })
    }

    pub fn set_linger(&self, _linger_ms: i32) -> Result<(), Error> {
        Ok(())
    }

    pub fn set_rcvtimeo(&self, timeout_ms: i32) -> Result<(), Error> {
        let timeout = if timeout_ms < 0 {
            None
        } else {
            Some(Duration::from_millis(timeout_ms as u64))
        };
        *self.rcvtimeo.lock().expect("rcvtimeo mutex poisoned") = timeout;
        Ok(())
    }

    pub fn set_sndtimeo(&self, timeout_ms: i32) -> Result<(), Error> {
        let timeout = if timeout_ms < 0 {
            None
        } else {
            Some(Duration::from_millis(timeout_ms as u64))
        };
        *self.sndtimeo.lock().expect("sndtimeo mutex poisoned") = timeout;
        Ok(())
    }

    pub fn set_identity(&self, identity: &[u8]) -> Result<(), Error> {
        self.request(CommandKind::SetIdentity(identity.to_vec()))
    }

    pub fn set_subscribe(&self, subscription: &[u8]) -> Result<(), Error> {
        self.request(CommandKind::Subscribe(subscription.to_vec()))
    }

    pub fn bind(&self, endpoint: &str) -> Result<(), Error> {
        self.request(CommandKind::Bind(endpoint.to_owned()))
    }

    pub fn connect(&self, endpoint: &str) -> Result<(), Error> {
        self.request(CommandKind::Connect(endpoint.to_owned()))
    }

    pub fn recv_multipart(&self, _flags: i32) -> Result<Vec<Vec<u8>>, Error> {
        let timeout = *self.rcvtimeo.lock().expect("rcvtimeo mutex poisoned");
        self.incoming.recv(timeout)
    }

    pub fn send_multipart(&self, frames: Vec<Vec<u8>>, _flags: i32) -> Result<(), Error> {
        let _timeout = *self.sndtimeo.lock().expect("sndtimeo mutex poisoned");
        self.request(CommandKind::SendMultipart(frames))
    }

    pub fn recv_bytes(&self, flags: i32) -> Result<Vec<u8>, Error> {
        let frames = self.recv_multipart(flags)?;
        if frames.len() != 1 {
            return Err(Error::Other(format!(
                "expected single frame, got {}",
                frames.len()
            )));
        }
        Ok(frames.into_iter().next().unwrap())
    }

    pub fn send<T: AsRef<[u8]>>(&self, data: T, flags: i32) -> Result<(), Error> {
        self.send_multipart(vec![data.as_ref().to_vec()], flags)
    }

    pub fn as_poll_item(&self, _events: i16) -> PollItem<'_> {
        PollItem {
            socket: self,
            readable: false,
        }
    }

    fn has_pending_events(&self) -> bool {
        self.incoming.has_pending()
    }

    fn request(&self, kind: CommandKind) -> Result<(), Error> {
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        self.command_tx
            .send(Command { kind, response_tx })
            .map_err(|_| Error::ETERM)?;
        response_rx.recv().map_err(|_| Error::ETERM)?
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        self.incoming.close();
    }
}

#[derive(Default)]
struct IncomingQueue {
    queue: Mutex<VecDeque<Result<Vec<Vec<u8>>, Error>>>,
    cvar: Condvar,
    closed: AtomicBool,
}

impl IncomingQueue {
    fn push(&self, event: Result<Vec<Vec<u8>>, Error>) {
        let mut queue = self.queue.lock().expect("incoming queue mutex poisoned");
        queue.push_back(event);
        self.cvar.notify_all();
    }

    fn recv(&self, timeout: Option<Duration>) -> Result<Vec<Vec<u8>>, Error> {
        let mut queue = self.queue.lock().expect("incoming queue mutex poisoned");

        if let Some(event) = queue.pop_front() {
            return event;
        }

        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::ETERM);
        }

        match timeout {
            Some(timeout) if timeout.is_zero() => Err(Error::EAGAIN),
            Some(timeout) => {
                let deadline = Instant::now() + timeout;
                loop {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let (next_queue, wait_result) = self
                        .cvar
                        .wait_timeout(queue, remaining)
                        .expect("incoming queue condvar poisoned");
                    queue = next_queue;

                    if let Some(event) = queue.pop_front() {
                        return event;
                    }
                    if self.closed.load(Ordering::SeqCst) {
                        return Err(Error::ETERM);
                    }
                    if wait_result.timed_out() {
                        return Err(Error::EAGAIN);
                    }
                }
            }
            None => loop {
                queue = self
                    .cvar
                    .wait(queue)
                    .expect("incoming queue condvar poisoned");
                if let Some(event) = queue.pop_front() {
                    return event;
                }
                if self.closed.load(Ordering::SeqCst) {
                    return Err(Error::ETERM);
                }
            },
        }
    }

    fn has_pending(&self) -> bool {
        !self
            .queue
            .lock()
            .expect("incoming queue mutex poisoned")
            .is_empty()
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.cvar.notify_all();
    }
}

struct Command {
    kind: CommandKind,
    response_tx: mpsc::SyncSender<Result<(), Error>>,
}

enum CommandKind {
    Bind(String),
    Connect(String),
    SendMultipart(Vec<Vec<u8>>),
    SetIdentity(Vec<u8>),
    Subscribe(Vec<u8>),
}

async fn socket_actor(
    socket_type: SocketType,
    mut command_rx: tokio_mpsc::UnboundedReceiver<Command>,
    incoming: Arc<IncomingQueue>,
) {
    let mut actor = SocketActor::new(socket_type);

    loop {
        tokio::select! {
            command = command_rx.recv() => {
                match command {
                    Some(command) => {
                        let result = actor.handle_command(command.kind).await;
                        let _ = command.response_tx.send(result);
                    }
                    None => break,
                }
            }
            recv_result = actor.recv(), if actor.can_recv() => {
                match recv_result {
                    Ok(message) => incoming.push(Ok(message)),
                    Err(Error::EAGAIN) => {}
                    Err(error) => {
                        incoming.push(Err(error));
                        break;
                    }
                }
            }
        }
    }

    incoming.close();
}

struct SocketActor {
    socket_type: SocketType,
    inner: ActorSocket,
    identity: Option<Vec<u8>>,
    subscriptions: Vec<Vec<u8>>,
    active: bool,
    recv_enabled: bool,
}

impl SocketActor {
    fn new(socket_type: SocketType) -> Self {
        Self {
            socket_type,
            inner: ActorSocket::new(socket_type, None),
            identity: None,
            subscriptions: Vec::new(),
            active: false,
            recv_enabled: false,
        }
    }

    fn can_recv(&self) -> bool {
        self.active && self.recv_enabled && self.inner.can_recv()
    }

    async fn handle_command(&mut self, command: CommandKind) -> Result<(), Error> {
        match command {
            CommandKind::Bind(endpoint) => {
                self.inner.bind(&endpoint).await?;
                self.active = true;
                self.recv_enabled = matches!(
                    self.socket_type,
                    SocketType::Router | SocketType::Sub | SocketType::Dealer | SocketType::Rep
                );
                Ok(())
            }
            CommandKind::Connect(endpoint) => {
                self.inner.connect(&endpoint).await?;
                self.active = true;
                self.recv_enabled = matches!(
                    self.socket_type,
                    SocketType::Router | SocketType::Sub | SocketType::Dealer | SocketType::Rep
                );
                if let ActorSocket::Sub(socket) = &mut self.inner {
                    for subscription in &self.subscriptions {
                        socket
                            .subscribe(&String::from_utf8_lossy(subscription))
                            .await?;
                    }
                }
                Ok(())
            }
            CommandKind::SendMultipart(frames) => {
                self.inner.send(frames).await?;
                if matches!(self.socket_type, SocketType::Req | SocketType::Rep) {
                    self.recv_enabled = true;
                }
                Ok(())
            }
            CommandKind::SetIdentity(identity) => {
                if self.active {
                    return Err(Error::Other(
                        "socket identity must be set before connect or bind".to_owned(),
                    ));
                }
                self.identity = Some(identity.clone());
                self.inner = ActorSocket::new(self.socket_type, Some(identity));
                Ok(())
            }
            CommandKind::Subscribe(subscription) => {
                self.subscriptions.push(subscription.clone());
                if let ActorSocket::Sub(socket) = &mut self.inner {
                    socket
                        .subscribe(&String::from_utf8_lossy(&subscription))
                        .await?;
                }
                Ok(())
            }
        }
    }

    async fn recv(&mut self) -> Result<Vec<Vec<u8>>, Error> {
        let message = self.inner.recv().await?;
        if matches!(self.socket_type, SocketType::Req | SocketType::Rep) {
            self.recv_enabled = false;
        }
        Ok(message)
    }
}

enum ActorSocket {
    Router(zeromq::RouterSocket),
    Pub(zeromq::PubSocket),
    Sub(zeromq::SubSocket),
    Dealer(zeromq::DealerSocket),
    Req(zeromq::ReqSocket),
    Rep(zeromq::RepSocket),
}

impl ActorSocket {
    fn new(socket_type: SocketType, identity: Option<Vec<u8>>) -> Self {
        match socket_type {
            SocketType::Router => Self::Router(zeromq::RouterSocket::new()),
            SocketType::Pub => Self::Pub(zeromq::PubSocket::new()),
            SocketType::Sub => Self::Sub(zeromq::SubSocket::new()),
            SocketType::Dealer => Self::Dealer(dealer_socket(identity)),
            SocketType::Req => Self::Req(req_socket(identity)),
            SocketType::Rep => Self::Rep(zeromq::RepSocket::new()),
        }
    }

    fn can_recv(&self) -> bool {
        !matches!(self, Self::Pub(_))
    }

    async fn bind(&mut self, endpoint: &str) -> Result<(), Error> {
        match self {
            Self::Router(socket) => socket.bind(endpoint).await.map(|_| ()).map_err(Into::into),
            Self::Pub(socket) => socket.bind(endpoint).await.map(|_| ()).map_err(Into::into),
            Self::Sub(socket) => socket.bind(endpoint).await.map(|_| ()).map_err(Into::into),
            Self::Dealer(socket) => socket.bind(endpoint).await.map(|_| ()).map_err(Into::into),
            Self::Req(socket) => socket.bind(endpoint).await.map(|_| ()).map_err(Into::into),
            Self::Rep(socket) => socket.bind(endpoint).await.map(|_| ()).map_err(Into::into),
        }
    }

    async fn connect(&mut self, endpoint: &str) -> Result<(), Error> {
        match self {
            Self::Router(socket) => socket.connect(endpoint).await.map_err(Into::into),
            Self::Pub(socket) => socket.connect(endpoint).await.map_err(Into::into),
            Self::Sub(socket) => socket.connect(endpoint).await.map_err(Into::into),
            Self::Dealer(socket) => socket.connect(endpoint).await.map_err(Into::into),
            Self::Req(socket) => socket.connect(endpoint).await.map_err(Into::into),
            Self::Rep(socket) => socket.connect(endpoint).await.map_err(Into::into),
        }
    }

    async fn send(&mut self, frames: Vec<Vec<u8>>) -> Result<(), Error> {
        let message = multipart_message(frames)?;
        match self {
            Self::Router(socket) => socket.send(message).await.map_err(Into::into),
            Self::Pub(socket) => socket.send(message).await.map_err(Into::into),
            Self::Sub(_) => Err(Error::Other("cannot send on SUB socket".to_owned())),
            Self::Dealer(socket) => socket.send(message).await.map_err(Into::into),
            Self::Req(socket) => socket.send(message).await.map_err(Into::into),
            Self::Rep(socket) => socket.send(message).await.map_err(Into::into),
        }
    }

    async fn recv(&mut self) -> Result<Vec<Vec<u8>>, Error> {
        let message = match self {
            Self::Router(socket) => socket.recv().await,
            Self::Pub(_) => return Err(Error::EAGAIN),
            Self::Sub(socket) => socket.recv().await,
            Self::Dealer(socket) => socket.recv().await,
            Self::Req(socket) => socket.recv().await,
            Self::Rep(socket) => socket.recv().await,
        };

        match message {
            Ok(message) => Ok(message_to_vec(message)),
            Err(zeromq::ZmqError::NoMessage) => Err(Error::EAGAIN),
            Err(error) => Err(error.into()),
        }
    }
}

fn dealer_socket(identity: Option<Vec<u8>>) -> zeromq::DealerSocket {
    if let Some(identity) = identity {
        let mut options = zeromq::SocketOptions::default();
        options.peer_identity(
            zeromq::util::PeerIdentity::try_from(identity)
                .expect("peer identity validated by zeromq"),
        );
        zeromq::DealerSocket::with_options(options)
    } else {
        zeromq::DealerSocket::new()
    }
}

fn req_socket(identity: Option<Vec<u8>>) -> zeromq::ReqSocket {
    if let Some(identity) = identity {
        let mut options = zeromq::SocketOptions::default();
        options.peer_identity(
            zeromq::util::PeerIdentity::try_from(identity)
                .expect("peer identity validated by zeromq"),
        );
        zeromq::ReqSocket::with_options(options)
    } else {
        zeromq::ReqSocket::new()
    }
}

fn multipart_message(frames: Vec<Vec<u8>>) -> Result<zeromq::ZmqMessage, Error> {
    let frames: Vec<Bytes> = frames.into_iter().map(Bytes::from).collect();
    zeromq::ZmqMessage::try_from(frames)
        .map_err(|_| Error::Other("empty multipart messages are not supported".to_owned()))
}

fn message_to_vec(message: zeromq::ZmqMessage) -> Vec<Vec<u8>> {
    message.into_vec().into_iter().map(|frame| frame.to_vec()).collect()
}
