use std::{fmt, future::Future, ops, pin::Pin, rc::Rc, task::Context, task::Poll};

use ntex::channel::{condition::Condition, condition::Waiter, oneshot};
use ntex::io::IoRef;
use ntex::util::{HashMap, PoolRef, Ready};

use crate::codec::protocol::{self as codec, Begin, Close, End, Error, Frame, Role};
use crate::codec::{types, AmqpCodec, AmqpFrame};
use crate::control::ControlQueue;
use crate::session::{Session, SessionInner, INITIAL_NEXT_OUTGOING_ID};
use crate::sndlink::{SenderLink, SenderLinkInner};
use crate::{cell::Cell, error::AmqpProtocolError, types::Action, Configuration};

pub struct Connection(ConnectionRef);

#[derive(Clone)]
pub struct ConnectionRef(pub(crate) Cell<ConnectionInner>);

#[derive(Debug)]
pub(crate) struct ConnectionInner {
    io: IoRef,
    state: ConnectionState,
    codec: AmqpCodec<AmqpFrame>,
    control_queue: Rc<ControlQueue>,
    pub(crate) sessions: slab::Slab<SessionState>,
    pub(crate) sessions_map: HashMap<u16, usize>,
    pub(crate) on_close: Condition,
    pub(crate) error: Option<AmqpProtocolError>,
    channel_max: u16,
    pub(crate) max_frame_size: u32,
}

#[derive(Debug)]
pub(crate) enum SessionState {
    Opening(Option<oneshot::Sender<Session>>, Cell<ConnectionInner>),
    Established(Cell<SessionInner>),
    Closing(Cell<SessionInner>),
}

impl SessionState {
    fn is_opening(&self) -> bool {
        matches!(self, SessionState::Opening(_, _))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ConnectionState {
    Normal,
    Closing,
    RemoteClose,
    Drop,
}

impl Connection {
    pub(crate) fn new(
        io: IoRef,
        local_config: &Configuration,
        remote_config: &Configuration,
    ) -> Connection {
        Connection(ConnectionRef(Cell::new(ConnectionInner {
            io,
            codec: AmqpCodec::new(),
            state: ConnectionState::Normal,
            sessions: slab::Slab::with_capacity(8),
            sessions_map: HashMap::default(),
            control_queue: Rc::default(),
            error: None,
            on_close: Condition::new(),
            channel_max: local_config.channel_max,
            max_frame_size: remote_config.max_frame_size,
        })))
    }

    pub fn get_ref(&self) -> ConnectionRef {
        self.0.clone()
    }
}

impl AsRef<ConnectionRef> for Connection {
    #[inline]
    fn as_ref(&self) -> &ConnectionRef {
        &self.0
    }
}

impl ops::Deref for Connection {
    type Target = ConnectionRef;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.0.force_close()
    }
}

impl ConnectionRef {
    #[inline]
    /// Get io tag for current connection
    pub fn tag(&self) -> &'static str {
        self.0.get_ref().io.tag()
    }

    #[inline]
    /// Force close connection
    pub fn force_close(&self) {
        let inner = self.0.get_mut();
        inner.state = ConnectionState::Drop;
        inner.io.force_close();
        inner.set_error(AmqpProtocolError::ConnectionDropped);
    }

    #[inline]
    /// Check connection state
    pub fn is_opened(&self) -> bool {
        let inner = self.0.get_mut();
        if inner.state != ConnectionState::Normal {
            return false;
        }
        inner.error.is_none() && !inner.io.is_closed()
    }

    /// Get waiter for `on_close` event
    pub fn on_close(&self) -> Waiter {
        self.0.get_ref().on_close.wait()
    }

    /// Get connection error
    pub fn get_error(&self) -> Option<AmqpProtocolError> {
        self.0.get_ref().error.clone()
    }

    /// Get existing session by local channel id
    pub fn get_session_by_local_id(&self, channel: u16) -> Option<Session> {
        if let Some(SessionState::Established(inner)) =
            self.0.get_ref().sessions.get(channel as usize)
        {
            Some(Session::new(inner.clone()))
        } else {
            None
        }
    }

    /// Gracefully close connection
    pub fn close(&self) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        let inner = self.0.get_mut();
        inner.post_frame(AmqpFrame::new(0, Frame::Close(Close { error: None })));
        inner.io.close();
        Ready::Ok(())
    }

    /// Close connection with error
    pub fn close_with_error<E>(&self, err: E) -> impl Future<Output = Result<(), AmqpProtocolError>>
    where
        Error: From<E>,
    {
        let inner = self.0.get_mut();
        inner.post_frame(AmqpFrame::new(
            0,
            Frame::Close(Close {
                error: Some(err.into()),
            }),
        ));
        inner.io.close();
        Ready::Ok(())
    }

    /// Opens the session
    pub fn open_session(&self) -> OpenSession {
        OpenSession::new(self.0.clone())
    }

    pub(crate) fn close_session(&self, id: usize) {
        if let Some(state) = self.0.get_mut().sessions.get_mut(id) {
            if let SessionState::Established(inner) = state {
                *state = SessionState::Closing(inner.clone());
            }
        }
    }

    pub(crate) fn post_frame(&self, frame: AmqpFrame) {
        let inner = self.0.get_mut();

        #[cfg(feature = "frame-trace")]
        log::trace!("{}: outgoing: {:#?}", inner.io.tag(), frame);

        if let Err(e) = inner.io.encode(frame, &inner.codec) {
            inner.set_error(e.into())
        }
    }

    pub(crate) fn set_error(&self, err: AmqpProtocolError) {
        self.0.get_mut().set_error(err)
    }

    pub(crate) fn get_control_queue(&self) -> &Rc<ControlQueue> {
        &self.0.get_ref().control_queue
    }

    pub(crate) fn handle_frame(&self, frame: AmqpFrame) -> Result<Action, AmqpProtocolError> {
        self.0.get_mut().handle_frame(frame, &self.0)
    }
}

impl ConnectionInner {
    pub(crate) fn memory_pool(&self) -> PoolRef {
        self.io.memory_pool()
    }

    pub(crate) fn set_error(&mut self, err: AmqpProtocolError) {
        log::trace!("{}: Set connection error: {:?}", self.io.tag(), err);
        for (_, channel) in self.sessions.iter_mut() {
            match channel {
                SessionState::Opening(_, _) | SessionState::Closing(_) => (),
                SessionState::Established(ref mut ses) => {
                    ses.get_mut().set_error(err.clone());
                }
            }
        }
        self.sessions.clear();
        self.sessions_map.clear();

        if self.error.is_none() {
            self.error = Some(err);
        }
        self.on_close.notify_and_lock_readiness();
    }

    pub(crate) fn post_frame(&mut self, frame: AmqpFrame) {
        #[cfg(feature = "frame-trace")]
        log::trace!("{}: outgoing: {:#?}", self.io.tag(), frame);

        if let Err(e) = self.io.encode(frame, &self.codec) {
            self.set_error(e.into())
        }
    }

    pub(crate) fn register_remote_session(
        &mut self,
        remote_channel_id: u16,
        begin: &Begin,
        cell: &Cell<ConnectionInner>,
    ) -> Result<(), AmqpProtocolError> {
        log::trace!(
            "{}: Remote session opened: {:?}",
            self.io.tag(),
            remote_channel_id
        );

        let entry = self.sessions.vacant_entry();
        let local_token = entry.key();

        let session = Cell::new(SessionInner::new(
            local_token,
            false,
            ConnectionRef(cell.clone()),
            remote_channel_id,
            begin.next_outgoing_id(),
            begin.incoming_window(),
            begin.outgoing_window(),
        ));
        entry.insert(SessionState::Established(session));
        self.sessions_map.insert(remote_channel_id, local_token);

        let begin = Begin(Box::new(codec::BeginInner {
            remote_channel: Some(remote_channel_id),
            next_outgoing_id: 1,
            incoming_window: u32::MAX,
            outgoing_window: begin.incoming_window(),
            handle_max: u32::MAX,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }));

        self.io
            .encode(
                AmqpFrame::new(local_token as u16, begin.into()),
                &self.codec,
            )
            .map(|_| ())
            .map_err(AmqpProtocolError::Codec)
    }

    pub(crate) fn complete_session_creation(
        &mut self,
        local_channel_id: u16,
        remote_channel_id: u16,
        begin: &Begin,
    ) {
        log::trace!(
            "{}: Begin response received: local {:?} remote {:?}",
            self.io.tag(),
            local_channel_id,
            remote_channel_id,
        );

        let local_token = local_channel_id as usize;

        if let Some(channel) = self.sessions.get_mut(local_token) {
            if channel.is_opening() {
                if let SessionState::Opening(tx, cell) = channel {
                    let session = Cell::new(SessionInner::new(
                        local_token,
                        true,
                        ConnectionRef(cell.clone()),
                        remote_channel_id,
                        begin.next_outgoing_id(),
                        begin.incoming_window(),
                        begin.outgoing_window(),
                    ));
                    self.sessions_map.insert(remote_channel_id, local_token);

                    // TODO: send end session if `tx` is None
                    tx.take()
                        .and_then(|tx| tx.send(Session::new(session.clone())).err());
                    *channel = SessionState::Established(session);

                    log::trace!(
                        "{}: Session established: local {:?} remote {:?}",
                        self.io.tag(),
                        local_channel_id,
                        remote_channel_id,
                    );
                }
            } else {
                // TODO: send error response
                log::warn!("{}: Begin received for channel not in opening state. local channel: {} (remote channel: {})", self.io.tag(), local_channel_id, remote_channel_id);
            }
        } else {
            // TODO: rogue begin right now - do nothing. in future might indicate incoming attach
            log::warn!(
                "{}: Begin received for unknown local channel: {} (remote channel: {})",
                self.io.tag(),
                local_channel_id,
                remote_channel_id
            );
        }
    }

    fn handle_frame(
        &mut self,
        frame: AmqpFrame,
        inner: &Cell<ConnectionInner>,
    ) -> Result<Action, AmqpProtocolError> {
        let (channel_id, frame) = frame.into_parts();

        match frame {
            Frame::Empty => {
                return Ok(Action::None);
            }
            Frame::Close(close) => {
                if self.state == ConnectionState::Closing {
                    log::trace!("{}: Connection closed: {:?}", self.io.tag(), close);
                    self.set_error(AmqpProtocolError::Disconnected);
                    return Ok(Action::None);
                } else {
                    log::trace!("{}: Connection closed remotely: {:?}", self.io.tag(), close);
                    let err = AmqpProtocolError::Closed(close.error);
                    self.set_error(err.clone());
                    let close = Close { error: None };
                    self.post_frame(AmqpFrame::new(0, close.into()));
                    self.state = ConnectionState::RemoteClose;
                    return Ok(Action::RemoteClose(err));
                }
            }
            Frame::Begin(begin) => {
                // response Begin for open session
                // the remote-channel property in the frame is the local channel id
                // we previously sent to the remote
                if let Some(local_channel_id) = begin.remote_channel() {
                    self.complete_session_creation(local_channel_id, channel_id, &begin);
                } else {
                    self.register_remote_session(channel_id, &begin, inner)?;
                }
                return Ok(Action::None);
            }
            _ => (),
        }

        if self.error.is_some() {
            log::error!(
                "{}: Connection closed but new framed is received: {:?}",
                self.io.tag(),
                frame
            );
            return Ok(Action::None);
        }

        // get local session id
        let state = if let Some(token) = self.sessions_map.get(&channel_id) {
            if let Some(state) = self.sessions.get_mut(*token) {
                state
            } else {
                log::error!("{}: Inconsistent internal state", self.io.tag());
                return Err(AmqpProtocolError::UnknownSession(frame));
            }
        } else {
            return Err(AmqpProtocolError::UnknownSession(frame));
        };

        // handle session frames
        match state {
            SessionState::Opening(_, _) => {
                log::error!(
                    "{}: Unexpected opening state: {}",
                    self.io.tag(),
                    channel_id
                );
                Err(AmqpProtocolError::UnexpectedOpeningState(frame))
            }
            SessionState::Established(ref mut session) => match frame {
                Frame::Attach(attach) => {
                    let cell = session.clone();
                    if session.get_mut().handle_attach(&attach, cell) {
                        Ok(Action::None)
                    } else {
                        match attach.0.role {
                            Role::Receiver => {
                                // remotly opened sender link
                                let (id, response) = session.get_mut().new_remote_sender(&attach);
                                let link = SenderLink::new(Cell::new(SenderLinkInner::with(
                                    id,
                                    &attach,
                                    session.clone(),
                                )));
                                Ok(Action::AttachSender(link, attach, response))
                            }
                            Role::Sender => {
                                // receiver link
                                let (response, link) = session
                                    .get_mut()
                                    .attach_remote_receiver_link(session.clone(), &attach);
                                Ok(Action::AttachReceiver(link, attach, response))
                            }
                        }
                    }
                }
                Frame::End(remote_end) => {
                    log::trace!("{}: Remote session end: {}", self.io.tag(), channel_id);
                    let id = session.get_mut().id();
                    let action = session
                        .get_mut()
                        .end(AmqpProtocolError::SessionEnded(remote_end.error));
                    if let Some(token) = self.sessions_map.remove(&channel_id) {
                        self.sessions.remove(token);
                    }
                    self.post_frame(AmqpFrame::new(id, End { error: None }.into()));
                    Ok(action)
                }
                _ => session.get_mut().handle_frame(frame),
            },
            SessionState::Closing(ref mut session) => match frame {
                Frame::End(frm) => {
                    log::trace!("{}: Session end is confirmed: {:?}", self.io.tag(), frm);
                    let _ = session
                        .get_mut()
                        .end(AmqpProtocolError::SessionEnded(frm.error));
                    if let Some(token) = self.sessions_map.remove(&channel_id) {
                        self.sessions.remove(token);
                    }
                    Ok(Action::None)
                }
                frm => {
                    log::trace!(
                        "{}: Got frame after initiated session end: {:?}",
                        self.io.tag(),
                        frm
                    );
                    Ok(Action::None)
                }
            },
        }
    }
}

impl fmt::Debug for ConnectionRef {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ConnectionRef").finish()
    }
}

/// Open new session
pub struct OpenSession {
    con: Cell<ConnectionInner>,
    fut: Option<Pin<Box<dyn Future<Output = Result<Session, AmqpProtocolError>>>>>,
    props: Option<HashMap<types::Symbol, types::Variant>>,
    offered_capabilities: Option<codec::Symbols>,
    desired_capabilities: Option<codec::Symbols>,
}

impl OpenSession {
    pub(crate) fn new(con: Cell<ConnectionInner>) -> Self {
        Self {
            con,
            fut: None,
            props: None,
            offered_capabilities: None,
            desired_capabilities: None,
        }
    }

    /// Set session offered capabilities
    pub fn offered_capabilities(mut self, caps: codec::Symbols) -> Self {
        self.offered_capabilities = Some(caps);
        self
    }

    /// Set session desired capabilities
    pub fn desired_capabilities(mut self, caps: codec::Symbols) -> Self {
        self.desired_capabilities = Some(caps);
        self
    }

    pub fn property<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<types::Symbol>,
        V: Into<types::Variant>,
    {
        if self.props.is_none() {
            self.props = Some(HashMap::default());
        }
        self.props
            .as_mut()
            .unwrap()
            .insert(key.into(), value.into());
        self
    }

    pub async fn attach(self) -> Result<Session, AmqpProtocolError> {
        open_session(
            self.con,
            self.offered_capabilities,
            self.desired_capabilities,
            self.props,
        )
        .await
    }
}

impl Future for OpenSession {
    type Output = Result<Session, AmqpProtocolError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut slf = self.as_mut();

        if slf.fut.is_none() {
            slf.fut = Some(Box::pin(open_session(
                slf.con.clone(),
                slf.offered_capabilities.take(),
                slf.desired_capabilities.take(),
                slf.props.take(),
            )));
        }

        Pin::new(slf.fut.as_mut().unwrap()).poll(cx)
    }
}

async fn open_session(
    con: Cell<ConnectionInner>,
    offered_capabilities: Option<codec::Symbols>,
    desired_capabilities: Option<codec::Symbols>,
    properties: Option<HashMap<types::Symbol, types::Variant>>,
) -> Result<Session, AmqpProtocolError> {
    let inner = con.get_mut();

    if let Some(ref e) = inner.error {
        log::error!("{}: Connection is in error state: {:?}", inner.io.tag(), e);
        Err(e.clone())
    } else {
        let (tx, rx) = oneshot::channel();

        let entry = inner.sessions.vacant_entry();
        let token = entry.key();

        if token >= inner.channel_max as usize {
            log::trace!("{}: Too many channels: {:?}", inner.io.tag(), token);
            Err(AmqpProtocolError::TooManyChannels)
        } else {
            entry.insert(SessionState::Opening(Some(tx), con.clone()));

            let begin = Begin(Box::new(codec::BeginInner {
                offered_capabilities,
                desired_capabilities,
                properties,
                remote_channel: None,
                next_outgoing_id: INITIAL_NEXT_OUTGOING_ID,
                incoming_window: u32::MAX,
                outgoing_window: u32::MAX,
                handle_max: u32::MAX,
            }));
            inner.post_frame(AmqpFrame::new(token as u16, begin.into()));
            let _ = inner;

            rx.await.map_err(|_| AmqpProtocolError::Disconnected)
        }
    }
}
