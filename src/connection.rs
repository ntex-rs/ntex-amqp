use std::{fmt, future::Future, ops, pin::Pin, rc::Rc, task::Context, task::Poll};

use ntex_io::{IoConfig, IoRef};
use ntex_service::cfg::Cfg;
use ntex_util::channel::{condition::Condition, condition::Waiter, oneshot};
use ntex_util::{HashMap, time::Seconds};

use crate::codec::protocol::{
    self as codec, AmqpError, Begin, Close, End, Error, ErrorCondition, Frame, Role, SessionError,
};
use crate::codec::{AmqpCodec, AmqpFrame, types};
use crate::control::ControlQueue;
use crate::session::{INITIAL_NEXT_OUTGOING_ID, Session, SessionInner};
use crate::{
    AmqpServiceConfig, RemoteServiceConfig, cell::Cell, detach, error::AmqpProtocolError,
    types::Action,
};

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
    handle_max: u32,
    pub(crate) max_frame_size: u32,
    pub(crate) link_attach_timeout: Seconds,
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
        local_config: &Cfg<AmqpServiceConfig>,
        remote_config: &RemoteServiceConfig,
    ) -> Connection {
        Connection(ConnectionRef(Cell::new(ConnectionInner {
            io,
            codec: AmqpCodec::new().max_encode_size(remote_config.max_frame_size as usize),
            state: ConnectionState::Normal,
            sessions: slab::Slab::with_capacity(8),
            sessions_map: HashMap::default(),
            control_queue: Rc::default(),
            error: None,
            on_close: Condition::new(),
            channel_max: local_config.channel_max,
            handle_max: local_config.handle_max,
            max_frame_size: remote_config.max_frame_size,
            link_attach_timeout: local_config.link_attach_timeout,
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
        self.0.force_close();
    }
}

impl ConnectionRef {
    #[inline]
    /// Get io tag for current connection
    pub fn tag(&self) -> &'static str {
        self.0.get_ref().io.tag()
    }

    #[inline]
    /// Get io configuration for current connection
    pub fn config(&self) -> &IoConfig {
        self.0.get_ref().io.cfg()
    }

    #[inline]
    /// Force close connection
    pub fn force_close(&self) {
        let inner = self.0.get_mut();
        inner.state = ConnectionState::Drop;
        inner.io.terminate();
        inner.set_error(AmqpProtocolError::ConnectionDropped);
    }

    #[inline]
    /// Check connection state
    pub fn is_opened(&self) -> bool {
        let inner = self.0.get_mut();
        if inner.state != ConnectionState::Normal {
            return false;
        }
        inner.error.is_none() && inner.io.is_active()
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
    pub async fn close(&self) -> Result<(), AmqpProtocolError> {
        let inner = self.0.get_mut();
        inner.post_frame(AmqpFrame::new(0, Frame::Close(Close { error: None })));
        inner.io.close();
        Ok(())
    }

    /// Close connection with error
    pub async fn close_with_error<E>(&self, err: E) -> Result<(), AmqpProtocolError>
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
        Ok(())
    }

    /// Opens the session
    pub fn open_session(&self) -> OpenSession {
        OpenSession::new(self.0.clone())
    }

    /// Mark session as closing, `local_id` is local channel id
    pub(crate) fn close_session(&self, local_id: usize) {
        if let Some(state) = self.0.get_mut().sessions.get_mut(local_id)
            && let SessionState::Established(inner) = state
        {
            *state = SessionState::Closing(inner.clone());
        }
    }

    pub(crate) fn post_frame(&self, frame: AmqpFrame) {
        let inner = self.0.get_mut();

        #[cfg(feature = "frame-trace")]
        log::trace!("{}: outgoing: {:#?}", inner.io.tag(), frame);

        if let Err(e) = inner.io.encode(frame, &inner.codec) {
            inner.set_error(e.into());
        }
    }

    pub(crate) fn set_error(&self, err: AmqpProtocolError) {
        self.0.get_mut().set_error(err);
    }

    pub(crate) fn get_control_queue(&self) -> &Rc<ControlQueue> {
        &self.0.get_ref().control_queue
    }

    pub(crate) fn handle_frame(&self, frame: AmqpFrame) -> Result<Action, AmqpProtocolError> {
        self.0.get_mut().handle_frame(frame, &self.0)
    }
}

impl ConnectionInner {
    pub(crate) fn set_error(&mut self, err: AmqpProtocolError) {
        log::trace!("{}: Set connection error: {:?}", self.io.tag(), err);
        for (_, channel) in &mut self.sessions {
            match channel {
                SessionState::Opening(_, _) => (),
                // closing session waits for remote end, links and `end()` wait as well
                SessionState::Established(ses) | SessionState::Closing(ses) => {
                    ses.get_mut().set_error(err.clone());
                }
            }
        }
        self.sessions.clear();
        self.sessions_map.clear();

        if self.error.is_none() {
            self.error = Some(err);
        }
        self.on_close.notify_and_lock(());
    }

    pub(crate) fn post_frame(&mut self, frame: AmqpFrame) {
        #[cfg(feature = "frame-trace")]
        log::trace!("{}: outgoing: {:#?}", self.io.tag(), frame);

        if let Err(e) = self.io.encode(frame, &self.codec) {
            self.set_error(e.into());
        }
    }

    pub(crate) fn register_remote_session(
        &mut self,
        remote_channel_id: u16,
        begin: Begin,
        cell: &Cell<ConnectionInner>,
    ) -> Result<(), AmqpProtocolError> {
        log::trace!(
            "{}: Remote session opened: {:?}",
            self.io.tag(),
            remote_channel_id
        );

        let entry = self.sessions.vacant_entry();
        let local_token = entry.key();
        if remote_channel_id > self.channel_max || local_token > self.channel_max as usize {
            log::trace!(
                "{}: Too many channels: {remote_channel_id} {local_token}",
                self.io.tag()
            );
            return Err(AmqpProtocolError::TooManyChannels);
        }
        let session = Cell::new(SessionInner::new(
            local_token,
            false,
            ConnectionRef(cell.clone()),
            remote_channel_id,
            begin,
        ));
        entry.insert(SessionState::Established(session));
        self.sessions_map.insert(remote_channel_id, local_token);

        let begin = Begin(Box::new(codec::BeginInner {
            outgoing_window: u32::MAX,
            remote_channel: Some(remote_channel_id),
            next_outgoing_id: INITIAL_NEXT_OUTGOING_ID,
            incoming_window: u32::MAX,
            handle_max: self.handle_max,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }));

        self.io
            .encode(
                AmqpFrame::new(local_token as u16, begin.into()),
                &self.codec,
            )
            .map_err(AmqpProtocolError::Codec)
    }

    pub(crate) fn complete_session_creation(
        &mut self,
        local_channel_id: u16,
        remote_channel_id: u16,
        begin: Begin,
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
                        begin,
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
                log::warn!(
                    "{}: Begin received for channel not in opening state. local channel: {} (remote channel: {})",
                    self.io.tag(),
                    local_channel_id,
                    remote_channel_id
                );
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
            Frame::Empty => Ok(Action::None),
            Frame::Close(close) => {
                if self.state == ConnectionState::Closing {
                    log::trace!("{}: Connection closed: {:?}", self.io.tag(), close);
                    self.set_error(AmqpProtocolError::Disconnected);
                    Ok(Action::None)
                } else {
                    log::trace!("{}: Connection closed remotely: {:?}", self.io.tag(), close);
                    let err = AmqpProtocolError::Closed(close.error);
                    self.set_error(err.clone());
                    let close = Close { error: None };
                    self.post_frame(AmqpFrame::new(0, close.into()));
                    self.state = ConnectionState::RemoteClose;
                    Ok(Action::RemoteClose(err))
                }
            }
            Frame::Begin(begin) => {
                if self.sessions_map.contains_key(&channel_id) {
                    log::trace!("{}: Channel {channel_id} is in use", self.io.tag());
                    return Err(AmqpProtocolError::Unexpected(Frame::Begin(begin)));
                }
                // begin frame is stored for session lifetime
                let begin = detach(&begin);

                // response Begin for open session
                // the remote-channel property in the frame is the local channel id
                // we previously sent to the remote
                if let Some(local_channel_id) = begin.remote_channel() {
                    self.complete_session_creation(local_channel_id, channel_id, begin);
                } else {
                    self.register_remote_session(channel_id, begin, inner)?;
                }
                Ok(Action::None)
            }
            _ => {
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
                    SessionState::Established(session) => match frame {
                        Frame::Attach(attach) => {
                            let handle = attach.handle();
                            let mut condition = if handle > self.handle_max {
                                Some(ErrorCondition::AmqpError(AmqpError::ResourceLimitExceeded))
                            } else if session.get_ref().is_remote_handle_used(handle) {
                                Some(ErrorCondition::SessionError(SessionError::HandleInUse))
                            } else {
                                None
                            };

                            // attach frame is stored for link lifetime
                            let attach = detach(&attach);
                            if condition.is_none() {
                                let cell = session.clone();
                                if session.get_mut().handle_attach(&attach, cell) {
                                    return Ok(Action::None);
                                }
                                // remotely opened link, local handle must be within remote handle-max
                                if !session.get_ref().check_handle() {
                                    condition = Some(ErrorCondition::AmqpError(
                                        AmqpError::ResourceLimitExceeded,
                                    ));
                                }
                            }

                            if let Some(condition) = condition {
                                log::trace!(
                                    "{}: Cannot attach link with handle {handle}: {condition:?}",
                                    self.io.tag()
                                );
                                let err = Error(Box::new(codec::ErrorInner {
                                    condition,
                                    description: None,
                                    info: None,
                                }));
                                let id = session.get_ref().id();
                                let action = session
                                    .get_mut()
                                    .end(AmqpProtocolError::SessionEnded(Some(err.clone())));
                                *state = SessionState::Closing(session.clone());
                                self.post_frame(AmqpFrame::new(
                                    id,
                                    End { error: Some(err) }.into(),
                                ));
                                return Ok(action);
                            }

                            match attach.0.role {
                                Role::Receiver => {
                                    // remotly opened sender link
                                    let (link, response) = session
                                        .get_mut()
                                        .new_remote_sender(session.clone(), &attach);
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
                    SessionState::Closing(session) => match frame {
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

    #[must_use]
    /// Set session offered capabilities
    pub fn offered_capabilities(mut self, caps: codec::Symbols) -> Self {
        self.offered_capabilities = Some(caps);
        self
    }

    #[must_use]
    /// Set session desired capabilities
    pub fn desired_capabilities(mut self, caps: codec::Symbols) -> Self {
        self.desired_capabilities = Some(caps);
        self
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    /// Set session property
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

    /// Attach session
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

        if token > inner.channel_max as usize {
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
                handle_max: inner.handle_max,
            }));
            inner.post_frame(AmqpFrame::new(token as u16, begin.into()));
            let _ = inner;

            rx.await.map_err(|_| AmqpProtocolError::Disconnected)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use ntex::codec::{Decoder, Encoder};
    use ntex_amqp_codec::AmqpCodecError;
    use ntex_amqp_codec::protocol::{
        Attach, AttachInner, Begin, BeginInner, DeliveryState, Detach, DetachInner, Disposition,
        DispositionInner, Flow, FlowInner, LinkError, Open, OpenInner, ReceiverSettleMode,
        Rejected, SenderSettleMode, Source, Target, TerminusDurability, TerminusExpiryPolicy,
        Transfer, TransferBody, TransferInner,
    };
    use ntex_amqp_codec::types::{Multiple, Symbol, Variant};
    use ntex_bytes::{BytePages, Bytes, BytesMut};
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::cfg::SharedCfg;

    use super::*;
    use crate::delivery::DeliveryInner;
    use crate::sndlink::SenderLink;

    const LONG: &str = "value-that-does-not-fit-into-inline-storage";

    /// Decode frame from a read buffer, returns frame and the buffer
    fn read(frame: Frame) -> (Frame, BytesMut) {
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut pages = BytePages::default();
        codec.encode(AmqpFrame::new(0, frame), &mut pages).unwrap();
        let mut buf = BytesMut::with_capacity(16 * 1024);
        buf.extend_from_slice(&pages.freeze());
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert!(buf.is_empty() && !buf.is_unique());
        (frame.into_parts().1, buf)
    }

    fn symbols() -> Multiple<Symbol> {
        Multiple(vec![Symbol::from(LONG)])
    }

    fn rejected() -> DeliveryState {
        DeliveryState::Rejected(Rejected {
            error: Some(Error(Box::new(codec::ErrorInner {
                condition: AmqpError::InternalError.into(),
                description: Some(LONG.into()),
                info: None,
            }))),
        })
    }

    pub(crate) fn begin() -> Frame {
        Begin(Box::new(BeginInner {
            remote_channel: None,
            next_outgoing_id: 1,
            incoming_window: 100,
            outgoing_window: 100,
            handle_max: 10,
            offered_capabilities: Some(symbols()),
            desired_capabilities: None,
            properties: None,
        }))
        .into()
    }

    fn attach() -> Frame {
        Attach(Box::new(AttachInner {
            name: LONG.into(),
            handle: 0,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(Source {
                address: Some(LONG.into()),
                durable: TerminusDurability::None,
                expiry_policy: TerminusExpiryPolicy::SessionEnd,
                timeout: 0,
                dynamic: false,
                dynamic_node_properties: None,
                distribution_mode: None,
                filter: None,
                default_outcome: None,
                outcomes: None,
                capabilities: Some(symbols()),
            }),
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }))
        .into()
    }

    fn transfer(delivery_id: u32, more: bool, state: Option<DeliveryState>, body: u8) -> Frame {
        Transfer(Box::new(TransferInner {
            handle: 0,
            delivery_id: Some(delivery_id),
            delivery_tag: Some(Bytes::from(LONG)),
            message_format: None,
            settled: Some(false),
            more,
            rcv_settle_mode: None,
            state,
            resume: false,
            aborted: false,
            batchable: false,
            body: Some(TransferBody::Data(Bytes::from(vec![body; 10]))),
        }))
        .into()
    }

    #[ntex::test]
    async fn frames_detached_from_read_buffer() {
        // remote open
        let open = Open(Box::new(OpenInner {
            container_id: LONG.into(),
            hostname: Some(LONG.into()),
            max_frame_size: 1024,
            channel_max: 10,
            idle_time_out: None,
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: Some(symbols()),
            desired_capabilities: Some(symbols()),
            properties: None,
        }));
        let (Frame::Open(open), buf) = read(open.into()) else {
            panic!()
        };
        let remote = RemoteServiceConfig::new(&open);
        drop(open);
        assert!(buf.is_unique(), "open");

        let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
        let io = Io::new(IoTest::create().0, cfg.clone());
        let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };

        // remote begin
        let (frame, buf) = read(begin());
        handle(frame).unwrap();
        assert!(buf.is_unique(), "begin");

        // remote attach
        let (frame, buf) = read(attach());
        let Ok(Action::AttachReceiver(link, _, response)) = handle(frame) else {
            panic!()
        };
        assert!(buf.is_unique(), "attach");
        link.confirm_receiver_link(response);
        link.set_link_credit(10);

        // partial transfers
        for (idx, more) in [true, true, false].into_iter().enumerate() {
            let (frame, buf) = read(transfer(0, more, (idx == 0).then(rejected), idx as u8));
            handle(frame).unwrap();
            assert!(buf.is_unique(), "transfer {idx}");
        }
        let (delivery, transfer) = link.get_delivery().unwrap();
        assert_eq!(delivery.tag(), LONG.as_bytes());
        let Some(TransferBody::Data(body)) = transfer.body() else {
            panic!()
        };
        assert_eq!(body, &[[0; 10], [1; 10], [2; 10]].concat());

        // disposition with state
        let session = inner.get_ref().sessions_map[&0];
        let SessionState::Established(session) = &inner.get_ref().sessions[session] else {
            panic!()
        };
        session
            .get_mut()
            .unsettled_snd_deliveries
            .insert(0, DeliveryInner::new(0));
        let disp = Disposition(Box::new(DispositionInner {
            role: Role::Receiver,
            first: 0,
            last: None,
            settled: false,
            state: Some(rejected()),
            batchable: false,
        }));
        let (frame, buf) = read(disp.into());
        handle(frame).unwrap();
        assert!(buf.is_unique(), "disposition");
    }

    #[ntex::test]
    async fn transfers_advance_next_incoming_id() {
        let remote = RemoteServiceConfig::new(&Open(Box::default()));
        let (server, client) = IoTest::create();
        client.remote_buffer_cap(64 * 1024);
        let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
        let io = Io::new(server, cfg.clone());
        let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };

        handle(begin()).unwrap();
        let Ok(Action::AttachReceiver(link, _, response)) = handle(attach()) else {
            panic!()
        };
        link.confirm_receiver_link(response);
        link.set_link_credit(10);
        for id in 0..3 {
            handle(transfer(id, false, None, 0)).unwrap();
        }
        handle(transfer(3, true, None, 0)).unwrap();
        link.set_link_credit(5);
        assert_eq!(link.credit(), 12);

        // session flow with echo, reply carries next-incoming-id
        let flow = Flow(Box::new(FlowInner {
            next_incoming_id: Some(1),
            incoming_window: 100,
            next_outgoing_id: 5,
            outgoing_window: 100,
            echo: true,
            ..Default::default()
        }));
        handle(flow.into()).unwrap();

        ntex::time::sleep(ntex::time::Millis(50)).await;
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut flows = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            if let Frame::Flow(flow) = frame.into_parts().1 {
                flows.push((flow.next_incoming_id(), flow.link_credit()));
            }
        }
        assert_eq!(
            flows,
            [(Some(1), Some(10)), (Some(5), Some(12)), (Some(5), None)]
        );
    }
    #[ntex::test]
    async fn receiver_delivery_count_wraps() {
        // overflow on single and on multi-frame transfer
        receiver_delivery_count(false).await;
        receiver_delivery_count(true).await;
    }

    async fn receiver_delivery_count(multi_first: bool) {
        let remote = RemoteServiceConfig::new(&Open(Box::default()));
        let (server, client) = IoTest::create();
        client.remote_buffer_cap(64 * 1024);
        let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
        let io = Io::new(server, cfg.clone());
        let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };

        handle(begin()).unwrap();
        let Frame::Attach(mut attach) = attach() else {
            panic!()
        };
        attach.0.initial_delivery_count = Some(u32::MAX);
        let Ok(Action::AttachReceiver(link, _, response)) = handle(attach.into()) else {
            panic!()
        };
        link.confirm_receiver_link(response);
        link.set_link_credit(10);

        for id in 0..2 {
            if (id == 0) == multi_first {
                handle(transfer(id, true, None, 0)).unwrap();
            }
            handle(transfer(id, false, None, 0)).unwrap();
            link.set_link_credit(1);
        }

        ntex::time::sleep(ntex::time::Millis(50)).await;
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut counts = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            if let Frame::Flow(flow) = frame.into_parts().1 {
                counts.push(flow.delivery_count());
            }
        }
        assert_eq!(counts, [Some(u32::MAX), Some(0), Some(1)]);
    }

    #[ntex::test]
    async fn local_receiver_delivery_count() {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let fut = ntex::rt::spawn(async move { s.build_receiver_link("r", "r").attach().await });
        ntex::time::sleep(ntex::time::Millis(10)).await;

        // remote sender initializes delivery-count
        let Frame::Attach(mut attach) = named_attach(Role::Sender, "r", "r", 0) else {
            panic!()
        };
        attach.0.initial_delivery_count = Some(100);
        let Ok(Action::None) = handle_frame(&conn, attach.into()) else {
            panic!()
        };
        let link = fut.await.unwrap().unwrap();
        link.set_link_credit(10);
        handle_frame(&conn, transfer(0, false, None, 0)).unwrap();
        link.set_link_credit(1);

        ntex::time::sleep(ntex::time::Millis(10)).await;
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut flows = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            if let Frame::Flow(flow) = frame.into_parts().1 {
                flows.push((flow.delivery_count(), flow.link_credit()));
            }
        }
        assert_eq!(flows, [(Some(100), Some(10)), (Some(101), Some(10))]);
    }

    #[ntex::test]
    async fn cancelled_local_attach_detached() {
        for sender in [true, false] {
            // attach response is received after or before attach future is dropped
            for delivered in [false, true] {
                let (_io, conn, client) = connection();
                handle_frame(&conn, begin()).unwrap();
                let s = session(&conn);
                let role = if sender { Role::Receiver } else { Role::Sender };
                let attach = |s: Session| async move {
                    if sender {
                        s.build_sender_link("l", "l").attach().await.map(|_| ())
                    } else {
                        s.build_receiver_link("l", "l").attach().await.map(|_| ())
                    }
                };
                {
                    let mut fut = std::pin::pin!(attach(s.clone()));
                    let pending =
                        std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_pending()))
                            .await;
                    assert!(pending);
                    if delivered {
                        let Ok(Action::None) = handle_frame(&conn, named_attach(role, "l", "l", 0))
                        else {
                            panic!()
                        };
                    }
                }
                if !delivered {
                    let Ok(Action::None) = handle_frame(&conn, named_attach(role, "l", "l", 0))
                    else {
                        panic!()
                    };
                }
                ntex::time::sleep(ntex::time::Millis(10)).await;
                let ctx = format!("sender: {sender} delivered: {delivered}");
                assert_eq!(
                    frame_names(&client),
                    ["Begin", "Attach l 0", "Detach 0"],
                    "{ctx}"
                );
                assert!(s.get_sender_link("l").is_none(), "{ctx}");
                assert!(s.get_sender_link_by_local_handle(0).is_none(), "{ctx}");
                assert!(s.get_receiver_link_by_local_handle(0).is_none(), "{ctx}");

                // name and handles are released on remote detach
                let Ok(Action::None) = handle_frame(&conn, peer_detach(0)) else {
                    panic!()
                };
                let fut = ntex::rt::spawn(attach(s.clone()));
                ntex::time::sleep(ntex::time::Millis(10)).await;
                let Ok(Action::None) = handle_frame(&conn, named_attach(role, "l", "l", 0)) else {
                    panic!()
                };
                ntex::time::timeout(ntex::time::Millis(1000), fut)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(frame_names(&client), ["Attach l 0"], "{ctx}");
            }
        }
    }

    #[ntex::test]
    async fn cancelled_local_attach_slot_reused() {
        for sender in [true, false] {
            let (_io, conn, client) = connection();
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);
            let role = if sender { Role::Receiver } else { Role::Sender };
            let attach = |s: Session, name: &'static str| async move {
                if sender {
                    s.build_sender_link(name, "l").attach().await.map(|_| ())
                } else {
                    s.build_receiver_link(name, "l").attach().await.map(|_| ())
                }
            };
            {
                let mut fut = std::pin::pin!(attach(s.clone(), "l"));
                let pending =
                    std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_pending()))
                        .await;
                assert!(pending);
                let Ok(Action::None) = handle_frame(&conn, named_attach(role, "l", "l", 0)) else {
                    panic!()
                };

                // remote detach releases slot, new link reuses it
                handle_frame(&conn, peer_detach(0)).unwrap();
                let new = ntex::rt::spawn(attach(s.clone(), "m"));
                ntex::time::sleep(ntex::time::Millis(10)).await;
                let Ok(Action::None) = handle_frame(&conn, named_attach(role, "m", "l", 1)) else {
                    panic!()
                };
                ntex::time::timeout(ntex::time::Millis(1000), new)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            }
            ntex::time::sleep(ntex::time::Millis(10)).await;
            assert_eq!(
                frame_names(&client),
                ["Begin", "Attach l 0", "Detach 0", "Attach m 0"],
                "sender: {sender}"
            );
            if sender {
                assert!(s.get_sender_link("m").is_some());
            } else {
                assert!(s.get_receiver_link_by_local_handle(0).is_some());
            }
        }
    }

    enum TestLink {
        Sender(SenderLink),
        Receiver(crate::ReceiverLink),
    }

    impl TestLink {
        async fn attach(
            s: Session,
            sender: bool,
            name: &'static str,
        ) -> Result<Self, AmqpProtocolError> {
            if sender {
                s.build_sender_link(name, "l")
                    .attach()
                    .await
                    .map(TestLink::Sender)
            } else {
                s.build_receiver_link(name, "l")
                    .attach()
                    .await
                    .map(TestLink::Receiver)
            }
        }

        async fn close(self) -> Result<(), AmqpProtocolError> {
            match self {
                TestLink::Sender(l) => l.close().await,
                TestLink::Receiver(l) => l.close().await,
            }
        }
    }

    #[ntex::test]
    async fn duplicate_local_link_name() {
        for sender in [true, false] {
            let ctx = format!("sender: {sender}");
            let (_io, conn, client) = connection();
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);
            let role = if sender { Role::Receiver } else { Role::Sender };
            let other = if sender { Role::Sender } else { Role::Receiver };
            let in_use = async |sender| {
                let res = ntex::time::timeout(
                    ntex::time::Millis(1000),
                    TestLink::attach(s.clone(), sender, "x"),
                )
                .await
                .unwrap();
                assert!(
                    matches!(res, Err(AmqpProtocolError::LinkNameInUse)),
                    "{ctx}"
                );
            };

            // name is used by opening link
            let first = ntex::rt::spawn(TestLink::attach(s.clone(), sender, "x"));
            ntex::time::sleep(ntex::time::Millis(10)).await;
            in_use(sender).await;

            let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 0)) else {
                panic!("{ctx}")
            };
            let first = ntex::time::timeout(ntex::time::Millis(1000), first)
                .await
                .unwrap()
                .unwrap()
                .unwrap();

            // name is used by established link
            in_use(sender).await;

            // same name in other direction is a different link
            let opposite = ntex::rt::spawn(TestLink::attach(s.clone(), !sender, "x"));
            ntex::time::sleep(ntex::time::Millis(10)).await;
            let Ok(Action::None) = handle_frame(&conn, named_attach(other, "x", "l", 1)) else {
                panic!("{ctx}")
            };
            ntex::time::timeout(ntex::time::Millis(1000), opposite)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            ntex::time::sleep(ntex::time::Millis(10)).await;
            assert_eq!(
                frame_names(&client),
                ["Begin", "Attach x 0", "Attach x 1"],
                "{ctx}"
            );

            // name of closing link can be reused
            let closed = ntex::rt::spawn(first.close());
            ntex::time::sleep(ntex::time::Millis(10)).await;
            let second = ntex::rt::spawn(TestLink::attach(s.clone(), sender, "x"));
            ntex::time::sleep(ntex::time::Millis(10)).await;
            handle_frame(&conn, peer_detach(0)).unwrap();
            let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 2)) else {
                panic!("{ctx}")
            };
            ntex::time::timeout(ntex::time::Millis(1000), closed)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            ntex::time::timeout(ntex::time::Millis(1000), second)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            ntex::time::sleep(ntex::time::Millis(10)).await;
            assert_eq!(frame_names(&client), ["Detach 0", "Attach x 2"], "{ctx}");
            if sender {
                assert_eq!(s.get_sender_link("x").unwrap().remote_handle(), 2, "{ctx}");
            } else {
                let link = s.get_receiver_link_by_remote_handle(2).unwrap();
                assert_eq!(link.handle(), 2, "{ctx}");
            }
        }
    }

    #[ntex::test]
    async fn local_link_name_used_by_remote_link() {
        for sender in [true, false] {
            let ctx = format!("sender: {sender}");
            let (_io, conn, client) = connection();
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);

            // remote receiver opens sender link, remote sender opens receiver link
            let role = if sender { Role::Receiver } else { Role::Sender };
            match handle_frame(&conn, named_attach(role, "x", "l", 0)) {
                Ok(Action::AttachSender(..)) if sender => (),
                Ok(Action::AttachReceiver(..)) if !sender => (),
                _ => panic!("{ctx}"),
            }
            let res = ntex::time::timeout(
                ntex::time::Millis(1000),
                TestLink::attach(s.clone(), sender, "x"),
            )
            .await
            .unwrap();
            assert!(
                matches!(res, Err(AmqpProtocolError::LinkNameInUse)),
                "{ctx}"
            );
            ntex::time::sleep(ntex::time::Millis(10)).await;
            assert_eq!(frame_names(&client), ["Begin"], "{ctx}");
        }
    }

    #[ntex::test]
    async fn local_sender_initial_delivery_count() {
        // (attach initial delivery-count, sent value, flow delivery-count)
        let cases = [
            (None, 0, Some(0)),
            (Some(Some(5)), 5, Some(5)),
            (Some(None), 0, Some(0)),
            (Some(Some(5)), 5, None),
        ];
        for (initial, sent, flow_count) in cases {
            let ctx = format!("initial: {initial:?} flow: {flow_count:?}");
            let (_io, conn, client) = connection();
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);
            let fut = ntex::rt::spawn(async move {
                let mut builder = s.build_sender_link("s", "s");
                if let Some(initial) = initial {
                    builder = builder.with_frame(|f| f.0.initial_delivery_count = initial);
                }
                builder.attach().await
            });
            ntex::time::sleep(ntex::time::Millis(10)).await;

            let codec = AmqpCodec::<AmqpFrame>::new();
            let mut buf = BytesMut::from(&client.read_any()[..]);
            let mut counts = Vec::new();
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                if let Frame::Attach(att) = frame.into_parts().1 {
                    counts.push(att.initial_delivery_count());
                }
            }
            assert_eq!(counts, [Some(sent)], "{ctx}");

            // receiver initial delivery-count is ignored
            let Frame::Attach(mut attach) = named_attach(Role::Receiver, "s", "s", 0) else {
                panic!()
            };
            attach.0.initial_delivery_count = Some(100);
            let Ok(Action::None) = handle_frame(&conn, attach.into()) else {
                panic!("{ctx}")
            };
            let link = ntex::time::timeout(ntex::time::Millis(1000), fut)
                .await
                .unwrap()
                .unwrap()
                .unwrap();

            let Frame::Flow(mut flow) = peer_flow(Some(10)) else {
                panic!()
            };
            flow.0.delivery_count = flow_count;
            handle_frame(&conn, flow.into()).unwrap();
            assert_eq!(link.credit(), 10, "{ctx}");

            // delivery-count is advanced by transfer
            ntex::time::timeout(
                ntex::time::Millis(1000),
                link.transfer(Bytes::from_static(b"1")).settled().send(),
            )
            .await
            .unwrap()
            .unwrap();
            let Frame::Flow(mut flow) = peer_flow(Some(10)) else {
                panic!()
            };
            flow.0.delivery_count = Some(sent + 1);
            handle_frame(&conn, flow.into()).unwrap();
            assert_eq!(link.credit(), 10, "{ctx}");
        }
    }

    async fn attach_with_timeout(
        s: Session,
        sender: bool,
        timeout: Option<Seconds>,
    ) -> Result<(), AmqpProtocolError> {
        if sender {
            let mut builder = s.build_sender_link("x", "l");
            if let Some(timeout) = timeout {
                builder = builder.attach_timeout(timeout);
            }
            builder.attach().await.map(|_| ())
        } else {
            let mut builder = s.build_receiver_link("x", "l");
            if let Some(timeout) = timeout {
                builder = builder.attach_timeout(timeout);
            }
            builder.attach().await.map(|_| ())
        }
    }

    #[ntex::test]
    async fn link_attach_timeout() {
        // (config timeout, builder timeout)
        let cases = [(Seconds(1), None), (Seconds::ZERO, Some(Seconds(1)))];
        for sender in [true, false] {
            for (config, builder) in cases {
                let ctx = format!("sender: {sender} config: {config:?} builder: {builder:?}");
                let (_io, conn, client) =
                    connection_with(AmqpServiceConfig::new().set_link_attach_timeout(config));
                handle_frame(&conn, begin()).unwrap();
                let s = session(&conn);
                let role = if sender { Role::Receiver } else { Role::Sender };

                let res = ntex::time::timeout(
                    ntex::time::Millis(3000),
                    attach_with_timeout(s.clone(), sender, builder),
                )
                .await
                .expect(&ctx);
                assert!(
                    matches!(res, Err(AmqpProtocolError::LinkAttachTimeout)),
                    "{ctx}"
                );

                // late attach response detaches link
                let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 0)) else {
                    panic!("{ctx}")
                };
                ntex::time::sleep(ntex::time::Millis(10)).await;
                assert_eq!(
                    frame_names(&client),
                    ["Begin", "Attach x 0", "Detach 0"],
                    "{ctx}"
                );
                handle_frame(&conn, peer_detach(0)).unwrap();

                // name is released
                let fut = ntex::rt::spawn(attach_with_timeout(s.clone(), sender, builder));
                ntex::time::sleep(ntex::time::Millis(10)).await;
                let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 1)) else {
                    panic!("{ctx}")
                };
                ntex::time::timeout(ntex::time::Millis(1000), fut)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            }
        }
    }

    #[ntex::test]
    async fn link_attached_before_timeout() {
        for sender in [true, false] {
            let ctx = format!("sender: {sender}");
            let (_io, conn, client) =
                connection_with(AmqpServiceConfig::new().set_link_attach_timeout(Seconds(1)));
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);
            let role = if sender { Role::Receiver } else { Role::Sender };

            let fut = ntex::rt::spawn(attach_with_timeout(s.clone(), sender, None));
            ntex::time::sleep(ntex::time::Millis(10)).await;
            let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 0)) else {
                panic!("{ctx}")
            };
            ntex::time::timeout(ntex::time::Millis(1000), fut)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            ntex::time::sleep(ntex::time::Millis(1500)).await;
            assert_eq!(frame_names(&client), ["Begin", "Attach x 0"], "{ctx}");
            if sender {
                assert!(s.get_sender_link("x").is_some(), "{ctx}");
            } else {
                assert!(s.get_receiver_link_by_local_handle(0).is_some(), "{ctx}");
            }
        }
    }

    #[ntex::test]
    async fn refused_local_attach() {
        let not_found = Error(Box::new(codec::ErrorInner {
            condition: AmqpError::NotFound.into(),
            description: None,
            info: None,
        }));
        for sender in [true, false] {
            for error in [None, Some(not_found.clone())] {
                for cancel in [false, true] {
                    let ctx = format!("sender: {sender} error: {error:?} cancel: {cancel}");
                    let (_io, conn, client) = connection();
                    handle_frame(&conn, begin()).unwrap();
                    let s = session(&conn);
                    let role = if sender { Role::Receiver } else { Role::Sender };

                    let mut fut = Some(Box::pin(attach_with_timeout(s.clone(), sender, None)));
                    let mut cx = Context::from_waker(std::task::Waker::noop());
                    assert!(
                        fut.as_mut().unwrap().as_mut().poll(&mut cx).is_pending(),
                        "{ctx}"
                    );

                    // refused, attach waits for remote detach
                    let Frame::Attach(mut attach) = named_attach(role, "x", "l", 3) else {
                        panic!()
                    };
                    if sender {
                        attach.0.target = None;
                    } else {
                        attach.0.source = None;
                    }
                    let Ok(Action::None) = handle_frame(&conn, attach.into()) else {
                        panic!("{ctx}")
                    };
                    assert!(
                        fut.as_mut().unwrap().as_mut().poll(&mut cx).is_pending(),
                        "{ctx}"
                    );
                    if cancel {
                        fut = None;
                    }

                    let detach = Detach(Box::new(DetachInner {
                        handle: 3,
                        closed: true,
                        error: error.clone(),
                    }));
                    let Ok(Action::None) = handle_frame(&conn, detach.into()) else {
                        panic!("{ctx}")
                    };
                    if let Some(mut fut) = fut {
                        let Poll::Ready(Err(AmqpProtocolError::LinkDetached(err))) =
                            fut.as_mut().poll(&mut cx)
                        else {
                            panic!("{ctx}")
                        };
                        assert_eq!(err, error, "{ctx}");
                    }
                    ntex::time::sleep(ntex::time::Millis(10)).await;
                    assert_eq!(
                        frame_names(&client),
                        ["Begin", "Attach x 0", "Detach 0"],
                        "{ctx}"
                    );

                    // name and handle are released
                    let fut = ntex::rt::spawn(attach_with_timeout(s.clone(), sender, None));
                    ntex::time::sleep(ntex::time::Millis(10)).await;
                    let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 3))
                    else {
                        panic!("{ctx}")
                    };
                    ntex::time::timeout(ntex::time::Millis(1000), fut)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    assert_eq!(frame_names(&client), ["Attach x 0"], "{ctx}");
                }
            }
        }
    }

    async fn detach_by_handle(
        s: &Session,
        sender: bool,
        handle: u32,
        error: Option<Error>,
    ) -> Result<(), AmqpProtocolError> {
        if sender {
            s.detach_sender_link(handle, error).await
        } else {
            s.detach_receiver_link(handle, error).await
        }
    }

    #[ntex::test]
    async fn detach_opening_local_link() {
        for sender in [true, false] {
            let ctx = format!("sender: {sender}");
            let (_io, conn, client) = connection();
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);
            let role = if sender { Role::Receiver } else { Role::Sender };

            let fut = ntex::rt::spawn(attach_with_timeout(s.clone(), sender, None));
            ntex::time::sleep(ntex::time::Millis(10)).await;

            // attach response is not received
            let res = ntex::time::timeout(
                ntex::time::Millis(1000),
                detach_by_handle(&s, sender, 0, None),
            )
            .await
            .expect(&ctx);
            assert!(
                matches!(res, Err(AmqpProtocolError::LinkNotAttached)),
                "{ctx}"
            );

            // attach is not affected
            let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 3)) else {
                panic!("{ctx}")
            };
            ntex::time::timeout(ntex::time::Millis(1000), fut)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            ntex::time::sleep(ntex::time::Millis(10)).await;
            assert_eq!(frame_names(&client), ["Begin", "Attach x 0"], "{ctx}");
            if sender {
                assert!(s.get_sender_link("x").is_some(), "{ctx}");
            } else {
                assert!(s.get_receiver_link_by_local_handle(0).is_some(), "{ctx}");
            }
        }
    }

    #[ntex::test]
    async fn detach_refused_local_link() {
        let not_found = Error(Box::new(codec::ErrorInner {
            condition: AmqpError::NotFound.into(),
            description: None,
            info: None,
        }));
        for sender in [true, false] {
            for error in [None, Some(not_found.clone())] {
                let ctx = format!("sender: {sender} error: {error:?}");
                let (_io, conn, client) = connection();
                handle_frame(&conn, begin()).unwrap();
                let s = session(&conn);
                let role = if sender { Role::Receiver } else { Role::Sender };
                let mut cx = Context::from_waker(std::task::Waker::noop());

                let mut fut = Box::pin(attach_with_timeout(s.clone(), sender, None));
                assert!(fut.as_mut().poll(&mut cx).is_pending(), "{ctx}");
                let Frame::Attach(mut attach) = named_attach(role, "x", "l", 3) else {
                    panic!()
                };
                if sender {
                    attach.0.target = None;
                } else {
                    attach.0.source = None;
                }
                let Ok(Action::None) = handle_frame(&conn, attach.into()) else {
                    panic!("{ctx}")
                };

                // local detach of refused link fails attach
                let mut detach = Box::pin(detach_by_handle(&s, sender, 0, error.clone()));
                assert!(detach.as_mut().poll(&mut cx).is_pending(), "{ctx}");
                let Poll::Ready(Err(AmqpProtocolError::LinkDetached(err))) =
                    fut.as_mut().poll(&mut cx)
                else {
                    panic!("{ctx}")
                };
                assert_eq!(err, error, "{ctx}");

                // remote detach completes local detach
                let Ok(Action::None) = handle_frame(&conn, peer_detach(3)) else {
                    panic!("{ctx}")
                };
                let Poll::Ready(Ok(())) = detach.as_mut().poll(&mut cx) else {
                    panic!("{ctx}")
                };
                ntex::time::sleep(ntex::time::Millis(10)).await;
                assert_eq!(
                    frame_names(&client),
                    ["Begin", "Attach x 0", "Detach 0"],
                    "{ctx}"
                );

                // name and handle are released
                let fut = ntex::rt::spawn(attach_with_timeout(s.clone(), sender, None));
                ntex::time::sleep(ntex::time::Millis(10)).await;
                let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 3)) else {
                    panic!("{ctx}")
                };
                ntex::time::timeout(ntex::time::Millis(1000), fut)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(frame_names(&client), ["Attach x 0"], "{ctx}");
            }
        }
    }

    #[ntex::test]
    async fn detach_unconfirmed_remote_sender_link() {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let Ok(Action::AttachSender(link, attach, response)) =
            handle_frame(&conn, named_attach(Role::Receiver, "n", "a", 3))
        else {
            panic!()
        };
        let res = ntex::time::timeout(
            ntex::time::Millis(1000),
            s.detach_sender_link(link.id(), None),
        )
        .await
        .unwrap();
        assert!(matches!(res, Err(AmqpProtocolError::LinkNotAttached)));

        // link is confirmed
        s.inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, link.inner.clone());
        assert!(s.get_sender_link("n").is_some());
        ntex::time::sleep(ntex::time::Millis(10)).await;
        assert_eq!(frame_names(&client), ["Begin", "Attach n 0"]);
    }

    #[ntex::test]
    async fn session_end_error_propagated() {
        let err = Error(Box::new(codec::ErrorInner {
            condition: AmqpError::InternalError.into(),
            description: None,
            info: None,
        }));
        let ended: [(Frame, AmqpProtocolError); 2] = [
            (
                End {
                    error: Some(err.clone()),
                }
                .into(),
                AmqpProtocolError::SessionEnded(Some(err.clone())),
            ),
            (
                Close {
                    error: Some(err.clone()),
                }
                .into(),
                AmqpProtocolError::Closed(Some(err.clone())),
            ),
        ];
        for (frame, expected) in ended {
            let ctx = format!("{expected:?}");
            let expected = Some(format!("{expected:?}"));
            let same = |res: Result<(), AmqpProtocolError>| res.err().map(|e| format!("{e:?}"));
            let (_io, conn, _client) = connection();
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);
            let mut cx = Context::from_waker(std::task::Waker::noop());

            // established and closing receivers
            let mut links = Vec::new();
            for (name, handle) in [("e", 1), ("c", 2)] {
                let fut = ntex::rt::spawn({
                    let s = s.clone();
                    async move { s.build_receiver_link(name, name).attach().await }
                });
                ntex::time::sleep(ntex::time::Millis(10)).await;
                let Ok(Action::None) =
                    handle_frame(&conn, named_attach(Role::Sender, name, name, handle))
                else {
                    panic!("{ctx}")
                };
                links.push(fut.await.unwrap().unwrap());
            }
            let mut close = Box::pin(links[1].close());
            assert!(close.as_mut().poll(&mut cx).is_pending(), "{ctx}");

            // pending local attaches
            let mut snd = Box::pin(attach_with_timeout(s.clone(), true, None));
            assert!(snd.as_mut().poll(&mut cx).is_pending(), "{ctx}");
            let mut rcv = Box::pin(attach_with_timeout(s.clone(), false, None));
            assert!(rcv.as_mut().poll(&mut cx).is_pending(), "{ctx}");

            // unconfirmed remote receiver
            let Ok(Action::AttachReceiver(remote, ..)) =
                handle_frame(&conn, named_attach(Role::Sender, "u", "u", 9))
            else {
                panic!("{ctx}")
            };

            handle_frame(&conn, frame).unwrap();

            let Poll::Ready(res) = snd.as_mut().poll(&mut cx) else {
                panic!("{ctx}")
            };
            assert_eq!(same(res), expected, "{ctx}");
            let Poll::Ready(res) = rcv.as_mut().poll(&mut cx) else {
                panic!("{ctx}")
            };
            assert_eq!(same(res), expected, "{ctx}");
            let Poll::Ready(res) = close.as_mut().poll(&mut cx) else {
                panic!("{ctx}")
            };
            assert_eq!(same(res), expected, "{ctx}");
            for link in [&links[0], &remote] {
                assert!(link.error().is_none(), "{ctx}");
                let Poll::Ready(Some(Err(e))) = link.poll_recv(&mut cx) else {
                    panic!("{ctx}")
                };
                assert_eq!(same(Err(e)), expected, "{ctx}");
                assert!(
                    matches!(link.poll_recv(&mut cx), Poll::Ready(None)),
                    "{ctx}"
                );
            }
        }
    }

    #[ntex::test]
    async fn receiver_remote_detach_error() {
        let err = Error(Box::new(codec::ErrorInner {
            condition: AmqpError::InternalError.into(),
            description: None,
            info: None,
        }));
        let (_io, conn, _client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let fut = ntex::rt::spawn({
            let s = s.clone();
            async move { s.build_receiver_link("r", "r").attach().await }
        });
        ntex::time::sleep(ntex::time::Millis(10)).await;
        let Ok(Action::None) = handle_frame(&conn, named_attach(Role::Sender, "r", "r", 1)) else {
            panic!()
        };
        let link = fut.await.unwrap().unwrap();

        let detach = Detach(Box::new(DetachInner {
            handle: 1,
            closed: true,
            error: Some(err.clone()),
        }));
        let Ok(Action::DetachReceiver(..)) = handle_frame(&conn, detach.into()) else {
            panic!()
        };
        assert_eq!(link.error(), Some(&err));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let Poll::Ready(Some(Err(AmqpProtocolError::LinkDetached(Some(e))))) =
            link.poll_recv(&mut cx)
        else {
            panic!()
        };
        assert_eq!(e, err);
        assert!(matches!(link.poll_recv(&mut cx), Poll::Ready(None)));
    }

    #[ntex::test]
    async fn receiver_link_detach_wakes_recv() {
        use ntex::time::{Millis, sleep, timeout};

        for queued in [false, true] {
            let (_io, conn, client) = connection();
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);
            let fut = ntex::rt::spawn({
                let s = s.clone();
                async move { s.build_receiver_link("r", "r").attach().await }
            });
            sleep(Millis(10)).await;
            let Ok(Action::None) = handle_frame(&conn, named_attach(Role::Sender, "r", "r", 0))
            else {
                panic!()
            };
            let link = fut.await.unwrap().unwrap();
            link.set_link_credit(10);
            if queued {
                handle_frame(&conn, transfer(0, false, None, 1)).unwrap();
            }
            let rx = ntex::rt::spawn({
                let link = link.clone();
                async move {
                    let mut items = Vec::new();
                    while let Some(item) = link.recv().await {
                        items.push(item.map(|(_, tr)| tr.delivery_id()));
                    }
                    items
                }
            });
            sleep(Millis(50)).await;
            frame_names(&client);

            let _d = s.detach_receiver_link(link.inner.get_ref().id(), None);
            let items = timeout(Millis(500), rx).await.unwrap().unwrap();
            if queued {
                assert!(matches!(items[..], [Ok(Some(0))]));
            } else {
                assert!(items.is_empty());
            }
            assert!(link.is_closed());
            sleep(Millis(50)).await;
            assert_eq!(frame_names(&client), ["Detach 0"]);
        }
    }

    #[ntex::test]
    async fn outbound_frames_limited_by_remote_max_frame_size() {
        let remote = RemoteServiceConfig::new(&Open(Box::new(OpenInner {
            max_frame_size: 512,
            ..Default::default()
        })));
        let (server, client) = IoTest::create();
        client.remote_buffer_cap(64 * 1024);
        let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
        let io = Io::new(server, cfg.clone());
        let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);

        // fits
        conn.get_ref().post_frame(AmqpFrame::new(0, attach()));
        assert!(conn.get_error().is_none());

        // properties do not fit into remote max frame size
        let Frame::Attach(mut attach) = attach() else {
            panic!()
        };
        attach.0.properties = Some(
            [(Symbol::from("key"), Variant::from("a".repeat(512)))]
                .into_iter()
                .collect(),
        );
        conn.get_ref().post_frame(AmqpFrame::new(0, attach.into()));
        assert!(matches!(
            conn.get_error(),
            Some(AmqpProtocolError::Codec(
                AmqpCodecError::MaxOutboundSizeExceeded
            ))
        ));

        ntex::time::sleep(ntex::time::Millis(50)).await;
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        assert!(matches!(
            codec.decode(&mut buf).unwrap().unwrap().performative(),
            Frame::Attach(_)
        ));
        assert!(buf.is_empty());
    }

    pub(crate) fn handle_frame(
        conn: &Connection,
        frame: Frame,
    ) -> Result<Action, AmqpProtocolError> {
        let inner = conn.get_ref().0;
        inner
            .get_mut()
            .handle_frame(AmqpFrame::new(0, frame), &inner)
    }

    pub(crate) fn connection() -> (Io, Connection, IoTest) {
        connection_with(AmqpServiceConfig::new())
    }

    fn connection_with(cfg: AmqpServiceConfig) -> (Io, Connection, IoTest) {
        let remote = RemoteServiceConfig::new(&Open(Box::default()));
        let (server, client) = IoTest::create();
        client.remote_buffer_cap(64 * 1024);
        let cfg = SharedCfg::new("T").add(cfg).build();
        let io = Io::new(server, cfg.clone());
        let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
        (io, conn, client)
    }

    fn session(conn: &Connection) -> Session {
        let inner = conn.get_ref().0;
        let SessionState::Established(session) =
            &inner.get_ref().sessions[inner.get_ref().sessions_map[&0]]
        else {
            panic!()
        };
        Session::new(session.clone())
    }

    fn peer_attach(role: Role, max_message_size: Option<u64>) -> Frame {
        let Frame::Attach(mut attach) = attach() else {
            panic!()
        };
        attach.0.role = role;
        attach.0.max_message_size = max_message_size;
        attach.0.target = Some(Target::default());
        attach.into()
    }

    #[ntex::test]
    async fn receiver_max_message_size() {
        // remotely attached link, max size is set by `set_max_message_size`
        assert_eq!(receive(false, 0, &[true, true, false]).await, (true, false));
        assert_eq!(
            receive(false, 30, &[true, true, false]).await,
            (true, false)
        );
        assert_eq!(
            receive(false, 25, &[true, true, false]).await,
            (false, true)
        );
        assert_eq!(receive(false, 5, &[true]).await, (false, true));
        assert_eq!(receive(false, 10, &[false]).await, (true, false));
        assert_eq!(receive(false, 5, &[false]).await, (false, true));

        // locally attached link, max size is set by link builder
        assert_eq!(receive(true, 0, &[true, true, false]).await, (true, false));
        assert_eq!(receive(true, 25, &[true, true, false]).await, (false, true));
    }

    /// Receive message, returns (delivered, detached with message-size-exceeded)
    async fn receive(local: bool, max: u64, frames: &[bool]) -> (bool, bool) {
        let (_io, conn, client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };
        handle(begin()).unwrap();

        let link = if local {
            let session = session(&conn);
            let fut = ntex::rt::spawn(async move {
                session
                    .build_receiver_link(LONG, LONG)
                    .max_message_size(max)
                    .attach()
                    .await
            });
            ntex::time::sleep(ntex::time::Millis(10)).await;
            handle(attach()).unwrap();
            fut.await.unwrap().unwrap()
        } else {
            let Ok(Action::AttachReceiver(link, _, response)) = handle(attach()) else {
                panic!()
            };
            link.set_max_message_size(max);
            link.confirm_receiver_link(response);
            link
        };
        link.set_link_credit(10);
        for (idx, more) in frames.iter().enumerate() {
            handle(transfer(0, *more, None, idx as u8)).unwrap();
        }

        ntex::time::sleep(ntex::time::Millis(50)).await;
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut exceeded = false;
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            if let Frame::Detach(detach) = frame.into_parts().1 {
                exceeded =
                    *detach.error().unwrap().condition() == LinkError::MessageSizeExceeded.into();
            }
        }
        (link.get_delivery().is_some(), exceeded)
    }

    #[ntex::test]
    async fn sender_max_message_size() {
        for local in [false, true] {
            // peer max-message-size 0 means no limit
            let (link, _conn) = sender(local, Some(0)).await;
            assert_eq!(link.max_message_size(), None);

            let (link, _conn) = sender(local, Some(10)).await;
            assert_eq!(link.max_message_size(), Some(10));
            assert!(matches!(
                link.transfer(Bytes::from(vec![0; 11])).send().await,
                Err(AmqpProtocolError::BodyTooLarge)
            ));
            link.set_max_message_size(0);
            assert_eq!(link.max_message_size(), None);

            let (link, _conn) = sender(local, Some(u64::MAX)).await;
            assert_eq!(link.max_message_size(), Some(u32::MAX));
        }
    }

    async fn sender(local: bool, max: Option<u64>) -> (SenderLink, (Io, Connection, IoTest)) {
        let (io, conn, client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };
        handle(begin()).unwrap();

        let link = if local {
            let session = session(&conn);
            let fut =
                ntex::rt::spawn(
                    async move { session.build_sender_link(LONG, LONG).attach().await },
                );
            ntex::time::sleep(ntex::time::Millis(10)).await;
            handle(peer_attach(Role::Receiver, max)).unwrap();
            fut.await.unwrap().unwrap()
        } else {
            let Ok(Action::AttachSender(link, _, _)) = handle(peer_attach(Role::Receiver, max))
            else {
                panic!()
            };
            link
        };
        (link, (io, conn, client))
    }

    #[ntex::test]
    async fn session_end_uses_local_channel() {
        let (_io, conn, client) = connection();
        let inner = conn.get_ref().0;
        let handle = |channel: u16, frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(channel, frame), &inner)
        };

        // local and remote channel ids are different
        handle(1, begin()).unwrap();
        handle(0, begin()).unwrap();
        let session = conn.get_ref().get_session_by_local_id(0).unwrap();
        assert_eq!(session.remote_channel_id(), 1);

        let fut = ntex::rt::spawn(async move { session.end().await });
        ntex::time::sleep(ntex::time::Millis(10)).await;
        assert!(matches!(
            inner.get_ref().sessions[0],
            SessionState::Closing(_)
        ));
        assert!(matches!(
            inner.get_ref().sessions[1],
            SessionState::Established(_)
        ));

        // other session handles frames
        let flow = Flow(Box::new(FlowInner {
            next_incoming_id: Some(1),
            incoming_window: 100,
            next_outgoing_id: 1,
            outgoing_window: 100,
            echo: true,
            ..Default::default()
        }));
        handle(0, flow.into()).unwrap();

        // remote confirms session end
        handle(1, End { error: None }.into()).unwrap();
        assert!(fut.await.unwrap().is_ok());
        assert!(!inner.get_ref().sessions.contains(0));
        assert!(inner.get_ref().sessions.contains(1));

        ntex::time::sleep(ntex::time::Millis(50)).await;
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut frames = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            match frame.into_parts() {
                (ch, Frame::End(_)) => frames.push(("end", ch)),
                (ch, Frame::Flow(_)) => frames.push(("flow", ch)),
                _ => (),
            }
        }
        assert_eq!(frames, [("end", 0), ("flow", 1)]);
    }

    #[ntex::test]
    async fn links_limited_by_remote_handle_max() {
        let (_io, conn, client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };

        // remote handle-max is 1, two links are allowed
        let Frame::Begin(mut begin) = begin() else { panic!() };
        begin.0.handle_max = 1;
        handle(begin.into()).unwrap();
        let session = session(&conn);

        // remotely opened link
        let Frame::Attach(mut r0) = attach() else { panic!() };
        r0.0.name = "r0".into();
        let Ok(Action::AttachReceiver(..)) = handle(r0.into()) else {
            panic!()
        };

        // locally opened links
        let s = session.clone();
        ntex::rt::spawn(async move { s.build_sender_link("s1", LONG).attach().await });
        ntex::time::sleep(ntex::time::Millis(10)).await;
        let ms = ntex::time::Millis(100);
        let s2 = ntex::time::timeout(ms, session.build_sender_link("s2", LONG).attach());
        assert!(matches!(s2.await, Ok(Err(AmqpProtocolError::TooManyLinks))));
        let s3 = ntex::time::timeout(ms, session.build_receiver_link("s3", LONG).attach());
        assert!(matches!(s3.await, Ok(Err(AmqpProtocolError::TooManyLinks))));

        // remotely opened link, no free local handles
        let Frame::Attach(mut r1) = attach() else { panic!() };
        r1.0.name = "r1".into();
        r1.0.handle = 1;
        let Ok(Action::SessionEnded(_)) = handle(r1.into()) else {
            panic!()
        };

        ntex::time::sleep(ntex::time::Millis(50)).await;
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut frames = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            match frame.into_parts().1 {
                Frame::Attach(attach) => frames.push(format!("attach {}", attach.handle())),
                Frame::End(end) => frames.push(format!(
                    "end {}",
                    *end.error.unwrap().condition() == AmqpError::ResourceLimitExceeded.into()
                )),
                _ => (),
            }
        }
        assert_eq!(frames, ["attach 1", "end true"]);
    }

    fn peer_flow(link_credit: Option<u32>) -> Frame {
        Flow(Box::new(FlowInner {
            next_incoming_id: Some(1),
            incoming_window: 100,
            next_outgoing_id: 1,
            outgoing_window: 100,
            handle: Some(0),
            delivery_count: Some(0),
            link_credit,
            available: None,
            drain: false,
            echo: false,
            properties: None,
        }))
        .into()
    }

    #[ntex::test]
    async fn remote_sender_flow_before_confirm() {
        let (_io, conn, _client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };
        handle(begin()).unwrap();
        let session = session(&conn);

        let Ok(Action::AttachSender(link, attach, response)) =
            handle(peer_attach(Role::Receiver, None))
        else {
            panic!()
        };

        // link flow before confirmation, last flow with link credit is applied
        for credit in [Some(5), Some(10), None] {
            let Ok(Action::None) = handle(peer_flow(credit)) else {
                panic!()
            };
        }
        assert_eq!(link.credit(), 0);

        let link = session.inner.get_mut().attach_remote_sender_link(
            &attach,
            response,
            link.inner.clone(),
        );
        assert_eq!(link.credit(), 10);
        assert!(link.ready().await);
    }

    pub(crate) fn named_attach(role: Role, name: &str, address: &str, handle: u32) -> Frame {
        let Frame::Attach(mut attach) = attach() else {
            panic!()
        };
        attach.0.role = role;
        attach.0.name = name.into();
        attach.0.handle = handle;
        attach.0.source.as_mut().unwrap().address = Some(address.into());
        attach.0.target = Some(Target {
            address: Some(address.into()),
            durable: TerminusDurability::None,
            expiry_policy: TerminusExpiryPolicy::SessionEnd,
            timeout: 0,
            dynamic: false,
            dynamic_node_properties: None,
            capabilities: None,
        });
        attach.into()
    }

    fn peer_detach(handle: u32) -> Frame {
        Detach(Box::new(DetachInner {
            handle,
            closed: true,
            error: None,
        }))
        .into()
    }

    #[ntex::test]
    async fn remote_sender_link_names() {
        let (_io, conn, _client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };
        handle(begin()).unwrap();
        let session = session(&conn);
        let confirm = |frame| {
            let Ok(Action::AttachSender(link, attach, response)) = handle(frame) else {
                panic!()
            };
            session
                .inner
                .get_mut()
                .attach_remote_sender_link(&attach, response, link.inner.clone())
        };

        // remote sender link is registered by link name
        let link = confirm(named_attach(Role::Receiver, "n", "a", 0));
        assert_eq!(link.name(), "n");
        assert_eq!(link.address().unwrap(), "a");
        assert_eq!(session.get_sender_link("n").unwrap().id(), link.id());
        assert!(session.get_sender_link("a").is_none());
        assert_eq!(
            session.get_sender_link_by_address("a").unwrap().id(),
            link.id()
        );
        assert!(session.get_sender_link_by_address("n").is_none());

        // link named as address of other link
        let link2 = confirm(named_attach(Role::Receiver, "a", "a", 1));
        assert_eq!(session.get_sender_link("a").unwrap().id(), link2.id());

        // duplicate name, newer link takes the name
        let link3 = confirm(named_attach(Role::Receiver, "a", "b", 2));
        assert_eq!(session.get_sender_link("a").unwrap().id(), link3.id());
        let Ok(Action::DetachSender(..)) = handle(peer_detach(1)) else {
            panic!()
        };
        assert_eq!(session.get_sender_link("a").unwrap().id(), link3.id());

        // name is removed on detach
        let Ok(Action::DetachSender(..)) = handle(peer_detach(0)) else {
            panic!()
        };
        assert!(session.get_sender_link("n").is_none());
        assert!(session.get_sender_link_by_address("a").is_none());
        let Ok(Action::DetachSender(..)) = handle(peer_detach(2)) else {
            panic!()
        };
        let inner = session.inner.get_ref();
        assert!(inner.sender_names.is_empty() && inner.link_names.is_empty());
    }

    #[ntex::test]
    async fn local_link_names() {
        let (_io, conn, _client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };
        handle(begin()).unwrap();
        let session = session(&conn);

        let s = session.clone();
        let fut = ntex::rt::spawn(async move { s.build_sender_link("x", "addr").attach().await });
        ntex::time::sleep(ntex::time::Millis(10)).await;

        // peer opens link with the same name in other direction
        let Ok(Action::AttachReceiver(..)) = handle(named_attach(Role::Sender, "x", "x", 0)) else {
            panic!()
        };
        assert!(!fut.is_finished());

        // local link confirmation
        let Ok(Action::None) = handle(named_attach(Role::Receiver, "x", "peer", 1)) else {
            panic!()
        };
        let link = fut.await.unwrap().unwrap();
        assert_eq!(link.name(), "x");
        assert_eq!(link.address().unwrap(), "peer");
        assert_eq!(session.get_sender_link("x").unwrap().id(), link.id());

        // name is removed after detach confirmation
        let l = link.clone();
        let fut = ntex::rt::spawn(async move { l.close().await });
        ntex::time::sleep(ntex::time::Millis(10)).await;
        handle(peer_detach(1)).unwrap();
        fut.await.unwrap().unwrap();
        assert!(session.get_sender_link("x").is_none());
        assert!(!session.inner.get_ref().sender_names.contains_key("x"));

        // remote attach with the name of removed link
        let Ok(Action::AttachSender(..)) = handle(named_attach(Role::Receiver, "x", "x", 2)) else {
            panic!()
        };
    }

    #[ntex::test]
    async fn remote_sender_flow_order() {
        let (_io, conn, _client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };
        handle(begin()).unwrap();
        let session = session(&conn);

        let Ok(Action::AttachSender(link, attach, response)) =
            handle(peer_attach(Role::Receiver, None))
        else {
            panic!()
        };
        let link = session.inner.get_mut().attach_remote_sender_link(
            &attach,
            response,
            link.inner.clone(),
        );

        let flow = |credit, window| {
            let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
                panic!()
            };
            flow.0.incoming_window = window;
            Frame::Flow(flow)
        };

        // session and link flows are applied in frames order,
        // before control service is notified
        let Ok(Action::Flow(..)) = handle(flow(10, 50)) else {
            panic!()
        };
        assert_eq!(link.credit(), 10);
        let window = session.remote_window_size();
        let Ok(Action::Flow(..)) = handle(flow(3, 80)) else {
            panic!()
        };
        assert_eq!(session.remote_window_size(), window + 30);
        assert_eq!(link.credit(), 3);
    }

    #[ntex::test]
    async fn remote_sender_detach_before_confirm() {
        for accept in [true, false] {
            let (_io, conn, client) = connection();
            let inner = conn.get_ref().0;
            let handle = |frame: Frame| {
                inner
                    .get_mut()
                    .handle_frame(AmqpFrame::new(0, frame), &inner)
            };
            handle(begin()).unwrap();
            let session = session(&conn);

            let Ok(Action::AttachSender(link, attach, response)) =
                handle(peer_attach(Role::Receiver, None))
            else {
                panic!()
            };
            let detach = Detach(Box::new(DetachInner {
                handle: 0,
                closed: true,
                error: None,
            }));
            let Ok(Action::None) = handle(detach.into()) else {
                panic!()
            };
            // flow after detach is ignored
            let Ok(Action::None) = handle(peer_flow(Some(10))) else {
                panic!()
            };

            if accept {
                let link = session.inner.get_mut().attach_remote_sender_link(
                    &attach,
                    response,
                    link.inner.clone(),
                );
                assert!(link.is_closed());
                assert_eq!(link.credit(), 0);
                assert!(!link.ready().await);

                // control service is notified
                let conn_ref = conn.get_ref();
                let queue = conn_ref.get_control_queue().pending.borrow();
                assert!(matches!(
                    queue.back().unwrap().kind(),
                    crate::ControlFrameKind::RemoteDetachSender(..)
                ));
            } else {
                session
                    .inner
                    .get_mut()
                    .detach_unconfirmed_sender_link(&attach, &link.inner, None);
                assert!(link.is_closed());
            }

            // remote handle and link name are released
            let Ok(Action::AttachSender(..)) = handle(peer_attach(Role::Receiver, None)) else {
                panic!()
            };

            ntex::time::sleep(ntex::time::Millis(50)).await;
            let codec = AmqpCodec::<AmqpFrame>::new();
            let mut buf = BytesMut::from(&client.read_any()[..]);
            let mut frames = Vec::new();
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                match frame.into_parts().1 {
                    Frame::Attach(attach) => {
                        frames.push(format!("attach {} {:?}", attach.handle(), attach.role()));
                    }
                    Frame::Detach(detach) => {
                        frames.push(format!("detach {} {}", detach.handle(), detach.closed()));
                    }
                    _ => (),
                }
            }
            assert_eq!(frames, ["attach 0 Sender", "detach 0 true"]);
        }
    }

    #[ntex::test]
    async fn remote_receiver_reject_detach() {
        let (_io, conn, _client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };
        handle(begin()).unwrap();
        let session = session(&conn);

        // rejected link keeps handles until remote detach
        let Ok(Action::AttachReceiver(link, _, _)) =
            handle(named_attach(Role::Sender, "r", "a", 5))
        else {
            panic!()
        };
        let closed = link.close_with_error(crate::error::LinkError::force_detach());
        assert!(session.inner.get_ref().is_remote_handle_used(5));

        let Ok(Action::AttachReceiver(link2, _, response)) =
            handle(named_attach(Role::Sender, "r2", "b", 6))
        else {
            panic!()
        };
        assert_eq!(link2.handle(), 1);
        link2.confirm_receiver_link(response);

        // remote detach releases rejected link only
        let Ok(Action::None) = handle(peer_detach(5)) else {
            panic!()
        };
        closed.await.unwrap();
        assert!(!link2.is_closed());
        assert!(!session.inner.get_ref().is_remote_handle_used(5));

        let Ok(Action::AttachReceiver(link3, _, _)) =
            handle(named_attach(Role::Sender, "r3", "c", 5))
        else {
            panic!()
        };
        assert_eq!(link3.handle(), 0);
    }

    fn frame_names(client: &IoTest) -> Vec<String> {
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut frames = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            frames.push(match frame.into_parts().1 {
                Frame::Attach(att) => format!("Attach {} {}", att.name(), att.handle()),
                Frame::Detach(det) => format!("Detach {}", det.handle()),
                Frame::Flow(flow) => {
                    format!("Flow {:?} {:?}", flow.handle(), flow.link_credit())
                }
                frame => frame.name().to_string(),
            });
        }
        frames
    }

    #[ntex::test]
    async fn flow_echo_link_state() {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let session = session(&conn);
        let echo = |handle: u32, link_credit: Option<u32>| {
            let Frame::Flow(mut flow) = peer_flow(link_credit) else {
                panic!()
            };
            flow.0.handle = Some(handle);
            flow.0.echo = true;
            handle_frame(&conn, flow.into()).unwrap();
        };

        let Ok(Action::AttachReceiver(rcv, _, response)) =
            handle_frame(&conn, named_attach(Role::Sender, "r", "r", 3))
        else {
            panic!()
        };
        assert!(rcv.confirm_receiver_link(response));
        rcv.set_link_credit(10);

        let Ok(Action::AttachSender(snd, attach, response)) =
            handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
        else {
            panic!()
        };
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, snd.inner.clone());
        let Ok(Action::AttachSender(..)) =
            handle_frame(&conn, named_attach(Role::Receiver, "o", "o", 5))
        else {
            panic!()
        };
        ntex::time::sleep(ntex::time::Millis(50)).await;
        assert_eq!(
            frame_names(&client),
            ["Begin", "Attach r 0", "Flow Some(0) Some(10)", "Attach s 1"]
        );

        // echo reply carries state of established links
        echo(4, Some(7));
        echo(3, None);
        // opening and unknown links, session state only
        echo(5, Some(7));
        echo(9, Some(7));
        ntex::time::sleep(ntex::time::Millis(50)).await;
        assert_eq!(
            frame_names(&client),
            [
                "Flow Some(1) Some(7)",
                "Flow Some(0) Some(10)",
                "Flow None None",
                "Flow None None"
            ]
        );
    }

    #[ntex::test]
    async fn sender_link_drain() {
        use ntex::time::{Millis, sleep, timeout};
        use std::{future::poll_fn, task::Poll};

        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let session = session(&conn);
        let flow = |credit: u32, delivery_count: u32, drain: bool| {
            let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
                panic!()
            };
            flow.0.handle = Some(4);
            flow.0.delivery_count = Some(delivery_count);
            flow.0.drain = drain;
            handle_frame(&conn, flow.into()).unwrap();
        };
        let frames = || {
            let codec = AmqpCodec::<AmqpFrame>::new();
            let mut buf = BytesMut::from(&client.read_any()[..]);
            let mut frames = Vec::new();
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                frames.push(match frame.into_parts().1 {
                    Frame::Flow(flow) => format!(
                        "Flow {:?} {:?} {:?} {}",
                        flow.handle(),
                        flow.delivery_count(),
                        flow.link_credit(),
                        flow.drain()
                    ),
                    frame => frame.name().to_string(),
                });
            }
            frames
        };

        let Ok(Action::AttachSender(snd, attach, response)) =
            handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
        else {
            panic!()
        };
        let snd =
            session
                .inner
                .get_mut()
                .attach_remote_sender_link(&attach, response, snd.inner.clone());
        sleep(Millis(50)).await;
        assert_eq!(frames(), ["Begin", "Attach"]);

        // idle link, credit is drained immediately
        flow(5, 0, true);
        assert_eq!(snd.credit(), 0);
        sleep(Millis(50)).await;
        assert_eq!(frames(), ["Flow Some(0) Some(5) Some(0) true"]);

        // queued transfers are sent before credit is drained
        let t1 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"1")).settled().send());
        let t2 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"2")).settled().send());
        sleep(Millis(50)).await;
        flow(3, 5, true);
        assert!(timeout(Millis(500), t1).await.unwrap().unwrap().is_ok());
        assert!(timeout(Millis(500), t2).await.unwrap().unwrap().is_ok());
        sleep(Millis(50)).await;
        assert_eq!(
            frames(),
            ["Transfer", "Transfer", "Flow Some(0) Some(8) Some(0) true"]
        );

        // woken transfer is dropped before it resumes
        let mut t3 = Box::pin(snd.transfer(Bytes::from_static(b"3")).settled().send());
        assert!(poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx).is_pending())).await);
        flow(2, 8, true);
        sleep(Millis(50)).await;
        assert!(frames().is_empty());
        assert_eq!(snd.credit(), 2);
        drop(t3);
        assert_eq!(snd.credit(), 0);
        sleep(Millis(50)).await;
        assert_eq!(frames(), ["Flow Some(0) Some(10) Some(0) true"]);

        // no drain
        flow(2, 10, false);
        assert_eq!(snd.credit(), 2);
        sleep(Millis(50)).await;
        assert!(frames().is_empty());

        // drain flow received before link confirmation
        let Ok(Action::AttachSender(lnk, attach, response)) =
            handle_frame(&conn, named_attach(Role::Receiver, "d", "d", 6))
        else {
            panic!()
        };
        let Frame::Flow(mut pending) = peer_flow(Some(3)) else {
            panic!()
        };
        pending.0.handle = Some(6);
        pending.0.drain = true;
        handle_frame(&conn, pending.into()).unwrap();
        let lnk =
            session
                .inner
                .get_mut()
                .attach_remote_sender_link(&attach, response, lnk.inner.clone());
        assert_eq!(lnk.credit(), 0);
        sleep(Millis(50)).await;
        assert_eq!(frames(), ["Attach", "Flow Some(1) Some(3) Some(0) true"]);
    }

    #[ntex::test]
    async fn sender_link_credit_on_window_wait() {
        use ntex::time::{Millis, sleep, timeout};
        use std::{future::poll_fn, task::Poll};

        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let session = session(&conn);
        let flow = |delivery_count: u32, credit: u32, drain: bool, window: u32| {
            let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
                panic!()
            };
            flow.0.handle = Some(4);
            flow.0.delivery_count = Some(delivery_count);
            flow.0.drain = drain;
            flow.0.incoming_window = window;
            handle_frame(&conn, flow.into()).unwrap();
        };
        let frames = || {
            let codec = AmqpCodec::<AmqpFrame>::new();
            let mut buf = BytesMut::from(&client.read_any()[..]);
            let mut frames = Vec::new();
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                frames.push(match frame.into_parts().1 {
                    Frame::Flow(flow) => format!(
                        "Flow {:?} {:?} {:?} {}",
                        flow.handle(),
                        flow.delivery_count(),
                        flow.link_credit(),
                        flow.drain()
                    ),
                    frame => frame.name().to_string(),
                });
            }
            frames
        };

        let Ok(Action::AttachSender(snd, attach, response)) =
            handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
        else {
            panic!()
        };
        let snd =
            session
                .inner
                .get_mut()
                .attach_remote_sender_link(&attach, response, snd.inner.clone());
        sleep(Millis(50)).await;
        assert_eq!(frames(), ["Begin", "Attach"]);

        // transfer waits for session window, link credit is kept on cancel
        flow(0, 2, false, 0);
        let mut t1 = Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send());
        assert!(poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx).is_pending())).await);
        assert_eq!(snd.credit(), 2);
        drop(t1);
        assert_eq!(snd.credit(), 2);

        // waiting transfer delays drain until it is dropped
        let mut t2 = Box::pin(snd.transfer(Bytes::from_static(b"2")).settled().send());
        assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);
        flow(0, 2, true, 0);
        assert_eq!(snd.credit(), 2);
        sleep(Millis(50)).await;
        assert!(frames().is_empty());
        drop(t2);
        assert_eq!(snd.credit(), 0);
        sleep(Millis(50)).await;
        assert_eq!(frames(), ["Flow Some(0) Some(2) Some(0) true"]);

        // waiting transfer is sent before drain
        let t3 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"3")).settled().send());
        sleep(Millis(50)).await;
        flow(2, 2, true, 0);
        sleep(Millis(50)).await;
        assert!(frames().is_empty());
        assert_eq!(snd.credit(), 2);
        flow(2, 2, true, 10);
        assert!(timeout(Millis(500), t3).await.unwrap().unwrap().is_ok());
        sleep(Millis(50)).await;
        assert_eq!(frames(), ["Transfer", "Flow Some(0) Some(4) Some(0) true"]);

        // link is detached while transfer waits for session window
        let session_flow = |handle: Option<u32>, window: u32| {
            let Frame::Flow(mut flow) = peer_flow(Some(2)) else {
                panic!()
            };
            flow.0.handle = handle;
            flow.0.delivery_count = Some(4);
            flow.0.next_incoming_id = Some(2);
            flow.0.incoming_window = window;
            handle_frame(&conn, flow.into()).unwrap();
        };
        session_flow(Some(4), 0);
        let t4 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"4")).settled().send());
        sleep(Millis(50)).await;
        let _ = handle_frame(&conn, peer_detach(4));
        assert!(timeout(Millis(500), t4).await.unwrap().unwrap().is_err());
        session_flow(None, 10);
        sleep(Millis(50)).await;
        assert_eq!(frames(), ["Detach"]);
    }

    #[ntex::test]
    async fn sender_link_close_fails_waiting_transfers() {
        use ntex::time::{Millis, sleep, timeout};
        use std::{future::poll_fn, task::Poll};

        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let session = session(&conn);
        let flow = |handle: u32, credit: u32, window: u32| {
            let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
                panic!()
            };
            flow.0.handle = Some(handle);
            flow.0.incoming_window = window;
            handle_frame(&conn, flow.into()).unwrap();
        };
        let attach = |name: &str, handle: u32| {
            let Ok(Action::AttachSender(snd, attach, response)) =
                handle_frame(&conn, named_attach(Role::Receiver, name, name, handle))
            else {
                panic!()
            };
            session
                .inner
                .get_mut()
                .attach_remote_sender_link(&attach, response, snd.inner.clone())
        };
        let send = |snd: &SenderLink, body: &'static [u8]| {
            ntex::rt::spawn(snd.transfer(Bytes::from_static(body)).settled().send())
        };

        // transfer waits for link credit
        let a = attach("a", 4);
        let t1 = send(&a, b"1");
        // transfer waits for session window
        let b = attach("b", 5);
        flow(5, 1, 0);
        let t2 = send(&b, b"2");
        sleep(Millis(50)).await;
        frame_names(&client);

        let (a2, b2) = (a.clone(), b.clone());
        let _c1 = ntex::rt::spawn(async move { a2.close().await });
        let _c2 = ntex::rt::spawn(async move { b2.close().await });
        assert!(timeout(Millis(500), t1).await.unwrap().unwrap().is_err());
        assert!(timeout(Millis(500), t2).await.unwrap().unwrap().is_err());

        // link is closed after transfer is woken up
        let c = attach("c", 6);
        let t3 = send(&c, b"3");
        sleep(Millis(50)).await;
        assert_eq!(frame_names(&client), ["Detach 0", "Detach 1", "Attach c 2"]);
        flow(6, 1, 10);
        let mut close = Box::pin(c.close());
        assert!(poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx).is_pending())).await);
        assert!(timeout(Millis(500), t3).await.unwrap().unwrap().is_err());

        // no transfers after detach
        sleep(Millis(50)).await;
        assert_eq!(frame_names(&client), ["Detach 2"]);
    }

    #[ntex::test]
    async fn sender_link_detach_fails_waiting_transfers() {
        use ntex::time::{Millis, sleep, timeout};

        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let session = session(&conn);
        let flow = |handle: u32, credit: u32, window: u32| {
            let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
                panic!()
            };
            flow.0.handle = Some(handle);
            flow.0.incoming_window = window;
            handle_frame(&conn, flow.into()).unwrap();
        };
        let attach = |name: &str, handle: u32| {
            let Ok(Action::AttachSender(snd, attach, response)) =
                handle_frame(&conn, named_attach(Role::Receiver, name, name, handle))
            else {
                panic!()
            };
            session
                .inner
                .get_mut()
                .attach_remote_sender_link(&attach, response, snd.inner.clone())
        };
        let send = |snd: &SenderLink, body: &'static [u8]| {
            ntex::rt::spawn(snd.transfer(Bytes::from_static(body)).settled().send())
        };

        // transfer waits for link credit
        let a = attach("a", 4);
        let t1 = send(&a, b"1");
        // transfer waits for session window
        let b = attach("b", 5);
        flow(5, 1, 0);
        let t2 = send(&b, b"2");
        sleep(Millis(50)).await;
        frame_names(&client);

        let _d1 = session.detach_sender_link(a.inner.get_ref().id(), None);
        let _d2 = session.detach_sender_link(b.inner.get_ref().id(), None);
        let res = timeout(Millis(500), t1).await.unwrap().unwrap();
        assert!(matches!(res, Err(AmqpProtocolError::Disconnected)));
        let res = timeout(Millis(500), t2).await.unwrap().unwrap();
        assert!(matches!(res, Err(AmqpProtocolError::Disconnected)));
        assert!(a.transfer(Bytes::from_static(b"3")).send().await.is_err());

        // no transfers after detach
        flow(5, 1, 10);
        sleep(Millis(50)).await;
        assert_eq!(frame_names(&client), ["Detach 0", "Detach 1"]);
    }

    #[ntex::test]
    async fn sender_link_detach_fails_partial_delivery() {
        use ntex::time::{Millis, sleep, timeout};

        let (_io, conn, client, snd) = small_frames_sender();
        let session = session(&conn);
        sleep(Millis(50)).await;
        transfer_frames(&client);

        // delivery waits for session window, next transfer waits for delivery
        let body = Bytes::from(vec![0u8; 2048]);
        let t1 = ntex::rt::spawn(snd.transfer(body).settled().send());
        let t2 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"2")).settled().send());
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client),
            [
                "Transfer Some(0) more:true aborted:false",
                "Transfer None more:true aborted:false"
            ]
        );

        let _d = session.detach_sender_link(snd.inner.get_ref().id(), None);
        let res = timeout(Millis(500), t1).await.unwrap().unwrap();
        assert!(matches!(res, Err(AmqpProtocolError::Disconnected)));
        let res = timeout(Millis(500), t2).await.unwrap().unwrap();
        assert!(matches!(res, Err(AmqpProtocolError::Disconnected)));

        // no transfers after detach
        session_window(&conn, 1, 10);
        sleep(Millis(50)).await;
        assert_eq!(transfer_frames(&client), ["Detach"]);
    }

    #[ntex::test]
    async fn sender_link_detach_fails_unsettled() {
        use ntex::time::{Millis, sleep, timeout};
        use std::{future::poll_fn, task::Poll};

        let (_io, conn, _client, snd) = small_frames_sender();
        let session = session(&conn);
        let delivery = snd.transfer(Bytes::from_static(b"1")).send().await.unwrap();
        let mut wait = Box::pin(delivery.wait());
        assert!(poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_pending())).await);

        // disposition could be received before detach confirmation
        let _d = session.detach_sender_link(snd.inner.get_ref().id(), None);
        sleep(Millis(50)).await;
        assert!(poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_pending())).await);

        handle_frame(&conn, peer_detach(4)).unwrap();
        let res = timeout(Millis(500), wait).await.unwrap();
        assert!(matches!(res, Err(AmqpProtocolError::LinkDetached(None))));
    }

    #[ntex::test]
    async fn sender_link_stale_flow() {
        for initial in [0, u32::MAX - 1] {
            sender_link_stale_flow_with(initial).await;
        }
    }

    async fn sender_link_stale_flow_with(initial: u32) {
        use ntex::time::{Millis, sleep, timeout};
        use std::{future::poll_fn, task::Poll};

        let (_io, conn, _client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let session = session(&conn);
        let flow = |delivery_count: Option<u32>, credit: u32| {
            let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
                panic!()
            };
            flow.0.handle = Some(4);
            flow.0.delivery_count = delivery_count.map(|dc| initial.wrapping_add(dc));
            handle_frame(&conn, flow.into()).unwrap();
        };

        let Frame::Attach(mut attach) = named_attach(Role::Receiver, "s", "s", 4) else {
            panic!()
        };
        attach.0.initial_delivery_count = Some(initial);
        let Ok(Action::AttachSender(snd, attach, response)) = handle_frame(&conn, attach.into())
        else {
            panic!()
        };
        let snd =
            session
                .inner
                .get_mut()
                .attach_remote_sender_link(&attach, response, snd.inner.clone());

        // receiver has not received attach yet, initial delivery count is used
        flow(None, 10);
        assert_eq!(snd.credit(), 10);
        for _ in 0..3 {
            let t = snd.transfer(Bytes::from_static(b"1")).settled().send();
            assert!(timeout(Millis(500), t).await.unwrap().is_ok());
        }
        assert_eq!(snd.credit(), 7);
        flow(None, 10);
        assert_eq!(snd.credit(), 7);

        // flow issued before receiver got sent transfers
        flow(Some(0), 2);
        assert_eq!(snd.credit(), 0);
        let mut t = Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send());
        assert!(poll_fn(|cx| Poll::Ready(t.as_mut().poll(cx).is_pending())).await);
        flow(Some(0), 5);
        assert!(timeout(Millis(500), t).await.unwrap().is_ok());
        assert_eq!(snd.credit(), 1);

        // receiver delivery count is ahead, credit is used as is
        flow(Some(10), 4);
        assert_eq!(snd.credit(), 4);
        sleep(Millis(10)).await;
    }

    fn small_frames_sender() -> (Io, Connection, IoTest, SenderLink) {
        let remote = RemoteServiceConfig::new(&Open(Box::new(OpenInner {
            max_frame_size: 512,
            ..Default::default()
        })));
        let (server, client) = IoTest::create();
        client.remote_buffer_cap(64 * 1024);
        let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
        let io = Io::new(server, cfg.clone());
        let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
        handle_frame(&conn, begin()).unwrap();

        let Ok(Action::AttachSender(snd, attach, response)) =
            handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
        else {
            panic!()
        };
        let snd = session(&conn).inner.get_mut().attach_remote_sender_link(
            &attach,
            response,
            snd.inner.clone(),
        );
        let Frame::Flow(mut flow) = peer_flow(Some(10)) else {
            panic!()
        };
        flow.0.handle = Some(4);
        flow.0.incoming_window = 2;
        handle_frame(&conn, flow.into()).unwrap();
        (io, conn, client, snd)
    }

    fn session_window(conn: &Connection, next_incoming_id: u32, window: u32) {
        let Frame::Flow(mut flow) = peer_flow(None) else {
            panic!()
        };
        flow.0.handle = None;
        flow.0.next_incoming_id = Some(next_incoming_id);
        flow.0.incoming_window = window;
        handle_frame(conn, flow.into()).unwrap();
    }

    fn transfer_frames(client: &IoTest) -> Vec<String> {
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut frames = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            frames.push(match frame.into_parts().1 {
                Frame::Transfer(tr) => format!(
                    "Transfer {:?} more:{} aborted:{}",
                    tr.delivery_id(),
                    tr.more(),
                    tr.aborted()
                ),
                Frame::Flow(flow) => format!("Flow {}", flow.next_outgoing_id()),
                frame => frame.name().to_string(),
            });
        }
        frames
    }

    #[ntex::test]
    async fn session_flow_window() {
        use ntex::time::{Millis, sleep, timeout};
        use std::{future::poll_fn, task::Poll};

        let (_io, conn, client, snd) = small_frames_sender();
        let session = session(&conn);
        sleep(Millis(50)).await;
        transfer_frames(&client);
        assert_eq!(session.remote_window_size(), 2);
        for _ in 0..2 {
            let t = snd.transfer(Bytes::from_static(b"1")).settled().send();
            assert!(timeout(Millis(500), t).await.unwrap().is_ok());
        }
        assert_eq!(session.remote_window_size(), 0);

        // flow issued before remote got sent transfers
        session_window(&conn, 1, 1);
        assert_eq!(session.remote_window_size(), 0);
        let mut t = Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send());
        assert!(poll_fn(|cx| Poll::Ready(t.as_mut().poll(cx).is_pending())).await);
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client),
            [
                "Transfer Some(0) more:false aborted:false",
                "Transfer Some(1) more:false aborted:false"
            ]
        );
        session_window(&conn, 1, 3);
        assert!(timeout(Millis(500), t).await.unwrap().is_ok());
        assert_eq!(session.remote_window_size(), 0);

        // remote has not received begin yet, initial outgoing id is used
        let Frame::Flow(mut flow) = peer_flow(None) else {
            panic!()
        };
        flow.0.handle = None;
        flow.0.next_incoming_id = None;
        flow.0.incoming_window = 5;
        handle_frame(&conn, flow.into()).unwrap();
        assert_eq!(session.remote_window_size(), 2);

        // remote next-incoming-id is ahead, window is used as is
        session_window(&conn, 10, 3);
        assert_eq!(session.remote_window_size(), 3);
    }

    #[ntex::test]
    async fn sender_multi_frame_delivery_window() {
        use ntex::time::{Millis, sleep};
        use std::{future::poll_fn, task::Poll};

        let (_io, conn, client, snd) = small_frames_sender();
        sleep(Millis(50)).await;
        transfer_frames(&client);

        // each transfer frame consumes session window
        let mut t1 = Box::pin(snd.transfer(Bytes::from(vec![b'a'; 1200])).send());
        let mut t2 = Box::pin(snd.transfer(Bytes::from_static(b"2")).send());
        assert!(poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx).is_pending())).await);
        assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);
        assert_eq!(snd.credit(), 9);
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client),
            [
                "Transfer Some(0) more:true aborted:false",
                "Transfer None more:true aborted:false"
            ]
        );

        // frames of the delivery are not interleaved with next delivery
        session_window(&conn, 3, 1);
        assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);
        sleep(Millis(50)).await;
        assert!(transfer_frames(&client).is_empty());
        let Poll::Ready(Ok(d1)) = poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx))).await else {
            panic!()
        };
        assert_eq!(d1.id(), 0);
        assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client),
            ["Transfer None more:false aborted:false"]
        );

        // delivery id is not a transfer id
        session_window(&conn, 4, 5);
        let Poll::Ready(Ok(d2)) = poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx))).await else {
            panic!()
        };
        assert_eq!(d2.id(), 1);
        assert_eq!(snd.credit(), 8);

        // next-outgoing-id counts transfer frames
        let Frame::Flow(mut flow) = peer_flow(None) else {
            panic!()
        };
        flow.0.handle = None;
        flow.0.echo = true;
        handle_frame(&conn, flow.into()).unwrap();
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client),
            ["Transfer Some(1) more:false aborted:false", "Flow 5"]
        );
    }

    #[ntex::test]
    async fn sender_cancelled_delivery_aborted() {
        use ntex::time::{Millis, sleep, timeout};
        use std::{future::poll_fn, task::Poll};

        let (_io, conn, client, snd) = small_frames_sender();
        let body = Bytes::from(vec![b'a'; 1200]);
        let unsettled = || {
            session(&conn)
                .inner
                .get_ref()
                .unsettled_snd_deliveries
                .len()
        };

        // delivery is cancelled while it waits for session window
        let mut t1 = Box::pin(snd.transfer(body.clone()).send());
        assert!(poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx).is_pending())).await);
        assert_eq!(unsettled(), 1);
        drop(t1);
        assert_eq!(unsettled(), 0);
        sleep(Millis(50)).await;
        assert_eq!(transfer_frames(&client).len(), 4);

        // abort is sent before next delivery
        session_window(&conn, 3, 5);
        sleep(Millis(50)).await;
        assert!(transfer_frames(&client).is_empty());
        let t2 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"2")).settled().send());
        assert_eq!(
            timeout(Millis(500), t2)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .id(),
            1
        );
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client),
            [
                "Transfer Some(0) more:false aborted:true",
                "Transfer Some(1) more:false aborted:false"
            ]
        );

        // abort is sent if session window is available
        session_window(&conn, 5, 2);
        let mut t3 = Box::pin(snd.transfer(body).send());
        assert!(poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx).is_pending())).await);
        session_window(&conn, 7, 5);
        drop(t3);
        assert_eq!(unsettled(), 0);
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client),
            [
                "Transfer Some(2) more:true aborted:false",
                "Transfer None more:true aborted:false",
                "Transfer Some(2) more:false aborted:true"
            ]
        );
        assert_eq!(snd.credit(), 7);
    }

    #[ntex::test]
    async fn session_outgoing_window() {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();

        let Ok(Action::AttachReceiver(rcv, _, response)) =
            handle_frame(&conn, named_attach(Role::Sender, "r", "r", 3))
        else {
            panic!()
        };
        assert!(rcv.confirm_receiver_link(response));
        rcv.set_link_credit(10);

        let Frame::Flow(mut flow) = peer_flow(None) else {
            panic!()
        };
        flow.0.handle = None;
        flow.0.incoming_window = 7;
        flow.0.echo = true;
        handle_frame(&conn, flow.into()).unwrap();
        ntex::time::sleep(ntex::time::Millis(50)).await;

        // local outgoing window is not limited, peer windows are not echoed
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut windows = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            match frame.into_parts().1 {
                Frame::Begin(begin) => windows.push(("Begin", begin.outgoing_window())),
                Frame::Flow(flow) => windows.push(("Flow", flow.outgoing_window())),
                _ => (),
            }
        }
        assert_eq!(
            windows,
            [("Begin", u32::MAX), ("Flow", u32::MAX), ("Flow", u32::MAX)]
        );
    }

    #[ntex::test]
    async fn closing_session_connection_error() {
        use ntex::time::{Millis, sleep, timeout};

        let (_io, conn, _client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let session = session(&conn);

        // local links wait for attach response
        let s = session.clone();
        let rcv = ntex::rt::spawn(async move { s.build_receiver_link("r", "r").attach().await });
        let s = session.clone();
        let snd = ntex::rt::spawn(async move { s.build_sender_link("s", "s").attach().await });
        sleep(Millis(10)).await;

        // local end waits for remote end
        let s = session.clone();
        let end = ntex::rt::spawn(async move { s.end().await });
        sleep(Millis(10)).await;
        assert!(!end.is_finished());

        conn.get_ref()
            .0
            .get_mut()
            .set_error(AmqpProtocolError::Disconnected);
        assert!(matches!(
            timeout(Millis(100), rcv).await.unwrap().unwrap(),
            Err(AmqpProtocolError::Disconnected)
        ));
        assert!(matches!(
            timeout(Millis(100), snd).await.unwrap().unwrap(),
            Err(AmqpProtocolError::Disconnected)
        ));
        timeout(Millis(100), end).await.unwrap().unwrap().unwrap();

        // session ended with error keeps the error
        let (_io, conn, _client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let session = super::tests::session(&conn);
        let Ok(Action::AttachReceiver(..)) =
            handle_frame(&conn, named_attach(Role::Sender, "a", "a", 3))
        else {
            panic!()
        };
        let Ok(Action::SessionEnded(_)) =
            handle_frame(&conn, named_attach(Role::Sender, "b", "b", 3))
        else {
            panic!()
        };
        conn.get_ref()
            .0
            .get_mut()
            .set_error(AmqpProtocolError::Disconnected);
        let err = session
            .build_receiver_link("c", "c")
            .attach()
            .await
            .unwrap_err();
        assert!(
            matches!(err, AmqpProtocolError::SessionEnded(Some(ref e)) if e.condition() == &SessionError::HandleInUse.into()),
            "{err:?}"
        );
    }

    #[ntex::test]
    async fn session_ending_sends_no_frames() {
        use ntex::time::{Millis, sleep, timeout};

        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let session = session(&conn);

        let Ok(Action::AttachReceiver(rcv, _, response)) =
            handle_frame(&conn, named_attach(Role::Sender, "r", "r", 3))
        else {
            panic!()
        };
        assert!(rcv.confirm_receiver_link(response));
        rcv.set_link_credit(10);
        let Ok(Action::AttachSender(snd, attach, response)) =
            handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
        else {
            panic!()
        };
        let snd =
            session
                .inner
                .get_mut()
                .attach_remote_sender_link(&attach, response, snd.inner.clone());
        let Frame::Flow(mut flow) = peer_flow(Some(10)) else {
            panic!()
        };
        flow.0.handle = Some(4);
        handle_frame(&conn, flow.into()).unwrap();
        assert_eq!(snd.credit(), 10);
        sleep(Millis(50)).await;
        assert_eq!(
            frame_names(&client),
            ["Begin", "Attach r 0", "Flow Some(0) Some(10)", "Attach s 1"]
        );

        let s = session.clone();
        let end = ntex::rt::spawn(async move { s.end().await });
        sleep(Millis(10)).await;

        // new operations fail, links are closed without frames
        let tr = timeout(Millis(500), snd.transfer(Bytes::from_static(b"m")).send());
        assert!(tr.await.unwrap().is_err());
        let attach = timeout(Millis(500), session.build_sender_link("a", "a").attach());
        assert!(attach.await.unwrap().is_err());
        let attach = timeout(Millis(500), session.build_receiver_link("b", "b").attach());
        assert!(attach.await.unwrap().is_err());
        rcv.set_link_credit(5);
        assert!(timeout(Millis(500), rcv.close()).await.unwrap().is_ok());
        assert!(timeout(Millis(500), snd.close()).await.unwrap().is_ok());
        sleep(Millis(50)).await;
        assert_eq!(frame_names(&client), ["End"]);

        handle_frame(&conn, End { error: None }.into()).unwrap();
        assert!(end.await.unwrap().is_ok());
    }

    #[ntex::test]
    async fn remote_receiver_stale_confirm() {
        let (_io, conn, client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };
        handle(begin()).unwrap();
        let session = session(&conn);

        let Ok(Action::AttachReceiver(link_a, _, response_a)) =
            handle(named_attach(Role::Sender, "a", "a", 0))
        else {
            panic!()
        };
        let closed = link_a.close_with_error(crate::error::LinkError::force_detach());
        handle(peer_detach(0)).unwrap();
        closed.await.unwrap();

        // new link reuses handle of closed link
        let Ok(Action::AttachReceiver(link_b, _, response_b)) =
            handle(named_attach(Role::Sender, "b", "b", 1))
        else {
            panic!()
        };
        assert_eq!(link_a.handle(), link_b.handle());

        // stale confirmation and credit are ignored
        assert!(!session.inner.get_mut().confirm_receiver_link(
            &link_a.inner,
            response_a.clone(),
            None
        ));
        assert!(!link_a.confirm_receiver_link(response_a));
        link_a.set_link_credit(10);
        assert_eq!(link_a.credit(), 0);

        assert!(link_b.confirm_receiver_link(response_b));
        link_b.set_link_credit(5);

        ntex::time::sleep(ntex::time::Millis(50)).await;
        assert_eq!(
            frame_names(&client),
            [
                "Begin",
                "Attach a 0",
                "Detach 0",
                "Attach b 0",
                "Flow Some(0) Some(5)"
            ]
        );
    }

    #[ntex::test]
    async fn remote_receiver_credit_before_confirm() {
        let (_io, conn, client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };
        handle(begin()).unwrap();

        let Ok(Action::AttachReceiver(link, _, response)) =
            handle(named_attach(Role::Sender, "a", "a", 0))
        else {
            panic!()
        };

        // credit is sent after attach response
        link.set_link_credit(10);
        ntex::time::sleep(ntex::time::Millis(50)).await;
        assert_eq!(frame_names(&client), ["Begin"]);

        assert!(link.confirm_receiver_link(response));
        link.set_link_credit(5);

        ntex::time::sleep(ntex::time::Millis(50)).await;
        assert_eq!(
            frame_names(&client),
            [
                "Attach a 0",
                "Flow Some(0) Some(10)",
                "Flow Some(0) Some(15)"
            ]
        );
    }

    #[ntex::test]
    async fn remote_sender_reject_detach() {
        let (_io, conn, _client) = connection();
        let inner = conn.get_ref().0;
        let handle = |frame: Frame| {
            inner
                .get_mut()
                .handle_frame(AmqpFrame::new(0, frame), &inner)
        };
        handle(begin()).unwrap();
        let session = session(&conn);

        // rejected link keeps handles until remote detach
        let Ok(Action::AttachSender(link, attach, _)) =
            handle(named_attach(Role::Receiver, "s", "a", 1))
        else {
            panic!()
        };
        session
            .inner
            .get_mut()
            .detach_unconfirmed_sender_link(&attach, &link.inner, None);
        assert!(session.inner.get_ref().is_remote_handle_used(1));

        // local handle of established link is equal to remote handle of rejected link
        let Ok(Action::AttachSender(link2, attach, response)) =
            handle(named_attach(Role::Receiver, "s2", "b", 7))
        else {
            panic!()
        };
        let link2 = session.inner.get_mut().attach_remote_sender_link(
            &attach,
            response,
            link2.inner.clone(),
        );
        assert_eq!(link2.id(), 1);

        // remote detach releases rejected link only
        let Ok(Action::None) = handle(peer_detach(1)) else {
            panic!()
        };
        assert!(!link2.is_closed());
        assert!(!session.inner.get_ref().is_remote_handle_used(1));

        // detach of unknown remote handle is ignored
        let Ok(Action::None) = handle(peer_detach(1)) else {
            panic!()
        };
        assert!(!link2.is_closed());

        let Ok(Action::AttachSender(link3, ..)) =
            handle(named_attach(Role::Receiver, "s3", "c", 1))
        else {
            panic!()
        };
        assert_eq!(link3.id(), 0);
    }

    #[ntex::test]
    async fn remote_receiver_detach_before_confirm() {
        for accept in [true, false] {
            let (_io, conn, client) = connection();
            let inner = conn.get_ref().0;
            let handle = |frame: Frame| {
                inner
                    .get_mut()
                    .handle_frame(AmqpFrame::new(0, frame), &inner)
            };
            handle(begin()).unwrap();
            let session = session(&conn);

            let Ok(Action::AttachReceiver(link, _, response)) =
                handle(named_attach(Role::Sender, "r", "a", 5))
            else {
                panic!()
            };
            let Ok(Action::None) = handle(peer_detach(5)) else {
                panic!()
            };
            assert!(!link.is_closed());

            if accept {
                assert!(!link.confirm_receiver_link(response));
                assert!(link.is_closed());

                // control service is notified
                let conn_ref = conn.get_ref();
                let queue = conn_ref.get_control_queue().pending.borrow();
                assert!(matches!(
                    queue.back().unwrap().kind(),
                    crate::ControlFrameKind::RemoteDetachReceiver(..)
                ));
            } else {
                link.close_with_error(crate::error::LinkError::force_detach())
                    .await
                    .unwrap();
            }

            // remote handle and link are released
            assert!(!session.inner.get_ref().is_remote_handle_used(5));
            let Ok(Action::AttachReceiver(link2, ..)) =
                handle(named_attach(Role::Sender, "r", "a", 5))
            else {
                panic!()
            };
            assert_eq!(link2.handle(), 0);

            ntex::time::sleep(ntex::time::Millis(50)).await;
            let codec = AmqpCodec::<AmqpFrame>::new();
            let mut buf = BytesMut::from(&client.read_any()[..]);
            let mut frames = Vec::new();
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                match frame.into_parts().1 {
                    Frame::Attach(attach) => {
                        frames.push(format!("attach {} {:?}", attach.handle(), attach.role()));
                    }
                    Frame::Detach(detach) => {
                        frames.push(format!("detach {} {}", detach.handle(), detach.closed()));
                    }
                    Frame::Flow(_) => frames.push("flow".into()),
                    _ => (),
                }
            }
            assert_eq!(frames, ["attach 0 Receiver", "detach 0 true"]);
        }
    }

    #[ntex::test]
    async fn remote_sender_end_before_confirm() {
        for (local, accept) in [(false, true), (false, false), (true, true), (true, false)] {
            let (_io, conn, client) = connection();
            let inner = conn.get_ref().0;
            let handle = |frame: Frame| {
                inner
                    .get_mut()
                    .handle_frame(AmqpFrame::new(0, frame), &inner)
            };
            handle(begin()).unwrap();
            let session = session(&conn);

            let Ok(Action::AttachSender(link, attach, response)) =
                handle(peer_attach(Role::Receiver, None))
            else {
                panic!()
            };

            if local {
                let s = session.clone();
                ntex::rt::spawn(async move { s.end().await });
                ntex::time::sleep(ntex::time::Millis(10)).await;

                let conn_ref = conn.get_ref();
                let queue = conn_ref.get_control_queue().pending.borrow();
                let crate::ControlFrameKind::LocalSessionEnded(links) =
                    queue.back().unwrap().kind()
                else {
                    panic!()
                };
                assert_eq!(links.len(), 1);
                assert!(!link.is_closed());
            } else {
                let Ok(Action::SessionEnded(links)) = handle(End { error: None }.into()) else {
                    panic!()
                };
                assert_eq!(links.len(), 1);
                assert!(link.is_closed());
                let ready = ntex::time::timeout(ntex::time::Millis(100), link.ready()).await;
                assert!(matches!(ready, Ok(false)));
            }

            // confirmation after end does not send frames
            if accept {
                session.inner.get_mut().attach_remote_sender_link(
                    &attach,
                    response,
                    link.inner.clone(),
                );
            } else {
                session
                    .inner
                    .get_mut()
                    .detach_unconfirmed_sender_link(&attach, &link.inner, None);
            }

            if local {
                // remote end confirms local end
                handle(End { error: None }.into()).unwrap();
                assert!(link.is_closed());
            }

            ntex::time::sleep(ntex::time::Millis(50)).await;
            let codec = AmqpCodec::<AmqpFrame>::new();
            let mut buf = BytesMut::from(&client.read_any()[..]);
            let mut frames = Vec::new();
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                frames.push(frame.into_parts().1.name());
            }
            assert_eq!(frames, ["Begin", "End"], "local: {local} accept: {accept}");
        }
    }

    #[ntex::test]
    async fn remote_receiver_end_before_confirm() {
        for (local, accept) in [(false, true), (false, false), (true, true), (true, false)] {
            let (_io, conn, client) = connection();
            let inner = conn.get_ref().0;
            let handle = |frame: Frame| {
                inner
                    .get_mut()
                    .handle_frame(AmqpFrame::new(0, frame), &inner)
            };
            handle(begin()).unwrap();
            let session = session(&conn);

            let Ok(Action::AttachReceiver(link, _, response)) =
                handle(named_attach(Role::Sender, "r", "a", 0))
            else {
                panic!()
            };

            if local {
                let s = session.clone();
                ntex::rt::spawn(async move { s.end().await });
                ntex::time::sleep(ntex::time::Millis(10)).await;

                let conn_ref = conn.get_ref();
                let queue = conn_ref.get_control_queue().pending.borrow();
                let crate::ControlFrameKind::LocalSessionEnded(links) =
                    queue.back().unwrap().kind()
                else {
                    panic!()
                };
                assert_eq!(links.len(), 1);
                assert!(!link.is_closed());
            } else {
                let Ok(Action::SessionEnded(links)) = handle(End { error: None }.into()) else {
                    panic!()
                };
                assert!(matches!(&links[..], [ntex::util::Either::Right(l)] if *l == link));
                assert!(link.is_closed());
            }

            // confirmation after end does not send frames
            if accept {
                assert!(!link.confirm_receiver_link(response));
            } else {
                ntex::time::timeout(
                    ntex::time::Millis(500),
                    link.close_with_error(crate::error::LinkError::force_detach()),
                )
                .await
                .unwrap()
                .unwrap();
            }

            if local {
                // remote end confirms local end
                handle(End { error: None }.into()).unwrap();
                assert!(link.is_closed());
            }

            ntex::time::sleep(ntex::time::Millis(50)).await;
            let codec = AmqpCodec::<AmqpFrame>::new();
            let mut buf = BytesMut::from(&client.read_any()[..]);
            let mut frames = Vec::new();
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                frames.push(frame.into_parts().1.name());
            }
            assert_eq!(frames, ["Begin", "End"], "local: {local} accept: {accept}");
        }
    }
}
