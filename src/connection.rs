use futures::{future, Future};

use ntex::channel::{condition::Condition, condition::Waiter, oneshot};
use ntex::framed::State;

use crate::cell::Cell;
use crate::codec::protocol::{Begin, Close, End, Error, Frame};
use crate::codec::{AmqpCodec, AmqpCodecError, AmqpFrame};
use crate::error::AmqpProtocolError;
use crate::session::{Session, SessionInner};
use crate::{Configuration, HashMap};

#[derive(Clone)]
pub struct Connection(pub(crate) Cell<ConnectionInner>);

pub(crate) struct ConnectionInner {
    st: ConnectionState,
    state: State<AmqpCodec<AmqpFrame>>,
    pub(crate) sessions: slab::Slab<ChannelState>,
    pub(crate) sessions_map: HashMap<u16, usize>,
    pub(crate) on_close: Condition,
    pub(crate) error: Option<AmqpProtocolError>,
    channel_max: usize,
    pub(crate) max_frame_size: usize,
}

pub(crate) enum ChannelState {
    Opening(Option<oneshot::Sender<Session>>, Cell<ConnectionInner>),
    Established(Cell<SessionInner>),
    #[allow(dead_code)]
    Closing(Option<oneshot::Sender<Result<(), AmqpProtocolError>>>),
}

impl ChannelState {
    fn is_opening(&self) -> bool {
        matches!(self, ChannelState::Opening(_, _))
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
        state: State<AmqpCodec<AmqpFrame>>,
        local_config: &Configuration,
        remote_config: &Configuration,
    ) -> Connection {
        Connection(Cell::new(ConnectionInner {
            state,
            st: ConnectionState::Normal,
            sessions: slab::Slab::with_capacity(8),
            sessions_map: HashMap::default(),
            error: None,
            on_close: Condition::new(),
            channel_max: local_config.channel_max,
            max_frame_size: remote_config.max_frame_size as usize,
        }))
    }

    #[inline]
    /// Force close connection
    pub fn force_close(&self) {
        let inner = self.0.get_mut();
        inner.st = ConnectionState::Drop;
        inner.state.shutdown();
    }

    #[inline]
    /// Check connection state
    pub fn is_opened(&mut self) -> bool {
        let inner = self.0.get_mut();
        if inner.st != ConnectionState::Normal {
            return false;
        }
        inner.error.is_none()
    }

    /// Get waiter for on_close event
    pub fn on_close(&self) -> Waiter {
        self.0.get_ref().on_close.wait()
    }

    /// Get connection error
    pub fn get_error(&self) -> Option<AmqpProtocolError> {
        self.0.get_ref().error.clone()
    }

    /// Gracefully close connection
    pub fn close(&self) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        self.0.get_ref().state.close();
        future::ok(())
    }

    // TODO: implement
    /// Close connection with error
    pub fn close_with_error<E>(
        &self,
        _err: E,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>>
    where
        Error: From<E>,
    {
        self.0.get_ref().state.close();
        future::ok(())
    }

    /// Opens the session
    pub fn open_session(&self) -> impl Future<Output = Result<Session, AmqpProtocolError>> {
        let cell = self.0.clone();
        let inner = self.0.clone();

        async move {
            let inner = inner.get_mut();

            if let Some(ref e) = inner.error {
                log::error!("Connection is in error state: {:?}", e);
                Err(e.clone())
            } else {
                let (tx, rx) = oneshot::channel();

                let entry = inner.sessions.vacant_entry();
                let token = entry.key();

                if token >= inner.channel_max {
                    log::trace!("Too many channels: {:?}", token);
                    Err(AmqpProtocolError::TooManyChannels)
                } else {
                    entry.insert(ChannelState::Opening(Some(tx), cell));

                    let begin = Begin {
                        remote_channel: None,
                        next_outgoing_id: 1,
                        incoming_window: std::u32::MAX,
                        outgoing_window: std::u32::MAX,
                        handle_max: std::u32::MAX,
                        offered_capabilities: None,
                        desired_capabilities: None,
                        properties: None,
                    };
                    inner.post_frame(AmqpFrame::new(token as u16, begin.into()));

                    rx.await.map_err(|_| AmqpProtocolError::Disconnected)
                }
            }
        }
    }

    /// Get session by remote id. This method panics if session does not exists or in opening/closing state.
    pub(crate) fn get_remote_session(&self, id: usize) -> Option<Cell<SessionInner>> {
        let inner = self.0.get_ref();
        inner.sessions_map.get(&(id as u16)).and_then(|token| {
            inner.sessions.get(*token).and_then(|channel| {
                if let ChannelState::Established(ref session) = channel {
                    Some(session.clone())
                } else {
                    None
                }
            })
        })
    }

    pub(crate) fn register_remote_session(
        &self,
        channel_id: u16,
        begin: &Begin,
    ) -> Result<(), AmqpCodecError> {
        trace!("remote session opened: {:?}", channel_id);

        let cell = self.0.clone();
        let inner = self.0.get_mut();
        let entry = inner.sessions.vacant_entry();
        let token = entry.key();

        let session = Cell::new(SessionInner::new(
            token,
            false,
            Connection(cell),
            token as u16,
            begin.next_outgoing_id(),
            begin.incoming_window(),
            begin.outgoing_window(),
        ));
        entry.insert(ChannelState::Established(session));
        inner.sessions_map.insert(channel_id, token);

        let begin = Begin {
            remote_channel: Some(channel_id),
            next_outgoing_id: 1,
            incoming_window: std::u32::MAX,
            outgoing_window: begin.incoming_window(),
            handle_max: std::u32::MAX,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };

        inner
            .state
            .write_item(AmqpFrame::new(token as u16, begin.into()))
            .map(|_| ())
    }

    pub(crate) fn post_frame(&self, frame: AmqpFrame) {
        let inner = self.0.get_mut();
        if let Err(e) = inner.state.write_item(frame) {
            inner.set_error(e.into())
        }
    }
}

impl ConnectionInner {
    pub(crate) fn set_error(&mut self, err: AmqpProtocolError) {
        log::trace!("Set connection error: {:?}", err);
        for (_, channel) in self.sessions.iter_mut() {
            match channel {
                ChannelState::Opening(_, _) | ChannelState::Closing(_) => (),
                ChannelState::Established(ref mut ses) => {
                    ses.get_mut().set_error(err.clone());
                }
            }
        }
        self.sessions.clear();
        self.sessions_map.clear();

        if self.error.is_none() {
            self.error = Some(err);
        }
    }

    pub(crate) fn post_frame(&mut self, frame: AmqpFrame) {
        if let Err(e) = self.state.write_item(frame) {
            self.set_error(e.into())
        }
    }

    pub(crate) fn complete_session_creation(
        &mut self,
        channel_id: u16,
        remote_channel_id: u16,
        begin: &Begin,
    ) {
        trace!(
            "Session opened: local {:?} remote {:?}",
            channel_id,
            remote_channel_id,
        );

        let id = remote_channel_id as usize;

        if let Some(channel) = self.sessions.get_mut(id) {
            if channel.is_opening() {
                if let ChannelState::Opening(tx, cell) = channel {
                    let session = Cell::new(SessionInner::new(
                        id,
                        true,
                        Connection(cell.clone()),
                        channel_id,
                        begin.next_outgoing_id(),
                        begin.incoming_window(),
                        begin.outgoing_window(),
                    ));
                    self.sessions_map.insert(channel_id, id);

                    // TODO: send end session if `tx` is None
                    tx.take()
                        .and_then(|tx| tx.send(Session::new(session.clone())).err());
                    *channel = ChannelState::Established(session)
                }
            } else {
                // TODO: send error response
            }
        } else {
            // TODO: rogue begin right now - do nothing. in future might indicate incoming attach
        }
    }

    pub(crate) fn handle_frame(
        &mut self,
        frame: AmqpFrame,
    ) -> Result<Option<AmqpFrame>, AmqpProtocolError> {
        if let Frame::Empty = frame.performative() {
            return Ok(None);
        }

        if let Frame::Close(ref close) = frame.performative() {
            self.set_error(AmqpProtocolError::Closed(close.error.clone()));

            if self.st == ConnectionState::Closing {
                log::trace!("Connection closed: {:?}", close);
                self.set_error(AmqpProtocolError::Disconnected);
            } else {
                log::trace!("Connection closed remotely: {:?}", close);
                let close = Close { error: None };
                self.post_frame(AmqpFrame::new(0, close.into()));
                self.st = ConnectionState::RemoteClose;
            }
            return Ok(None);
        }

        if self.error.is_some() {
            error!("Connection closed but new framed is received: {:?}", frame);
            return Ok(None);
        }

        // get local session id
        let state = if let Some(token) = self.sessions_map.get(&frame.channel_id()) {
            if let Some(state) = self.sessions.get_mut(*token) {
                state
            } else {
                log::error!("Inconsistent internal state");
                let (id, frame) = frame.into_parts();
                return Err(AmqpProtocolError::UnknownSession(
                    id as usize,
                    Box::new(frame),
                ));
            }
        } else {
            // we dont have channel info, only Begin frame is allowed on new channel
            if let Frame::Begin(ref begin) = frame.performative() {
                // response Begin for open session
                if let Some(id) = begin.remote_channel() {
                    self.complete_session_creation(frame.channel_id(), id, begin);
                    return Ok(None);
                } else {
                    return Ok(Some(frame));
                }
            } else {
                let (id, frame) = frame.into_parts();
                return Err(AmqpProtocolError::UnknownSession(
                    id as usize,
                    Box::new(frame),
                ));
            }
        };

        // handle session frames
        match state {
            ChannelState::Opening(_, _) => {
                error!("Unexpected opening state: {}", frame.channel_id());
                Err(AmqpProtocolError::UnexpectedOpeningState(Box::new(
                    frame.into_parts().1,
                )))
            }
            ChannelState::Established(ref mut session) => match frame.performative() {
                Frame::Attach(attach) => {
                    let cell = session.clone();
                    if !session.get_mut().handle_attach(attach, cell) {
                        Ok(Some(frame))
                    } else {
                        Ok(None)
                    }
                }
                Frame::Flow(_) | Frame::Detach(_) => Ok(Some(frame)),
                Frame::End(remote_end) => {
                    trace!("Remote session end: {}", frame.channel_id());
                    let end = End { error: None };
                    session
                        .get_mut()
                        .set_error(AmqpProtocolError::SessionEnded(remote_end.error.clone()));
                    let id = session.get_mut().id();
                    self.post_frame(AmqpFrame::new(id, end.into()));
                    if let Some(token) = self.sessions_map.remove(&frame.channel_id()) {
                        self.sessions.remove(token);
                    }
                    Ok(None)
                }
                _ => {
                    session.get_mut().handle_frame(frame.into_parts().1);
                    Ok(None)
                }
            },
            ChannelState::Closing(ref mut tx) => match frame.performative() {
                Frame::End(frm) => {
                    trace!("Session end is confirmed: {:?}", frm);
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(Ok(()));
                    }
                    if let Some(token) = self.sessions_map.remove(&frame.channel_id()) {
                        self.sessions.remove(token);
                    }
                    Ok(None)
                }
                frm => {
                    trace!("Got frame after initiated session end: {:?}", frm);
                    Ok(None)
                }
            },
        }
    }
}
