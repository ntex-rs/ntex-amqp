use std::collections::VecDeque;
use std::time::Duration;

use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_utils::time::LowResTimeService;
use futures::future::{err, Either};
use futures::task::AtomicTask;
use futures::unsync::oneshot;
use futures::{future, Async, Future, Poll, Sink, Stream};
use hashbrown::HashMap;

use amqp_codec::protocol::{Begin, Close, End, Error, Frame};
use amqp_codec::{AmqpCodec, AmqpCodecError, AmqpFrame};

use crate::cell::{Cell, WeakCell};
use crate::errors::AmqpTransportError;
use crate::hb::{Heartbeat, HeartbeatAction};
use crate::session::{Session, SessionInner};
use crate::Configuration;

pub struct Connection<T: AsyncRead + AsyncWrite> {
    inner: Cell<ConnectionInner>,
    framed: Framed<T, AmqpCodec<AmqpFrame>>,
    hb: Heartbeat,
}

pub(crate) enum ChannelState {
    Opening(Option<oneshot::Sender<Session>>, WeakCell<ConnectionInner>),
    Established(Cell<SessionInner>),
    Closing(Option<oneshot::Sender<Result<(), AmqpTransportError>>>),
}

impl ChannelState {
    fn is_opening(&self) -> bool {
        match self {
            ChannelState::Opening(_, _) => true,
            _ => false,
        }
    }
}

pub(crate) struct ConnectionInner {
    local: Configuration,
    remote: Configuration,
    write_queue: VecDeque<AmqpFrame>,
    write_task: AtomicTask,
    sessions: slab::Slab<ChannelState>,
    sessions_map: HashMap<u16, usize>,
    error: Option<AmqpTransportError>,
    state: State,
}

#[derive(PartialEq)]
enum State {
    Normal,
    Closing,
    RemoteClose,
    Drop,
}

impl<T: AsyncRead + AsyncWrite> Connection<T> {
    pub fn new(
        framed: Framed<T, AmqpCodec<AmqpFrame>>,
        local: Configuration,
        remote: Configuration,
        time: Option<LowResTimeService>,
    ) -> Connection<T> {
        Connection {
            framed,
            hb: Heartbeat::new(
                local.timeout().unwrap(),
                remote.timeout(),
                time.unwrap_or_else(|| LowResTimeService::with(Duration::from_secs(1))),
            ),
            inner: Cell::new(ConnectionInner::new(local, remote)),
        }
    }

    pub(crate) fn new_server(
        framed: Framed<T, AmqpCodec<AmqpFrame>>,
        inner: Cell<ConnectionInner>,
        time: Option<LowResTimeService>,
    ) -> Connection<T> {
        let l_timeout = inner.get_ref().local.timeout().unwrap();
        let r_timeout = inner.get_ref().remote.timeout();
        Connection {
            framed,
            inner,
            hb: Heartbeat::new(
                l_timeout,
                r_timeout,
                time.unwrap_or_else(|| LowResTimeService::with(Duration::from_secs(1))),
            ),
        }
    }

    /// Connection controller
    pub fn controller(&self) -> ConnectionController {
        ConnectionController(self.inner.clone())
    }

    /// Get remote configuration
    pub fn remote_config(&self) -> &Configuration {
        &self.inner.get_ref().remote
    }

    /// Gracefully close connection
    pub fn close(&mut self) -> impl Future<Item = (), Error = AmqpTransportError> {
        future::ok(())
    }

    // TODO: implement
    /// Close connection with error
    pub fn close_with_error(
        &mut self,
        _err: Error,
    ) -> impl Future<Item = (), Error = AmqpTransportError> {
        future::ok(())
    }

    /// Opens the session
    pub fn open_session(&mut self) -> impl Future<Item = Session, Error = AmqpTransportError> {
        let cell = self.inner.downgrade();
        let inner = self.inner.get_mut();

        if let Some(ref e) = inner.error {
            Either::A(err(e.clone()))
        } else {
            let (tx, rx) = oneshot::channel();

            let entry = inner.sessions.vacant_entry();
            let token = entry.key();

            if token >= inner.local.channel_max {
                Either::A(err(AmqpTransportError::TooManyChannels))
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
                Either::B(rx.map_err(|_e| AmqpTransportError::Disconnected))
            }
        }
    }

    /// Get session by id. This method panics if session does not exists or in opening/closing state.
    pub(crate) fn get_session(&self, id: usize) -> Cell<SessionInner> {
        if let Some(channel) = self.inner.get_ref().sessions.get(id) {
            if let ChannelState::Established(ref session) = channel {
                return session.clone();
            }
        }
        panic!("Session not found: {}", id);
    }

    pub(crate) fn register_remote_session(&mut self, channel_id: u16, begin: &Begin) {
        trace!("remote session opened: {:?}", channel_id);

        let cell = self.inner.clone();
        let inner = self.inner.get_mut();
        let entry = inner.sessions.vacant_entry();
        let token = entry.key();

        let session = Cell::new(SessionInner::new(
            token,
            false,
            ConnectionController(cell),
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
        inner.post_frame(AmqpFrame::new(token as u16, begin.into()));
    }

    pub(crate) fn send_frame(&mut self, frame: AmqpFrame) {
        self.inner.get_mut().post_frame(frame)
    }

    pub(crate) fn register_write_task(&self) {
        self.inner.write_task.register();
    }

    pub(crate) fn poll_outgoing(&mut self) -> Poll<(), AmqpCodecError> {
        let inner = self.inner.get_mut();
        let mut update = false;
        loop {
            while !self.framed.is_write_buf_full() {
                if let Some(frame) = inner.pop_next_frame() {
                    trace!("outgoing: {:#?}", frame);
                    update = true;
                    if let Err(e) = self.framed.force_send(frame) {
                        inner.set_error(e.clone().into());
                        return Err(e);
                    }
                } else {
                    break;
                }
            }

            if !self.framed.is_write_buf_empty() {
                match self.framed.poll_complete() {
                    Ok(Async::NotReady) => break,
                    Err(e) => {
                        trace!("error sending data: {}", e);
                        inner.set_error(e.clone().into());
                        return Err(e);
                    }
                    Ok(Async::Ready(_)) => (),
                }
            } else {
                break;
            }
        }
        self.hb.update_remote(update);

        if inner.state == State::Drop {
            Ok(Async::Ready(()))
        } else if inner.state == State::RemoteClose
            && inner.write_queue.is_empty()
            && self.framed.is_write_buf_empty()
        {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }

    pub(crate) fn poll_incoming(&mut self) -> Poll<Option<AmqpFrame>, AmqpCodecError> {
        let inner = self.inner.get_mut();

        let mut update = false;
        loop {
            match self.framed.poll() {
                Ok(Async::Ready(Some(frame))) => {
                    trace!("incoming: {:#?}", frame);

                    update = true;

                    if let Frame::Empty = frame.performative() {
                        self.hb.update_local(update);
                        continue;
                    }

                    // handle connection close
                    if let Frame::Close(ref close) = frame.performative() {
                        inner.set_error(AmqpTransportError::Closed(close.error.clone()));

                        if inner.state == State::Closing {
                            inner.sessions.clear();
                            return Ok(Async::Ready(None));
                        } else {
                            let close = Close { error: None };
                            inner.post_frame(AmqpFrame::new(0, close.into()));
                            inner.state = State::RemoteClose;
                        }
                    }

                    if inner.error.is_some() {
                        error!("connection closed but new framed is received: {:?}", frame);
                        return Ok(Async::Ready(None));
                    }

                    // get local session id
                    let channel_id =
                        if let Some(token) = inner.sessions_map.get(&frame.channel_id()) {
                            *token
                        } else {
                            // we dont have channel info, only Begin frame is allowed on new channel
                            if let Frame::Begin(ref begin) = frame.performative() {
                                if begin.remote_channel().is_some() {
                                    inner.complete_session_creation(frame.channel_id(), begin);
                                } else {
                                    return Ok(Async::Ready(Some(frame)));
                                }
                            } else {
                                warn!("Unexpected frame: {:#?}", frame);
                            }
                            continue;
                        };

                    // handle session frames
                    if let Some(channel) = inner.sessions.get_mut(channel_id) {
                        match channel {
                            ChannelState::Opening(_, _) => {
                                error!("Unexpected opening state: {}", channel_id);
                            }
                            ChannelState::Established(ref mut session) => {
                                match frame.performative() {
                                    Frame::Attach(attach) => {
                                        let cell = session.clone();
                                        if !session.get_mut().handle_attach(attach, cell) {
                                            return Ok(Async::Ready(Some(frame)));
                                        }
                                    }
                                    Frame::Flow(_) => {
                                        return Ok(Async::Ready(Some(frame)));
                                    }
                                    Frame::Detach(detach) => {
                                        session.get_mut().handle_detach(detach);
                                    }
                                    Frame::End(remote_end) => {
                                        trace!("Remote session end: {}", frame.channel_id());
                                        let end = End { error: None };
                                        session.get_mut().set_error(
                                            AmqpTransportError::SessionEnded(
                                                remote_end.error.clone(),
                                            ),
                                        );
                                        let id = session.get_mut().id();
                                        inner.post_frame(AmqpFrame::new(id, end.into()));
                                        inner.sessions.remove(channel_id);
                                        inner.sessions_map.remove(&frame.channel_id());
                                    }
                                    _ => session.get_mut().handle_frame(frame),
                                }
                            }
                            ChannelState::Closing(ref mut tx) => match frame.performative() {
                                Frame::End(_) => {
                                    if let Some(tx) = tx.take() {
                                        let _ = tx.send(Ok(()));
                                    }
                                    inner.sessions.remove(channel_id);
                                    inner.sessions_map.remove(&frame.channel_id());
                                }
                                frm => trace!("Got frame after initiated session end: {:?}", frm),
                            },
                        }
                    } else {
                        error!("Can not find channel: {}", channel_id);
                        continue;
                    }
                }
                Ok(Async::Ready(None)) => {
                    inner.set_error(AmqpTransportError::Disconnected);
                    return Ok(Async::Ready(None));
                }
                Ok(Async::NotReady) => {
                    self.hb.update_local(update);
                    break;
                }
                Err(e) => {
                    trace!("error reading: {:?}", e);
                    inner.set_error(e.clone().into());
                    return Err(e.into());
                }
            }
        }

        Ok(Async::NotReady)
    }
}

impl<T: AsyncRead + AsyncWrite> Drop for Connection<T> {
    fn drop(&mut self) {
        self.inner
            .get_mut()
            .set_error(AmqpTransportError::Disconnected);
    }
}

impl<T: AsyncRead + AsyncWrite> Future for Connection<T> {
    type Item = ();
    type Error = AmqpCodecError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let inner = self.inner.get_mut();

        // connection heartbeat
        match self.hb.poll() {
            Ok(act) => match act {
                HeartbeatAction::None => (),
                HeartbeatAction::Close => {
                    inner.set_error(AmqpTransportError::Timeout);
                    return Ok(Async::Ready(()));
                }
                HeartbeatAction::Heartbeat => {
                    inner.write_queue.push_back(AmqpFrame::new(0, Frame::Empty));
                }
            },
            Err(e) => {
                inner.set_error(e);
                return Ok(Async::Ready(()));
            }
        }

        loop {
            match self.poll_incoming()? {
                Async::Ready(None) => return Ok(Async::Ready(())),
                Async::Ready(Some(frame)) => {
                    warn!("Unexpected frame: {:?}", frame);
                }
                Async::NotReady => break,
            }
        }
        let _ = self.poll_outgoing()?;
        self.register_write_task();

        match self.poll_incoming()? {
            Async::Ready(None) => return Ok(Async::Ready(())),
            Async::Ready(Some(frame)) => {
                warn!("Unexpected frame: {:?}", frame);
            }
            Async::NotReady => (),
        }

        Ok(Async::NotReady)
    }
}

#[derive(Clone)]
pub struct ConnectionController(pub(crate) Cell<ConnectionInner>);

impl ConnectionController {
    pub(crate) fn new(local: Configuration) -> ConnectionController {
        ConnectionController(Cell::new(ConnectionInner {
            local,
            remote: Configuration::default(),
            write_queue: VecDeque::new(),
            write_task: AtomicTask::new(),
            sessions: slab::Slab::with_capacity(8),
            sessions_map: HashMap::new(),
            error: None,
            state: State::Normal,
        }))
    }

    pub(crate) fn set_remote(&mut self, remote: Configuration) {
        self.0.get_mut().remote = remote;
    }

    #[inline]
    /// Get remote connection configuration
    pub fn remote_config(&self) -> &Configuration {
        &self.0.get_ref().remote
    }

    #[inline]
    /// Drop connection
    pub fn drop_connection(&mut self) {
        let inner = self.0.get_mut();
        inner.state = State::Drop;
        inner.write_task.notify()
    }

    pub(crate) fn post_frame(&mut self, frame: AmqpFrame) {
        self.0.get_mut().post_frame(frame)
    }

    pub(crate) fn drop_session_copy(&mut self, _id: usize) {}
}

impl ConnectionInner {
    pub(crate) fn new(local: Configuration, remote: Configuration) -> ConnectionInner {
        ConnectionInner {
            local,
            remote,
            write_queue: VecDeque::new(),
            write_task: AtomicTask::new(),
            sessions: slab::Slab::with_capacity(8),
            sessions_map: HashMap::new(),
            error: None,
            state: State::Normal,
        }
    }

    fn set_error(&mut self, err: AmqpTransportError) {
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

        self.error = Some(err);
    }

    fn pop_next_frame(&mut self) -> Option<AmqpFrame> {
        self.write_queue.pop_front()
    }

    fn post_frame(&mut self, frame: AmqpFrame) {
        self.write_queue.push_back(frame);
        self.write_task.notify();
    }

    fn complete_session_creation(&mut self, channel_id: u16, begin: &Begin) {
        trace!(
            "session opened: {:?} {:?}",
            channel_id,
            begin.remote_channel()
        );

        let id = begin.remote_channel().unwrap() as usize;

        if let Some(channel) = self.sessions.get_mut(id) {
            if channel.is_opening() {
                if let ChannelState::Opening(tx, cell) = channel {
                    let cell = cell.upgrade().unwrap();
                    let session = Cell::new(SessionInner::new(
                        id,
                        true,
                        ConnectionController(cell),
                        channel_id,
                        begin.next_outgoing_id(),
                        begin.incoming_window(),
                        begin.outgoing_window(),
                    ));
                    self.sessions_map.insert(channel_id, id);

                    if tx
                        .take()
                        .unwrap()
                        .send(Session::new(session.clone()))
                        .is_err()
                    {
                        // todo: send end session
                    }
                    *channel = ChannelState::Established(session)
                }
            } else {
                // send error response
            }
        } else {
            // todo: rogue begin right now - do nothing. in future might indicate incoming attach
        }
    }
}
