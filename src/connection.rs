use std::collections::VecDeque;
use std::time::Duration;

use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_utils::time::LowResTimeService;
use futures::future::{err, Either};
use futures::task::AtomicTask;
use futures::unsync::oneshot;
use futures::{future, Async, Future, Poll, Sink, Stream};

use amqp::codec::Encode;
use amqp::protocol::{Begin, Error, Frame};
use amqp::{AmqpCodec, AmqpCodecError, AmqpFrame};

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
    Opening(oneshot::Sender<Session>, WeakCell<ConnectionInner>),
    Established(Cell<SessionInner>),
    Closing(Cell<SessionInner>),
    None,
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
    channels: slab::Slab<ChannelState>,
    error: Option<AmqpTransportError>,
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

    /// Close connection with error
    pub fn close_with_error(
        &mut self,
        err: Error,
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

            let entry = inner.channels.vacant_entry();
            let token = entry.key();

            if token >= inner.local.channel_max {
                Either::A(err(AmqpTransportError::TooManyChannels))
            } else {
                entry.insert(ChannelState::Opening(tx, cell));

                let begin = Begin {
                    // todo: let user specify settings
                    remote_channel: None,
                    next_outgoing_id: 1,
                    incoming_window: 0,
                    outgoing_window: ::std::u32::MAX,
                    handle_max: ::std::u32::MAX,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                    body: None,
                };
                inner.post_frame(AmqpFrame::new(token as u16, begin.into()));
                Either::B(rx.map_err(|_e| AmqpTransportError::Disconnected))
            }
        }
    }

    /// Get session by id. This method panics if session does not exists or in opening/closing state.
    pub(crate) fn get_session(&self, id: usize) -> Cell<SessionInner> {
        if let Some(channel) = self.inner.get_ref().channels.get(id) {
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
        let entry = inner.channels.vacant_entry();
        let token = entry.key();

        let session = Cell::new(SessionInner::new(
            token,
            ConnectionController(cell),
            token as u16,
            begin.incoming_window(),
            begin.next_outgoing_id(),
            begin.outgoing_window(),
        ));
        entry.insert(ChannelState::Established(session));

        let begin = Begin {
            remote_channel: Some(channel_id),
            next_outgoing_id: 1,
            incoming_window: 0,
            outgoing_window: ::std::u32::MAX,
            handle_max: ::std::u32::MAX,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
            body: None,
        };
        inner.post_frame(AmqpFrame::new(token as u16, begin.into()));
    }

    pub(crate) fn send_frame(&mut self, frame: AmqpFrame) {
        self.inner.get_mut().post_frame(frame)
    }

    pub(crate) fn poll_outgoing(&mut self) -> Poll<(), AmqpCodecError> {
        let inner = self.inner.get_mut();
        let mut update = false;
        loop {
            while !self.framed.is_write_buf_full() {
                if let Some(frame) = inner.pop_next_frame() {
                    trace!(
                        "outgoing: {:?} - {:?}",
                        frame.performative().name(),
                        frame.encoded_size()
                    );
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
                    Ok(Async::Ready(_)) => {
                        inner.write_task.register();
                    }
                }
            } else {
                break;
            }
        }
        self.hb.update_remote(update);

        Ok(Async::NotReady)
    }

    pub(crate) fn poll_incoming(&mut self) -> Poll<Option<AmqpFrame>, AmqpCodecError> {
        let inner = self.inner.get_mut();

        let mut update = false;
        loop {
            match self.framed.poll() {
                Ok(Async::Ready(Some(frame))) => {
                    trace!(
                        "incoming: {:?} - {:?}",
                        frame.performative().name(),
                        frame.encoded_size()
                    );
                    update = true;
                    if inner.error.is_some() {
                        error!("connection closed but new framed is received: {:?}", frame);
                        return Ok(Async::Ready(None));
                    }

                    match *frame.performative() {
                        Frame::Begin(ref begin) if begin.remote_channel().is_some() => {
                            inner.complete_session_creation(frame.channel_id() as usize, begin);
                            continue;
                        }
                        // todo: handle Close, End?
                        Frame::End(_) | Frame::Close(_) => {
                            println!("todo: unexpected frame: {:#?}", frame);
                        }
                        _ => (), // todo: handle unexpected frames
                    }

                    // handle session frames
                    if let Some(channel) = inner.channels.get_mut(frame.channel_id() as usize) {
                        match channel {
                            ChannelState::Established(ref mut session) => {
                                match frame.performative() {
                                    Frame::Attach(attach) => {
                                        let cell = session.clone();
                                        if !session.get_mut().handle_attach(attach, cell) {
                                            return Ok(Async::Ready(Some(frame)));
                                        }
                                    }
                                    Frame::Detach(detach) => {
                                        session.get_mut().handle_detach(detach);
                                    }
                                    _ => session.get_mut().handle_frame(frame),
                                }
                            }
                            _ => (),
                        }
                    } else {
                        return Ok(Async::Ready(Some(frame)));
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

        Ok(Async::NotReady)
    }
}

pub struct ConnectionController(Cell<ConnectionInner>);

impl ConnectionController {
    pub fn close_session(&mut self) {
        unimplemented!()
    }

    pub(crate) fn post_frame(&mut self, frame: AmqpFrame) {
        self.0.get_mut().post_frame(frame)
    }

    pub(crate) fn drop_session_copy(&mut self, id: usize) {}
}

impl ConnectionInner {
    pub fn new(local: Configuration, remote: Configuration) -> ConnectionInner {
        ConnectionInner {
            local,
            remote,
            write_queue: VecDeque::new(),
            write_task: AtomicTask::new(),
            channels: slab::Slab::new(),
            error: None,
        }
    }

    fn set_error(&mut self, err: AmqpTransportError) {
        for (_, channel) in self.channels.iter_mut() {
            match channel {
                ChannelState::Opening(_, _) | ChannelState::None => (),
                ChannelState::Established(ref mut ses) | ChannelState::Closing(ref mut ses) => {
                    ses.get_mut().set_error(err.clone());
                }
            }
        }
        self.channels.clear();

        self.error = Some(err);
    }

    fn pop_next_frame(&mut self) -> Option<AmqpFrame> {
        self.write_queue.pop_front()
    }

    fn post_frame(&mut self, frame: AmqpFrame) {
        self.write_queue.push_back(frame);
        self.write_task.notify();
    }

    fn complete_session_creation(&mut self, channel_id: usize, begin: &Begin) {
        trace!(
            "session opened: {:?} {:?}",
            channel_id,
            begin.remote_channel()
        );

        let id = begin.remote_channel().unwrap() as usize;

        if let Some(channel) = self.channels.get_mut(id) {
            if channel.is_opening() {
                let item = std::mem::replace(channel, ChannelState::None);

                if let ChannelState::Opening(tx, cell) = item {
                    let cell = cell.upgrade().unwrap();
                    let session = Cell::new(SessionInner::new(
                        id,
                        ConnectionController(cell),
                        begin.remote_channel().unwrap(),
                        begin.incoming_window(),
                        begin.next_outgoing_id(),
                        begin.outgoing_window(),
                    ));

                    if tx.send(Session::new(session.clone())).is_err() {
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
