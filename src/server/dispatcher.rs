use std::fmt;

use actix_codec::{AsyncRead, AsyncWrite};
use actix_service::Service;
use amqp_codec::protocol::{Error, Frame, Role};
use amqp_codec::AmqpCodecError;
use futures::{Async, Future, Poll};
use slab::Slab;

use crate::cell::Cell;
use crate::connection::{ChannelState, Connection};
use crate::rcvlink::ReceiverLink;
use crate::session::Session;

use super::control::{ControlFrame, ControlFrameKind, ControlFrameService};
use super::errors::LinkError;
use super::Link;

/// Amqp server connection dispatcher.
pub struct Dispatcher<Io, St, Sr>
where
    Io: AsyncRead + AsyncWrite,
    Sr: Service<Request = Link<St>, Response = ()>,
{
    conn: Connection<Io>,
    state: Cell<St>,
    service: Sr,
    control_srv: Option<ControlFrameService<St>>,
    control_frame: Option<ControlFrame<St>>,
    control_fut: Option<<ControlFrameService<St> as Service>::Future>,
    receivers: Vec<(ReceiverLink, Sr::Future)>,
    _channels: slab::Slab<ChannelState>,
}

enum IncomingResult {
    Control,
    Done,
    Disconnect,
}

impl<Io, St, Sr> Dispatcher<Io, St, Sr>
where
    Io: AsyncRead + AsyncWrite,
    Sr: Service<Request = Link<St>, Response = ()>,
    Sr::Error: fmt::Display + Into<Error>,
{
    pub(crate) fn new(
        conn: Connection<Io>,
        state: Cell<St>,
        service: Sr,
        control_srv: Option<ControlFrameService<St>>,
    ) -> Self {
        Dispatcher {
            conn,
            service,
            state,
            control_srv,
            control_frame: None,
            control_fut: None,
            receivers: Vec::with_capacity(16),
            _channels: Slab::with_capacity(16),
        }
    }

    fn handle_control_fut(&mut self) -> bool {
        // process control frame
        if let Some(ref mut fut) = self.control_fut {
            match fut.poll() {
                Ok(Async::Ready(_)) => {
                    self.control_fut.take();
                    let frame = self.control_frame.take().unwrap();
                    self.handle_control_frame(&frame, None);
                }
                Ok(Async::NotReady) => return false,
                Err(e) => {
                    let _ = self.control_fut.take();
                    let frame = self.control_frame.take().unwrap();
                    self.handle_control_frame(&frame, Some(e));
                }
            }
        }
        true
    }

    fn handle_control_frame(&self, frame: &ControlFrame<St>, err: Option<LinkError>) {
        if let Some(e) = err {
            error!("Error in link handler: {}", e);
        } else {
            match frame.0.kind {
                ControlFrameKind::Attach(ref frm) => {
                    let cell = frame.0.session.inner.clone();
                    frame
                        .0
                        .session
                        .inner
                        .get_mut()
                        .confirm_sender_link(cell, &frm);
                }
                ControlFrameKind::Flow(ref frm, ref link) => {
                    if let Some(err) = err {
                        link.close_with_error(err.into());
                    } else {
                        frame.0.session.inner.get_mut().apply_flow(frm);
                    }
                }
                ControlFrameKind::Detach(ref frm, ref link) => {
                    if let Some(err) = err {
                        link.close_with_error(err.into());
                    } else {
                        frame
                            .0
                            .session
                            .inner
                            .get_mut()
                            .handle_frame(Frame::Detach(frm.clone()));
                    }
                }
            }
        }
    }

    fn poll_incoming(&mut self) -> Result<IncomingResult, AmqpCodecError> {
        loop {
            // handle remote begin and attach
            match self.conn.poll_incoming() {
                Ok(Async::Ready(Some(frame))) => {
                    let (channel_id, frame) = frame.into_parts();
                    let channel_id = channel_id as usize;

                    match frame {
                        Frame::Begin(frm) => {
                            self.conn.register_remote_session(channel_id as u16, &frm);
                        }
                        Frame::Flow(frm) => {
                            // apply flow to specific link
                            let session = self.conn.get_session(channel_id);
                            if self.control_srv.is_some() {
                                if let Some(link) =
                                    session.get_sender_link_by_handle(frm.handle.unwrap())
                                {
                                    self.control_frame = Some(ControlFrame::new(
                                        self.state.clone(),
                                        Session::new(session.clone()),
                                        ControlFrameKind::Flow(frm, link.clone()),
                                    ));
                                    return Ok(IncomingResult::Control);
                                }
                            }
                            session.get_mut().apply_flow(&frm);
                        }
                        Frame::Attach(attach) => match attach.role {
                            Role::Receiver => {
                                // remotly opened sender link
                                let session = self.conn.get_session(channel_id);
                                let cell = session.clone();
                                if self.control_srv.is_some() {
                                    self.control_frame = Some(ControlFrame::new(
                                        self.state.clone(),
                                        Session::new(cell.clone()),
                                        ControlFrameKind::Attach(attach),
                                    ));
                                    return Ok(IncomingResult::Control);
                                } else {
                                    session.get_mut().confirm_sender_link(cell, &attach);
                                }
                            }
                            Role::Sender => {
                                // receiver link
                                let session = self.conn.get_session(channel_id);
                                let cell = session.clone();
                                let link = session.get_mut().open_receiver_link(cell, attach);
                                let fut = self
                                    .service
                                    .call(Link::new(link.clone(), self.state.clone()));
                                self.receivers.push((link, fut));
                            }
                        },
                        Frame::Detach(frm) => {
                            let session = self.conn.get_session(channel_id);
                            let cell = session.clone();

                            if self.control_srv.is_some() {
                                if let Some(link) = session.get_sender_link_by_handle(frm.handle) {
                                    self.control_frame = Some(ControlFrame::new(
                                        self.state.clone(),
                                        Session::new(cell.clone()),
                                        ControlFrameKind::Detach(frm, link.clone()),
                                    ));

                                    return Ok(IncomingResult::Control);
                                }
                            }
                            session.get_mut().handle_frame(Frame::Detach(frm));
                        }
                        _ => {
                            trace!("Unexpected frame {:#?}", frame);
                        }
                    }
                }
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => return Ok(IncomingResult::Disconnect),
                Err(e) => return Err(e),
            }
        }

        Ok(IncomingResult::Done)
    }
}

impl<Io, St, Sr> Future for Dispatcher<Io, St, Sr>
where
    Io: AsyncRead + AsyncWrite,
    Sr: Service<Request = Link<St>, Response = ()>,
    Sr::Error: fmt::Display + Into<Error>,
{
    type Item = ();
    type Error = AmqpCodecError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // process control frame
        if !self.handle_control_fut() {
            return Ok(Async::NotReady);
        }

        // check control frames service
        if self.control_frame.is_some() {
            let srv = self.control_srv.as_mut().unwrap();
            match srv.poll_ready() {
                Ok(Async::Ready(_)) => {
                    self.control_fut = Some(srv.call(self.control_frame.as_ref().unwrap().clone()));
                    if !self.handle_control_fut() {
                        return Ok(Async::NotReady);
                    }
                }
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => {
                    let frame = self.control_frame.take().unwrap();
                    self.handle_control_frame(&frame, Some(e));
                }
            }
        }

        match self.poll_incoming()? {
            IncomingResult::Control => return self.poll(),
            IncomingResult::Disconnect => return Ok(Async::Ready(())),
            IncomingResult::Done => (),
        }

        // process service responses
        let mut idx = 0;
        while idx < self.receivers.len() {
            match self.receivers[idx].1.poll() {
                Ok(Async::Ready(_detach)) => {
                    let (mut link, _) = self.receivers.swap_remove(idx);
                    let _ = link.close();
                }
                Ok(Async::NotReady) => idx += 1,
                Err(e) => {
                    let (mut link, _) = self.receivers.swap_remove(idx);
                    error!("Error in link handler: {}", e);
                    let _ = link.close_with_error(e.into());
                }
            }
        }

        let res = self.conn.poll_outgoing();
        self.conn.register_write_task();
        res
    }
}
