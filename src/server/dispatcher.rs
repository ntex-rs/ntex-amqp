use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt, io};

use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::service::Service;
use ntex_amqp_codec::protocol::{Error, Frame, Role};
use ntex_amqp_codec::AmqpCodecError;
use slab::Slab;

use crate::cell::Cell;
use crate::connection::{ChannelState, Connection};
use crate::rcvlink::ReceiverLink;
use crate::session::Session;
use crate::sndlink::{SenderLink, SenderLinkInner};

use super::control::{ControlFrame, ControlFrameKind, ControlFrameService};
use super::{Link, LinkError, State};

/// Amqp server connection dispatcher.
#[pin_project::pin_project]
pub(crate) struct Dispatcher<Io, St, Sr>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    Sr: Service<Request = Link<St>, Response = ()>,
{
    conn: Connection<Io>,
    state: State<St>,
    service: Sr,
    control_srv: Option<ControlFrameService<St>>,
    control_frame: Option<ControlFrame<St>>,
    #[pin]
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
    Io: AsyncRead + AsyncWrite + Unpin,
    Sr: Service<Request = Link<St>, Response = ()>,
    Sr::Error: fmt::Display + Into<Error>,
{
    pub(crate) fn new(
        conn: Connection<Io>,
        state: State<St>,
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

    fn handle_control_fut(&mut self, cx: &mut Context<'_>) -> bool {
        // process control frame
        if let Some(ref mut fut) = self.control_fut {
            match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(_)) => {
                    self.control_fut.take();
                    let mut frame = self.control_frame.take().unwrap();
                    self.handle_control_frame(&mut frame, None);
                }
                Poll::Pending => return false,
                Poll::Ready(Err(e)) => {
                    log::trace!("Control service failed: {:?}", e);
                    let _ = self.control_fut.take();
                    let mut frame = self.control_frame.take().unwrap();
                    self.handle_control_frame(&mut frame, Some(e));
                }
            }
        }
        true
    }

    fn handle_control_frame(&mut self, frame: &mut ControlFrame<St>, err: Option<LinkError>) {
        if let Some(e) = err {
            error!("Error in link handler: {}", e);
            if let ControlFrameKind::AttachSender(ref frm, _) = frame.0.kind {
                frame
                    .0
                    .session
                    .inner
                    .get_mut()
                    .detach_unconfirmed_sender_link(&frm, Some(e.into()));
            }
        } else {
            match frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(ref link) => {
                    let fut = self
                        .service
                        .call(Link::new(link.clone(), self.state.clone()));
                    self.receivers.push((link.clone(), fut));
                }

                ControlFrameKind::AttachSender(ref frm, ref link) => {
                    frame
                        .0
                        .session
                        .inner
                        .get_mut()
                        .confirm_sender_link_inner(&frm, link.inner.clone());
                }
                ControlFrameKind::Flow(ref frm, ref link) => {
                    if let Some(err) = err {
                        let _ = link.close_with_error(err.into());
                    } else {
                        frame.0.session.inner.get_mut().apply_flow(frm);
                    }
                }
                ControlFrameKind::DetachSender(ref mut frm, ref link) => {
                    if let Some(err) = err {
                        let _ = link.close_with_error(err.into());
                    } else {
                        frame.0.session.inner.get_mut().handle_detach(frm);
                    }
                }
                ControlFrameKind::DetachReceiver(ref mut frm, ref link) => {
                    if let Some(err) = err {
                        let _ = link.close_with_error(err.into());
                    } else {
                        frame.0.session.inner.get_mut().handle_detach(frm);
                    }
                }
            }
        }
    }

    fn poll_incoming(&mut self, cx: &mut Context<'_>) -> Result<IncomingResult, AmqpCodecError> {
        loop {
            // handle remote begin and attach
            match self.conn.poll_incoming(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    let (channel_id, frame) = frame.into_parts();
                    let channel_id = channel_id as usize;

                    if let Frame::Begin(frm) = frame {
                        self.conn.register_remote_session(channel_id as u16, &frm);
                        continue;
                    }

                    let session = self
                        .conn
                        .get_remote_session(channel_id)
                        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Unknown session"))?;

                    match frame {
                        Frame::Flow(frm) => {
                            // apply flow to specific link
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
                        Frame::Attach(attach) => {
                            match attach.role {
                                Role::Receiver => {
                                    // remotly opened sender link
                                    let cell = session.clone();
                                    if self.control_srv.is_some() {
                                        let link = SenderLink::new(Cell::new(
                                            SenderLinkInner::with(&attach, cell.clone()),
                                        ));

                                        self.control_frame = Some(ControlFrame::new(
                                            self.state.clone(),
                                            Session::new(cell),
                                            ControlFrameKind::AttachSender(attach, link),
                                        ));
                                        return Ok(IncomingResult::Control);
                                    } else {
                                        session.get_mut().confirm_sender_link(&attach, cell);
                                    }
                                }
                                Role::Sender => {
                                    // receiver link
                                    let cell = session.clone();
                                    let link =
                                        session.get_mut().open_receiver_link(cell.clone(), attach);

                                    if self.control_srv.is_some() {
                                        self.control_frame = Some(ControlFrame::new(
                                            self.state.clone(),
                                            Session::new(cell),
                                            ControlFrameKind::AttachReceiver(link),
                                        ));
                                        return Ok(IncomingResult::Control);
                                    } else {
                                        let fut = self
                                            .service
                                            .call(Link::new(link.clone(), self.state.clone()));
                                        self.receivers.push((link, fut));
                                    }
                                }
                            }
                        }
                        Frame::Detach(frm) => {
                            let cell = session.clone();

                            if self.control_srv.is_some() {
                                if let Some(link) = session.get_sender_link_by_handle(frm.handle) {
                                    self.control_frame = Some(ControlFrame::new(
                                        self.state.clone(),
                                        Session::new(cell),
                                        ControlFrameKind::DetachSender(frm, link.clone()),
                                    ));

                                    return Ok(IncomingResult::Control);
                                } else if let Some(link) =
                                    session.get_receiver_link_by_handle(frm.handle)
                                {
                                    self.control_frame = Some(ControlFrame::new(
                                        self.state.clone(),
                                        Session::new(cell),
                                        ControlFrameKind::DetachReceiver(frm, link.clone()),
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
                Poll::Pending => break,
                Poll::Ready(None) => return Ok(IncomingResult::Disconnect),
                Poll::Ready(Some(Err(e))) => {
                    log::trace!("Polling for incoming frame failed: {:?}", e);
                    return Err(e);
                }
            }
        }

        Ok(IncomingResult::Done)
    }
}

impl<Io, St, Sr> Future for Dispatcher<Io, St, Sr>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    Sr: Service<Request = Link<St>, Response = ()>,
    Sr::Error: fmt::Display + Into<Error>,
{
    type Output = Result<(), AmqpCodecError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // process control frame
        if !self.handle_control_fut(cx) {
            return Poll::Pending;
        }

        // check control frames service
        if self.control_frame.is_some() {
            let srv = self.control_srv.as_mut().unwrap();
            match srv.poll_ready(cx) {
                Poll::Ready(Ok(_)) => {
                    let frame = self.control_frame.as_ref().unwrap().clone();
                    let srv = self.control_srv.as_mut().unwrap();
                    self.control_fut = Some(srv.call(frame));
                    if !self.handle_control_fut(cx) {
                        return Poll::Pending;
                    }
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    let mut frame = self.control_frame.take().unwrap();
                    self.handle_control_frame(&mut frame, Some(e));
                }
            }
        }

        match self.poll_incoming(cx)? {
            IncomingResult::Control => return self.poll(cx),
            IncomingResult::Disconnect => return Poll::Ready(Ok(())),
            IncomingResult::Done => (),
        }

        // process service responses
        let mut idx = 0;
        while idx < self.receivers.len() {
            match unsafe { Pin::new_unchecked(&mut self.receivers[idx].1) }.poll(cx) {
                Poll::Ready(Ok(_detach)) => {
                    let (link, _) = self.receivers.swap_remove(idx);
                    let _ = link.close();
                }
                Poll::Pending => idx += 1,
                Poll::Ready(Err(e)) => {
                    let (link, _) = self.receivers.swap_remove(idx);
                    error!("Error in link handler: {}", e);
                    let _ = link.close_with_error(e.into());
                }
            }
        }

        let res = self.conn.poll_outgoing(cx);
        self.conn.register_write_task(cx);
        res
    }
}
