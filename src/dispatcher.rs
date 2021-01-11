use std::{cell::RefCell, fmt, future::Future, pin::Pin, task::Context, task::Poll, time};

use futures::future::{ready, FutureExt, Ready};
use ntex::rt::time::{delay_for, Delay};
use ntex::service::Service;

use crate::cell::Cell;
use crate::codec::protocol::{Frame, Role};
use crate::codec::{AmqpCodec, AmqpFrame};
use crate::error::{AmqpProtocolError, DispatcherError, Error};
use crate::sndlink::{SenderLink, SenderLinkInner};
use crate::{
    connection::Connection, io::DispatcherItem, types, ControlFrame, ControlFrameKind, State,
};

/// Amqp server dispatcher service.
pub(crate) struct Dispatcher<St, Sr, Ctl: Service> {
    state: State<St>,
    sink: Connection,
    service: Sr,
    ctl_service: Ctl,
    ctl_fut: RefCell<Option<(ControlFrame, Pin<Box<Ctl::Future>>)>>,
    shutdown: std::cell::Cell<bool>,
    expire: RefCell<Delay>,
    idle_timeout: usize,
}

impl<St, Sr, Ctl> Dispatcher<St, Sr, Ctl>
where
    Sr: Service<Request = types::Link<St>, Response = ()>,
    Sr::Error: 'static,
    Sr::Future: 'static,
    Ctl: Service<Request = ControlFrame, Response = ()>,
    Ctl::Error: 'static,
    Error: From<Sr::Error> + From<Ctl::Error>,
{
    pub(crate) fn new(
        state: State<St>,
        sink: Connection,
        service: Sr,
        ctl_service: Ctl,
        idle_timeout: usize,
    ) -> Self {
        Dispatcher {
            sink,
            state,
            service,
            ctl_service,
            idle_timeout,
            ctl_fut: RefCell::new(None),
            shutdown: std::cell::Cell::new(false),
            expire: RefCell::new(delay_for(time::Duration::from_secs(idle_timeout as u64))),
        }
    }

    fn handle_idle_timeout(&self, cx: &mut Context<'_>) {
        let idle_timeout = self.idle_timeout;
        if idle_timeout > 0 {
            let mut expire = self.expire.borrow_mut();
            if Pin::new(&mut *expire).poll(cx).is_ready() {
                self.sink.post_frame(AmqpFrame::new(0, Frame::Empty));
                *expire = delay_for(time::Duration::from_secs(idle_timeout as u64));
                let _ = Pin::new(&mut *expire).poll(cx);
            }
        }
    }

    fn handle_control_fut(&self, cx: &mut Context<'_>) -> Result<bool, DispatcherError> {
        let mut inner = self.ctl_fut.borrow_mut();

        // process control frame
        if let Some(ref mut item) = &mut *inner {
            match Pin::new(&mut item.1).poll(cx) {
                Poll::Ready(Ok(_)) => {
                    let (frame, _) = inner.take().unwrap();
                    self.handle_control_frame(frame, None)?
                }
                Poll::Pending => return Ok(false),
                Poll::Ready(Err(e)) => {
                    let (frame, _) = inner.take().unwrap();
                    self.handle_control_frame(frame, Some(e.into()))?
                }
            }
        }
        Ok(true)
    }

    fn handle_control_frame(
        &self,
        frame: ControlFrame,
        err: Option<Error>,
    ) -> Result<(), DispatcherError> {
        if let Some(err) = err {
            match &frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(ref link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::AttachSender(ref frm, _) => {
                    frame
                        .session()
                        .get_mut()
                        .detach_unconfirmed_sender_link(&frm, Some(err));
                }
                ControlFrameKind::Flow(_, ref link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::DetachSender(_, ref link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::DetachReceiver(_, ref link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::ProtocolError(ref err) => return Err(err.clone().into()),
                _ => (),
            }
        } else {
            match frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(ref link) => {
                    let link = link.clone();
                    ntex::rt::spawn(
                        self.service
                            .call(types::Link::new(link.clone(), self.state.clone()))
                            .then(|res| async move {
                                match res {
                                    Ok(_) => link.close().await,
                                    Err(err) => link.close_with_error(Error::from(err)).await,
                                }
                            }),
                    );
                }
                ControlFrameKind::AttachSender(ref frm, ref link) => {
                    frame
                        .session()
                        .get_mut()
                        .confirm_sender_link_inner(&frm, link.inner.clone());
                }
                ControlFrameKind::Flow(ref frm, _) => {
                    frame.session().get_mut().apply_flow(frm);
                }
                ControlFrameKind::DetachSender(ref mut frm, _) => {
                    frame.session().get_mut().handle_detach(frm);
                }
                ControlFrameKind::DetachReceiver(ref mut frm, _) => {
                    frame.session().get_mut().handle_detach(frm);
                }
                ControlFrameKind::ProtocolError(ref err) => return Err(err.clone().into()),
                _ => (),
            }
        }
        Ok(())
    }
}

impl<St, Sr, Ctl> Service for Dispatcher<St, Sr, Ctl>
where
    Sr: Service<Request = types::Link<St>, Response = ()>,
    Sr::Error: fmt::Debug + 'static,
    Sr::Future: 'static,
    Ctl: Service<Request = ControlFrame, Response = ()>,
    Ctl::Error: fmt::Debug + 'static,
    Ctl::Future: 'static,
    Error: From<Sr::Error> + From<Ctl::Error>,
{
    type Request = DispatcherItem<AmqpCodec<AmqpFrame>>;
    type Response = ();
    type Error = DispatcherError;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // process control frame
        let res0 = !self.handle_control_fut(cx)?;

        // check readiness
        let res1 = self.service.poll_ready(cx).map_err(|err| {
            error!("Error during publish service readiness check: {:?}", err);
            let _ = self.sink.close_with_error(err);
            DispatcherError::Service
        })?;
        let res2 = self.ctl_service.poll_ready(cx).map_err(|err| {
            error!("Error during control service readiness check: {:?}", err);
            let _ = self.sink.close_with_error(err);
            DispatcherError::Service
        })?;

        if res0 || res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        if !self.shutdown.get() {
            self.shutdown.set(true);
            ntex::rt::spawn(
                self.ctl_service
                    .call(ControlFrame::new_kind(ControlFrameKind::Closed(is_error)))
                    .map(|_| ()),
            );
        }

        let res1 = self.service.poll_shutdown(cx, is_error);
        let res2 = self.ctl_service.poll_shutdown(cx, is_error);
        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    fn call(&self, request: Self::Request) -> Self::Future {
        match request {
            DispatcherItem::Item(frame) => {
                // trace!("incoming: {:#?}", frame);
                let item = try_ready_err!(self
                    .sink
                    .0
                    .get_mut()
                    .handle_frame(frame)
                    .map_err(DispatcherError::Protocol));
                let frame = if let Some(item) = item {
                    item
                } else {
                    return ready(Ok(()));
                };

                let (channel_id, frame) = frame.into_parts();

                // remote session
                if let Frame::Begin(frm) = frame {
                    return ready(
                        self.sink
                            .register_remote_session(channel_id, &frm)
                            .map_err(DispatcherError::Codec),
                    );
                }

                let id = channel_id as usize;
                let session = match self.sink.get_remote_session(id) {
                    Some(session) => session,
                    None => {
                        return ready(Err(
                            AmqpProtocolError::UnknownSession(id, Box::new(frame)).into()
                        ))
                    }
                };

                let result = match frame {
                    Frame::Flow(frm) => {
                        // apply flow to specific link
                        if let Some(link) = session.get_sender_link_by_handle(frm.handle.unwrap()) {
                            let frame = ControlFrame::new(
                                session.clone(),
                                ControlFrameKind::Flow(frm, link.clone()),
                            );
                            *self.ctl_fut.borrow_mut() =
                                Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                            return ready(Ok(()));
                        }
                        session.get_mut().apply_flow(&frm);
                        Ok(())
                    }
                    Frame::Attach(attach) => {
                        match attach.role {
                            Role::Receiver => {
                                // remotly opened sender link
                                let link = SenderLink::new(Cell::new(SenderLinkInner::with(
                                    &attach,
                                    session.clone(),
                                )));
                                let frame = ControlFrame::new(
                                    session,
                                    ControlFrameKind::AttachSender(Box::new(attach), link),
                                );

                                *self.ctl_fut.borrow_mut() =
                                    Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                                Ok(())
                            }
                            Role::Sender => {
                                // receiver link
                                let link = session
                                    .get_mut()
                                    .open_receiver_link(session.clone(), attach);

                                let frame = ControlFrame::new(
                                    session,
                                    ControlFrameKind::AttachReceiver(link),
                                );
                                *self.ctl_fut.borrow_mut() =
                                    Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                                Ok(())
                            }
                        }
                    }
                    Frame::Detach(frm) => {
                        if let Some(link) = session.get_sender_link_by_handle(frm.handle) {
                            let frame = ControlFrame::new(
                                session.clone(),
                                ControlFrameKind::DetachSender(frm, link.clone()),
                            );
                            *self.ctl_fut.borrow_mut() =
                                Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                        } else if let Some(link) = session.get_receiver_link_by_handle(frm.handle) {
                            let frame = ControlFrame::new(
                                session.clone(),
                                ControlFrameKind::DetachReceiver(frm, link.clone()),
                            );
                            *self.ctl_fut.borrow_mut() =
                                Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                        } else {
                            session.get_mut().handle_frame(Frame::Detach(frm));
                        }
                        Ok(())
                    }
                    _ => Err(AmqpProtocolError::Unexpected(Box::new(frame)).into()),
                };

                ready(result)
            }
            DispatcherItem::EncoderError(err) | DispatcherItem::DecoderError(err) => {
                let frame = ControlFrame::new_kind(ControlFrameKind::ProtocolError(err.into()));
                *self.ctl_fut.borrow_mut() =
                    Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                ready(Ok(()))
            }
            DispatcherItem::KeepAliveTimeout => ready(Ok(())),
            DispatcherItem::IoError(_err) => ready(Ok(())),
        }
    }
}
