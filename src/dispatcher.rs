use std::{cell::RefCell, convert::TryFrom, future::Future, pin::Pin, task::Context, task::Poll};

use futures::future::{ready, FutureExt, Ready};
use ntex::service::Service;
use ntex_amqp_codec::protocol::{Frame, Role};
use ntex_amqp_codec::{AmqpCodec, AmqpFrame};

use crate::cell::Cell;
use crate::error::{AmqpProtocolError, DispatcherError, LinkError};
use crate::sndlink::{SenderLink, SenderLinkInner};
use crate::{
    connection::Connection, io::DispatcherItem, types, ControlFrame, ControlFrameKind, State,
};

/// Amqp server dispatcher service.
pub(crate) struct Dispatcher<St, Sr, Ctl: Service, E, E2> {
    state: State<St>,
    sink: Connection,
    service: Sr,
    ctl_service: Ctl,
    ctl_fut: RefCell<Option<(ControlFrame<E>, Pin<Box<Ctl::Future>>)>>,
    shutdown: std::cell::Cell<bool>,
    _t: std::marker::PhantomData<E2>,
}

impl<St, Sr, Ctl, E, E2> Dispatcher<St, Sr, Ctl, E, E2>
where
    E: From<E2>,
    E2: 'static,
    Sr: Service<Request = types::Link<St>, Response = (), Error = E2>,
    Sr::Future: 'static,
    Ctl: Service<Request = ControlFrame<E>, Response = (), Error = E2>,
    LinkError: TryFrom<E2, Error = E>,
{
    pub(crate) fn new(state: State<St>, sink: Connection, service: Sr, ctl_service: Ctl) -> Self {
        Dispatcher {
            sink,
            state,
            service,
            ctl_service,
            ctl_fut: RefCell::new(None),
            shutdown: std::cell::Cell::new(false),
            _t: std::marker::PhantomData,
        }
    }

    fn handle_control_fut(&self, cx: &mut Context<'_>) -> Result<bool, DispatcherError<E>> {
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
                    self.handle_control_frame(frame, Some(e))?
                }
            }
        }
        Ok(true)
    }

    fn handle_control_frame(
        &self,
        frame: ControlFrame<E>,
        err: Option<E2>,
    ) -> Result<(), DispatcherError<E>> {
        if let Some(err) = err {
            let err = LinkError::try_from(err)
                .map_err(DispatcherError::Service)?
                .into();

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
                                    Err(err) => match LinkError::try_from(err) {
                                        Ok(err) => link.close_with_error(err.into()).await,
                                        Err(_err) => link.close().await,
                                    },
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

impl<St, Sr, Ctl, E, E2> Service for Dispatcher<St, Sr, Ctl, E, E2>
where
    E: From<E2>,
    E2: 'static,
    Sr: Service<Request = types::Link<St>, Response = (), Error = E2>,
    Sr::Future: 'static,
    Ctl: Service<Request = ControlFrame<E>, Response = (), Error = E2>,
    Ctl::Future: 'static,
    LinkError: TryFrom<E2, Error = E>,
{
    type Request = DispatcherItem<AmqpCodec<AmqpFrame>>;
    type Response = ();
    type Error = DispatcherError<E>;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // process control frame
        let res0 = !self.handle_control_fut(cx)?;

        // check readiness
        let res1 = self
            .service
            .poll_ready(cx)
            .map_err(|err| DispatcherError::Service(err.into()))?;
        let res2 = self
            .ctl_service
            .poll_ready(cx)
            .map_err(|err| DispatcherError::Service(err.into()))?;

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
