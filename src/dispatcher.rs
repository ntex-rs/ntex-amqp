use std::{cell, fmt, future::Future, marker, pin::Pin, rc::Rc, task::Context, task::Poll};

use ntex::io::DispatchItem;
use ntex::service::Service;
use ntex::time::{sleep, Millis, Sleep};
use ntex::util::{Either, Ready};

use crate::codec::protocol::Frame;
use crate::codec::{AmqpCodec, AmqpFrame};
use crate::error::{AmqpProtocolError, DispatcherError, Error};
use crate::{connection::Connection, types, ControlFrame, ControlFrameKind, ReceiverLink};

/// Amqp server dispatcher service.
pub(crate) struct Dispatcher<Sr: Service, Ctl: Service> {
    sink: Connection,
    service: Sr,
    ctl_service: Ctl,
    ctl_fut: cell::RefCell<Option<(Option<ControlFrame>, Pin<Box<Ctl::Future>>)>>,
    shutdown: cell::RefCell<Option<Pin<Box<Ctl::Future>>>>,
    expire: Sleep,
    idle_timeout: Millis,
}

impl<Sr, Ctl> Dispatcher<Sr, Ctl>
where
    Sr: Service<Request = types::Message, Response = ()>,
    Sr::Error: Into<Error> + 'static,
    Sr::Future: 'static,
    Ctl: Service<Request = ControlFrame, Response = ()>,
    Ctl::Error: Into<Error> + 'static,
{
    pub(crate) fn new(
        sink: Connection,
        service: Sr,
        ctl_service: Ctl,
        idle_timeout: Millis,
    ) -> Self {
        Dispatcher {
            sink,
            service,
            ctl_service,
            idle_timeout,
            ctl_fut: cell::RefCell::new(None),
            shutdown: cell::RefCell::new(None),
            expire: sleep(idle_timeout),
        }
    }

    fn handle_idle_timeout(&self, cx: &mut Context<'_>) {
        if self.idle_timeout.non_zero() && self.expire.poll_elapsed(cx).is_ready() {
            log::trace!(
                "Send keep-alive ping, timeout: {:?} secs",
                self.idle_timeout
            );
            self.sink.post_frame(AmqpFrame::new(0, Frame::Empty));
            self.expire.reset(self.idle_timeout);
            self.handle_idle_timeout(cx);
        }
    }

    fn handle_control_fut(&self, cx: &mut Context<'_>) -> Result<bool, DispatcherError> {
        let mut inner = self.ctl_fut.borrow_mut();

        // process control frame
        if let Some(ref mut item) = &mut *inner {
            match Pin::new(&mut item.1).poll(cx) {
                Poll::Ready(Ok(_)) => {
                    let (frame, _) = inner.take().unwrap();
                    if let Some(frame) = frame {
                        self.handle_control_frame(&frame, None)?;
                    }
                }
                Poll::Pending => return Ok(false),
                Poll::Ready(Err(e)) => {
                    let (frame, _) = inner.take().unwrap();
                    if let Some(frame) = frame {
                        self.handle_control_frame(&frame, Some(e.into()))?;
                    }
                }
            }
        }
        Ok(true)
    }

    fn handle_control_frame(
        &self,
        frame: &ControlFrame,
        err: Option<Error>,
    ) -> Result<(), DispatcherError> {
        if let Some(err) = err {
            match &frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(ref link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::AttachSender(ref frm, _) => {
                    frame
                        .session_cell()
                        .get_mut()
                        .detach_unconfirmed_sender_link(frm, Some(err));
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
                ControlFrameKind::ProtocolError(ref err) => {
                    self.sink.set_error(err.clone());
                    return Err(err.clone().into());
                }
                ControlFrameKind::Closed(_) | ControlFrameKind::Disconnected => {
                    self.sink.set_error(AmqpProtocolError::Disconnected);
                }
                ControlFrameKind::SessionEnded(_) => (),
            }
        } else {
            match frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(ref link) => {
                    let link = link.clone();
                    let fut = self.service.call(types::Message::Attached(link.clone()));
                    ntex::rt::spawn(async move {
                        if let Err(err) = fut.await {
                            let _ = link.close_with_error(err.into()).await;
                        } else {
                            link.confirm_receiver_link();
                            link.set_link_credit(50);
                        }
                    });
                }
                ControlFrameKind::AttachSender(ref frm, ref link) => {
                    frame
                        .session_cell()
                        .get_mut()
                        .attach_remote_sender_link(frm, link.inner.clone());
                }
                ControlFrameKind::Flow(ref frm, ref link) => {
                    frame.session_cell().get_mut().handle_flow(frm, Some(link));
                }
                ControlFrameKind::DetachSender(_, _) => {
                    // frame.session_cell().get_mut().handle_detach(frm);
                }
                ControlFrameKind::DetachReceiver(_, _) => {
                    // frame.session_cell().get_mut().handle_detach(frm);
                }
                ControlFrameKind::ProtocolError(ref err) => {
                    self.sink.set_error(err.clone());
                    return Err(err.clone().into());
                }
                ControlFrameKind::Closed(_) | ControlFrameKind::Disconnected => {
                    self.sink.set_error(AmqpProtocolError::Disconnected);
                }
                ControlFrameKind::SessionEnded(_) => (),
            }
        }
        Ok(())
    }
}

impl<Sr, Ctl> Service for Dispatcher<Sr, Ctl>
where
    Sr: Service<Request = types::Message, Response = ()>,
    Sr::Error: Into<Error> + fmt::Debug + 'static,
    Sr::Future: 'static,
    Ctl: Service<Request = ControlFrame, Response = ()>,
    Ctl::Error: Into<Error> + fmt::Debug + 'static,
    Ctl::Future: 'static,
{
    type Request = DispatchItem<AmqpCodec<AmqpFrame>>;
    type Response = ();
    type Error = DispatcherError;
    type Future = Either<ServiceResult<Sr::Future, Sr::Error>, Ready<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // idle ttimeout
        self.handle_idle_timeout(cx);

        // process control frame
        let res0 = !self.handle_control_fut(cx)?;

        // check readiness
        let res1 = self.service.poll_ready(cx).map_err(|err| {
            error!("Publish service readiness check failed: {:?}", err);
            let _ = self.sink.close_with_error(err.into());
            DispatcherError::Service
        })?;
        let res2 = self.ctl_service.poll_ready(cx).map_err(|err| {
            error!("Control service readiness check failed: {:?}", err);
            let _ = self.sink.close_with_error(err.into());
            DispatcherError::Service
        })?;

        if res0 || res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        let mut shutdown = self.shutdown.borrow_mut();
        if !shutdown.is_some() {
            let sink = self.sink.0.get_mut();
            sink.on_close.notify();
            sink.set_error(AmqpProtocolError::Disconnected);
            *shutdown = Some(Box::pin(
                self.ctl_service
                    .call(ControlFrame::new_kind(ControlFrameKind::Closed(is_error))),
            ));
        }

        let res0 = shutdown.as_mut().expect("guard above").as_mut().poll(cx);
        let res1 = self.service.poll_shutdown(cx, is_error);
        let res2 = self.ctl_service.poll_shutdown(cx, is_error);
        if res0.is_pending() || res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    fn call(&self, request: Self::Request) -> Self::Future {
        match request {
            DispatchItem::Item(frame) => {
                #[cfg(feature = "frame-trace")]
                log::trace!("incoming: {:#?}", frame);

                let action = match self
                    .sink
                    .handle_frame(frame)
                    .map_err(DispatcherError::Protocol)
                {
                    Ok(a) => a,
                    Err(e) => return Either::Right(Ready::Err(e)),
                };

                match action {
                    types::Action::Transfer(link) => {
                        return Either::Left(ServiceResult {
                            link: link.clone(),
                            fut: self.service.call(types::Message::Transfer(link)),
                            _t: marker::PhantomData,
                        });
                    }
                    types::Action::Flow(link, frm) => {
                        // apply flow to specific link
                        let frame = ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::Flow(frm, link.clone()),
                        );
                        *self.ctl_fut.borrow_mut() =
                            Some((Some(frame.clone()), Box::pin(self.ctl_service.call(frame))));
                    }
                    types::Action::AttachSender(link, frame) => {
                        let frame = ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::AttachSender(frame, link),
                        );
                        *self.ctl_fut.borrow_mut() =
                            Some((Some(frame.clone()), Box::pin(self.ctl_service.call(frame))));
                    }
                    types::Action::AttachReceiver(link) => {
                        let frame = ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::AttachReceiver(link),
                        );
                        *self.ctl_fut.borrow_mut() =
                            Some((Some(frame.clone()), Box::pin(self.ctl_service.call(frame))));
                    }
                    types::Action::DetachSender(link, frm) => {
                        let frame = ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::DetachSender(frm, link.clone()),
                        );
                        *self.ctl_fut.borrow_mut() =
                            Some((Some(frame.clone()), Box::pin(self.ctl_service.call(frame))));
                    }
                    types::Action::DetachReceiver(link, frm) => {
                        let frame = ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::DetachReceiver(frm, link.clone()),
                        );
                        *self.ctl_fut.borrow_mut() =
                            Some((Some(frame.clone()), Box::pin(self.ctl_service.call(frame))));
                    }
                    types::Action::SessionEnded(links) => {
                        let frame = ControlFrame::new_kind(ControlFrameKind::SessionEnded(links));
                        *self.ctl_fut.borrow_mut() =
                            Some((None, Box::pin(self.ctl_service.call(frame))));
                    }
                    types::Action::RemoteClose(err) => {
                        let frame = ControlFrame::new_kind(ControlFrameKind::ProtocolError(err));
                        *self.ctl_fut.borrow_mut() =
                            Some((Some(frame.clone()), Box::pin(self.ctl_service.call(frame))));
                    }
                    types::Action::None => (),
                };

                Either::Right(Ready::Ok(()))
            }
            DispatchItem::EncoderError(err) | DispatchItem::DecoderError(err) => {
                let frame = ControlFrame::new_kind(ControlFrameKind::ProtocolError(err.into()));
                *self.ctl_fut.borrow_mut() =
                    Some((Some(frame.clone()), Box::pin(self.ctl_service.call(frame))));
                Either::Right(Ready::Ok(()))
            }
            DispatchItem::KeepAliveTimeout => {
                let frame = ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                    AmqpProtocolError::KeepAliveTimeout,
                ));
                *self.ctl_fut.borrow_mut() =
                    Some((Some(frame.clone()), Box::pin(self.ctl_service.call(frame))));
                Either::Right(Ready::Ok(()))
            }
            DispatchItem::Disconnect(e) => {
                let frame = if e.is_some() {
                    ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                        AmqpProtocolError::Io(Rc::new(err))))
                } else {
                    ControlFrame::new_kind(ControlFrameKind::Disconnected)
                };
                *self.ctl_fut.borrow_mut() =
                    Some((Some(frame.clone()), Box::pin(self.ctl_service.call(frame))));
                Either::Right(Ready::Ok(()))
            }
            DispatchItem::WBackPressureEnabled | DispatchItem::WBackPressureDisabled => {
                Either::Right(Ready::Ok(()))
            }
        }
    }
}

pin_project_lite::pin_project! {
    pub struct ServiceResult<F, E> {
        #[pin]
        fut: F,
        link: ReceiverLink,
        _t: marker::PhantomData<E>,
    }
}

impl<F, E> Future for ServiceResult<F, E>
where
    F: Future<Output = Result<(), E>>,
    E: Into<Error> + fmt::Debug,
{
    type Output = Result<(), DispatcherError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.fut.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => {
                log::trace!("Service error {:?}", e);
                let _ = this.link.close_with_error(e.into());
                Poll::Ready(Ok::<_, DispatcherError>(()))
            }
        }
    }
}
