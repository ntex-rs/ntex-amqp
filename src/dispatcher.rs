use std::{cell, future::Future, marker, pin::Pin, rc::Rc, task::Context, task::Poll};

use ntex::time::{sleep, Millis, Sleep};
use ntex::util::{ready, BoxFuture, Either, Ready};
use ntex::{io::DispatchItem, service::Service, task::LocalWaker};

use crate::codec::{protocol::Frame, AmqpCodec, AmqpFrame};
use crate::error::{AmqpDispatcherError, AmqpProtocolError, Error};
use crate::{connection::Connection, types, ControlFrame, ControlFrameKind, ReceiverLink};

type ControlItem<R, E> = (ControlFrame, BoxFuture<'static, Result<R, E>>);

/// Amqp server dispatcher service.
pub(crate) struct Dispatcher<Sr, Ctl: Service<ControlFrame>> {
    sink: Connection,
    service: Rc<Sr>,
    ctl_service: Rc<Ctl>,
    ctl_fut: cell::RefCell<Vec<ControlItem<Ctl::Response, Ctl::Error>>>,
    ctl_waker: LocalWaker,
    shutdown: cell::RefCell<Option<BoxFuture<'static, ()>>>,
    expire: Sleep,
    idle_timeout: Millis,
}

impl<Sr, Ctl> Dispatcher<Sr, Ctl>
where
    Sr: Service<types::Message, Response = ()> + 'static,
    Ctl: Service<ControlFrame, Response = ()> + 'static,
    Error: From<Sr::Error> + From<Ctl::Error>,
{
    pub(crate) fn new(
        sink: Connection,
        service: Sr,
        ctl_service: Ctl,
        idle_timeout: Millis,
    ) -> Self {
        Dispatcher {
            sink,
            idle_timeout,
            service: Rc::new(service),
            ctl_service: Rc::new(ctl_service),
            ctl_fut: cell::RefCell::new(Vec::new()),
            ctl_waker: LocalWaker::new(),
            shutdown: cell::RefCell::new(None),
            expire: sleep(idle_timeout),
        }
    }

    fn call_control_service(&self, frame: ControlFrame) {
        let ctl = self.ctl_service.clone();
        self.ctl_fut.borrow_mut().push((
            frame.clone(),
            Box::pin(async move { ctl.call(frame).await }),
        ));
        self.ctl_waker.wake();
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

    fn handle_control_fut(&self, cx: &mut Context<'_>) -> Result<bool, AmqpDispatcherError> {
        let mut ready = true;
        let mut inner = self.ctl_fut.borrow_mut();

        // process control frame
        let mut idx = 0;
        while inner.len() > idx {
            let item = &mut inner[idx];
            let res = match Pin::new(&mut item.1).poll(cx) {
                Poll::Pending => {
                    idx += 1;
                    ready = false;
                    continue;
                }
                Poll::Ready(res) => res,
            };
            let (frame, _) = inner.swap_remove(idx);
            match res {
                Ok(_) => {
                    self.handle_control_frame(&frame, None)?;
                }
                Err(e) => {
                    self.handle_control_frame(&frame, Some(e.into()))?;
                }
            }
        }
        Ok(ready)
    }

    fn handle_control_frame(
        &self,
        frame: &ControlFrame,
        err: Option<Error>,
    ) -> Result<(), AmqpDispatcherError> {
        if let Some(err) = err {
            match &frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(_, ref link) => {
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
                ControlFrameKind::Closed | ControlFrameKind::Disconnected(_) => {
                    self.sink.set_error(AmqpProtocolError::Disconnected);
                }
                ControlFrameKind::SessionEnded(_) => (),
            }
        } else {
            match frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(ref frm, ref link) => {
                    let link = link.clone();
                    let frm = frm.clone();
                    let service = self.service.clone();
                    ntex::rt::spawn(async move {
                        let result = service
                            .call(types::Message::Attached(frm.clone(), link.clone()))
                            .await;
                        if let Err(err) = result {
                            let _ = link.close_with_error(Error::from(err)).await;
                        } else {
                            link.confirm_receiver_link(&frm);
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
                ControlFrameKind::Closed | ControlFrameKind::Disconnected(_) => {
                    self.sink.set_error(AmqpProtocolError::Disconnected);
                }
                ControlFrameKind::SessionEnded(_) => (),
            }
        }
        Ok(())
    }
}

impl<Sr, Ctl> Service<DispatchItem<AmqpCodec<AmqpFrame>>> for Dispatcher<Sr, Ctl>
where
    Sr: Service<types::Message, Response = ()> + 'static,
    Ctl: Service<ControlFrame, Response = ()> + 'static,
    Error: From<Sr::Error> + From<Ctl::Error>,
{
    type Response = Option<AmqpFrame>;
    type Error = AmqpDispatcherError;
    type Future<'f> =
        Either<ServiceResult<'f, Sr::Future<'f>, Sr::Error>, Ready<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.ctl_waker.register(cx.waker());

        // idle timeout
        self.handle_idle_timeout(cx);

        // process control frame
        let res0 = !self.handle_control_fut(cx)?;

        // check readiness
        let res1 = self.service.poll_ready(cx).map_err(|err| {
            let err = Error::from(err);
            error!("Publish service readiness check failed: {:?}", err);
            let _ = self.sink.close_with_error(err);
            AmqpDispatcherError::Service
        })?;
        let res2 = self.ctl_service.poll_ready(cx).map_err(|err| {
            let err = Error::from(err);
            error!("Control service readiness check failed: {:?}", err);
            let _ = self.sink.close_with_error(err);
            AmqpDispatcherError::Service
        })?;

        if res0 || res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut shutdown = self.shutdown.borrow_mut();
        if !shutdown.is_some() {
            let sink = self.sink.0.get_mut();
            sink.on_close.notify();
            sink.set_error(AmqpProtocolError::Disconnected);
            let ctl_service = self.ctl_service.clone();
            *shutdown = Some(Box::pin(async move {
                let _ = ctl_service
                    .call(ControlFrame::new_kind(ControlFrameKind::Closed))
                    .await;
            }));
        }

        let res0 = shutdown.as_mut().expect("guard above").as_mut().poll(cx);
        let res1 = self.service.poll_shutdown(cx);
        let res2 = self.ctl_service.poll_shutdown(cx);
        if res0.is_pending() || res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    fn call(&self, request: DispatchItem<AmqpCodec<AmqpFrame>>) -> Self::Future<'_> {
        match request {
            DispatchItem::Item(frame) => {
                #[cfg(feature = "frame-trace")]
                log::trace!("incoming: {:#?}", frame);

                let action = match self
                    .sink
                    .handle_frame(frame)
                    .map_err(AmqpDispatcherError::Protocol)
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
                        log::trace!("FLOW {:?} {:?}", link, frm);
                        // apply flow to specific link
                        self.call_control_service(ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::Flow(frm, link.clone()),
                        ));
                    }
                    types::Action::AttachSender(link, frame) => {
                        self.call_control_service(ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::AttachSender(frame, link),
                        ));
                    }
                    types::Action::AttachReceiver(link, frm) => {
                        self.call_control_service(ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::AttachReceiver(frm, link),
                        ));
                    }
                    types::Action::DetachSender(link, frm) => {
                        self.call_control_service(ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::DetachSender(frm, link.clone()),
                        ));
                    }
                    types::Action::DetachReceiver(link, frm) => {
                        self.call_control_service(ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::DetachReceiver(frm, link.clone()),
                        ));
                    }
                    types::Action::SessionEnded(links) => {
                        self.call_control_service(ControlFrame::new_kind(
                            ControlFrameKind::SessionEnded(links),
                        ));
                    }
                    types::Action::RemoteClose(err) => {
                        self.call_control_service(ControlFrame::new_kind(
                            ControlFrameKind::ProtocolError(err),
                        ));
                    }
                    types::Action::None => (),
                };

                Either::Right(Ready::Ok(None))
            }
            DispatchItem::EncoderError(err) | DispatchItem::DecoderError(err) => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                    err.into(),
                )));
                Either::Right(Ready::Ok(None))
            }
            DispatchItem::KeepAliveTimeout => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                    AmqpProtocolError::KeepAliveTimeout,
                )));
                Either::Right(Ready::Ok(None))
            }
            DispatchItem::Disconnect(e) => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::Disconnected(
                    e,
                )));
                Either::Right(Ready::Ok(None))
            }
            DispatchItem::WBackPressureEnabled | DispatchItem::WBackPressureDisabled => {
                Either::Right(Ready::Ok(None))
            }
        }
    }
}

pin_project_lite::pin_project! {
    pub struct ServiceResult<'f, F, E>
    where F: 'f
    {
        #[pin]
        fut: F,
        link: ReceiverLink,
        _t: marker::PhantomData<&'f E>,
    }
}

impl<'f, F, E> Future for ServiceResult<'f, F, E>
where
    F: Future<Output = Result<(), E>>,
    E: Into<Error>,
{
    type Output = Result<Option<AmqpFrame>, AmqpDispatcherError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if let Err(e) = ready!(this.fut.poll(cx)) {
            let e = e.into();
            log::trace!("Service error {:?}", e);
            let _ = this.link.close_with_error(e);
            Poll::Ready(Ok::<_, AmqpDispatcherError>(None))
        } else {
            Poll::Ready(Ok(None))
        }
    }
}
