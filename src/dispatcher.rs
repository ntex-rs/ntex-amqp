use std::task::{Context, Poll};
use std::{cell, cmp, future::poll_fn, future::Future, marker, pin::Pin};

use ntex::service::{Pipeline, PipelineBinding, PipelineCall, Service, ServiceCtx};
use ntex::time::{sleep, Millis, Sleep};
use ntex::util::{ready, Either};
use ntex::{io::DispatchItem, rt::spawn, task::LocalWaker};

use crate::codec::{protocol::Frame, AmqpCodec, AmqpFrame};
use crate::error::{AmqpDispatcherError, AmqpProtocolError, Error};
use crate::{connection::Connection, types, ControlFrame, ControlFrameKind, ReceiverLink};

/// Amqp server dispatcher service.
pub(crate) struct Dispatcher<Sr: Service<types::Message>, Ctl: Service<ControlFrame>> {
    sink: Connection,
    service: PipelineBinding<Sr, types::Message>,
    ctl_service: PipelineBinding<Ctl, ControlFrame>,
    ctl_fut: cell::RefCell<Vec<(ControlFrame, PipelineCall<Ctl, ControlFrame>)>>,
    ctl_error: cell::Cell<Option<AmqpDispatcherError>>,
    ctl_error_waker: LocalWaker,
    idle_sleep: Sleep,
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
        service: Pipeline<Sr>,
        ctl_service: Pipeline<Ctl>,
        idle_timeout: Millis,
    ) -> Self {
        let idle_timeout = Millis(cmp::min(idle_timeout.0 >> 1, 1000));
        Dispatcher {
            sink,
            idle_timeout,
            service: service.bind(),
            ctl_service: ctl_service.bind(),
            ctl_fut: cell::RefCell::new(Vec::new()),
            ctl_error: cell::Cell::new(None),
            ctl_error_waker: LocalWaker::default(),
            idle_sleep: sleep(idle_timeout),
        }
    }

    fn call_control_service(&self, frame: ControlFrame) {
        let fut = self.ctl_service.call(frame.clone());
        self.ctl_fut.borrow_mut().push((frame, fut));
        self.sink.get_control_queue().waker.wake();
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

    async fn ready(&self, _: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        poll_fn(|cx| {
            if let Some(err) = self.ctl_error.take() {
                log::error!("{}: Control service failed: {:?}", self.sink.tag(), err);
                let _ = self.sink.close();
                return Poll::Ready(Err(err));
            }

            // check readiness
            let service_poll = self.service.poll_ready(cx).map_err(|err| {
                let err = Error::from(err);
                log::error!(
                    "{}: Publish service readiness check failed: {:?}",
                    self.sink.tag(),
                    err
                );
                let _ = self.sink.close_with_error(err);
                AmqpDispatcherError::Service
            })?;

            let ctl_service_poll = self.ctl_service.poll_ready(cx).map_err(|err| {
                let err = Error::from(err);
                log::error!(
                    "{}: Control service readiness check failed: {:?}",
                    self.sink.tag(),
                    err
                );
                let _ = self.sink.close_with_error(err);
                AmqpDispatcherError::Service
            })?;

            if service_poll.is_pending() || ctl_service_poll.is_pending() {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        })
        .await
    }

    fn poll(&self, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        let mut futs = self.ctl_fut.borrow_mut();
        let queue = self.sink.get_control_queue();
        queue.waker.register(cx.waker());

        // enqueue pending control frames
        queue.pending.borrow_mut().drain(..).for_each(|frame| {
            let fut = self.ctl_service.call(frame.clone());
            futs.push((frame, fut));
        });

        // process control frame
        let mut idx = 0;
        while futs.len() > idx {
            let item = &mut futs[idx];
            let res = match Pin::new(&mut item.1).poll(cx) {
                Poll::Pending => {
                    idx += 1;
                    continue;
                }
                Poll::Ready(res) => res,
            };
            let (frame, _) = futs.swap_remove(idx);
            let result = match res {
                Ok(_) => self.handle_control_frame(&frame, None),
                Err(e) => self.handle_control_frame(&frame, Some(e.into())),
            };

            if let Err(err) = result {
                self.ctl_error.set(Some(err));
                self.ctl_error_waker.wake();
                return Ok(());
            }
        }

        // handle idle timeout
        if self.idle_timeout.non_zero() && self.idle_sleep.poll_elapsed(cx).is_ready() {
            log::trace!(
                "{}: Send keep-alive ping, timeout: {:?} secs",
                self.sink.tag(),
                self.idle_timeout
            );
            self.sink.post_frame(AmqpFrame::new(0, Frame::Empty));
            self.idle_sleep.reset(self.idle_timeout);
        }

        Ok(())
    }

    async fn shutdown(&self) {
        self.sink
            .0
            .get_mut()
            .set_error(AmqpProtocolError::Disconnected);
        let _ = self
            .ctl_service
            .call(ControlFrame::new_kind(ControlFrameKind::Closed))
            .await;

        self.service.shutdown().await;
        self.ctl_service.shutdown().await;
    }

    async fn call(
        &self,
        request: DispatchItem<AmqpCodec<AmqpFrame>>,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        match request {
            DispatchItem::Item(frame) => {
                #[cfg(feature = "frame-trace")]
                log::trace!("{}: incoming: {:#?}", self.sink.tag(), frame);

                let action = match self
                    .sink
                    .handle_frame(frame)
                    .map_err(AmqpDispatcherError::Protocol)
                {
                    Ok(a) => a,
                    Err(e) => return Err(e),
                };

                match action {
                    types::Action::Transfer(link) => {
                        if self.sink.is_opened() {
                            let lnk = link.clone();
                            if let Err(e) = self.service.call(types::Message::Transfer(link)).await
                            {
                                let e = Error::from(e);
                                log::trace!("Service error {:?}", e);
                                let _ = lnk.close_with_error(e);
                            }
                        }
                    }
                    types::Action::Flow(link, frm) => {
                        // apply flow to specific link
                        self.call_control_service(ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::Flow(frm, link.clone()),
                        ));
                    }
                    types::Action::AttachSender(link, frame, pfrm) => {
                        self.call_control_service(ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::AttachSender(frame, types::Wrapper::new(pfrm), link),
                        ));
                    }
                    types::Action::AttachReceiver(link, frm, pfrm) => {
                        self.call_control_service(ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::AttachReceiver(frm, types::Wrapper::new(pfrm), link),
                        ));
                    }
                    types::Action::DetachSender(link, frm) => {
                        self.call_control_service(ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::RemoteDetachSender(frm, link.clone()),
                        ));
                    }
                    types::Action::DetachReceiver(link, frm) => {
                        let lnk = link.clone();
                        let fut = self.service.call(types::Message::Detached(lnk));
                        let _ = spawn(async move {
                            let _ = fut.await;
                        });
                        self.call_control_service(ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::RemoteDetachReceiver(frm, link),
                        ));
                    }
                    types::Action::SessionEnded(links) => {
                        let receivers = links
                            .iter()
                            .filter_map(|either| match either {
                                Either::Right(link) => Some(link.clone()),
                                Either::Left(_) => None,
                            })
                            .collect();

                        let fut = self.service.call(types::Message::DetachedAll(receivers));
                        let _ = spawn(async move {
                            let _ = fut.await;
                        });
                        self.call_control_service(ControlFrame::new_kind(
                            ControlFrameKind::RemoteSessionEnded(links),
                        ));
                    }
                    types::Action::RemoteClose(err) => {
                        self.call_control_service(ControlFrame::new_kind(
                            ControlFrameKind::ProtocolError(err),
                        ));
                    }
                    types::Action::None => (),
                };

                Ok(None)
            }
            DispatchItem::EncoderError(err) | DispatchItem::DecoderError(err) => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                    err.into(),
                )));
                Ok(None)
            }
            DispatchItem::KeepAliveTimeout => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                    AmqpProtocolError::KeepAliveTimeout,
                )));
                Ok(None)
            }
            DispatchItem::ReadTimeout => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                    AmqpProtocolError::ReadTimeout,
                )));
                Ok(None)
            }
            DispatchItem::Disconnect(e) => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::Disconnected(
                    e,
                )));
                Ok(None)
            }
            DispatchItem::WBackPressureEnabled | DispatchItem::WBackPressureDisabled => Ok(None),
        }
    }
}

impl<Sr, Ctl> Dispatcher<Sr, Ctl>
where
    Sr: Service<types::Message, Response = ()> + 'static,
    Ctl: Service<ControlFrame, Response = ()> + 'static,
    Error: From<Sr::Error> + From<Ctl::Error>,
{
    fn handle_control_frame(
        &self,
        frame: &ControlFrame,
        err: Option<Error>,
    ) -> Result<(), AmqpDispatcherError> {
        if let Some(err) = err {
            match &frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(_, _, ref link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::AttachSender(ref frm, _, ref link) => {
                    frame
                        .session_cell()
                        .get_mut()
                        .detach_unconfirmed_sender_link(frm, link.inner.clone(), Some(err));
                }
                ControlFrameKind::Flow(_, ref link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::LocalDetachSender(..) => {}
                ControlFrameKind::LocalDetachReceiver(..) => {}
                ControlFrameKind::RemoteDetachSender(_, ref link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::RemoteDetachReceiver(_, ref link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::ProtocolError(ref err) => {
                    self.sink.set_error(err.clone());
                    return Err(err.clone().into());
                }
                ControlFrameKind::Closed | ControlFrameKind::Disconnected(_) => {
                    self.sink.set_error(AmqpProtocolError::Disconnected);
                }
                ControlFrameKind::LocalSessionEnded(_)
                | ControlFrameKind::RemoteSessionEnded(_) => (),
            }
        } else {
            match frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(ref frm, ref mut pfrm, ref link) => {
                    let link = link.clone();
                    let fut = self
                        .service
                        .call(types::Message::Attached(frm.clone(), link.clone()));
                    let response = pfrm.take();

                    let _ = ntex::rt::spawn(async move {
                        let result = fut.await;
                        if let Err(err) = result {
                            let _ = link.close_with_error(Error::from(err)).await;
                        } else {
                            link.confirm_receiver_link(response);
                            link.set_link_credit(50);
                        }
                    });
                }
                ControlFrameKind::AttachSender(ref frm, ref mut pfrm, ref link) => {
                    frame.session_cell().get_mut().attach_remote_sender_link(
                        frm,
                        pfrm.take(),
                        link.inner.clone(),
                    );
                }
                ControlFrameKind::Flow(ref frm, ref link) => {
                    frame.session_cell().get_mut().handle_flow(frm, Some(link));
                }
                ControlFrameKind::ProtocolError(ref err) => {
                    self.sink.set_error(err.clone());
                    return Err(err.clone().into());
                }
                ControlFrameKind::Closed | ControlFrameKind::Disconnected(_) => {
                    self.sink.set_error(AmqpProtocolError::Disconnected);
                }
                ControlFrameKind::LocalDetachSender(..)
                | ControlFrameKind::LocalDetachReceiver(..)
                | ControlFrameKind::LocalSessionEnded(_)
                | ControlFrameKind::RemoteDetachSender(..)
                | ControlFrameKind::RemoteDetachReceiver(..)
                | ControlFrameKind::RemoteSessionEnded(_) => (),
            }
        }
        Ok(())
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

impl<F, E> Future for ServiceResult<'_, F, E>
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
