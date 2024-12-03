use std::task::{Context, Poll};
use std::{cell, cmp, future::poll_fn, future::Future, marker, pin::Pin, rc::Rc};

use ntex::service::{Pipeline, PipelineBinding, PipelineCall, Service, ServiceCtx};
use ntex::time::{sleep, Millis, Sleep};
use ntex::util::{ready, select, Either};
use ntex::{io::DispatchItem, rt::spawn, task::LocalWaker};

use crate::codec::{protocol::Frame, AmqpCodec, AmqpFrame};
use crate::error::{AmqpDispatcherError, AmqpProtocolError, Error};
use crate::{connection::Connection, types, ControlFrame, ControlFrameKind, ReceiverLink};

/// Amqp server dispatcher service.
pub(crate) struct Dispatcher<Sr: Service<types::Message>, Ctl: Service<ControlFrame>>(
    Rc<DispatcherInner<Sr, Ctl>>,
);

struct DispatcherInner<Sr: Service<types::Message>, Ctl: Service<ControlFrame>> {
    sink: Connection,
    service: PipelineBinding<Sr, types::Message>,
    ctl_service: PipelineBinding<Ctl, ControlFrame>,
    ctl_error: cell::Cell<Option<AmqpDispatcherError>>,
    ctl_error_waker: LocalWaker,
    idle_sleep: Sleep,
    idle_timeout: Millis,
    stopped: cell::Cell<bool>,
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
        let inner = Rc::new(DispatcherInner {
            sink,
            idle_timeout,
            service: service.bind(),
            ctl_service: ctl_service.bind(),
            ctl_error: cell::Cell::new(None),
            ctl_error_waker: LocalWaker::default(),
            idle_sleep: sleep(idle_timeout),
            stopped: cell::Cell::new(false),
        });

        let disp = Dispatcher(inner);
        disp.start_idle_timer();
        disp.start_control_queue();
        disp
    }

    fn call_control_service(&self, frame: ControlFrame) {
        self.0.sink.get_control_queue().enqueue_frame(frame);
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
            if let Some(err) = self.0.ctl_error.take() {
                log::error!("{}: Control service failed: {:?}", self.0.sink.tag(), err);
                let _ = self.0.sink.close();
                return Poll::Ready(Err(err));
            }

            // check readiness
            let service_poll = self.0.service.poll_ready(cx).map_err(|err| {
                let err = Error::from(err);
                log::error!(
                    "{}: Publish service readiness check failed: {:?}",
                    self.0.sink.tag(),
                    err
                );
                let _ = self.0.sink.close_with_error(err);
                AmqpDispatcherError::Service
            })?;

            let ctl_service_poll = self.0.ctl_service.poll_ready(cx).map_err(|err| {
                let err = Error::from(err);
                log::error!(
                    "{}: Control service readiness check failed: {:?}",
                    self.0.sink.tag(),
                    err
                );
                let _ = self.0.sink.close_with_error(err);
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

    async fn not_ready(&self) {
        select(
            select(
                poll_fn(|cx| self.0.service.poll_not_ready(cx)),
                poll_fn(|cx| self.0.ctl_service.poll_not_ready(cx)),
            ),
            poll_fn(|cx| {
                self.0.ctl_error_waker.register(cx.waker());
                if let Some(err) = self.0.ctl_error.take() {
                    self.0.ctl_error.set(Some(err));
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await;
    }

    async fn shutdown(&self) {
        self.0
            .sink
            .0
            .get_mut()
            .set_error(AmqpProtocolError::Disconnected);
        let _ = self
            .0
            .ctl_service
            .call(ControlFrame::new_kind(ControlFrameKind::Closed))
            .await;

        self.0.service.shutdown().await;
        self.0.ctl_service.shutdown().await;
    }

    async fn call(
        &self,
        request: DispatchItem<AmqpCodec<AmqpFrame>>,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        match request {
            DispatchItem::Item(frame) => {
                #[cfg(feature = "frame-trace")]
                log::trace!("{}: incoming: {:#?}", self.0.sink.tag(), frame);

                let action = match self
                    .0
                    .sink
                    .handle_frame(frame)
                    .map_err(AmqpDispatcherError::Protocol)
                {
                    Ok(a) => a,
                    Err(e) => return Err(e),
                };

                match action {
                    types::Action::Transfer(link) => {
                        if self.0.sink.is_opened() {
                            let lnk = link.clone();
                            if let Err(e) =
                                self.0.service.call(types::Message::Transfer(link)).await
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
                            ControlFrameKind::RemoteDetachSender(frm, link.clone()),
                        ));
                    }
                    types::Action::DetachReceiver(link, frm) => {
                        let lnk = link.clone();
                        let fut = self.0.service.call(types::Message::Detached(lnk));
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

                        let fut = self.0.service.call(types::Message::DetachedAll(receivers));
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
    fn start_idle_timer(&self) {
        if self.0.idle_timeout.non_zero() {
            let slf = self.0.clone();
            ntex::rt::spawn(async move {
                poll_fn(|cx| slf.idle_sleep.poll_elapsed(cx)).await;
                if slf.stopped.get() || !slf.sink.is_opened() {
                    return;
                }
                log::trace!(
                    "{}: Send keep-alive ping, timeout: {:?} secs",
                    slf.sink.tag(),
                    slf.idle_timeout
                );
                slf.sink.post_frame(AmqpFrame::new(0, Frame::Empty));
                slf.idle_sleep.reset(slf.idle_timeout);
            });
        }
    }

    fn start_control_queue(&self) {
        let slf = self.0.clone();
        let queue = self.0.sink.get_control_queue().clone();
        let on_close = self.0.sink.get_ref().on_close();
        ntex::rt::spawn(async move {
            let mut futs: Vec<(ControlFrame, PipelineCall<Ctl, ControlFrame>)> = Vec::new();
            poll_fn(|cx| {
                queue.waker.register(cx.waker());

                // enqueue pending control frames
                queue.pending.borrow_mut().drain(..).for_each(|frame| {
                    let fut = slf.ctl_service.call(frame.clone());
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
                        Ok(_) => slf.handle_control_frame(&frame, None),
                        Err(e) => slf.handle_control_frame(&frame, Some(e.into())),
                    };

                    if let Err(err) = result {
                        slf.ctl_error.set(Some(err));
                        slf.ctl_error_waker.wake();
                        return Poll::Ready(());
                    }
                }

                if !slf.sink.is_opened() {
                    let _ = on_close.poll_ready(cx);
                }

                if !futs.is_empty() || !slf.stopped.get() || slf.sink.is_opened() {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
        });
    }
}

impl<Sr, Ctl> Drop for Dispatcher<Sr, Ctl>
where
    Sr: Service<types::Message>,
    Ctl: Service<ControlFrame>,
{
    fn drop(&mut self) {
        self.0.stopped.set(true);
        self.0.idle_sleep.elapse();
    }
}

impl<Sr, Ctl> DispatcherInner<Sr, Ctl>
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
                ControlFrameKind::AttachReceiver(_, ref link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::AttachSender(ref frm, ref link) => {
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
                ControlFrameKind::AttachReceiver(ref frm, ref link) => {
                    let link = link.clone();
                    let frm = frm.clone();
                    let fut = self
                        .service
                        .call(types::Message::Attached(frm.clone(), link.clone()));
                    let _ = ntex::rt::spawn(async move {
                        let result = fut.await;
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
