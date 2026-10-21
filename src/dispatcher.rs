use std::task::{Poll, ready};
use std::{cell, future::poll_fn, rc::Rc};

use ntex_dispatcher::{DispatchItem, Reason};
use ntex_rt::spawn;
use ntex_service::pipeline::{Pipeline, PipelineBinding};
use ntex_service::{BindCtx, Ctx, Service};
use ntex_util::channel::condition::Condition;
use ntex_util::future::{Either, select};
use ntex_util::time::{CallbackTimer, Millis};

use crate::codec::{AmqpCodec, AmqpFrame, protocol::Frame};
use crate::connection::{Connection, ConnectionRef};
use crate::error::{AmqpDispatcherError, AmqpProtocolError, Error};
use crate::{ControlFrame, ControlFrameKind, types};

/// Amqp server dispatcher service.
pub(crate) struct Dispatcher {
    sink: Connection,
    service: Pipeline<types::Message, (), Error>,
    ctl_service: Pipeline<ControlFrame, (), Error>,
    ctl_state: Rc<ControlState>,
}

/// State shared with spawned control service calls
struct ControlState {
    sink: ConnectionRef,
    service: PipelineBinding<types::Message, (), Error>,
    error: cell::Cell<Option<AmqpDispatcherError>>,
    idle_timer: Option<CallbackTimer>,
    // cancels in-flight control service calls
    stop: Condition,
}

impl Dispatcher {
    pub(crate) fn new(
        sink: Connection,
        service: Pipeline<types::Message, (), Error>,
        ctl_service: Pipeline<ControlFrame, (), Error>,
        idle_timeout: Millis,
    ) -> Self {
        let idle_timeout = Millis(if idle_timeout.non_zero() {
            (idle_timeout.0 >> 1).clamp(1, 1000)
        } else {
            0
        });
        Dispatcher {
            ctl_state: Rc::new_cyclic(|weak: &std::rc::Weak<ControlState>| {
                let weak = weak.clone();
                ControlState {
                    sink: sink.clone(),
                    service: service.bind(),
                    error: cell::Cell::new(None),
                    idle_timer: idle_timeout.non_zero().then(|| {
                        CallbackTimer::new(idle_timeout, move || {
                            if let Some(state) = weak.upgrade()
                                && state.sink.is_opened()
                            {
                                log::trace!(
                                    "{}: Send keep-alive ping, timeout: {:?}",
                                    state.sink.tag(),
                                    idle_timeout
                                );
                                if let Some(timer) = &state.idle_timer {
                                    timer.reset(idle_timeout);
                                }
                                state.sink.post_frame(AmqpFrame::new(0, Frame::Empty));
                            }
                        })
                    }),
                    stop: Condition::new(),
                }
            }),
            sink,
            service,
            ctl_service,
        }
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        self.ctl_state.stop();
    }
}

impl Service<(), DispatchItem<AmqpCodec<AmqpFrame>>> for Dispatcher {
    type Res = Option<AmqpFrame>;
    type Error = AmqpDispatcherError;

    fn bind(&self, ctx: BindCtx<'_>) {
        self.service.bind_to(&ctx);
        self.ctl_service.bind_to(&ctx);
        ctx.subscribe(&self.sink.get_control_queue().changed);
    }

    async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
        poll_fn(|cx| {
            // Process queue notifications even while the publish service is pending.
            self.dispatch_control_queue();

            if let Some(err) = self.ctl_state.error.take() {
                log::error!("{}: Control service failed: {:?}", self.sink.tag(), err);
                let _ = self.sink.close();
                return Poll::Ready(Err(err));
            }

            if let Err(err) = ready!(self.service.poll_ready(cx)) {
                log::error!(
                    "{}: Publish service readiness check failed: {:?}",
                    self.sink.tag(),
                    err
                );
                let _ = self.sink.close_with_error(err);
                return Poll::Ready(Err(AmqpDispatcherError::Service));
            }

            if let Err(err) = ready!(self.ctl_service.poll_ready(cx)) {
                log::error!(
                    "{}: Control service readiness check failed: {:?}",
                    self.sink.tag(),
                    err
                );
                let _ = self.sink.close_with_error(err);
                return Poll::Ready(Err(AmqpDispatcherError::Service));
            }

            Poll::Ready(Ok(()))
        })
        .await
    }

    async fn shutdown(&self, _: Ctx<'_, Self, ()>) {
        self.ctl_state.stop();
        self.sink.set_error(AmqpProtocolError::Disconnected);
        let _ = self
            .ctl_service
            .call(ControlFrame::new_kind(ControlFrameKind::Closed))
            .await;

        self.service.shutdown().await;
        self.ctl_service.shutdown().await;
    }

    async fn call(
        &self,
        req: DispatchItem<AmqpCodec<AmqpFrame>>,
        _: Ctx<'_, Self, ()>,
    ) -> Result<Self::Res, Self::Error> {
        match req {
            DispatchItem::Item(frame) => {
                #[cfg(feature = "frame-trace")]
                log::trace!("{}: incoming: {frame:#?}", self.sink.tag());

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
                                log::trace!("Service error {e:?}");
                                let _ = lnk.close_with_error(e);
                            }
                        }
                    }
                    types::Action::Flow(link, frm) => {
                        // link flow is already applied, notify control service
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
                        self.spawn_publish_call(types::Message::Detached(link.clone()));
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

                        self.spawn_publish_call(types::Message::DetachedAll(receivers));
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
                }

                Ok(None)
            }
            DispatchItem::Stop(Reason::Encoder(err) | Reason::Decoder(err)) => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                    err.into(),
                )));
                Ok(None)
            }
            DispatchItem::Stop(Reason::KeepAlive) => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                    AmqpProtocolError::KeepAliveTimeout,
                )));
                Ok(None)
            }
            DispatchItem::Stop(Reason::ReadTimeout) => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                    AmqpProtocolError::ReadTimeout,
                )));
                Ok(None)
            }
            DispatchItem::Stop(Reason::WriteTimeout) => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::ProtocolError(
                    AmqpProtocolError::WriteTimeout,
                )));
                Ok(None)
            }
            DispatchItem::Stop(Reason::Io(e)) => {
                self.call_control_service(ControlFrame::new_kind(ControlFrameKind::Disconnected(
                    e,
                )));
                Ok(None)
            }
            // the service failure was already reported and closed the connection
            DispatchItem::Control(_) | DispatchItem::Stop(Reason::Service) => Ok(None),
        }
    }
}

impl Dispatcher {
    fn dispatch_control_queue(&self) {
        let queue = self.sink.get_control_queue();

        // enqueue pending control frames
        loop {
            let frame = queue.pending.borrow_mut().pop_front();
            let Some(frame) = frame else {
                break;
            };
            // release publish service resources of locally detached links
            match frame.kind() {
                ControlFrameKind::LocalDetachReceiver(_, link) => {
                    self.spawn_publish_call(types::Message::Detached(link.clone()));
                }
                ControlFrameKind::LocalSessionEnded(links) => {
                    let receivers = links
                        .iter()
                        .filter_map(|either| match either {
                            Either::Right(link) => Some(link.clone()),
                            Either::Left(_) => None,
                        })
                        .collect();
                    self.spawn_publish_call(types::Message::DetachedAll(receivers));
                }
                _ => (),
            }
            self.spawn_control_call(frame);
        }
    }

    fn spawn_publish_call(&self, msg: types::Message) {
        let fut = self.service.call_static(msg);
        let stop = self.ctl_state.stop.wait();

        // publish service call is dropped on dispatcher shutdown
        spawn(async move {
            let _ = select(stop.ready(), fut).await;
        });
    }

    fn call_control_service(&self, frame: ControlFrame) {
        self.spawn_control_call(frame);
    }

    fn spawn_control_call(&self, frame: ControlFrame) {
        let fut = self.ctl_service.call_static(frame.clone());
        let state = self.ctl_state.clone();
        let stop = state.stop.wait();

        spawn(async move {
            // in-flight calls are dropped on dispatcher shutdown
            if let Either::Right(res) = select(stop.ready(), fut).await
                && let Err(err) = state.handle_control_frame(&frame, res.err())
            {
                state.error.set(Some(err));
                state.sink.get_control_queue().changed.notify();
            }
        });
    }
}

impl ControlState {
    fn stop(&self) {
        if let Some(timer) = &self.idle_timer {
            timer.cancel();
        }
        self.stop.notify_and_lock(());
    }

    fn handle_control_frame(
        &self,
        frame: &ControlFrame,
        err: Option<Error>,
    ) -> Result<(), AmqpDispatcherError> {
        if let Some(err) = err {
            match &frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(_, _, link)
                | ControlFrameKind::RemoteDetachReceiver(_, link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::AttachSender(frm, _, link) => {
                    frame
                        .session_cell()
                        .get_mut()
                        .detach_unconfirmed_sender_link(frm, &link.inner, Some(err));
                }
                ControlFrameKind::Flow(_, link) | ControlFrameKind::RemoteDetachSender(_, link) => {
                    let _ = link.close_with_error(err);
                }
                ControlFrameKind::LocalDetachSender(..)
                | ControlFrameKind::LocalDetachReceiver(..) => {}
                ControlFrameKind::ProtocolError(err) => {
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
                // session is ended or link is closed by control service
                ControlFrameKind::AttachReceiver(_, _, ref link) if link.is_closed() => {}
                ControlFrameKind::AttachReceiver(ref frm, ref mut pfrm, ref link) => {
                    let link = link.clone();
                    let fut = self
                        .service
                        .call_static(types::Message::Attached(frm.clone(), link.clone()));
                    let response = pfrm.take();
                    let service = self.service.clone();
                    let stop = self.stop.wait();

                    ntex_rt::spawn(async move {
                        // publish service call is dropped on dispatcher shutdown
                        let _ = select(stop.ready(), async move {
                            let result = fut.await;
                            if let Err(err) = result {
                                let _ = link.close_with_error(err);
                            } else if link.confirm_receiver_link(response) {
                                link.set_link_credit(50);
                            } else {
                                // link is detached, release publish service resources
                                let _ = service.call(types::Message::Detached(link)).await;
                            }
                        })
                        .await;
                    });
                }
                ControlFrameKind::AttachSender(ref frm, ref mut pfrm, ref link) => {
                    frame.session_cell().get_mut().attach_remote_sender_link(
                        frm,
                        pfrm.take(),
                        link.inner.clone(),
                    );
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
                | ControlFrameKind::Flow(..)
                | ControlFrameKind::RemoteDetachSender(..)
                | ControlFrameKind::RemoteDetachReceiver(..)
                | ControlFrameKind::RemoteSessionEnded(_) => (),
            }
        }
        Ok(())
    }
}
