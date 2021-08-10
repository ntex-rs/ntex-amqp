use std::{cell::RefCell, fmt, future::Future, pin::Pin, task::Context, task::Poll, time};

use ntex::framed::DispatchItem;
use ntex::rt::time::{sleep, Sleep};
use ntex::service::Service;
use ntex::util::Ready;

use crate::codec::protocol::Frame;
use crate::codec::{AmqpCodec, AmqpFrame};
use crate::error::{AmqpProtocolError, DispatcherError, Error};
use crate::{connection::Connection, types, ControlFrame, ControlFrameKind, State};

/// Amqp server dispatcher service.
pub(crate) struct Dispatcher<St, Sr, Ctl: Service> {
    state: State<St>,
    sink: Connection,
    service: Sr,
    ctl_service: Ctl,
    ctl_fut: RefCell<Option<(ControlFrame, Pin<Box<Ctl::Future>>)>>,
    shutdown: std::cell::Cell<bool>,
    expire: RefCell<Pin<Box<Sleep>>>,
    idle_timeout: usize,
}

impl<St, Sr, Ctl> Dispatcher<St, Sr, Ctl>
where
    Sr: Service<Request = types::Message, Response = ()>,
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
            expire: RefCell::new(Box::pin(sleep(time::Duration::from_secs(
                idle_timeout as u64,
            )))),
        }
    }

    fn handle_idle_timeout(&self, cx: &mut Context<'_>) {
        let idle_timeout = self.idle_timeout;
        if idle_timeout > 0 {
            let mut expire = self.expire.borrow_mut();
            if Pin::new(&mut *expire).poll(cx).is_ready() {
                log::trace!("Send keep-alive ping, timeout: {:?} secs", idle_timeout);
                self.sink.post_frame(AmqpFrame::new(0, Frame::Empty));
                *expire = Box::pin(sleep(time::Duration::from_secs(idle_timeout as u64)));
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
                    self.handle_control_frame(frame, None)?;
                }
                Poll::Pending => return Ok(false),
                Poll::Ready(Err(e)) => {
                    let (frame, _) = inner.take().unwrap();
                    self.handle_control_frame(frame, Some(e.into()))?;
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
                ControlFrameKind::ProtocolError(ref err) => return Err(err.clone().into()),
                ControlFrameKind::Closed(_) => (),
            }
        } else {
            match frame.0.get_mut().kind {
                ControlFrameKind::AttachReceiver(ref link) => {
                    let link = link.clone();
                    link.confirm_receiver_link();
                    let fut = self.service.call(types::Message::Attached(link.clone()));
                    ntex::rt::spawn(async move {
                        if let Err(err) = fut.await {
                            link.close_with_error(Error::from(err)).await;
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
                    frame.session_cell().get_mut().handle_flow(frm, Some(&link));
                }
                ControlFrameKind::DetachSender(_, _) => {
                    // frame.session_cell().get_mut().handle_detach(frm);
                }
                ControlFrameKind::DetachReceiver(_, _) => {
                    // frame.session_cell().get_mut().handle_detach(frm);
                }
                ControlFrameKind::ProtocolError(ref err) => return Err(err.clone().into()),
                ControlFrameKind::Closed(_) => (),
            }
        }
        Ok(())
    }
}

impl<St, Sr, Ctl> Service for Dispatcher<St, Sr, Ctl>
where
    Sr: Service<Request = types::Message, Response = ()>,
    Sr::Error: fmt::Debug + 'static,
    Sr::Future: 'static,
    Ctl: Service<Request = ControlFrame, Response = ()>,
    Ctl::Error: fmt::Debug + 'static,
    Ctl::Future: 'static,
    Error: From<Sr::Error> + From<Ctl::Error>,
{
    type Request = DispatchItem<AmqpCodec<AmqpFrame>>;
    type Response = ();
    type Error = DispatcherError;
    type Future = Ready<Self::Response, Self::Error>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // process control frame
        let res0 = !self.handle_control_fut(cx)?;

        // check readiness
        let res1 = self.service.poll_ready(cx).map_err(|err| {
            error!("Publish service readiness check failed: {:?}", err);
            let _ = self.sink.close_with_error(err);
            DispatcherError::Service
        })?;
        let res2 = self.ctl_service.poll_ready(cx).map_err(|err| {
            error!("Control service readiness check failed: {:?}", err);
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
            let sink = self.sink.0.get_mut();
            if is_error {
                sink.set_error(AmqpProtocolError::Disconnected);
            }
            sink.on_close.notify();
            sink.set_error(AmqpProtocolError::Disconnected);
            let fut = self
                .ctl_service
                .call(ControlFrame::new_kind(ControlFrameKind::Closed(is_error)));
            ntex::rt::spawn(async move {
                let _ = fut.await;
            });
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
            DispatchItem::Item(frame) => {
                #[cfg(feature = "frame-trace")]
                log::trace!("incoming: {:#?}", frame);

                let action = try_ready_err!(self
                    .sink
                    .0
                    .get_mut()
                    .handle_frame(frame, &self.sink.0)
                    .map_err(DispatcherError::Protocol));

                let result = match action {
                    types::Action::Flow(link, frm) => {
                        // apply flow to specific link
                        let frame = ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::Flow(frm, link.clone()),
                        );
                        *self.ctl_fut.borrow_mut() =
                            Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                        Ok(())
                    }
                    types::Action::AttachSender(link, frame) => {
                        let frame = ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::AttachSender(Box::new(frame), link),
                        );

                        *self.ctl_fut.borrow_mut() =
                            Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                        Ok(())
                    }
                    types::Action::AttachReceiver(link) => {
                        let frame = ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::AttachReceiver(link),
                        );
                        *self.ctl_fut.borrow_mut() =
                            Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                        Ok(())
                    }
                    types::Action::DetachSender(link, frm) => {
                        let frame = ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::DetachSender(frm, link.clone()),
                        );
                        *self.ctl_fut.borrow_mut() =
                            Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                        Ok(())
                    }
                    types::Action::DetachReceiver(link, frm) => {
                        let frame = ControlFrame::new(
                            link.session().inner.clone(),
                            ControlFrameKind::DetachReceiver(frm, link.clone()),
                        );
                        *self.ctl_fut.borrow_mut() =
                            Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                        Ok(())
                    }
                    types::Action::None => Ok(()),
                };

                Ready::from(result)
            }
            DispatchItem::EncoderError(err) | DispatchItem::DecoderError(err) => {
                let frame = ControlFrame::new_kind(ControlFrameKind::ProtocolError(err.into()));
                *self.ctl_fut.borrow_mut() =
                    Some((frame.clone(), Box::pin(self.ctl_service.call(frame))));
                Ready::from(Ok(()))
            }
            DispatchItem::KeepAliveTimeout => {
                self.sink
                    .0
                    .get_mut()
                    .set_error(AmqpProtocolError::KeepAliveTimeout);
                Ready::from(Ok(()))
            }
            DispatchItem::IoError(_) => {
                self.sink
                    .0
                    .get_mut()
                    .set_error(AmqpProtocolError::Disconnected);
                Ready::from(Ok(()))
            }
            DispatchItem::WBackPressureEnabled | DispatchItem::WBackPressureDisabled => {
                Ready::from(Ok(()))
            }
        }
    }
}
