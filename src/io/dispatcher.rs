//! Framed transport dispatcher
use std::task::{Context, Poll};
use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc, time::Duration, time::Instant};

use futures::FutureExt;
use ntex::service::{IntoService, Service};
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use super::state::Flags;
use crate::io::{DispatcherItem, IoRead, IoState, IoWrite, Timer};

/// Framed dispatcher - is a future that reads frames from Framed object
/// and pass then to the service.
#[pin_project::pin_project]
pub(crate) struct IoDispatcher<S, U>
where
    S: Service<Request = DispatcherItem<U>, Response = ()>,
    S::Error: 'static,
    S::Future: 'static,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    service: S,
    state: IoState<U>,
    inner: Rc<RefCell<IoDispatcherInner<S, U>>>,
    st: IoDispatcherState,
    timer: Timer<U>,
    updated: Instant,
    keepalive_timeout: u16,
    #[pin]
    response: Option<S::Future>,
}

struct IoDispatcherInner<S, U>
where
    S: Service<Request = DispatcherItem<U>, Response = ()>,
    S::Error: 'static,
    S::Future: 'static,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    error: Option<IoDispatcherError<S::Error, <U as Encoder>::Error>>,
    inflight: usize,
}

enum ServiceResult<T> {
    Pending,
    Ready(T),
}

impl<T> ServiceResult<T> {
    fn take(&mut self) -> Option<T> {
        let slf = std::mem::replace(self, ServiceResult::Pending);
        match slf {
            ServiceResult::Pending => None,
            ServiceResult::Ready(result) => Some(result),
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum IoDispatcherState {
    Processing,
    Stop,
    Shutdown,
}

pub(crate) enum IoDispatcherError<S, U> {
    None,
    KeepAlive,
    Encoder(U),
    Service(S),
}

impl<E1, E2: std::fmt::Debug> IoDispatcherError<E1, E2> {
    fn take<U>(&mut self) -> Option<DispatcherItem<U>>
    where
        U: Encoder<Error = E2> + Decoder,
    {
        match self {
            IoDispatcherError::KeepAlive => {
                *self = IoDispatcherError::None;
                Some(DispatcherItem::KeepAliveTimeout)
            }
            IoDispatcherError::Encoder(_) => {
                let err = std::mem::replace(self, IoDispatcherError::None);
                match err {
                    IoDispatcherError::Encoder(err) => Some(DispatcherItem::EncoderError(err)),
                    _ => None,
                }
            }
            IoDispatcherError::None | IoDispatcherError::Service(_) => None,
        }
    }
}

impl<S, U> IoDispatcher<S, U>
where
    S: Service<Request = DispatcherItem<U>, Response = ()> + 'static,
    U: Decoder + Encoder + 'static,
    <U as Encoder>::Item: 'static,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(crate) fn with<T, F: IntoService<S>>(
        io: T,
        state: IoState<U>,
        service: F,
        timer: Timer<U>,
    ) -> Self
    where
        T: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let updated = timer.now();
        let keepalive_timeout: u16 = 30;
        let io = Rc::new(RefCell::new(io));

        // register keepalive timer
        let expire = updated + Duration::from_secs(keepalive_timeout as u64);
        timer.register(expire, expire, &state);

        // start support tasks
        ntex::rt::spawn(IoRead::new(io.clone(), state.clone()));
        ntex::rt::spawn(IoWrite::new(io, state.clone()));

        let inner = Rc::new(RefCell::new(IoDispatcherInner {
            error: None,
            inflight: 0,
        }));

        IoDispatcher {
            st: IoDispatcherState::Processing,
            service: service.into_service(),
            response: None,
            state,
            inner,
            timer,
            updated,
            keepalive_timeout,
        }
    }

    /// Set keep-alive timeout in seconds.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default keep-alive timeout is set to 30 seconds.
    pub(crate) fn keepalive_timeout(mut self, timeout: u16) -> Self {
        // register keepalive timer
        let prev = self.updated + Duration::from_secs(self.keepalive_timeout as u64);
        if timeout == 0 {
            self.timer.unregister(prev, &self.state);
        } else {
            let expire = self.updated + Duration::from_secs(timeout as u64);
            self.timer.register(expire, prev, &self.state);
        }

        self.keepalive_timeout = timeout;

        self
    }

    /// Set connection disconnect timeout in milliseconds.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 1 seconds.
    pub(crate) fn disconnect_timeout(self, val: u16) -> Self {
        self.state.inner.borrow_mut().disconnect_timeout = val;
        self
    }
}

impl<S, U> IoDispatcherInner<S, U>
where
    S: Service<Request = DispatcherItem<U>, Response = ()>,
    S::Error: 'static,
    S::Future: 'static,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    fn handle_result(&mut self, state: &IoState<U>, wake: bool) {
        let state = state.inner.borrow_mut();
        if wake {
            state.dispatch_task.wake();
        }
    }
}

impl<S, U> Future for IoDispatcher<S, U>
where
    S: Service<Request = DispatcherItem<U>, Response = ()> + 'static,
    U: Decoder + Encoder + 'static,
    <U as Encoder>::Item: 'static,
{
    type Output = Result<(), S::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        // log::trace!("IO-DISP poll :{:?}:", this.st);

        // handle service response future
        if let Some(fut) = this.response.as_mut().as_pin_mut() {
            match fut.poll(cx) {
                Poll::Pending => (),
                Poll::Ready(_) => {
                    this.inner.borrow_mut().handle_result(this.state, false);
                    this.response.set(None);
                }
            }
        }

        match this.st {
            IoDispatcherState::Processing => {
                loop {
                    // log::trace!("IO-DISP state :{:?}:", state.flags);

                    match this.service.poll_ready(cx) {
                        Poll::Ready(Ok(_)) => {
                            let mut retry = false;
                            let mut state = this.state.inner.borrow_mut();

                            // service is ready, wake io read task
                            if state.flags.contains(Flags::RD_PAUSED) {
                                state.flags.remove(Flags::RD_PAUSED);
                                state.read_task.wake();
                            }

                            let item = if state.flags.contains(Flags::DSP_STOP) {
                                let mut inner = this.inner.borrow_mut();

                                // check keepalive timeout
                                if state.flags.contains(Flags::DSP_KEEPALIVE) {
                                    if inner.error.is_none() {
                                        inner.error = Some(IoDispatcherError::KeepAlive);
                                    }
                                } else if *this.keepalive_timeout != 0 {
                                    // unregister keep-alive timer
                                    this.timer.unregister(
                                        *this.updated
                                            + Duration::from_secs(*this.keepalive_timeout as u64),
                                        this.state,
                                    );
                                }

                                // check for errors
                                let item =
                                    inner.error.as_mut().and_then(|err| err.take()).or_else(|| {
                                        state.error.take().map(DispatcherItem::IoError)
                                    });
                                *this.st = IoDispatcherState::Stop;
                                retry = true;

                                item
                            } else {
                                // decode incoming bytes stream
                                if state.flags.contains(Flags::RD_READY) {
                                    // log::trace!(
                                    //    "attempt to decode frame, buffer size is {}",
                                    //    state.read_buf.len()
                                    //);

                                    match state.decode_item() {
                                        Ok(Some(el)) => {
                                            // log::trace!(
                                            //     "frame is succesfully decoded, remaining buffer {}",
                                            //     state.read_buf.len()
                                            // );

                                            // update keep-alive timer
                                            if *this.keepalive_timeout != 0 {
                                                let updated = this.timer.now();
                                                if updated != *this.updated {
                                                    let ka = Duration::from_secs(
                                                        *this.keepalive_timeout as u64,
                                                    );
                                                    this.timer.register(
                                                        updated + ka,
                                                        *this.updated + ka,
                                                        this.state,
                                                    );
                                                    *this.updated = updated;
                                                }
                                            }

                                            Some(DispatcherItem::Item(el))
                                        }
                                        Ok(None) => {
                                            // log::trace!("not enough data to decode next frame, register dispatch task");
                                            state.dispatch_task.register(cx.waker());
                                            state.flags.remove(Flags::RD_READY);
                                            state.read_task.wake();
                                            return Poll::Pending;
                                        }
                                        Err(err) => {
                                            // log::warn!("frame decode error");
                                            retry = true;
                                            *this.st = IoDispatcherState::Stop;
                                            state
                                                .flags
                                                .insert(Flags::DSP_STOP | Flags::IO_SHUTDOWN);
                                            state.write_task.wake();
                                            state.read_task.wake();

                                            // unregister keep-alive timer
                                            if *this.keepalive_timeout != 0 {
                                                this.timer.unregister(
                                                    *this.updated
                                                        + Duration::from_secs(
                                                            *this.keepalive_timeout as u64,
                                                        ),
                                                    this.state,
                                                );
                                            }

                                            Some(DispatcherItem::DecoderError(err))
                                        }
                                    }
                                } else {
                                    // log::trace!(
                                    //     "read task is not ready, register dispatch task"
                                    // );
                                    state.dispatch_task.register(cx.waker());
                                    return Poll::Pending;
                                }
                            };

                            // call service
                            if let Some(item) = item {
                                // optimize first call
                                if this.response.is_none() {
                                    drop(state);
                                    this.response.set(Some(this.service.call(item)));

                                    let res = this.response.as_mut().as_pin_mut().unwrap().poll(cx);
                                    if let Poll::Ready(_) = res {
                                        this.response.set(None);
                                    }
                                } else {
                                    drop(state);

                                    let st = this.state.clone();
                                    let inner = this.inner.clone();
                                    ntex::rt::spawn(this.service.call(item).map(move |_| {
                                        inner.borrow_mut().handle_result(&st, true);
                                    }));
                                }
                            } else {
                                drop(state);
                            }

                            // run again
                            if retry {
                                return self.poll(cx);
                            }
                        }
                        Poll::Pending => {
                            // pause io read task
                            log::trace!("service is not ready, register dispatch task");
                            let mut state = this.state.inner.borrow_mut();
                            state.flags.insert(Flags::RD_PAUSED);
                            state.dispatch_task.register(cx.waker());
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => {
                            log::trace!("service readiness check failed, stopping");
                            // service readiness error
                            *this.st = IoDispatcherState::Stop;
                            this.inner.borrow_mut().error = Some(IoDispatcherError::Service(err));
                            this.state.inner.borrow_mut().flags.insert(Flags::DSP_STOP);

                            // unregister keep-alive timer
                            if *this.keepalive_timeout != 0 {
                                this.timer.unregister(
                                    *this.updated
                                        + Duration::from_secs(*this.keepalive_timeout as u64),
                                    this.state,
                                );
                            }

                            return self.poll(cx);
                        }
                    }
                }
            }
            // drain service responses
            IoDispatcherState::Stop => {
                // service may relay on poll_ready for response results
                let _ = this.service.poll_ready(cx);

                let mut state = this.state.inner.borrow_mut();
                if this.inner.borrow().inflight == 0 {
                    state.read_task.wake();
                    state.write_task.wake();
                    state.flags.insert(Flags::IO_SHUTDOWN);
                    *this.st = IoDispatcherState::Shutdown;
                    drop(state);
                    self.poll(cx)
                } else {
                    state.dispatch_task.register(cx.waker());
                    Poll::Pending
                }
            }
            // shutdown service
            IoDispatcherState::Shutdown => {
                let is_err = this.inner.borrow().error.is_some();

                return if this.service.poll_shutdown(cx, is_err).is_ready() {
                    log::trace!("service shutdown is completed, stop");

                    Poll::Ready(
                        if let Some(IoDispatcherError::Service(err)) =
                            this.inner.borrow_mut().error.take()
                        {
                            Err(err)
                        } else {
                            Ok(())
                        },
                    )
                } else {
                    Poll::Pending
                };
            }
        }
    }
}
