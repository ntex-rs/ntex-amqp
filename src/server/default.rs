use std::marker::PhantomData;
use std::task::{Context, Poll};

use futures::future::{ok, Ready};
use ntex::service::{Service, ServiceFactory};

use crate::{ControlFrame, State};

/// Default control service
pub struct DefaultControlService<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for DefaultControlService<S, E> {
    fn default() -> Self {
        DefaultControlService(PhantomData)
    }
}

impl<S, E> ServiceFactory for DefaultControlService<S, E> {
    type Config = State<S>;
    type Request = ControlFrame<E>;
    type Response = ();
    type Error = E;
    type InitError = E;
    type Service = DefaultControlService<S, E>;
    type Future = Ready<Result<Self::Service, Self::InitError>>;

    fn new_service(&self, _: State<S>) -> Self::Future {
        ok(DefaultControlService(PhantomData))
    }
}

impl<S, E> Service for DefaultControlService<S, E> {
    type Request = ControlFrame<E>;
    type Response = ();
    type Error = E;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    #[inline]
    fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn call(&self, _pkt: Self::Request) -> Self::Future {
        log::warn!("AMQP Control service is not configured");

        // ok(pkt.disconnect_with(super::codec::Disconnect::new(
        //   super::codec::DisconnectReasonCode::UnspecifiedError,
        // )))
        ok(())
    }
}
