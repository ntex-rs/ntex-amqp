use std::marker::PhantomData;
use std::task::{Context, Poll};

use futures::future::{err, ok, Ready};
use ntex::service::{Service, ServiceFactory};

use crate::error::LinkError;
use crate::{types::Link, ControlFrame, State};

/// Default publish service
pub(crate) struct DefaultPublishService<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for DefaultPublishService<S, E> {
    fn default() -> Self {
        DefaultPublishService(PhantomData)
    }
}

impl<S, E> ServiceFactory for DefaultPublishService<S, E> {
    type Config = State<S>;
    type Request = Link<S>;
    type Response = ();
    type Error = E;
    type InitError = LinkError;
    type Service = DefaultPublishService<S, E>;
    type Future = Ready<Result<Self::Service, Self::InitError>>;

    fn new_service(&self, _: State<S>) -> Self::Future {
        err(LinkError::force_detach().description("not configured"))
    }
}

impl<S, E> Service for DefaultPublishService<S, E> {
    type Request = Link<S>;
    type Response = ();
    type Error = E;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    #[inline]
    fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn call(&self, _pkt: Self::Request) -> Self::Future {
        log::warn!("AMQP Publish service is not configured");
        ok(())
    }
}

/// Default control service
pub struct DefaultControlService<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for DefaultControlService<S, E> {
    fn default() -> Self {
        DefaultControlService(PhantomData)
    }
}

impl<S, E> ServiceFactory for DefaultControlService<S, E> {
    type Config = State<S>;
    type Request = ControlFrame;
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
    type Request = ControlFrame;
    type Response = ();
    type Error = E;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    #[inline]
    fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn call(&self, _pkt: Self::Request) -> Self::Future {
        ok(())
    }
}
