use std::marker::PhantomData;

use ntex::service::{Ctx, Service, ServiceFactory};
use ntex::util::Ready;

use crate::error::LinkError;
use crate::{types::Link, ControlFrame, State};

/// Default publish service
pub(crate) struct DefaultPublishService<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for DefaultPublishService<S, E> {
    fn default() -> Self {
        DefaultPublishService(PhantomData)
    }
}

impl<S, E> ServiceFactory<Link<S>, State<S>> for DefaultPublishService<S, E> {
    type Response = ();
    type Error = E;
    type InitError = LinkError;
    type Service = DefaultPublishService<S, E>;
    type Future<'f> = Ready<Self::Service, Self::InitError> where Self: 'f;

    fn create(&self, _: State<S>) -> Self::Future<'_> {
        Ready::Err(LinkError::force_detach().description("not configured"))
    }
}

impl<S, E> Service<Link<S>> for DefaultPublishService<S, E> {
    type Response = ();
    type Error = E;
    type Future<'f> = Ready<Self::Response, Self::Error> where Self: 'f;

    #[inline]
    fn call<'a>(&'a self, _: Link<S>, _: Ctx<'a, Self>) -> Self::Future<'a> {
        log::warn!("AMQP Publish service is not configured");
        Ready::Ok(())
    }
}

/// Default control service
pub struct DefaultControlService<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for DefaultControlService<S, E> {
    fn default() -> Self {
        DefaultControlService(PhantomData)
    }
}

impl<S, E> ServiceFactory<ControlFrame, State<S>> for DefaultControlService<S, E> {
    type Response = ();
    type Error = E;
    type InitError = E;
    type Service = DefaultControlService<S, E>;
    type Future<'f> = Ready<Self::Service, Self::InitError> where Self: 'f;

    fn create(&self, _: State<S>) -> Self::Future<'_> {
        Ready::Ok(DefaultControlService(PhantomData))
    }
}

impl<S, E> Service<ControlFrame> for DefaultControlService<S, E> {
    type Response = ();
    type Error = E;
    type Future<'f> = Ready<Self::Response, Self::Error> where Self: 'f;

    #[inline]
    fn call<'a>(&'a self, _: ControlFrame, _: Ctx<'a, Self>) -> Self::Future<'a> {
        Ready::Ok(())
    }
}
