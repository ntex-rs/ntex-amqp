#![allow(clippy::unused_async_trait_impl)]
use std::marker::PhantomData;

use ntex_error::ErrorInfo;
use ntex_service::{Ctx, Service, ServiceFactory};

use crate::{ControlFrame, State, error::Error, error::LinkError, types::Link};

#[allow(dead_code)]
/// Default publish service
pub(crate) struct DefaultPublishService<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for DefaultPublishService<S, E> {
    fn default() -> Self {
        DefaultPublishService(PhantomData)
    }
}

impl<S, E> ServiceFactory<State<S>, Link<S>> for DefaultPublishService<S, E> {
    type Res = ();
    type Error = E;
    type InitError = LinkError;
    type Service = DefaultPublishService<S, E>;

    async fn create(&self, _: &State<S>) -> Result<Self::Service, Self::InitError> {
        Err(LinkError::force_detach().description("not configured"))
    }
}

impl<S, E> Service<State<S>, Link<S>> for DefaultPublishService<S, E> {
    type Res = ();
    type Error = E;

    async fn call(&self, _: Link<S>, _: Ctx<'_, Self, State<S>>) -> Result<Self::Res, Self::Error> {
        log::warn!("AMQP Publish service is not configured");
        Ok(())
    }
}

/// Default control service
pub(crate) struct DefaultControlService<S>(PhantomData<S>);

impl<S> Default for DefaultControlService<S> {
    fn default() -> Self {
        DefaultControlService(PhantomData)
    }
}

impl<S> ServiceFactory<State<S>, ControlFrame> for DefaultControlService<S> {
    type Res = ();
    type Error = Error;
    type InitError = ErrorInfo;
    type Service = DefaultControlService<S>;

    async fn create(&self, _: &State<S>) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultControlService(PhantomData))
    }
}

impl<S> Service<State<S>, ControlFrame> for DefaultControlService<S> {
    type Res = ();
    type Error = Error;

    async fn call(&self, _: ControlFrame, _: Ctx<'_, Self, State<S>>) -> Result<Self::Res, Self::Error> {
        Ok(())
    }
}
