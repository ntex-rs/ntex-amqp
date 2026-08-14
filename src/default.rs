#![allow(clippy::unused_async_trait_impl)]
use std::marker::PhantomData;

use ntex_service::{Service, ServiceCtx, ServiceFactory};

use crate::{ControlFrame, State, error::LinkError, types::Link};

#[allow(dead_code)]
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
    type Data = ();

    async fn create(&self, _: State<S>) -> Result<Self::Service, Self::InitError> {
        Err(LinkError::force_detach().description("not configured"))
    }

    async fn map_data(&self, _: &State<S>, _: &Self::Data) -> Result<(), Self::InitError> {
        Ok(())
    }
}

impl<S, E> Service<Link<S>> for DefaultPublishService<S, E> {
    type Response = ();
    type Error = E;
    type Data = ();

    async fn call(
        &self,
        _: Link<S>,
        _: &Self::Data,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::warn!("AMQP Publish service is not configured");
        Ok(())
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
    type Data = ();

    async fn create(&self, _: State<S>) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultControlService(PhantomData))
    }

    async fn map_data(&self, _: &State<S>, _: &Self::Data) -> Result<(), Self::InitError> {
        Ok(())
    }
}

impl<S, E> Service<ControlFrame> for DefaultControlService<S, E> {
    type Response = ();
    type Error = E;
    type Data = ();

    async fn call(
        &self,
        _: ControlFrame,
        _: &Self::Data,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        Ok(())
    }
}
