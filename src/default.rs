use std::marker::PhantomData;

use ntex::service::{Service, ServiceCtx, ServiceFactory};

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

    async fn create(&self, _: State<S>) -> Result<Self::Service, Self::InitError> {
        Err(LinkError::force_detach().description("not configured"))
    }
}

impl<S, E> Service<Link<S>> for DefaultPublishService<S, E> {
    type Response = ();
    type Error = E;

    async fn call(
        &self,
        _: Link<S>,
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

    async fn create(&self, _: State<S>) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultControlService(PhantomData))
    }
}

impl<S, E> Service<ControlFrame> for DefaultControlService<S, E> {
    type Response = ();
    type Error = E;

    async fn call(
        &self,
        _: ControlFrame,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        Ok(())
    }
}
