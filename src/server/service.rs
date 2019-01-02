use std::marker::PhantomData;

use actix_service::{IntoNewService, NewService, Service};
use amqp::protocol::Error;
use futures::{future::Join, Async, Future, IntoFuture, Poll};

use super::link::OpenLink;
use crate::cell::Cell;

pub struct ServiceFactory;

impl ServiceFactory {
    /// Set state factory
    pub fn state<State, FState, FStOut, E>(
        state: FState,
    ) -> ServiceFactoryBuilder<State, FState, FStOut, E>
    where
        FState: Fn() -> FStOut,
        FStOut: IntoFuture<Item = State, Error = E>,
        E: Into<Error>,
    {
        ServiceFactoryBuilder {
            state,
            _t: PhantomData,
        }
    }

    /// Set service factory
    pub fn service<F, S>(
        st: F,
    ) -> ServiceFactoryService<
        (),
        impl Fn() -> Result<(), S::InitError>,
        Result<(), S::InitError>,
        S,
        S::InitError,
    >
    where
        F: IntoNewService<S, OpenLink<()>>,
        S: NewService<OpenLink<()>, Response = ()>,
        S::InitError: Into<Error>,
    {
        ServiceFactoryService {
            inner: Cell::new(Inner {
                state: || Ok(()),
                service: st.into_new_service(),
                _t: PhantomData,
            }),
        }
    }
}

pub struct ServiceFactoryBuilder<State, FState, FStOut, E> {
    state: FState,
    _t: PhantomData<(State, FStOut, E)>,
}

impl<State, FState, FStOut, E> ServiceFactoryBuilder<State, FState, FStOut, E>
where
    FState: Fn() -> FStOut,
    FStOut: IntoFuture<Item = State, Error = E>,
    E: Into<Error>,
{
    /// Set service factory
    fn service<F, S>(self, st: F) -> ServiceFactoryService<State, FState, FStOut, S, E>
    where
        F: IntoNewService<S, OpenLink<State>>,
        S: NewService<OpenLink<State>, Response = (), InitError = E>,
    {
        ServiceFactoryService {
            inner: Cell::new(Inner {
                state: self.state,
                service: st.into_new_service(),
                _t: PhantomData,
            }),
        }
    }
}

pub struct ServiceFactoryService<State, FState, FStOut, Srv, E>
where
    FState: Fn() -> FStOut,
    FStOut: IntoFuture<Item = State, Error = E>,
    Srv: NewService<OpenLink<State>, Response = (), InitError = E>,
    E: Into<Error>,
{
    inner: Cell<Inner<State, FState, FStOut, Srv, E>>,
}

pub struct Inner<State, FState, FStOut, Srv, E>
where
    FState: Fn() -> FStOut,
    FStOut: IntoFuture<Item = State, Error = E>,
    Srv: NewService<OpenLink<State>, Response = (), InitError = E>,
    E: Into<Error>,
{
    state: FState,
    service: Srv,
    _t: PhantomData<(E,)>,
}

impl<State, FState, FStOut, Srv, E> Clone for ServiceFactoryService<State, FState, FStOut, Srv, E>
where
    FState: Fn() -> FStOut,
    FStOut: IntoFuture<Item = State, Error = E>,
    Srv: NewService<OpenLink<State>, Response = (), InitError = E>,
    E: Into<Error>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<State, FState, FStOut, Srv, E> Service<()>
    for ServiceFactoryService<State, FState, FStOut, Srv, E>
where
    FState: Fn() -> FStOut,
    FStOut: IntoFuture<Item = State, Error = E>,
    Srv: NewService<OpenLink<State>, Response = (), InitError = E>,
    E: Into<Error>,
{
    type Response = (State, Srv::Service);
    type Error = E;
    type Future = Join<FStOut::Future, Srv::Future>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: ()) -> Self::Future {
        let inner = self.inner.get_mut();
        (inner.state)()
            .into_future()
            .join(inner.service.new_service())
    }
}
