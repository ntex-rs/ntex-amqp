use std::fmt::Display;
use std::marker::PhantomData;

use actix_codec::{AsyncRead, AsyncWrite};
use actix_service::{NewService, Service};
use amqp::protocol::Error;
use futures::future::{ok, FutureResult};
use futures::{Async, Poll};

use super::dispatcher::Dispatcher;
use super::link::OpenLink;
use crate::connection::Connection;

/// Server dispatcher factory
pub struct ServerFactory<Io, F, St, S> {
    factory: F,
    _t: PhantomData<(Io, St, S)>,
}

impl<Io, F, St, S> ServerFactory<Io, F, St, S>
where
    Io: AsyncRead + AsyncWrite,
    F: Service<(), Response = (St, S)> + Clone,
    F::Error: Display + Into<Error>,
    S: Service<OpenLink<St>, Response = ()>,
    S::Error: Display + Into<Error>,
{
    /// Create server dispatcher factory
    pub fn new(factory: F) -> Self {
        Self {
            factory,
            _t: PhantomData,
        }
    }
}

impl<Io, F: Clone, St, S> Clone for ServerFactory<Io, F, St, S> {
    fn clone(&self) -> Self {
        ServerFactory {
            factory: self.factory.clone(),
            _t: PhantomData,
        }
    }
}

impl<Io, F, St, S> NewService<Connection<Io>> for ServerFactory<Io, F, St, S>
where
    Io: AsyncRead + AsyncWrite,
    F: Service<(), Response = (St, S)> + Clone,
    F::Error: Display + Into<Error>,
    S: Service<OpenLink<St>, Response = ()>,
    S::Error: Display + Into<Error>,
{
    type Response = ();
    type Error = ();
    type Service = Server<Io, F, St, S>;
    type InitError = ();
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self) -> Self::Future {
        ok(Server {
            factory: self.factory.clone(),
            _t: PhantomData,
        })
    }
}

/// Server dispatcher
pub struct Server<Io, F, St, S> {
    factory: F,
    _t: PhantomData<(Io, St, S)>,
}

impl<Io, F, St, S> Service<Connection<Io>> for Server<Io, F, St, S>
where
    Io: AsyncRead + AsyncWrite,
    F: Service<(), Response = (St, S)> + Clone,
    F::Error: Display + Into<Error>,
    S: Service<OpenLink<St>, Response = ()>,
    S::Error: Display + Into<Error>,
{
    type Response = ();
    type Error = ();
    type Future = Dispatcher<Io, F, St, S>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: Connection<Io>) -> Self::Future {
        Dispatcher::new(req, &mut self.factory)
    }
}
