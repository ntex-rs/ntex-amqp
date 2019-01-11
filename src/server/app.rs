use std::fmt;
use std::marker::PhantomData;

use actix_router::{Path, Router, RouterBuilder};
use actix_service::{IntoService, NewService, Service};
use amqp_codec::protocol::Error;
use bytes::Bytes;
use futures::future::{err, ok, Either, FutureResult};
use futures::{Async, Future, Poll};

use crate::cell::Cell;

use super::errors::LinkError;
use super::link::OpenLink;

pub struct App<S = ()> {
    router: RouterBuilder<BoxedHandle<S>>,
}

impl<S: 'static> App<S> {
    pub fn new() -> App<S> {
        App {
            router: Router::build(),
        }
    }

    pub fn service<F, U: 'static>(mut self, address: &str, service: F) -> Self
    where
        F: IntoService<U, OpenLink<S>>,
        U: Service<OpenLink<S>, Response = ()>,
        U::Error: Into<Error>,
    {
        let hnd = Handle {
            service: service.into_service(),
            _t: PhantomData,
        };
        self.router.path(address, Box::new(hnd));

        self
    }

    pub fn finish(
        self,
    ) -> impl NewService<
        OpenLink<S>,
        Response = (),
        InitError = impl Into<Error> + fmt::Display,
        Error = impl Into<Error> + fmt::Display,
    > {
        let router = Cell::new(self.router.finish());

        move || -> FutureResult<AppService<S>, Error> {
            ok(AppService {
                router: router.clone(),
            })
        }
    }
}

struct AppService<S> {
    router: Cell<Router<BoxedHandle<S>>>,
}

impl<S: 'static> Service<OpenLink<S>> for AppService<S> {
    type Response = ();
    type Error = Error;
    type Future = Either<FutureResult<(), Error>, Box<Future<Item = (), Error = Error>>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, link: OpenLink<S>) -> Self::Future {
        let path = link
            .frame()
            .target
            .as_ref()
            .and_then(|target| target.address.as_ref().map(|addr| Path::new(addr.clone())));

        if let Some(mut path) = path {
            if let Some((hnd, _info)) = self.router.get_mut().recognize_mut(&mut path) {
                Either::B(Box::new(hnd.call(link)))
            } else {
                Either::A(err(LinkError::force_detach()
                    .description(Bytes::from_static(b"Target address is not supported"))
                    .into()))
            }
        } else {
            Either::A(err(LinkError::force_detach()
                .description(Bytes::from_static(b"Target address is required"))
                .into()))
        }
    }
}

type BoxedHandle<S> = Box<
    Service<
        OpenLink<S>,
        Response = (),
        Error = Error,
        Future = Box<Future<Item = (), Error = Error>>,
    >,
>;

struct Handle<T, S> {
    service: T,
    _t: PhantomData<(S,)>,
}

impl<T, S: 'static> Service<OpenLink<S>> for Handle<T, S>
where
    T: Service<OpenLink<S>, Response = ()>,
    T::Error: Into<Error>,
    T::Future: 'static,
{
    type Response = ();
    type Error = Error;
    type Future = Box<Future<Item = (), Error = Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, link: OpenLink<S>) -> Self::Future {
        Box::new(self.service.call(link).map_err(|e| e.into()))
    }
}
