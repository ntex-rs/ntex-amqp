use std::fmt;
use std::marker::PhantomData;

use actix_router::{Path, Router};
use actix_service::{IntoNewService, NewService, Service, ServiceExt};
use amqp_codec::protocol::Error;
use futures::future::{err, join_all, ok, Either, FutureResult};
use futures::{Async, Future, Poll};

use crate::cell::Cell;

use super::errors::LinkError;
use super::link::OpenLink;

pub struct App<S = ()>(Vec<(String, BoxedNewHandle<S>)>);

impl<S: 'static> App<S> {
    pub fn new() -> App<S> {
        App(Vec::new())
    }

    pub fn service<F, U: 'static>(mut self, address: &str, service: F) -> Self
    where
        F: IntoNewService<U, OpenLink<S>>,
        U: NewService<OpenLink<S>, Response = ()>,
        U::Error: Into<Error>,
        U::InitError: Into<Error> + fmt::Display,
    {
        let hnd = NewHandle {
            service: service.into_new_service(),
            _t: PhantomData,
        };
        self.0.push((address.to_string(), Box::new(hnd)));

        self
    }

    pub fn finish(
        self,
    ) -> impl NewService<OpenLink<S>, Response = (), Error = Error, InitError = Error> {
        let routes = Cell::new(self.0);

        move || {
            let mut fut = Vec::new();
            for (addr, hnd) in routes.clone().get_mut().iter_mut() {
                let addr = addr.clone();
                fut.push(hnd.new_service().map(move |srv| (addr, srv)))
            }

            join_all(fut).and_then(move |services| {
                let mut router = Router::build();
                for (addr, hnd) in services {
                    router.path(&addr, hnd);
                }
                ok(AppService {
                    router: router.finish(),
                })
            })
        }
    }
}

struct AppService<S> {
    router: Router<BoxedHandle<S>>,
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
            if let Some((hnd, _info)) = self.router.recognize_mut(&mut path) {
                Either::B(Box::new(hnd.call(link)))
            } else {
                Either::A(err(LinkError::force_detach()
                    .description("Target address is not supported")
                    .into()))
            }
        } else {
            Either::A(err(LinkError::force_detach()
                .description("Target address is required")
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
    T: Service<OpenLink<S>, Response = (), Error = Error>,
    T::Future: 'static,
{
    type Response = ();
    type Error = Error;
    type Future = Box<Future<Item = (), Error = Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, link: OpenLink<S>) -> Self::Future {
        Box::new(self.service.call(link))
    }
}

type BoxedNewHandle<S> = Box<
    NewService<
        OpenLink<S>,
        Response = (),
        Error = Error,
        InitError = Error,
        Service = BoxedHandle<S>,
        Future = Box<Future<Item = BoxedHandle<S>, Error = Error>>,
    >,
>;

struct NewHandle<T, S> {
    service: T,
    _t: PhantomData<(S,)>,
}

impl<T, S: 'static> NewService<OpenLink<S>> for NewHandle<T, S>
where
    T: NewService<OpenLink<S>, Response = ()>,
    T::Service: 'static,
    T::Error: Into<Error>,
    T::InitError: Into<Error> + fmt::Display,
    T::Future: 'static,
{
    type Response = ();
    type Error = Error;
    type InitError = Error;
    type Service = BoxedHandle<S>;
    type Future = Box<Future<Item = Self::Service, Error = Self::InitError>>;

    fn new_service(&self) -> Self::Future {
        Box::new(
            self.service
                .new_service()
                .map_err(|e| {
                    error!("Can not create new service: {}", e);
                    e.into()
                })
                .map(|srv| {
                    let srv: BoxedHandle<S> = Box::new(Handle {
                        service: srv.map_err(|e| e.into()),
                        _t: PhantomData,
                    });
                    srv
                }),
        )
    }
}
