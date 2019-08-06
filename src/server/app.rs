use actix_router::Router;
use actix_service::{boxed, new_service_fn, IntoNewService, NewService, Service};
use amqp_codec::protocol::{DeliveryNumber, DeliveryState, Disposition, Error, Rejected, Role};
use futures::future::{err, ok, Either, FutureResult};
use futures::{Async, Future, Poll, Stream};

use crate::cell::Cell;
use crate::rcvlink::ReceiverLink;

use super::errors::LinkError;
use super::link::Link;
use super::message::{Message, Outcome};

type Handle<S> = boxed::BoxedNewService<Link<S>, Message<S>, Outcome, Error, Error>;

pub struct App<S = ()>(Vec<(String, Handle<S>)>);

impl<S: 'static> App<S> {
    pub fn new() -> App<S> {
        App(Vec::new())
    }

    pub fn service<F, U: 'static>(mut self, address: &str, service: F) -> Self
    where
        F: IntoNewService<U>,
        U: NewService<Config = Link<S>, Request = Message<S>, Response = Outcome>,
        U::Error: Into<Error>,
        U::InitError: Into<Error>,
    {
        self.0.push((
            address.to_string(),
            boxed::new_service(
                service
                    .into_new_service()
                    .map_init_err(|e| e.into())
                    .map_err(|e| e.into()),
            ),
        ));

        self
    }

    pub fn finish(
        self,
    ) -> impl NewService<Config = (), Request = Link<S>, Response = (), Error = Error, InitError = Error>
    {
        let mut router = Router::build();
        for (addr, hnd) in self.0 {
            router.path(&addr, hnd);
        }
        let router = Cell::new(router.finish());

        new_service_fn(move || {
            ok(AppService {
                router: router.clone(),
            })
        })
    }
}

struct AppService<S> {
    router: Cell<Router<Handle<S>>>,
}

impl<S: 'static> Service for AppService<S> {
    type Request = Link<S>;
    type Response = ();
    type Error = Error;
    type Future = Either<FutureResult<(), Error>, AppServiceResponse<S>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, mut link: Link<S>) -> Self::Future {
        let path = link
            .frame()
            .target
            .as_ref()
            .and_then(|target| target.address.as_ref().map(|addr| addr.clone()));

        if let Some(path) = path {
            link.path_mut().set(path);
            if let Some((hnd, _info)) = self.router.recognize(link.path_mut()) {
                let fut = hnd.new_service(&link);
                Either::B(AppServiceResponse {
                    link: link.link.clone(),
                    app_state: link.state.clone(),
                    state: AppServiceResponseState::NewService(fut),
                    // has_credit: true,
                })
            } else {
                Either::A(err(LinkError::force_detach()
                    .description(format!(
                        "Target address is not supported: {}",
                        link.path().get_ref()
                    ))
                    .into()))
            }
        } else {
            Either::A(err(LinkError::force_detach()
                .description("Target address is required")
                .into()))
        }
    }
}

struct AppServiceResponse<S> {
    link: ReceiverLink,
    app_state: Cell<S>,
    state: AppServiceResponseState<S>,
    // has_credit: bool,
}

enum AppServiceResponseState<S> {
    Service(boxed::BoxedService<Message<S>, Outcome, Error>),
    NewService(Box<Future<Item = boxed::BoxedService<Message<S>, Outcome, Error>, Error = Error>>),
}

impl<S> Future for AppServiceResponse<S> {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.state {
                AppServiceResponseState::Service(ref mut srv) => {
                    // check readiness
                    match srv.poll_ready() {
                        Ok(Async::Ready(_)) => (),
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(e) => {
                            self.link.close_with_error(
                                LinkError::force_detach()
                                    .description(format!("error: {}", e))
                                    .into(),
                            );
                            return Ok(Async::Ready(()));
                        }
                    }

                    match self.link.poll() {
                        Ok(Async::Ready(Some(transfer))) => {
                            // #2.7.5 delivery_id MUST be set. batching is not supported atm
                            if transfer.delivery_id.is_none() {
                                self.link.close_with_error(
                                    LinkError::force_detach()
                                        .description("delivery_id MUST be set")
                                        .into(),
                                );
                                return Ok(Async::Ready(()));
                            }
                            if self.link.credit() == 0 {
                                // self.has_credit = self.link.credit() != 0;
                                self.link.set_link_credit(50);
                            }

                            let delivery_id = transfer.delivery_id.unwrap();
                            let msg =
                                Message::new(self.app_state.clone(), transfer, self.link.clone());

                            let mut fut = srv.call(msg);
                            match fut.poll() {
                                Ok(Async::Ready(outcome)) => settle(
                                    &mut self.link,
                                    delivery_id,
                                    outcome.into_delivery_state(),
                                ),
                                Ok(Async::NotReady) => {
                                    tokio_current_thread::spawn(HandleMessage {
                                        fut,
                                        delivery_id,
                                        link: self.link.clone(),
                                    });
                                }
                                Err(e) => settle(
                                    &mut self.link,
                                    delivery_id,
                                    DeliveryState::Rejected(Rejected { error: Some(e) }),
                                ),
                            }
                        }
                        Ok(Async::Ready(None)) => return Ok(Async::Ready(())),
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(_e) => {
                            self.link.close_with_error(LinkError::force_detach().into());
                            return Ok(Async::Ready(()));
                        }
                    }
                }
                AppServiceResponseState::NewService(ref mut fut) => match fut.poll() {
                    Ok(Async::Ready(srv)) => {
                        self.link.open();
                        self.link.set_link_credit(50);
                        self.state = AppServiceResponseState::Service(srv);
                        continue;
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(e) => return Err(e),
                },
            }
        }
    }
}

struct HandleMessage {
    link: ReceiverLink,
    delivery_id: DeliveryNumber,
    fut: Either<FutureResult<Outcome, Error>, Box<Future<Item = Outcome, Error = Error>>>,
}

impl Future for HandleMessage {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        match self.fut.poll() {
            Ok(Async::Ready(outcome)) => {
                settle(
                    &mut self.link,
                    self.delivery_id,
                    outcome.into_delivery_state(),
                );
                Ok(Async::Ready(()))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                settle(
                    &mut self.link,
                    self.delivery_id,
                    DeliveryState::Rejected(Rejected { error: Some(e) }),
                );
                Ok(Async::Ready(()))
            }
        }
    }
}

fn settle(link: &mut ReceiverLink, id: DeliveryNumber, state: DeliveryState) {
    let disposition = Disposition {
        state: Some(state),
        role: Role::Receiver,
        first: id,
        last: None,
        settled: true,
        batchable: false,
    };
    link.send_disposition(disposition);
}
