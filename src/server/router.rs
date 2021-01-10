use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::{err, ok, Either, Ready};
use futures::Stream;
use ntex::service::{boxed, fn_factory_with_config, IntoServiceFactory, Service, ServiceFactory};
use ntex_amqp_codec::protocol::{
    DeliveryNumber, DeliveryState, Disposition, Error, Rejected, Role,
};
use ntex_router::{IntoPattern, Router as PatternRouter};

use crate::{cell::Cell, rcvlink::ReceiverLink, State};

use super::link::Link;
use super::transfer::{Outcome, Transfer};
use super::LinkError;

type Handle<S> = boxed::BoxServiceFactory<Link<S>, Transfer<S>, Outcome, Error, Error>;

pub struct Router<S = ()>(Vec<(Vec<String>, Handle<S>)>);

impl<S: 'static> Default for Router<S> {
    fn default() -> Router<S> {
        Router::new()
    }
}

impl<S: 'static> Router<S> {
    pub fn new() -> Router<S> {
        Router(Vec::new())
    }

    pub fn service<T, F, U: 'static>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoServiceFactory<U>,
        U: ServiceFactory<Config = Link<S>, Request = Transfer<S>, Response = Outcome>,
        U::Error: Into<Error>,
        U::InitError: Into<Error>,
    {
        self.0.push((
            address.patterns(),
            boxed::factory(
                service
                    .into_factory()
                    .map_init_err(|e| e.into())
                    .map_err(|e| e.into()),
            ),
        ));

        self
    }

    pub fn finish(
        self,
    ) -> impl ServiceFactory<
        Config = State<S>,
        Request = Link<S>,
        Response = (),
        Error = Error,
        InitError = Error,
    > {
        let mut router = PatternRouter::build();
        for (addr, hnd) in self.0 {
            router.path(addr, hnd);
        }
        let router = Cell::new(router.finish());

        fn_factory_with_config(move |_: State<S>| {
            ok(RouterService {
                router: router.clone(),
            })
        })
    }
}

struct RouterService<S> {
    router: Cell<PatternRouter<Handle<S>>>,
}

impl<S: 'static> Service for RouterService<S> {
    type Request = Link<S>;
    type Response = ();
    type Error = Error;
    type Future = Either<Ready<Result<(), Error>>, RouterServiceResponse<S>>;

    #[inline]
    fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&self, mut link: Link<S>) -> Self::Future {
        let path = link
            .frame()
            .target
            .as_ref()
            .and_then(|target| target.address.as_ref().cloned());

        if let Some(path) = path {
            link.path_mut().set(path);
            if let Some((hnd, _info)) = self.router.recognize(link.path_mut()) {
                trace!("Create handler service for {}", link.path().get_ref());
                let fut = hnd.new_service(link.clone());
                Either::Right(RouterServiceResponse {
                    link: link.link.clone(),
                    app_state: link.state.clone(),
                    state: RouterServiceResponseState::NewService(fut),
                })
            } else {
                trace!(
                    "Target address is not recognized: {}",
                    link.path().get_ref()
                );
                Either::Left(err(LinkError::force_detach()
                    .description(format!(
                        "Target address is not supported: {}",
                        link.path().get_ref()
                    ))
                    .into()))
            }
        } else {
            Either::Left(err(LinkError::force_detach()
                .description("Target address is required")
                .into()))
        }
    }
}

struct RouterServiceResponse<S> {
    link: ReceiverLink,
    app_state: State<S>,
    state: RouterServiceResponseState<S>,
}

enum RouterServiceResponseState<S> {
    Service(boxed::BoxService<Transfer<S>, Outcome, Error>),
    NewService(
        Pin<
            Box<dyn Future<Output = Result<boxed::BoxService<Transfer<S>, Outcome, Error>, Error>>>,
        >,
    ),
}

impl<S> Future for RouterServiceResponse<S> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut();
        let mut link = this.link.clone();
        let app_state = this.app_state.clone();

        loop {
            match this.state {
                RouterServiceResponseState::Service(ref mut srv) => {
                    // check readiness
                    match srv.poll_ready(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Pending => {
                            log::trace!(
                                "Handler service is not ready for {}",
                                this.link
                                    .frame()
                                    .target
                                    .as_ref()
                                    .map(|t| t.address.as_ref().map(|s| s.as_ref()).unwrap_or(""))
                                    .unwrap_or("")
                            );
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(e)) => {
                            log::trace!("Service readiness check failed: {:?}", e);
                            let _ = this.link.close_with_error(
                                LinkError::force_detach()
                                    .description(format!("error: {}", e))
                                    .into(),
                            );
                            return Poll::Ready(Ok(()));
                        }
                    }

                    match Pin::new(&mut link).poll_next(cx) {
                        Poll::Ready(Some(Ok(transfer))) => {
                            // #2.7.5 delivery_id MUST be set. batching is not supported atm
                            if transfer.delivery_id.is_none() {
                                let _ = this.link.close_with_error(
                                    LinkError::force_detach()
                                        .description("delivery_id MUST be set")
                                        .into(),
                                );
                                return Poll::Ready(Ok(()));
                            }
                            if link.credit() == 0 {
                                // self.has_credit = self.link.credit() != 0;
                                link.set_link_credit(50);
                            }

                            let delivery_id = transfer.delivery_id.unwrap();
                            let msg = Transfer::new(app_state.clone(), transfer, link.clone());

                            let mut fut = srv.call(msg);
                            match Pin::new(&mut fut).poll(cx) {
                                Poll::Ready(Ok(outcome)) => settle(
                                    &mut this.link,
                                    delivery_id,
                                    outcome.into_delivery_state(),
                                ),
                                Poll::Pending => {
                                    ntex::rt::spawn(HandleMessage {
                                        fut,
                                        delivery_id,
                                        link: this.link.clone(),
                                    });
                                }
                                Poll::Ready(Err(e)) => {
                                    log::trace!("Service response error: {:?}", e);
                                    settle(
                                        &mut this.link,
                                        delivery_id,
                                        DeliveryState::Rejected(Rejected { error: Some(e) }),
                                    )
                                }
                            }
                        }
                        Poll::Ready(None) => {
                            // TODO: shutdown service
                            log::trace!("Link is gone");
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(Err(e))) => {
                            log::trace!("Link is failed: {:?}", e);
                            let _ = this.link.close_with_error(LinkError::force_detach().into());
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
                RouterServiceResponseState::NewService(ref mut fut) => match Pin::new(fut).poll(cx)
                {
                    Poll::Ready(Ok(srv)) => {
                        log::trace!(
                            "Handler service is created for {}",
                            this.link
                                .frame()
                                .target
                                .as_ref()
                                .map(|t| t.address.as_ref().map(|s| s.as_ref()).unwrap_or(""))
                                .unwrap_or("")
                        );
                        this.link.open();
                        this.link.set_link_credit(50);
                        this.state = RouterServiceResponseState::Service(srv);
                        continue;
                    }
                    Poll::Ready(Err(e)) => {
                        log::error!(
                            "Failed to create link service for {} err: {:?}",
                            this.link
                                .frame()
                                .target
                                .as_ref()
                                .map(|t| t.address.as_ref().map(|s| s.as_ref()).unwrap_or(""))
                                .unwrap_or(""),
                            e
                        );
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

struct HandleMessage {
    link: ReceiverLink,
    delivery_id: DeliveryNumber,
    fut: Pin<Box<dyn Future<Output = Result<Outcome, Error>>>>,
}

impl Future for HandleMessage {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut();

        match Pin::new(&mut this.fut).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(outcome)) => {
                log::trace!(
                    "Outcome is ready {:?} for {}",
                    outcome,
                    this.link
                        .frame()
                        .target
                        .as_ref()
                        .map(|t| t.address.as_ref().map(|s| s.as_ref()).unwrap_or(""))
                        .unwrap_or("")
                );
                let delivery_id = this.delivery_id;
                settle(&mut this.link, delivery_id, outcome.into_delivery_state());
                Poll::Ready(())
            }
            Poll::Ready(Err(e)) => {
                log::trace!(
                    "Outcome is failed {:?} for {}",
                    e,
                    this.link
                        .frame()
                        .target
                        .as_ref()
                        .map(|t| t.address.as_ref().map(|s| s.as_ref()).unwrap_or(""))
                        .unwrap_or("")
                );

                let delivery_id = this.delivery_id;
                settle(
                    &mut this.link,
                    delivery_id,
                    DeliveryState::Rejected(Rejected { error: Some(e) }),
                );
                Poll::Ready(())
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
