use std::{convert::TryFrom, future::poll_fn, marker, rc::Rc};

use ntex::router::{IntoPattern, Router as PatternRouter};
use ntex::service::{
    boxed, fn_factory_with_config, IntoServiceFactory, Pipeline, Service, ServiceCtx,
    ServiceFactory,
};
use ntex::util::{join_all, HashMap, Ready};

use crate::codec::protocol::{DeliveryState, Error, Rejected, Transfer};
use crate::error::LinkError;
use crate::types::{Link, Message, Outcome};
use crate::{cell::Cell, rcvlink::ReceiverLink, Delivery, State};

type Handle<S> = boxed::BoxServiceFactory<Link<S>, Transfer, Outcome, Error, Error>;
type HandleService = boxed::BoxService<Transfer, Outcome, Error>;

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

    pub fn service<T, F, U>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoServiceFactory<U, Transfer, Link<S>>,
        U: ServiceFactory<Transfer, Link<S>, Response = Outcome> + 'static,
        Error: From<U::Error> + From<U::InitError>,
        Outcome: TryFrom<U::Error, Error = Error>,
    {
        self.0.push((
            address.patterns(),
            ResourceServiceFactory::create(service.into_factory()),
        ));

        self
    }

    pub fn finish(
        self,
    ) -> impl ServiceFactory<
        Message,
        State<S>,
        Response = (),
        Error = Error,
        InitError = std::convert::Infallible,
    > {
        let mut router = PatternRouter::build();
        for (addr, hnd) in self.0 {
            router.path(addr, hnd);
        }
        let router = Rc::new(router.finish());

        fn_factory_with_config(move |state: State<S>| {
            Ready::Ok(RouterService(Cell::new(RouterServiceInner {
                state,
                router: router.clone(),
                handlers: HashMap::default(),
            })))
        })
    }
}

struct RouterService<S>(Cell<RouterServiceInner<S>>);

struct RouterServiceInner<S> {
    state: State<S>,
    router: Rc<PatternRouter<Handle<S>>>,
    handlers: HashMap<ReceiverLink, Option<Pipeline<HandleService>>>,
}

impl<S: 'static> Service<Message> for RouterService<S> {
    type Response = ();
    type Error = Error;

    async fn call(&self, msg: Message, _: ServiceCtx<'_, Self>) -> Result<(), Error> {
        match msg {
            Message::Attached(frm, link) => {
                let path = frm
                    .target()
                    .and_then(|target| target.address.as_ref().cloned());

                if let Some(path) = path {
                    let inner = self.0.get_mut();
                    let mut link = Link::new(frm, link, inner.state.clone(), path);
                    if let Some((hnd, _info)) = inner.router.recognize(link.path_mut()) {
                        log::trace!("Create handler service for {}", link.path().get_ref());
                        let rcv_link = link.link.clone();
                        inner.handlers.insert(link.receiver().clone(), None);

                        match hnd.create(link).await {
                            Ok(srv) => {
                                log::trace!("Handler service is created for {}", rcv_link.name());
                                self.0
                                    .get_mut()
                                    .handlers
                                    .insert(rcv_link.clone(), Some(Pipeline::new(srv)));
                                if let Some((delivery, tr)) = rcv_link.get_delivery() {
                                    service_call(rcv_link, delivery, tr, &self.0).await
                                } else {
                                    Ok(())
                                }
                            }
                            Err(e) => {
                                log::error!(
                                    "Failed to create link service for {} err: {:?}",
                                    rcv_link.name(),
                                    e
                                );
                                Err(e)
                            }
                        }
                    } else {
                        log::trace!(
                            "Target address is not recognized: {}",
                            link.path().get_ref()
                        );
                        Err(LinkError::force_detach()
                            .description(format!(
                                "Target address is not supported: {}",
                                link.path().get_ref()
                            ))
                            .into())
                    }
                } else {
                    Err(LinkError::force_detach()
                        .description("Target address is required")
                        .into())
                }
            }
            Message::Detached(link) => {
                if let Some(Some(srv)) = self.0.get_mut().handlers.remove(&link) {
                    log::trace!("Releasing handler service for {}", link.name());
                    let name = link.name().clone();
                    ntex::rt::spawn(async move {
                        poll_fn(move |cx| srv.poll_shutdown(cx)).await;
                        log::trace!("Handler service for {} has shutdown", name);
                    });
                }
                Ok(())
            }
            Message::DetachedAll(links) => {
                let futs: Vec<_> = links
                    .into_iter()
                    .filter_map(|link| {
                        self.0.get_mut().handlers.remove(&link).and_then(|srv| {
                            srv.map(|srv| {
                                log::trace!(
                                    "Releasing handler service for {} (session ended)",
                                    link.name()
                                );
                                poll_fn(move |cx| srv.poll_shutdown(cx))
                            })
                        })
                    })
                    .collect();

                log::trace!(
                    "Shutting down {} handler services (session ended)",
                    futs.len()
                );

                ntex::rt::spawn(async move {
                    let len = futs.len();
                    let _ = join_all(futs).await;
                    log::trace!(
                        "Handler services for {} links have shutdown (session ended)",
                        len
                    );
                });
                Ok(())
            }
            Message::Transfer(link) => {
                if let Some(Some(_)) = self.0.get_ref().handlers.get(&link) {
                    if let Some((delivery, tr)) = link.get_delivery() {
                        service_call(link, delivery, tr, &self.0).await?;
                    }
                }
                Ok(())
            }
        }
    }
}

async fn service_call<S>(
    link: ReceiverLink,
    mut delivery: Delivery,
    tr: Transfer,
    inner: &Cell<RouterServiceInner<S>>,
) -> Result<(), Error> {
    if let Some(Some(srv)) = inner.handlers.get(&link) {
        // check readiness
        if let Err(e) = srv.ready().await {
            log::trace!("Service readiness check failed: {:?}", e);
            let _ = link
                .close_with_error(LinkError::force_detach().description(format!("error: {}", e)));
            return Ok(());
        }

        if link.credit() == 0 {
            // self.has_credit = self.link.credit() != 0;
            link.set_link_credit(50);
        }

        match srv.call(tr).await {
            Ok(outcome) => {
                log::trace!("Outcome is ready {:?} for {}", outcome, link.name());
                delivery.settle(outcome.into_delivery_state());
            }
            Err(e) => {
                log::trace!("Service response error: {:?}", e);
                delivery.settle(DeliveryState::Rejected(Rejected { error: Some(e) }));
            }
        }
    }
    Ok(())
}

struct ResourceServiceFactory<S, T> {
    factory: T,
    _t: marker::PhantomData<S>,
}

impl<S, T> ResourceServiceFactory<S, T>
where
    S: 'static,
    T: ServiceFactory<Transfer, Link<S>, Response = Outcome> + 'static,
    Error: From<T::Error> + From<T::InitError>,
    Outcome: TryFrom<T::Error, Error = Error>,
{
    fn create(factory: T) -> Handle<S> {
        boxed::factory(ResourceServiceFactory {
            factory,
            _t: marker::PhantomData,
        })
    }
}

impl<S, T> ServiceFactory<Transfer, Link<S>> for ResourceServiceFactory<S, T>
where
    T: ServiceFactory<Transfer, Link<S>, Response = Outcome>,
    Error: From<T::Error> + From<T::InitError>,
    Outcome: TryFrom<T::Error, Error = Error>,
{
    type Response = Outcome;
    type Error = Error;
    type InitError = Error;
    type Service = ResourceService<S, T::Service>;

    async fn create(&self, cfg: Link<S>) -> Result<Self::Service, Self::InitError> {
        let service = self.factory.create(cfg).await?;

        Ok(ResourceService {
            service,
            _t: marker::PhantomData,
        })
    }
}

struct ResourceService<S, T> {
    service: T,
    _t: marker::PhantomData<S>,
}

impl<S, T> Service<Transfer> for ResourceService<S, T>
where
    T: Service<Transfer, Response = Outcome>,
    Error: From<T::Error>,
    Outcome: TryFrom<T::Error, Error = Error>,
{
    type Response = Outcome;
    type Error = Error;

    ntex::forward_poll_ready!(service);
    ntex::forward_poll_shutdown!(service);

    async fn call(
        &self,
        req: Transfer,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        match ctx.call(&self.service, req).await {
            Ok(v) => Ok(v),
            Err(err) => Outcome::try_from(err),
        }
    }
}
