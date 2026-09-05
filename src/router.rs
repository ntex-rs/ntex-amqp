use std::{marker, rc::Rc};

use ntex_router::{IntoPattern, Router as PatternRouter};
use ntex_service::{Ctx, IntoServiceFactory, Pipeline, Service, ServiceFactory, boxed, factory};
use ntex_util::{HashMap, future::join_all};

use crate::codec::protocol::{DeliveryState, Error, Rejected, Transfer};
use crate::types::{Link, Message, Outcome};
use crate::{Delivery, State, cell::Cell, error::LinkError, rcvlink::ReceiverLink};

type Handle<S> = boxed::BoxServiceFactory<Link<S>, Transfer, Outcome, Error, Error>;

pub struct Router<S = ()>(Vec<(Vec<String>, Handle<S>)>);

impl<S: 'static> Default for Router<S> {
    fn default() -> Router<S> {
        Router::builder()
    }
}

impl<S: 'static> Router<S> {
    pub fn builder() -> Router<S> {
        Router(Vec::new())
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn service<T, F, U>(mut self, address: T, f: F) -> Self
    where
        T: IntoPattern,
        F: IntoServiceFactory<U, Link<S>, Transfer>,
        U: ServiceFactory<Link<S>, Transfer, Res = Outcome> + 'static,
        Error: From<U::Error> + From<U::InitError>,
        Outcome: TryFrom<U::Error, Error = Error>,
    {
        self.0.push((
            address.patterns(),
            ResourceServiceFactory::create(f.into_factory()),
        ));

        self
    }

    pub fn build(
        self,
    ) -> impl ServiceFactory<State<S>, Message, Res = (), Error = Error, InitError = std::convert::Infallible>
    {
        let mut router = PatternRouter::build();
        for (addr, hnd) in self.0 {
            router.path(addr, hnd);
        }
        let router = Rc::new(router.finish());

        factory(async move |state: &State<S>| {
            Ok(RouterService(Cell::new(RouterServiceInner {
                state: state.clone(),
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
    handlers: HashMap<ReceiverLink, Option<Pipeline<Transfer, Outcome, Error>>>,
}

impl<S: 'static> Service<State<S>, Message> for RouterService<S> {
    type Res = ();
    type Error = Error;

    async fn call(&self, msg: Message, _: Ctx<'_, Self, State<S>>) -> Result<(), Error> {
        match msg {
            Message::Attached(frm, link) => {
                let path = frm.target().and_then(|target| target.address.clone());

                if let Some(path) = path {
                    let inner = self.0.get_mut();
                    let mut link = Link::new(frm, link, inner.state.clone(), path);
                    if let Some((hnd, _info)) = inner.router.recognize(link.path_mut()) {
                        log::trace!("Create handler service for {}", link.path().get_ref());
                        let rcv_link = link.link.clone();
                        inner.handlers.insert(link.receiver().clone(), None);

                        match hnd.create(&link).await {
                            Ok(srv) => {
                                log::trace!("Handler service is created for {}", rcv_link.name());
                                self.0
                                    .get_mut()
                                    .handlers
                                    .insert(rcv_link.clone(), Some(Pipeline::new(link.clone(), srv)));
                                if let Some((delivery, tr)) = rcv_link.get_delivery() {
                                    service_call(rcv_link, delivery, tr, &self.0).await
                                } else {
                                    Ok(())
                                }
                            }
                            Err(e) => {
                                log::error!(
                                    "Failed to create link service for {} err: {e:?}",
                                    rcv_link.name()
                                );
                                Err(e)
                            }
                        }
                    } else {
                        log::trace!("Target address is not recognized: {}", link.path().get_ref());
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
                    ntex_rt::spawn(async move {
                        srv.shutdown().await;
                        log::trace!("Handler service for {name} has shutdown");
                    });
                }
                Ok(())
            }
            Message::DetachedAll(links) => {
                let links: Vec<_> = links
                    .into_iter()
                    .filter_map(|link| {
                        self.0
                            .get_mut()
                            .handlers
                            .remove(&link)
                            .and_then(move |srv| srv.map(|srv| (link, srv)))
                    })
                    .collect();

                log::trace!("Shutting down {} handler services (session ended)", links.len());

                ntex_rt::spawn(async move {
                    let futs: Vec<_> = links
                        .iter()
                        .map(|(link, srv)| {
                            log::trace!("Releasing handler service for {} (session ended)", link.name());
                            srv.shutdown()
                        })
                        .collect();

                    let len = futs.len();
                    let _ = join_all(futs).await;
                    log::trace!("Handler services for {len} links have shutdown (session ended)");
                });
                Ok(())
            }
            Message::Transfer(link) => {
                if let Some(Some(_)) = self.0.get_ref().handlers.get(&link)
                    && let Some((delivery, tr)) = link.get_delivery()
                {
                    service_call(link, delivery, tr, &self.0).await?;
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
            log::trace!("Service readiness check failed: {e:?}");
            let _ = link.close_with_error(LinkError::force_detach().description(format!("error: {e}")));
            return Ok(());
        }

        if link.credit() == 0 {
            // self.has_credit = self.link.credit() != 0;
            link.set_link_credit(50);
        }

        match srv.call(tr).await {
            Ok(outcome) => {
                log::trace!("Outcome is ready {outcome:?} for {}", link.name());
                delivery.settle(outcome.into_delivery_state());
            }
            Err(e) => {
                log::trace!("Service response error: {e:?}");
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
    T: ServiceFactory<Link<S>, Transfer, Res = Outcome> + 'static,
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

impl<S, T> ServiceFactory<Link<S>, Transfer> for ResourceServiceFactory<S, T>
where
    T: ServiceFactory<Link<S>, Transfer, Res = Outcome>,
    Error: From<T::Error> + From<T::InitError>,
    Outcome: TryFrom<T::Error, Error = Error>,
{
    type Res = Outcome;
    type Error = Error;

    type Service = ResourceService<S, T::Service>;
    type InitError = Error;

    async fn create(&self, cfg: &Link<S>) -> Result<Self::Service, Self::InitError> {
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

impl<S, T> Service<Link<S>, Transfer> for ResourceService<S, T>
where
    T: Service<Link<S>, Transfer, Res = Outcome>,
    Error: From<T::Error>,
    Outcome: TryFrom<T::Error, Error = Error>,
{
    type Res = Outcome;
    type Error = Error;

    ntex_service::forward_ready!(Link<S>, service);
    ntex_service::forward_shutdown!(Link<S>, service);

    async fn call(&self, req: Transfer, ctx: Ctx<'_, Self, Link<S>>) -> Result<Self::Res, Self::Error> {
        match ctx.call(&self.service, req).await {
            Ok(v) => Ok(v),
            Err(err) => Outcome::try_from(err),
        }
    }
}
