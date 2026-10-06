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
    ) -> impl ServiceFactory<
        State<S>,
        Message,
        Res = (),
        Error = Error,
        InitError = std::convert::Infallible,
    > {
        let mut router = PatternRouter::builder();
        for (addr, hnd) in self.0 {
            router.path(addr, hnd);
        }
        let router = Rc::new(router.build());

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
    handlers: HashMap<ReceiverLink, Pipeline<Transfer, Outcome, Error>>,
}

impl<S: 'static> Service<State<S>, Message> for RouterService<S> {
    type Res = ();
    type Error = Error;

    async fn shutdown(&self, _: Ctx<'_, Self, State<S>>) {
        let handlers: Vec<_> = self
            .0
            .get_mut()
            .handlers
            .drain()
            .map(|(_, srv)| srv)
            .collect();
        log::trace!("Shutting down {} handler services", handlers.len());
        let _ = join_all(handlers.iter().map(Pipeline::shutdown).collect::<Vec<_>>()).await;
    }

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

                        match hnd.create(&link).await {
                            Ok(srv) => {
                                log::trace!("Handler service is created for {}", rcv_link.name());
                                self.0
                                    .get_mut()
                                    .handlers
                                    .insert(rcv_link.clone(), Pipeline::new(link.clone(), srv));
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
                if let Some(srv) = self.0.get_mut().handlers.remove(&link) {
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
                            .map(move |srv| (link, srv))
                    })
                    .collect();

                log::trace!(
                    "Shutting down {} handler services (session ended)",
                    links.len()
                );

                ntex_rt::spawn(async move {
                    let futs: Vec<_> = links
                        .iter()
                        .map(|(link, srv)| {
                            log::trace!(
                                "Releasing handler service for {} (session ended)",
                                link.name()
                            );
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
                if self.0.get_ref().handlers.contains_key(&link)
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
    if let Some(srv) = inner.handlers.get(&link) {
        // check readiness
        if let Err(e) = srv.ready().await {
            log::trace!("Service readiness check failed: {e:?}");
            let _ =
                link.close_with_error(LinkError::force_detach().description(format!("error: {e}")));
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

    async fn call(
        &self,
        req: Transfer,
        ctx: Ctx<'_, Self, Link<S>>,
    ) -> Result<Self::Res, Self::Error> {
        match ctx.call(&self.service, req).await {
            Ok(v) => Ok(v),
            Err(err) => Outcome::try_from(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ntex_amqp_codec::protocol::Role;

    use super::*;
    use crate::connection::tests::{begin, connection, handle_frame, named_attach};
    use crate::types::Action;

    struct Srv(Rc<AtomicUsize>);

    impl Service<Link<()>, Transfer> for Srv {
        type Res = Outcome;
        type Error = LinkError;

        async fn call(
            &self,
            _: Transfer,
            _: Ctx<'_, Self, Link<()>>,
        ) -> Result<Outcome, LinkError> {
            Ok(Outcome::Accept)
        }

        async fn shutdown(&self, _: Ctx<'_, Self, Link<()>>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[ntex::test]
    async fn link_service_lifecycle() {
        let (_io, conn, _client) = connection();
        let attach = |name: &str, handle: u32| {
            let Ok(Action::AttachReceiver(link, frm, _)) =
                handle_frame(&conn, named_attach(Role::Sender, name, name, handle))
            else {
                panic!()
            };
            Message::Attached(frm, link)
        };
        handle_frame(&conn, begin()).unwrap();

        let shutdowns = Rc::new(AtomicUsize::new(0));
        let cnt = shutdowns.clone();
        let router = Router::<()>::builder()
            .service("fail", async |_: &Link<()>| {
                Err::<Srv, _>(LinkError::force_detach())
            })
            .service("ok", async move |_: &Link<()>| {
                Ok::<_, LinkError>(Srv(cnt.clone()))
            });
        let mut patterns = PatternRouter::builder();
        for (addr, hnd) in router.0 {
            patterns.path(addr, hnd);
        }
        let inner = Cell::new(RouterServiceInner {
            state: State::new(()),
            router: Rc::new(patterns.build()),
            handlers: HashMap::default(),
        });
        let srv = Pipeline::new(State::new(()), RouterService(inner.clone()));

        // failed link service creation does not keep link
        for handle in 0..3 {
            assert!(srv.call(attach("fail", handle)).await.is_err());
        }
        assert!(inner.get_ref().handlers.is_empty());

        // detached link service is released
        let Message::Attached(frm, link) = attach("ok", 3) else {
            panic!()
        };
        srv.call(Message::Attached(frm, link.clone()))
            .await
            .unwrap();
        assert_eq!(inner.get_ref().handlers.len(), 1);
        srv.call(Message::Detached(link)).await.unwrap();
        assert!(inner.get_ref().handlers.is_empty());

        // link services are shutdown with router
        srv.call(attach("ok", 4)).await.unwrap();
        srv.call(attach("ok", 5)).await.unwrap();
        assert_eq!(inner.get_ref().handlers.len(), 2);
        ntex::time::sleep(ntex::time::Millis(50)).await;
        assert_eq!(shutdowns.load(Ordering::Relaxed), 1);
        srv.shutdown().await;
        assert!(inner.get_ref().handlers.is_empty());
        assert_eq!(shutdowns.load(Ordering::Relaxed), 3);
    }
}
