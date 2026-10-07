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
    // handler could be removed while call is in progress
    if let Some(srv) = inner.handlers.get(&link).map(Pipeline::bind) {
        // check readiness
        if let Err(e) = srv.ready().await {
            log::trace!("Service readiness check failed: {e:?}");
            let _ =
                link.close_with_error(LinkError::force_detach().description(format!("error: {e}")));
            return Ok(());
        }

        if link.needs_credit() {
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

    use ntex::time::{Millis, sleep};
    use ntex_amqp_codec::protocol::{Frame, Role};

    use super::*;
    use crate::tests::connection::{
        ACCEPTED, begin, connection, detaches, handle_frame, named_attach, read_frames, session,
        transfer,
    };
    use crate::types::Action;

    fn router_service(
        router: Router<()>,
    ) -> (Cell<RouterServiceInner<()>>, Pipeline<Message, (), Error>) {
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
        (inner, srv)
    }

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

    /// Pauses link on first transfer
    struct PauseSrv(ReceiverLink, std::cell::Cell<bool>);

    impl Service<Link<()>, Transfer> for PauseSrv {
        type Res = Outcome;
        type Error = LinkError;

        async fn call(
            &self,
            _: Transfer,
            _: Ctx<'_, Self, Link<()>>,
        ) -> Result<Outcome, LinkError> {
            if !self.1.replace(true) {
                self.0.reset_link_credit(0);
            }
            Ok(Outcome::Accept)
        }
    }

    #[ntex::test]
    async fn paused_link_credit() {
        let (_io, conn, _client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let Ok(Action::AttachReceiver(link, frm, response)) =
            handle_frame(&conn, named_attach(Role::Sender, "p", "p", 0))
        else {
            panic!()
        };

        let router = Router::<()>::builder().service("p", async |link: &Link<()>| {
            Ok::<_, LinkError>(PauseSrv(
                link.receiver().clone(),
                std::cell::Cell::new(false),
            ))
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
        srv.call(Message::Attached(frm, link.clone()))
            .await
            .unwrap();
        assert!(link.confirm_receiver_link(response));
        link.set_link_credit(2);

        let transfer = async |id| {
            let Ok(Action::Transfer(link)) = handle_frame(&conn, transfer(id, false, None, 1))
            else {
                panic!()
            };
            srv.call(Message::Transfer(link)).await.unwrap();
        };

        // handler pauses link, in-flight transfer does not restore credit
        transfer(0).await;
        transfer(1).await;
        assert_eq!(link.credit(), 0);

        // added credit resumes router credit management
        link.set_link_credit(1);
        transfer(2).await;
        assert_eq!(link.credit(), 50);
    }

    /// Error that cannot be converted into an `Outcome`
    #[derive(Debug)]
    struct Fatal;

    impl From<Fatal> for Error {
        fn from(_: Fatal) -> Error {
            LinkError::force_detach().description("fatal").into()
        }
    }

    impl TryFrom<Fatal> for Outcome {
        type Error = Error;

        fn try_from(err: Fatal) -> Result<Self, Error> {
            Err(err.into())
        }
    }

    struct FatalSrv(bool);

    impl Service<Link<()>, Transfer> for FatalSrv {
        type Res = Outcome;
        type Error = Fatal;

        async fn ready(&self, _: Ctx<'_, Self, Link<()>>) -> Result<(), Fatal> {
            if self.0 { Err(Fatal) } else { Ok(()) }
        }

        async fn call(&self, _: Transfer, _: Ctx<'_, Self, Link<()>>) -> Result<Outcome, Fatal> {
            Err(Fatal)
        }
    }

    #[ntex::test]
    async fn unroutable_target_address() {
        let (_io, conn, _client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let (inner, srv) =
            router_service(Router::<()>::builder().service("ok", async |_: &Link<()>| {
                Ok::<_, LinkError>(Srv(Rc::default()))
            }));

        // (target address, expected error description)
        let cases = [
            (Some("nope"), "Target address is not supported: nope"),
            (None, "Target address is required"),
        ];
        for (handle, (address, expected)) in cases.into_iter().enumerate() {
            let handle = handle as u32;
            let Ok(Action::AttachReceiver(link, mut frm, _)) =
                handle_frame(&conn, named_attach(Role::Sender, "x", "nope", handle))
            else {
                panic!()
            };
            if address.is_none() {
                frm.0.target = None;
            }
            let err = srv.call(Message::Attached(frm, link)).await.err().unwrap();
            assert_eq!(
                err.description().map(ntex_bytes::ByteString::as_str),
                Some(expected)
            );
        }
        assert!(inner.get_ref().handlers.is_empty());
    }

    #[ntex::test]
    async fn detached_all_releases_handlers() {
        let (_io, conn, _client) = connection();
        handle_frame(&conn, begin()).unwrap();

        let shutdowns = Rc::new(AtomicUsize::new(0));
        let cnt = shutdowns.clone();
        let (inner, srv) = router_service(
            Router::<()>::builder().service("ok", async move |_: &Link<()>| {
                Ok::<_, LinkError>(Srv(cnt.clone()))
            }),
        );

        let mut links = Vec::new();
        for handle in 0..2 {
            let Ok(Action::AttachReceiver(link, frm, _)) =
                handle_frame(&conn, named_attach(Role::Sender, "ok", "ok", handle))
            else {
                panic!()
            };
            srv.call(Message::Attached(frm, link.clone()))
                .await
                .unwrap();
            links.push(link);
        }
        assert_eq!(inner.get_ref().handlers.len(), 2);

        // unknown links are ignored, known links are released and shut down
        let Ok(Action::AttachReceiver(unknown, ..)) =
            handle_frame(&conn, named_attach(Role::Sender, "other", "other", 2))
        else {
            panic!()
        };
        links.push(unknown);
        srv.call(Message::DetachedAll(links)).await.unwrap();
        assert!(inner.get_ref().handlers.is_empty());
        ntex::time::sleep(ntex::time::Millis(50)).await;
        assert_eq!(shutdowns.load(Ordering::Relaxed), 2);
    }

    #[ntex::test]
    async fn service_error_rejects_delivery() {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let Ok(Action::AttachReceiver(link, frm, response)) =
            handle_frame(&conn, named_attach(Role::Sender, "f", "f", 0))
        else {
            panic!()
        };
        let (_inner, srv) =
            router_service(Router::<()>::builder().service("f", async |_: &Link<()>| {
                Ok::<_, LinkError>(FatalSrv(false))
            }));
        srv.call(Message::Attached(frm, link.clone()))
            .await
            .unwrap();
        assert!(link.confirm_receiver_link(response));
        link.set_link_credit(2);

        // service error that cannot be mapped to an outcome rejects the delivery
        let Ok(Action::Transfer(link)) = handle_frame(&conn, transfer(0, false, None, 1)) else {
            panic!()
        };
        srv.call(Message::Transfer(link)).await.unwrap();
        ntex::time::sleep(ntex::time::Millis(25)).await;

        let frames = read_frames(&client);
        let Some(Frame::Disposition(disp)) = frames.last() else {
            panic!()
        };
        let Some(DeliveryState::Rejected(Rejected { error: Some(err) })) = disp.state() else {
            panic!()
        };
        assert_eq!(
            err.description().map(ntex_bytes::ByteString::as_str),
            Some("fatal")
        );
    }

    #[ntex::test]
    async fn readiness_failure_detaches_link() {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let Ok(Action::AttachReceiver(link, frm, response)) =
            handle_frame(&conn, named_attach(Role::Sender, "f", "f", 0))
        else {
            panic!()
        };
        let (_inner, srv) = router_service(
            Router::<()>::builder()
                .service("f", async |_: &Link<()>| Ok::<_, LinkError>(FatalSrv(true))),
        );
        srv.call(Message::Attached(frm, link.clone()))
            .await
            .unwrap();
        assert!(link.confirm_receiver_link(response));
        link.set_link_credit(2);

        let Ok(Action::Transfer(link)) = handle_frame(&conn, transfer(0, false, None, 1)) else {
            panic!()
        };
        srv.call(Message::Transfer(link)).await.unwrap();
        ntex::time::sleep(ntex::time::Millis(25)).await;

        // link is detached instead of the delivery being settled
        let detaches = detaches(&client);
        assert_eq!(detaches.len(), 1);
        let (_, closed, err) = &detaches[0];
        assert!(closed);
        assert_eq!(
            err.as_ref()
                .unwrap()
                .description()
                .map(ntex_bytes::ByteString::as_str),
            Some(
                "error: Error(ErrorInner { condition: LinkError(DetachForced), \
                 description: Some(\"fatal\"), info: None })"
            )
        );
    }

    #[ntex::test]
    async fn router_factory_dispatches_messages() {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();

        let shutdowns = Rc::new(AtomicUsize::new(0));
        let factory = {
            let shutdowns = shutdowns.clone();
            Router::<()>::default()
                .service("ok", move |_: &Link<()>| {
                    let shutdowns = shutdowns.clone();
                    async move { Ok::<_, LinkError>(Srv(shutdowns)) }
                })
                .build()
        };
        let state = State::new(());
        let srv = Pipeline::new(state.clone(), factory.create(&state).await.unwrap());

        let Ok(Action::AttachReceiver(link, frm, response)) =
            handle_frame(&conn, named_attach(Role::Sender, "ok", "ok", 0))
        else {
            panic!()
        };
        srv.call(Message::Attached(frm, link.clone()))
            .await
            .unwrap();
        assert!(link.confirm_receiver_link(response));
        link.set_link_credit(1);

        let Ok(Action::Transfer(link)) = handle_frame(&conn, transfer(0, false, None, 1)) else {
            panic!()
        };
        srv.call(Message::Transfer(link)).await.unwrap();
        sleep(Millis(25)).await;
        assert!(
            read_frames(&client)
                .iter()
                .any(|f| matches!(f, Frame::Disposition(d) if d.state() == Some(&ACCEPTED)))
        );

        // link service is dropped and shut down when the link detaches
        let link = session(&conn)
            .get_receiver_link_by_remote_handle(0)
            .cloned()
            .unwrap();
        srv.call(Message::Detached(link)).await.unwrap();
        sleep(Millis(50)).await;
        assert_eq!(shutdowns.load(Ordering::Relaxed), 1);
    }
}
