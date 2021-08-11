use std::{convert::TryFrom, future::Future, marker, pin::Pin, rc::Rc, task::Context, task::Poll};

use ntex::router::{IntoPattern, Router as PatternRouter};
use ntex::service::{boxed, fn_factory_with_config, IntoServiceFactory, Service, ServiceFactory};
use ntex::util::{Either, HashMap, Ready};

use crate::codec::protocol::{
    DeliveryNumber, DeliveryState, Disposition, Error, Rejected, Role, Transfer,
};
use crate::error::LinkError;
use crate::types::{Link, Message, Outcome};
use crate::{cell::Cell, rcvlink::ReceiverLink, State};

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

    pub fn service<T, F, U: 'static>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoServiceFactory<U>,
        U: ServiceFactory<Config = Link<S>, Request = Transfer, Response = Outcome>,
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
        Config = State<S>,
        Request = Message,
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
    handlers: HashMap<ReceiverLink, Option<HandleService>>,
}

impl<S: 'static> Service for RouterService<S> {
    type Request = Message;
    type Response = ();
    type Error = Error;
    type Future = Either<Ready<(), Error>, RouterServiceResponse<S>>;

    #[inline]
    fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&self, msg: Message) -> Self::Future {
        match msg {
            Message::Attached(link) => {
                let path = link
                    .frame()
                    .target
                    .as_ref()
                    .and_then(|target| target.address.as_ref().cloned());

                if let Some(path) = path {
                    let inner = self.0.get_mut();
                    let mut link = Link::new(link, inner.state.clone(), path);
                    if let Some((hnd, _info)) = inner.router.recognize(link.path_mut()) {
                        trace!("Create handler service for {}", link.path().get_ref());
                        let fut = hnd.new_service(link.clone());
                        inner.handlers.insert(link.receiver().clone(), None);
                        Either::Right(RouterServiceResponse {
                            inner: self.0.clone(),
                            link: link.link.clone(),
                            state: RouterServiceResponseState::NewService(fut),
                        })
                    } else {
                        trace!(
                            "Target address is not recognized: {}",
                            link.path().get_ref()
                        );
                        Either::Left(Ready::Err(
                            LinkError::force_detach()
                                .description(format!(
                                    "Target address is not supported: {}",
                                    link.path().get_ref()
                                ))
                                .into(),
                        ))
                    }
                } else {
                    Either::Left(Ready::Err(
                        LinkError::force_detach()
                            .description("Target address is required")
                            .into(),
                    ))
                }
            }
            Message::Transfer(link) => {
                if let Some(Some(_)) = self.0.get_ref().handlers.get(&link) {
                    if let Some(tr) = link.get_transfer() {
                        return Either::Right(RouterServiceResponse {
                            inner: self.0.clone(),
                            link,
                            state: RouterServiceResponseState::Service(Some(tr)),
                        });
                    }
                }
                Either::Left(Ready::Ok(()))
            }
        }
    }
}

struct RouterServiceResponse<S> {
    inner: Cell<RouterServiceInner<S>>,
    link: ReceiverLink,
    state: RouterServiceResponseState,
}

enum RouterServiceResponseState {
    Service(Option<Transfer>),
    Transfer(Pin<Box<dyn Future<Output = Result<Outcome, Error>>>>, u32),
    NewService(
        Pin<Box<dyn Future<Output = Result<boxed::BoxService<Transfer, Outcome, Error>, Error>>>>,
    ),
}

impl<S> Future for RouterServiceResponse<S> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut();
        let link = this.link.clone();
        let inner = this.inner.clone();

        loop {
            match this.state {
                RouterServiceResponseState::Service(ref mut tr) => {
                    if let Some(Some(srv)) = inner.handlers.get(&link) {
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
                                        .map(|t| t
                                            .address
                                            .as_ref()
                                            .map(|s| s.as_ref())
                                            .unwrap_or(""))
                                        .unwrap_or("")
                                );
                                return Poll::Pending;
                            }
                            Poll::Ready(Err(e)) => {
                                log::trace!("Service readiness check failed: {:?}", e);
                                let _ = this.link.close_with_error(
                                    LinkError::force_detach().description(format!("error: {}", e)),
                                );
                                return Poll::Ready(Ok(()));
                            }
                        }

                        let tr = tr.take().unwrap();
                        let delivery_id = match tr.delivery_id {
                            None => {
                                // #2.7.5 delivery_id MUST be set. batching is handled on lower level
                                let _ = this.link.close_with_error(
                                    LinkError::force_detach()
                                        .description("delivery_id MUST be set"),
                                );
                                return Poll::Ready(Ok(()));
                            }
                            Some(delivery_id) => {
                                if this.link.credit() == 0 {
                                    // self.has_credit = self.link.credit() != 0;
                                    this.link.set_link_credit(50);
                                }
                                delivery_id
                            }
                        };

                        this.state =
                            RouterServiceResponseState::Transfer(srv.call(tr), delivery_id);
                    } else {
                        return Poll::Ready(Ok(()));
                    }
                }
                RouterServiceResponseState::Transfer(ref mut fut, delivery_id) => {
                    match Pin::new(fut).poll(cx) {
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
                            settle(&mut this.link, delivery_id, outcome.into_delivery_state());
                        }
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(e)) => {
                            log::trace!("Service response error: {:?}", e);
                            settle(
                                &mut this.link,
                                delivery_id,
                                DeliveryState::Rejected(Rejected { error: Some(e) }),
                            );
                        }
                    }
                    if let Some(tr) = this.link.get_transfer() {
                        this.state = RouterServiceResponseState::Service(Some(tr));
                    } else {
                        return Poll::Ready(Ok(()));
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
                        this.inner
                            .get_mut()
                            .handlers
                            .insert(this.link.clone(), Some(srv));
                        if let Some(tr) = this.link.get_transfer() {
                            this.state = RouterServiceResponseState::Service(Some(tr));
                        } else {
                            return Poll::Ready(Ok(()));
                        }
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

struct ResourceServiceFactory<S, T> {
    factory: T,
    _t: marker::PhantomData<S>,
}

impl<S, T> ResourceServiceFactory<S, T>
where
    S: 'static,
    T: ServiceFactory<Config = Link<S>, Request = Transfer, Response = Outcome> + 'static,
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

impl<S, T> ServiceFactory for ResourceServiceFactory<S, T>
where
    T: ServiceFactory<Config = Link<S>, Request = Transfer, Response = Outcome>,
    Error: From<T::Error> + From<T::InitError>,
    Outcome: TryFrom<T::Error, Error = Error>,
{
    type Config = Link<S>;
    type Request = Transfer;
    type Response = Outcome;
    type Error = Error;
    type InitError = Error;
    type Service = ResourceService<S, T::Service>;
    type Future = ResourceServiceFactoryFut<S, T>;

    fn new_service(&self, cfg: Link<S>) -> Self::Future {
        ResourceServiceFactoryFut {
            fut: self.factory.new_service(cfg),
            _t: marker::PhantomData,
        }
    }
}

pin_project_lite::pin_project! {
    struct ResourceServiceFactoryFut<S, T: ServiceFactory> {
        #[pin] fut: T::Future,
        _t: marker::PhantomData<S>,
    }
}

impl<S, T> Future for ResourceServiceFactoryFut<S, T>
where
    T: ServiceFactory<Config = Link<S>, Request = Transfer, Response = Outcome>,
    Error: From<T::Error> + From<T::InitError>,
    Outcome: TryFrom<T::Error, Error = Error>,
{
    type Output = Result<ResourceService<S, T::Service>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let service = match this.fut.poll(cx).map_err(Error::from)? {
            Poll::Ready(service) => service,
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(Ok(ResourceService {
            service,
            _t: marker::PhantomData,
        }))
    }
}

struct ResourceService<S, T> {
    service: T,
    _t: marker::PhantomData<S>,
}

impl<S, T> Service for ResourceService<S, T>
where
    T: Service<Request = Transfer, Response = Outcome>,
    Error: From<T::Error>,
    Outcome: TryFrom<T::Error, Error = Error>,
{
    type Request = Transfer;
    type Response = Outcome;
    type Error = Error;
    type Future = ResourceServiceFut<S, T>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(Error::from)
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        self.service.poll_shutdown(cx, is_error)
    }

    #[inline]
    fn call(&self, req: Transfer) -> Self::Future {
        ResourceServiceFut {
            fut: self.service.call(req),
            _t: marker::PhantomData,
        }
    }
}

pin_project_lite::pin_project! {
    struct ResourceServiceFut<S, T: Service> {
        #[pin] fut: T::Future,
        _t: marker::PhantomData<S>,
    }
}

impl<S, T> Future for ResourceServiceFut<S, T>
where
    T: Service<Request = Transfer, Response = Outcome>,
    Error: From<T::Error>,
    Outcome: TryFrom<T::Error, Error = Error>,
{
    type Output = Result<Outcome, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(match self.project().fut.poll(cx) {
            Poll::Ready(Ok(res)) => Ok(res),
            Poll::Ready(Err(err)) => Outcome::try_from(err),
            Poll::Pending => return Poll::Pending,
        })
    }
}
