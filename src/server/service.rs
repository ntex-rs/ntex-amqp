use std::{fmt, future::Future, marker, pin::Pin, rc::Rc, task::Context, task::Poll};

use ntex::io::{into_boxed, Dispatcher as FramedDispatcher, Filter, Io, IoBoxed, Timer};
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::time::{timeout, Millis, Seconds};

use crate::codec::{protocol::ProtocolId, AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError};
use crate::{default::DefaultControlService, Configuration, Connection, ControlFrame, State};
use crate::{dispatcher::Dispatcher, types::Message};

use super::handshake::{Handshake, HandshakeAck};
use super::{Error, HandshakeError, ServerError};

/// Server dispatcher factory
pub struct Server<St, H, Ctl> {
    handshake: H,
    control: Ctl,
    config: Rc<Configuration>,
    max_size: usize,
    handshake_timeout: Seconds,
    disconnect_timeout: Seconds,
    _t: marker::PhantomData<St>,
}

pub(super) struct ServerInner<St, Ctl, Pb> {
    control: Ctl,
    publish: Pb,
    config: Rc<Configuration>,
    max_size: usize,
    handshake_timeout: Seconds,
    disconnect_timeout: Seconds,
    time: Timer,
    _t: marker::PhantomData<St>,
}

impl<St, H> Server<St, H, DefaultControlService<St, H::Error>>
where
    St: 'static,
    H: ServiceFactory<Config = (), Request = Handshake, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
{
    /// Create server factory and provide handshake service
    pub fn new<F>(handshake: F) -> Self
    where
        F: IntoServiceFactory<H>,
    {
        Self {
            handshake: handshake.into_factory(),
            handshake_timeout: Seconds(5),
            disconnect_timeout: Seconds(3),
            control: DefaultControlService::default(),
            max_size: 0,
            config: Rc::new(Configuration::default()),
            _t: marker::PhantomData,
        }
    }
}

impl<St, H, Ctl> Server<St, H, Ctl> {
    /// Provide connection configuration
    pub fn config(mut self, config: Configuration) -> Self {
        self.config = Rc::new(config);
        self
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Set handshake timeout.
    ///
    /// By default handshake timeuot is 5 seconds.
    pub fn handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Set server connection disconnect timeout.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 3 seconds.
    pub fn disconnect_timeout(mut self, val: Seconds) -> Self {
        self.disconnect_timeout = val;
        self
    }
}

impl<St, H, Ctl> Server<St, H, Ctl>
where
    St: 'static,
    H: ServiceFactory<Config = (), Request = Handshake, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
    Ctl: ServiceFactory<Config = State<St>, Request = ControlFrame, Response = ()> + 'static,
    Ctl::Error: fmt::Debug,
    Ctl::InitError: fmt::Debug,
    Error: From<Ctl::Error>,
{
    /// Service to call with control frames
    pub fn control<F, S>(self, service: F) -> Server<St, H, S>
    where
        F: IntoServiceFactory<S>,
        S: ServiceFactory<Config = State<St>, Request = ControlFrame, Response = ()> + 'static,
        S::Error: fmt::Debug,
        S::InitError: fmt::Debug,
        Error: From<S::Error>,
    {
        Server {
            config: self.config,
            handshake: self.handshake,
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
            control: service.into_factory(),
            max_size: self.max_size,
            _t: marker::PhantomData,
        }
    }

    /// Set service to execute for incoming links and create service factory
    pub fn finish<F, S, Pb>(
        self,
        service: S,
    ) -> impl ServiceFactory<
        Config = (),
        Request = Io<F>,
        Response = (),
        Error = ServerError<H::Error>,
        InitError = H::InitError,
    >
    where
        F: Filter,
        S: IntoServiceFactory<Pb>,
        Pb: ServiceFactory<Config = State<St>, Request = Message, Response = ()> + 'static,
        Pb::Error: fmt::Debug,
        Pb::InitError: fmt::Debug,
        Error: From<Pb::Error> + From<Ctl::Error>,
    {
        into_boxed(ServerImpl {
            handshake: self.handshake,
            inner: Rc::new(ServerInner {
                handshake_timeout: self.handshake_timeout,
                config: self.config,
                publish: service.into_factory(),
                control: self.control,
                disconnect_timeout: self.disconnect_timeout,
                max_size: self.max_size,
                time: Timer::new(Millis::ONE_SEC),
                _t: marker::PhantomData,
            }),
        })
    }
}

struct ServerImpl<St, H, Ctl, Pb> {
    handshake: H,
    inner: Rc<ServerInner<St, Ctl, Pb>>,
}

impl<St, H, Ctl, Pb> ServiceFactory for ServerImpl<St, H, Ctl, Pb>
where
    St: 'static,
    H: ServiceFactory<Config = (), Request = Handshake, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
    Ctl: ServiceFactory<Config = State<St>, Request = ControlFrame, Response = ()> + 'static,
    Ctl::Error: fmt::Debug,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Config = State<St>, Request = Message, Response = ()> + 'static,
    Pb::Error: fmt::Debug,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Config = ();
    type Request = IoBoxed;
    type Response = ();
    type Error = ServerError<H::Error>;
    type Service = ServerImplService<St, H::Service, Ctl, Pb>;
    type InitError = H::InitError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Service, Self::InitError>>>>;

    fn new_service(&self, _: ()) -> Self::Future {
        let inner = self.inner.clone();
        let fut = self.handshake.new_service(());

        Box::pin(async move {
            fut.await.map(move |handshake| ServerImplService {
                inner,
                handshake: Rc::new(handshake),
            })
        })
    }
}

struct ServerImplService<St, H, Ctl, Pb> {
    handshake: Rc<H>,
    inner: Rc<ServerInner<St, Ctl, Pb>>,
}

impl<St, H, Ctl, Pb> Service for ServerImplService<St, H, Ctl, Pb>
where
    St: 'static,
    H: Service<Request = Handshake, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
    Ctl: ServiceFactory<Config = State<St>, Request = ControlFrame, Response = ()> + 'static,
    Ctl::Error: fmt::Debug,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Config = State<St>, Request = Message, Response = ()> + 'static,
    Pb::Error: fmt::Debug,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Request = IoBoxed;
    type Response = ();
    type Error = ServerError<H::Error>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.handshake
            .as_ref()
            .poll_ready(cx)
            .map(|res| res.map_err(ServerError::Service))
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        self.handshake.as_ref().poll_shutdown(cx, is_error)
    }

    fn call(&self, req: Self::Request) -> Self::Future {
        req.set_disconnect_timeout(self.inner.disconnect_timeout.into());

        let keepalive = self.inner.config.idle_time_out / 1000;
        let handshake_timeout = self.inner.handshake_timeout;
        let disconnect_timeout = self.inner.disconnect_timeout;
        let fut = handshake(
            req,
            self.inner.max_size,
            self.handshake.clone(),
            self.inner.clone(),
        );
        let inner = self.inner.clone();

        Box::pin(async move {
            let (state, codec, sink, st, idle_timeout) = if handshake_timeout.is_zero() {
                fut.await?
            } else {
                timeout(handshake_timeout, fut)
                    .await
                    .map_err(|_| HandshakeError::Timeout)??
            };

            // create publish service
            let pb_srv = inner.publish.new_service(st.clone()).await.map_err(|e| {
                error!("Publish service init error: {:?}", e);
                ServerError::PublishServiceError
            })?;

            // create control service
            let ctl_srv = inner.control.new_service(st.clone()).await.map_err(|e| {
                error!("Control service init error: {:?}", e);
                ServerError::ControlServiceError
            })?;

            let dispatcher = Dispatcher::new(sink, pb_srv, ctl_srv, idle_timeout.into())
                .map(|_| Option::<AmqpFrame>::None);

            FramedDispatcher::new(state, codec, dispatcher, inner.time.clone())
                .keepalive_timeout(Seconds::checked_new(keepalive as usize))
                .disconnect_timeout(disconnect_timeout)
                .await
                .map_err(|_| ServerError::Disconnected)
        })
    }
}

async fn handshake<St, H, Ctl, Pb>(
    state: IoBoxed,
    max_size: usize,
    handshake: Rc<H>,
    inner: Rc<ServerInner<St, Ctl, Pb>>,
) -> Result<
    (
        IoBoxed,
        AmqpCodec<AmqpFrame>,
        Connection,
        State<St>,
        Seconds,
    ),
    ServerError<H::Error>,
>
where
    St: 'static,
    H: Service<Request = Handshake, Response = HandshakeAck<St>>,
    Ctl: ServiceFactory<Config = State<St>, Request = ControlFrame, Response = ()> + 'static,
    Pb: ServiceFactory<Config = State<St>, Request = Message, Response = ()> + 'static,
{
    let protocol = state
        .next(&ProtocolIdCodec)
        .await
        .ok_or_else(|| {
            log::trace!("Server amqp is disconnected during handshake");
            HandshakeError::Disconnected
        })
        .and_then(|res| res.map_err(HandshakeError::from))?;

    let (sink, state, codec, st, idle_timeout) = match protocol {
        // start amqp processing
        ProtocolId::Amqp | ProtocolId::AmqpSasl => {
            state
                .send(protocol, &ProtocolIdCodec)
                .await
                .map_err(HandshakeError::from)?;

            let ack = handshake
                .call(if protocol == ProtocolId::Amqp {
                    Handshake::new_plain(state, inner.config.clone())
                } else {
                    Handshake::new_sasl(state, inner.config.clone())
                })
                .await
                .map_err(ServerError::Service)?;

            let (st, sink, idle_timeout, state) = ack.into_inner();

            let codec = AmqpCodec::new().max_size(max_size);

            // confirm Open
            let local = inner.config.to_open();
            state
                .send(AmqpFrame::new(0, local.into()), &codec)
                .await
                .map_err(HandshakeError::from)?;

            let st = State::new(st);

            (sink, state, codec, st, idle_timeout)
        }
        ProtocolId::AmqpTls => {
            return Err(HandshakeError::from(ProtocolIdError::Unexpected {
                exp: ProtocolId::Amqp,
                got: ProtocolId::AmqpTls,
            })
            .into())
        }
    };

    Ok((state, codec, sink, st, idle_timeout))
}
