use std::{
    convert::TryFrom, fmt, marker::PhantomData, pin::Pin, rc::Rc, task::Context, task::Poll, time,
};

use futures::Future;
use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex_amqp_codec::{
    protocol::ProtocolId, AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError, SaslFrame,
};

use crate::io::{IoDispatcher, IoState, Timer};
use crate::{Configuration, Connection, ControlFrame, State};

use super::default::DefaultControlService;
use super::handshake::{Handshake, HandshakeAck};
use super::{dispatcher::Dispatcher, link::Link, LinkError, ServerError};

/// Server dispatcher factory
pub struct Server<Io, St, H: ServiceFactory, Ctl: ServiceFactory> {
    handshake: H,
    control: Ctl,
    config: Rc<Configuration>,
    max_size: usize,
    handshake_timeout: u64,
    disconnect_timeout: u16,
    _t: PhantomData<(Io, St)>,
}

pub(super) struct ServerInner<St, H: ServiceFactory, Ctl, Pb> {
    handshake: H,
    control: Ctl,
    publish: Pb,
    config: Rc<Configuration>,
    max_size: usize,
    handshake_timeout: u64,
    disconnect_timeout: u16,
    time: Timer<AmqpCodec<AmqpFrame>>,
    _t: PhantomData<St>,
}

impl<Io, St, H> Server<Io, St, H, DefaultControlService<St, H::Error>>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    H: ServiceFactory<Config = (), Request = Handshake<Io>, Response = HandshakeAck<Io, St>>
        + 'static,
{
    /// Create server factory and provide handshake service
    pub fn new<F>(handshake: F) -> Self
    where
        F: IntoServiceFactory<H>,
    {
        Self {
            handshake: handshake.into_factory(),
            handshake_timeout: 5000,
            disconnect_timeout: 30,
            control: DefaultControlService::default(),
            max_size: 0,
            config: Rc::new(Configuration::default()),
            _t: PhantomData,
        }
    }
}

impl<Io, St, H, Ctl> Server<Io, St, H, Ctl>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    H: ServiceFactory<Config = (), Request = Handshake<Io>, Response = HandshakeAck<Io, St>>
        + 'static,
    Ctl: ServiceFactory<Config = State<St>, Request = ControlFrame<H::Error>, Response = ()>
        + 'static,
{
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

    /// Set handshake timeout in millis.
    ///
    /// By default handshake timeuot is 5 seconds.
    pub fn handshake_timeout(mut self, timeout: u64) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Set server connection disconnect timeout in milliseconds.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 3 seconds.
    pub fn disconnect_timeout(mut self, val: u16) -> Self {
        self.disconnect_timeout = val;
        self
    }

    /// Service to call with control frames
    pub fn control<F, S>(self, service: F) -> Server<Io, St, H, S>
    where
        F: IntoServiceFactory<S>,
        S: ServiceFactory<Config = State<St>, Request = ControlFrame<H::Error>, Response = ()>
            + 'static,
        H::Error: From<S::InitError>,
    {
        Server {
            config: self.config,
            handshake: self.handshake,
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
            control: service.into_factory(),
            max_size: self.max_size,
            _t: PhantomData,
        }
    }

    /// Set service to execute for incoming links and create service factory
    pub fn finish<F, Pb>(
        self,
        service: F,
    ) -> impl ServiceFactory<Config = (), Request = Io, Response = (), Error = ServerError<H::Error>>
    where
        F: IntoServiceFactory<Pb>,
        H::Error: From<Ctl::InitError> + From<Pb::Error> + From<Pb::InitError> + fmt::Debug,
        Pb: ServiceFactory<Config = State<St>, Request = Link<St>, Response = ()> + 'static,
        Pb::Error: fmt::Display + From<Ctl::Error>,
        LinkError: TryFrom<Pb::Error, Error = H::Error>,
    {
        ServerImpl {
            inner: Rc::new(ServerInner {
                handshake: self.handshake,
                handshake_timeout: self.handshake_timeout,
                config: self.config,
                publish: service.into_factory(),
                control: self.control,
                disconnect_timeout: self.disconnect_timeout,
                max_size: self.max_size,
                time: Timer::with(time::Duration::from_secs(1)),
                _t: PhantomData,
            }),
            _t: PhantomData,
        }
    }
}

struct ServerImpl<Io, St, H: ServiceFactory, Ctl, Pb> {
    inner: Rc<ServerInner<St, H, Ctl, Pb>>,
    _t: PhantomData<(Io,)>,
}

impl<Io, St, H, Ctl, Pb> ServiceFactory for ServerImpl<Io, St, H, Ctl, Pb>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    H: ServiceFactory<Config = (), Request = Handshake<Io>, Response = HandshakeAck<Io, St>>
        + 'static,
    H::Error: From<Ctl::InitError> + From<Pb::Error> + From<Pb::InitError> + fmt::Debug,
    Ctl: ServiceFactory<Config = State<St>, Request = ControlFrame<H::Error>, Response = ()>
        + 'static,
    Pb: ServiceFactory<Config = State<St>, Request = Link<St>, Response = ()> + 'static,
    Pb::Error: fmt::Display + From<Ctl::Error>,
    LinkError: TryFrom<Pb::Error, Error = H::Error>,
{
    type Config = ();
    type Request = Io;
    type Response = ();
    type Error = ServerError<H::Error>;
    type Service = ServerImplService<Io, St, H, Ctl, Pb>;
    type InitError = H::InitError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Service, Self::InitError>>>>;

    fn new_service(&self, _: ()) -> Self::Future {
        let inner = self.inner.clone();

        Box::pin(async move {
            inner
                .handshake
                .new_service(())
                .await
                .map(move |handshake| ServerImplService {
                    inner,
                    handshake: Rc::new(handshake),
                    _t: PhantomData,
                })
        })
    }
}

struct ServerImplService<Io, St, H: ServiceFactory, Ctl, Pb> {
    handshake: Rc<H::Service>,
    inner: Rc<ServerInner<St, H, Ctl, Pb>>,
    _t: PhantomData<(Io,)>,
}

impl<Io, St, H, Ctl, Pb> Service for ServerImplService<Io, St, H, Ctl, Pb>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    H: ServiceFactory<Config = (), Request = Handshake<Io>, Response = HandshakeAck<Io, St>>
        + 'static,
    H::Error: From<Ctl::InitError> + From<Pb::Error> + From<Pb::InitError> + fmt::Debug,
    Ctl: ServiceFactory<Config = State<St>, Request = ControlFrame<H::Error>, Response = ()>
        + 'static,
    Pb: ServiceFactory<Config = State<St>, Request = Link<St>, Response = ()> + 'static,
    Pb::Error: fmt::Display + From<Ctl::Error>,
    LinkError: TryFrom<Pb::Error, Error = H::Error>,
{
    type Request = Io;
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
        let timeout = self.inner.handshake_timeout;
        let keepalive = self.inner.config.idle_time_out / 1000;
        let disconnect_timeout = self.inner.disconnect_timeout;
        let inner = self.inner.clone();
        let fut = handshake(
            req,
            self.inner.max_size,
            self.handshake.clone(),
            self.inner.clone(),
        );

        Box::pin(async move {
            let (io, state, sink, st) = if timeout == 0 {
                fut.await?
            } else {
                ntex::rt::time::timeout(time::Duration::from_millis(timeout), fut)
                    .await
                    .map_err(|_| ServerError::HandshakeTimeout)??
            };

            // create publish service
            let pb_srv = inner
                .publish
                .new_service(st.clone())
                .await
                .map_err(|e| ServerError::Service(e.into()))?;

            // create control service
            let ctl_srv = inner
                .control
                .new_service(st.clone())
                .await
                .map_err(|e| ServerError::Service(e.into()))?
                .map_err(|e| From::from(e));

            let dispatcher = Dispatcher::new(sink, pb_srv, st, ctl_srv);

            IoDispatcher::with(io, state, dispatcher, inner.time.clone())
                .keepalive_timeout(keepalive as u16)
                .disconnect_timeout(disconnect_timeout)
                .await
                .map_err(|_| ServerError::Disconnected)
        })
    }
}

async fn handshake<Io, St, H, Ctl, Pb>(
    mut io: Io,
    max_size: usize,
    handshake: Rc<H::Service>,
    inner: Rc<ServerInner<St, H, Ctl, Pb>>,
) -> Result<(Io, IoState<AmqpCodec<AmqpFrame>>, Connection, State<St>), ServerError<H::Error>>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    H: ServiceFactory<Config = (), Request = Handshake<Io>, Response = HandshakeAck<Io, St>>,
    H::Error: 'static,
    H::Error: From<Ctl::InitError> + From<Pb::Error> + From<Pb::InitError> + fmt::Debug,
    Ctl: ServiceFactory<Config = State<St>, Request = ControlFrame<H::Error>, Response = ()>
        + 'static,
    Pb: ServiceFactory<Config = State<St>, Request = Link<St>, Response = ()> + 'static,
    Pb::Error: fmt::Display + From<Ctl::Error>,
    LinkError: TryFrom<Pb::Error, Error = H::Error>,
{
    let state = IoState::new(ProtocolIdCodec);

    let protocol = state
        .next(&mut io)
        .await
        .map_err(ServerError::from)?
        .ok_or_else(|| {
            log::trace!("Server amqp is disconnected during handshake");
            ServerError::Disconnected
        })?;

    let (io, sink, state, st) = match protocol {
        // start amqp processing
        ProtocolId::Amqp | ProtocolId::AmqpSasl => {
            state
                .send(&mut io, protocol)
                .await
                .map_err(ServerError::from)?;

            let ack = handshake
                .call(if protocol == ProtocolId::Amqp {
                    Handshake::new_plain(
                        io,
                        state.map_codec(|_| AmqpCodec::<AmqpFrame>::new()),
                        inner.config.clone(),
                    )
                } else {
                    Handshake::new_sasl(
                        io,
                        state.map_codec(|_| AmqpCodec::<SaslFrame>::new()),
                        inner.config.clone(),
                    )
                })
                .await
                .map_err(ServerError::Service)?;

            let (st, mut io, sink, state) = ack.into_inner();
            state.with_codec(|codec| codec.max_size(max_size));

            // confirm Open
            let local = inner.config.to_open();
            state
                .send(&mut io, AmqpFrame::new(0, local.into()))
                .await
                .map_err(ServerError::from)?;

            let st = State::new(st);

            (io, sink, state, st)
        }
        ProtocolId::AmqpTls => {
            return Err(ServerError::from(ProtocolIdError::Unexpected {
                exp: ProtocolId::Amqp,
                got: ProtocolId::AmqpTls,
            }))
        }
    };

    Ok((io, state, sink, st))
}
