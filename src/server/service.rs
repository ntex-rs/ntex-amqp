use std::{fmt, marker, rc::Rc};

use ntex_dispatcher::Dispatcher as IoDispatcher;
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{IntoServiceFactory, Pipeline, Service, ServiceCtx, ServiceFactory};
use ntex_util::time::{Millis, timeout_checked};

use crate::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError, protocol::ProtocolId};
use crate::{AmqpServiceConfig, Connection, ControlFrame, State, default::DefaultControlService};
use crate::{dispatcher::Dispatcher, types::Message};

use super::handshake::{Handshake, HandshakeAck};
use super::{Error, HandshakeError, ServerError};

/// Amqp server factory
pub struct Server<St, H, Ctl, Pb> {
    handshake: H,
    inner: Rc<ServerInner<St, Ctl, Pb>>,
}

/// Amqp server builder
pub struct ServerBuilder<St, H, Ctl> {
    handshake: H,
    control: Ctl,
    _t: marker::PhantomData<St>,
}

pub(super) struct ServerInner<St, Ctl, Pb> {
    control: Ctl,
    publish: Pb,
    _t: marker::PhantomData<St>,
}

impl<St> Server<St, (), (), ()>
where
    St: 'static,
{
    /// Start server building process with provided handshake service
    pub fn build<F, H>(handshake: F) -> ServerBuilder<St, H, DefaultControlService<St, H::Error>>
    where
        F: IntoServiceFactory<H, Handshake, SharedCfg>,
        H: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>, Data = ()>,
    {
        ServerBuilder {
            handshake: handshake.into_factory(),
            control: DefaultControlService::default(),
            _t: marker::PhantomData,
        }
    }
}

impl<St, H, Ctl> ServerBuilder<St, H, Ctl>
where
    St: 'static,
    H: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>, Data = ()> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = (), Data = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Error: From<Ctl::Error>,
{
    /// Service to call with control frames
    pub fn control<F, S>(self, service: F) -> ServerBuilder<St, H, S>
    where
        F: IntoServiceFactory<S, ControlFrame, State<St>>,
        S: ServiceFactory<ControlFrame, State<St>, Response = (), Data = ()> + 'static,
        S::InitError: fmt::Debug,
        Error: From<S::Error>,
    {
        ServerBuilder {
            handshake: self.handshake,
            control: service.into_factory(),
            _t: marker::PhantomData,
        }
    }

    /// Set service to execute for incoming links and create service factory
    pub fn finish<S, Pb>(self, service: S) -> Server<St, H, Ctl, Pb>
    where
        S: IntoServiceFactory<Pb, Message, State<St>>,
        Pb: ServiceFactory<Message, State<St>, Response = (), Data = ()> + 'static,
        Pb::InitError: fmt::Debug,
        Error: From<Pb::Error> + From<Ctl::Error>,
    {
        Server {
            handshake: self.handshake,
            inner: Rc::new(ServerInner {
                publish: service.into_factory(),
                control: self.control,
                _t: marker::PhantomData,
            }),
        }
    }
}

impl<F, St, H, Ctl, Pb> ServiceFactory<Io<F>, SharedCfg> for Server<St, H, Ctl, Pb>
where
    F: Filter,
    St: 'static,
    H: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>, Data = ()> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = (), Data = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Message, State<St>, Response = (), Data = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Response = ();
    type Error = ServerError<H::Error>;
    type Service = ServerHandler<St, H::Service, Ctl, Pb>;
    type InitError = H::InitError;
    type Data = ();

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        self.handshake
            .pipeline(cfg.clone(), &())
            .await
            .map(move |handshake| ServerHandler {
                handshake,
                cfg: cfg.get(),
                inner: self.inner.clone(),
            })
    }

    async fn map_data(&self, _: &SharedCfg, _: &Self::Data) -> Result<(), Self::InitError> {
        Ok(())
    }
}

impl<St, H, Ctl, Pb> ServiceFactory<IoBoxed, SharedCfg> for Server<St, H, Ctl, Pb>
where
    St: 'static,
    H: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>, Data = ()> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = (), Data = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Message, State<St>, Response = (), Data = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Response = ();
    type Error = ServerError<H::Error>;
    type Service = ServerHandler<St, H::Service, Ctl, Pb>;
    type InitError = H::InitError;
    type Data = ();

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        self.handshake
            .pipeline(cfg.clone(), &())
            .await
            .map(move |handshake| ServerHandler {
                handshake,
                cfg: cfg.get(),
                inner: self.inner.clone(),
            })
    }

    async fn map_data(&self, _: &SharedCfg, _: &Self::Data) -> Result<(), Self::InitError> {
        Ok(())
    }
}

/// Amqp connections handler
pub struct ServerHandler<St, H, Ctl, Pb>
where
    H: Service<Handshake>,
{
    cfg: Cfg<AmqpServiceConfig>,
    handshake: Pipeline<H, H::Data>,
    inner: Rc<ServerInner<St, Ctl, Pb>>,
}

impl<St, H, Ctl, Pb> ServerHandler<St, H, Ctl, Pb>
where
    St: 'static,
    H: Service<Handshake, Response = HandshakeAck<St>> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = (), Data = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Message, State<St>, Response = (), Data = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    async fn create(&self, req: IoBoxed) -> Result<(), ServerError<H::Error>> {
        let fut = handshake(req, &self.handshake, self.cfg.clone());
        let inner = self.inner.clone();

        let (state, codec, sink, st, idle_timeout) =
            timeout_checked(self.cfg.handshake_timeout, fut)
                .await
                .map_err(|()| HandshakeError::Timeout)??;

        // create publish service
        let pb_srv = inner.publish.pipeline(st.clone(), &()).await.map_err(|e| {
            log::error!("Publish service init error: {e:?}");
            ServerError::PublishServiceError
        })?;

        // create control service
        let ctl_srv = inner.control.pipeline(st.clone(), &()).await.map_err(|e| {
            log::error!("Control service init error: {e:?}");
            ServerError::ControlServiceError
        })?;

        IoDispatcher::new(
            state,
            codec,
            Dispatcher::new(sink, pb_srv, ctl_srv, idle_timeout),
        )
        .await
        .map_err(ServerError::Dispatcher)
    }
}

impl<F, St, H, Ctl, Pb> Service<Io<F>> for ServerHandler<St, H, Ctl, Pb>
where
    F: Filter,
    St: 'static,
    H: Service<Handshake, Response = HandshakeAck<St>> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = (), Data = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Message, State<St>, Response = (), Data = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Response = ();
    type Error = ServerError<H::Error>;
    type Data = ();

    #[inline]
    async fn ready(&self, _: &Self::Data, _: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        self.handshake.ready().await.map_err(ServerError::Service)
    }

    #[inline]
    async fn shutdown(&self, _: &Self::Data) {
        self.handshake.shutdown().await;
    }

    async fn call(
        &self,
        req: Io<F>,
        _: &Self::Data,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        self.create(IoBoxed::from(req)).await
    }
}

impl<St, H, Ctl, Pb> Service<IoBoxed> for ServerHandler<St, H, Ctl, Pb>
where
    St: 'static,
    H: Service<Handshake, Response = HandshakeAck<St>> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = (), Data = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Message, State<St>, Response = (), Data = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Response = ();
    type Error = ServerError<H::Error>;
    type Data = ();

    #[inline]
    async fn ready(&self, _: &Self::Data, _: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        self.handshake.ready().await.map_err(ServerError::Service)
    }

    #[inline]
    async fn shutdown(&self, _: &Self::Data) {
        self.handshake.shutdown().await;
    }

    #[inline]
    async fn call(
        &self,
        req: IoBoxed,
        _: &Self::Data,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        self.create(req).await
    }
}

async fn handshake<St, H>(
    io: IoBoxed,
    handshake: &Pipeline<H, H::Data>,
    cfg: Cfg<AmqpServiceConfig>,
) -> Result<(IoBoxed, AmqpCodec<AmqpFrame>, Connection, State<St>, Millis), ServerError<H::Error>>
where
    St: 'static,
    H: Service<Handshake, Response = HandshakeAck<St>>,
{
    let protocol = io
        .recv(&ProtocolIdCodec)
        .await
        .map_err(HandshakeError::from)?
        .ok_or_else(|| {
            log::trace!("{}: Server amqp is disconnected during handshake", io.tag());
            HandshakeError::Disconnected(None)
        })?;

    match protocol {
        // start amqp processing
        ProtocolId::Amqp | ProtocolId::AmqpSasl => {
            // confirm protocol
            io.send(protocol, &ProtocolIdCodec)
                .await
                .map_err(HandshakeError::from)?;

            // handshake protocol
            let ack = handshake
                .call(if protocol == ProtocolId::Amqp {
                    Handshake::new_plain(io, cfg.clone())
                } else {
                    Handshake::new_sasl(io, cfg.clone())
                })
                .await
                .map_err(ServerError::Service)?;

            let (st, sink, idle_timeout, io) = ack.into_inner();

            let codec = AmqpCodec::new().max_size(cfg.max_size);

            // confirm Open
            let local = cfg.to_open();
            io.send(AmqpFrame::new(0, local.into()), &codec)
                .await
                .map_err(HandshakeError::from)?;

            Ok((io, codec, sink, State::new(st), Millis::from(idle_timeout)))
        }
        ProtocolId::AmqpTls => Err(ServerError::Handshake(HandshakeError::from(
            ProtocolIdError::Unexpected {
                exp: ProtocolId::Amqp,
                got: ProtocolId::AmqpTls,
            },
        ))),
    }
}
