use std::{fmt, marker, rc::Rc};

use ntex_dispatcher::Dispatcher as IoDispatcher;
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::cfg::{Cfg, Configuration};
use ntex_service::pipeline::Pipeline;
use ntex_service::{Ctx, IntoService, IntoServiceFactory, Service, ServiceFactory};
use ntex_util::time::{Millis, timeout_checked};

use crate::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError, protocol::ProtocolId};
use crate::{AmqpServiceConfig, Connection, ControlFrame, State, default::DefaultControlService};
use crate::{dispatcher::Dispatcher, types::Message};

use super::handshake::{Handshake, HandshakeAck};
use super::{Error, HandshakeError, ServerError};

/// Amqp server factory
pub struct Server<St, Err, Ctl, Pb> {
    handshake: Pipeline<Handshake, HandshakeAck<St>, Err>,
    inner: Rc<ServerInner<St, Ctl, Pb>>,
}

/// Amqp server builder
pub struct ServerBuilder<St, Err, Ctl> {
    handshake: Pipeline<Handshake, HandshakeAck<St>, Err>,
    control: Ctl,
    _t: marker::PhantomData<(St, Err)>,
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
    pub fn build<F, H>(handshake: F) -> ServerBuilder<St, H::Error, DefaultControlService<St, H::Error>>
    where
        F: IntoService<H, (), Handshake>,
        H: Service<(), Handshake, Res = HandshakeAck<St>> + 'static,
    {
        ServerBuilder {
            handshake: Pipeline::new(handshake.into_service()),
            control: DefaultControlService::default(),
            _t: marker::PhantomData,
        }
    }
}

impl<St, Err, Ctl> ServerBuilder<St, Err, Ctl>
where
    St: 'static,
    Ctl: ServiceFactory<(), ControlFrame, State<St>, Res = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Error: From<Ctl::Error>,
{
    /// Service to call with control frames
    pub fn control<Sf>(
        self,
        f: impl IntoServiceFactory<Sf, (), ControlFrame, State<St>>,
    ) -> ServerBuilder<St, Err, Sf>
    where
        Sf: ServiceFactory<(), ControlFrame, State<St>, Res = ()> + 'static,
        Sf::InitError: fmt::Debug,
        Error: From<Sf::Error>,
    {
        ServerBuilder {
            control: f.into_factory(),
            handshake: self.handshake,
            _t: marker::PhantomData,
        }
    }

    /// Set service to execute for incoming links and create service factory
    pub fn finish<Sf>(
        self,
        f: impl IntoServiceFactory<Sf, (), Message, State<St>>,
    ) -> Server<St, Err, Ctl, Sf>
    where
        Sf: ServiceFactory<(), Message, State<St>, Res = ()> + 'static,
        Sf::InitError: fmt::Debug,
        Error: From<Sf::Error> + From<Ctl::Error>,
    {
        Server {
            handshake: self.handshake,
            inner: Rc::new(ServerInner {
                publish: f.into_factory(),
                control: self.control,
                _t: marker::PhantomData,
            }),
        }
    }
}

impl<St, Err, Ctl, Pb> Server<St, Err, Ctl, Pb>
where
    St: 'static,
    Err: 'static,
    Ctl: ServiceFactory<(), ControlFrame, State<St>, Res = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<(), Message, State<St>, Res = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    async fn create(&self, io: IoBoxed) -> Result<(), ServerError<Err>> {
        let cfg: Cfg<AmqpServiceConfig> = io.cfg().ctx().get();
        let fut = handshake(io, &self.handshake, cfg.clone());

        let (state, codec, sink, st, idle_timeout) = timeout_checked(cfg.handshake_timeout, fut)
            .await
            .map_err(|()| HandshakeError::Timeout)??;

        // create publish service
        let pb_svc = self.inner.publish.create(&st).await.map_err(|e| {
            log::error!("Publish service init error: {e:?}");
            ServerError::PublishServiceError
        })?;

        // create control service
        let ctl_svc = self.inner.control.create(&st).await.map_err(|e| {
            log::error!("Control service init error: {e:?}");
            ServerError::ControlServiceError
        })?;

        IoDispatcher::new(
            state,
            codec,
            Pipeline::new(Dispatcher::new(sink, pb_svc, ctl_svc, idle_timeout)),
        )
        .await
        .map_err(ServerError::Dispatcher)
    }
}

impl<St, Err, Ctl, Pb> Service<(), IoBoxed> for Server<St, Err, Ctl, Pb>
where
    St: 'static,
    Err: 'static,
    Ctl: ServiceFactory<(), ControlFrame, State<St>, Res = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<(), Message, State<St>, Res = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Res = ();
    type Error = ServerError<Err>;

    #[inline]
    async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
        self.handshake.ready().await.map_err(ServerError::Service)
    }

    #[inline]
    async fn shutdown(&self, _: Ctx<'_, Self, ()>) {
        self.handshake.shutdown().await;
    }

    async fn call(&self, req: IoBoxed, _: Ctx<'_, Self, ()>) -> Result<Self::Res, Self::Error> {
        self.create(req).await
    }
}

impl<F, St, Err, Ctl, Pb> Service<(), Io<F>> for Server<St, Err, Ctl, Pb>
where
    F: Filter,
    St: 'static,
    Err: 'static,
    Ctl: ServiceFactory<(), ControlFrame, State<St>, Res = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<(), Message, State<St>, Res = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Res = ();
    type Error = ServerError<Err>;

    #[inline]
    async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
        self.handshake.ready().await.map_err(ServerError::Service)
    }

    #[inline]
    async fn shutdown(&self, _: Ctx<'_, Self, ()>) {
        self.handshake.shutdown().await;
    }

    #[inline]
    async fn call(&self, req: Io<F>, _: Ctx<'_, Self, ()>) -> Result<Self::Res, Self::Error> {
        self.create(IoBoxed::from(req)).await
    }
}

async fn handshake<St, Err>(
    io: IoBoxed,
    handshake: &Pipeline<Handshake, HandshakeAck<St>, Err>,
    cfg: Cfg<AmqpServiceConfig>,
) -> Result<(IoBoxed, AmqpCodec<AmqpFrame>, Connection, State<St>, Millis), ServerError<Err>>
where
    St: 'static,
    Err: 'static,
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
