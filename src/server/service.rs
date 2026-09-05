use ntex_dispatcher::Dispatcher as IoDispatcher;
use ntex_error::{self as error, ErrorDiagnostic, ErrorInfo};
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::cfg::{Cfg, Configuration};
use ntex_service::pipeline::{Pipeline, PipelineFactory, PipelineState};
use ntex_service::{Ctx, IntoService, IntoServiceFactory, Service, ServiceFactory};
use ntex_util::time::{Millis, timeout_checked};

use crate::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError, protocol::ProtocolId};
use crate::{AmqpServiceConfig, Connection, ControlFrame, State, default::DefaultControlService};
use crate::{dispatcher::Dispatcher, types::Message};

use super::handshake::{Handshake, HandshakeAck};
use super::{Error, HandshakeError, ServerError};

/// Amqp server factory
pub struct Server<St, AppSt, Err> {
    handshake: PipelineState<St, Handshake, HandshakeAck<AppSt>, Err>,
    control: PipelineFactory<State<AppSt>, ControlFrame, (), Error, ServerError<Err>>,
    publish: PipelineFactory<State<AppSt>, Message, (), Error, ServerError<Err>>,
}

/// Amqp server builder
pub struct ServerBuilder<St, AppSt, Err> {
    handshake: PipelineState<St, Handshake, HandshakeAck<AppSt>, Err>,
    control: PipelineFactory<State<AppSt>, ControlFrame, (), Error, ServerError<Err>>,
}

impl<St, AppSt> Server<St, AppSt, ()>
where
    St: 'static,
    AppSt: 'static,
{
    /// Start server building process with provided handshake service
    pub fn build<F, H>(f: F) -> ServerBuilder<St, AppSt, H::Error>
    where
        F: IntoService<H, St, Handshake>,
        H: Service<St, Handshake, Res = HandshakeAck<AppSt>> + 'static,
    {
        ServerBuilder {
            handshake: PipelineState::new(f.into_service()),
            control: PipelineFactory::new(
                DefaultControlService::default().map_init_err(ServerError::ControlService),
            ),
        }
    }
}

impl<St, AppSt, Err> ServerBuilder<St, AppSt, Err>
where
    St: 'static,
    AppSt: 'static,
    Err: 'static,
{
    #[must_use]
    /// Service to call with control frames
    pub fn control<Sf>(self, f: impl IntoServiceFactory<Sf, State<AppSt>, ControlFrame>) -> Self
    where
        Sf: ServiceFactory<State<AppSt>, ControlFrame, Res = ()> + 'static,
        Sf::InitError: ErrorDiagnostic,
        Error: From<Sf::Error>,
    {
        ServerBuilder {
            handshake: self.handshake,
            control: PipelineFactory::new(
                f.into_factory()
                    .map_err(Into::into)
                    .map_init_err(|e| ServerError::ControlService(ErrorInfo::from(error::Error::from(e)))),
            ),
        }
    }

    /// Set service to execute for incoming links and create service factory
    pub fn finish<Sf>(self, f: impl IntoServiceFactory<Sf, State<AppSt>, Message>) -> Server<St, AppSt, Err>
    where
        Sf: ServiceFactory<State<AppSt>, Message, Res = ()> + 'static,
        Sf::InitError: ErrorDiagnostic,
        Error: From<Sf::Error>,
    {
        Server {
            control: self.control,
            handshake: self.handshake,
            publish: PipelineFactory::new(
                f.into_factory()
                    .map_err(Into::into)
                    .map_init_err(|e| ServerError::PublishService(ErrorInfo::from(error::Error::from(e)))),
            ),
        }
    }
}

impl<St, AppSt, Err> Server<St, AppSt, Err>
where
    St: 'static,
    AppSt: 'static,
    Err: 'static,
{
    async fn create(&self, st: &St, io: IoBoxed) -> Result<(), ServerError<Err>> {
        let cfg: Cfg<AmqpServiceConfig> = io.cfg().ctx().get();
        let fut = handshake(st, io, &self.handshake, cfg.clone());

        let (state, codec, sink, st, idle_timeout) = timeout_checked(cfg.handshake_timeout, fut)
            .await
            .map_err(|()| HandshakeError::Timeout)??;

        // create publish service
        let pb_svc = self.publish.create(st.clone()).await?;

        // create control service
        let ctl_svc = self.control.create(st).await?;

        IoDispatcher::new(
            state,
            codec,
            Pipeline::new((), Dispatcher::new(sink, pb_svc, ctl_svc, idle_timeout)),
        )
        .await
        .map_err(ServerError::Dispatcher)
    }
}

impl<St, AppSt, Err> Service<St, IoBoxed> for Server<St, AppSt, Err>
where
    St: 'static,
    AppSt: 'static,
    Err: 'static,
{
    type Res = ();
    type Error = ServerError<Err>;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        self.handshake.ready(ctx.st()).await.map_err(ServerError::Service)
    }

    #[inline]
    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        self.handshake.shutdown(ctx.st()).await;
    }

    async fn call(&self, req: IoBoxed, ctx: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
        self.create(ctx.st(), req).await
    }
}

impl<F, St, AppSt, Err> Service<St, Io<F>> for Server<St, AppSt, Err>
where
    F: Filter,
    St: 'static,
    AppSt: 'static,
    Err: 'static,
{
    type Res = ();
    type Error = ServerError<Err>;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        self.handshake.ready(ctx.st()).await.map_err(ServerError::Service)
    }

    #[inline]
    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        self.handshake.shutdown(ctx.st()).await;
    }

    #[inline]
    async fn call(&self, req: Io<F>, ctx: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
        self.create(ctx.st(), IoBoxed::from(req)).await
    }
}

async fn handshake<St, AppSt, Err>(
    st: &St,
    io: IoBoxed,
    handshake: &PipelineState<St, Handshake, HandshakeAck<AppSt>, Err>,
    cfg: Cfg<AmqpServiceConfig>,
) -> Result<(IoBoxed, AmqpCodec<AmqpFrame>, Connection, State<AppSt>, Millis), ServerError<Err>>
where
    St: 'static,
    AppSt: 'static,
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
                .call(
                    if protocol == ProtocolId::Amqp {
                        Handshake::new_plain(io, cfg.clone())
                    } else {
                        Handshake::new_sasl(io, cfg.clone())
                    },
                    st,
                )
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
