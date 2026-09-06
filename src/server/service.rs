use ntex_dispatcher::Dispatcher as IoDispatcher;
use ntex_error::{self as error, ErrorDiagnostic, ErrorInfo};
use ntex_io::IoBoxed;
use ntex_service::cfg::{Cfg, Configuration};
use ntex_service::pipeline::{Pipeline, PipelineFactory, PipelineState};
use ntex_service::{Ctx, IntoService, IntoServiceFactory, RequestState, Service, ServiceFactory};
use ntex_util::time::{Millis, timeout_checked};

use crate::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError, protocol::ProtocolId};
use crate::{AmqpServiceConfig, Connection, ControlFrame, State, default::DefaultControlService};
use crate::{dispatcher::Dispatcher, types::Message};

use super::handshake::{Handshake, HandshakeAck};
use super::{Error, HandshakeError, ServerError};

/// Amqp server factory
pub struct Server<St, AppSt, Req: RequestState<IoBoxed>, Err> {
    handshake: PipelineState<St, Handshake<Req::State>, HandshakeAck<AppSt>, Err>,
    control: PipelineFactory<State<AppSt>, ControlFrame, (), Error, ServerError<Err>>,
    publish: PipelineFactory<State<AppSt>, Message, (), Error, ServerError<Err>>,
}

/// Amqp server builder
pub struct ServerBuilder<St, AppSt, Req: RequestState<IoBoxed>, Err> {
    handshake: PipelineState<St, Handshake<Req::State>, HandshakeAck<AppSt>, Err>,
    control: PipelineFactory<State<AppSt>, ControlFrame, (), Error, ServerError<Err>>,
}

impl<St, AppSt, Req> Server<St, AppSt, Req, ()>
where
    St: 'static,
    AppSt: 'static,
    Req: RequestState<IoBoxed>,
    Req::State: Clone,
{
    /// Start server building process with provided handshake service
    pub fn build<F, H>(f: F) -> ServerBuilder<St, AppSt, Req, H::Error>
    where
        F: IntoService<H, St, Handshake<Req::State>>,
        H: Service<St, Handshake<Req::State>, Res = HandshakeAck<AppSt>> + 'static,
    {
        ServerBuilder {
            handshake: PipelineState::new(f.into_service()),
            control: PipelineFactory::new(
                DefaultControlService::default().map_init_err(ServerError::ControlService),
            ),
        }
    }
}

impl<St, AppSt, Req, Err> ServerBuilder<St, AppSt, Req, Err>
where
    St: 'static,
    AppSt: 'static,
    Req: RequestState<IoBoxed>,
    Req::State: Clone,
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
            control: PipelineFactory::new(f.into_factory().map_err(Into::into).map_init_err(|e| {
                ServerError::ControlService(ErrorInfo::from(error::Error::from(e)))
            })),
        }
    }

    /// Set service to execute for incoming links and create service factory
    pub fn finish<Sf>(
        self,
        f: impl IntoServiceFactory<Sf, State<AppSt>, Message>,
    ) -> Server<St, AppSt, Req, Err>
    where
        Sf: ServiceFactory<State<AppSt>, Message, Res = ()> + 'static,
        Sf::InitError: ErrorDiagnostic,
        Error: From<Sf::Error>,
    {
        Server {
            control: self.control,
            handshake: self.handshake,
            publish: PipelineFactory::new(f.into_factory().map_err(Into::into).map_init_err(|e| {
                ServerError::PublishService(ErrorInfo::from(error::Error::from(e)))
            })),
        }
    }
}

impl<St, AppSt, Req, Err> Server<St, AppSt, Req, Err>
where
    St: 'static,
    AppSt: 'static,
    Req: RequestState<IoBoxed>,
    Req::State: Clone,
    Err: 'static,
{
    async fn create(&self, st: &St, req: Req) -> Result<(), ServerError<Err>> {
        let (req, io) = req.unpack();
        let cfg: Cfg<AmqpServiceConfig> = io.cfg().ctx().get();
        let fut = handshake(st, req, io, &self.handshake, cfg.clone());

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

impl<St, AppSt, Req, Err> Service<St, Req> for Server<St, AppSt, Req, Err>
where
    St: 'static,
    AppSt: 'static,
    Req: RequestState<IoBoxed>,
    Req::State: Clone,
    Err: 'static,
{
    type Res = ();
    type Error = ServerError<Err>;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        self.handshake
            .ready(ctx.st())
            .await
            .map_err(ServerError::Service)
    }

    #[inline]
    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        self.handshake.shutdown(ctx.st()).await;
    }

    async fn call(&self, req: Req, ctx: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
        self.create(ctx.st(), req).await
    }
}

async fn handshake<St, ReqSt, AppSt, Err>(
    st: &St,
    req: ReqSt,
    io: IoBoxed,
    handshake: &PipelineState<St, Handshake<ReqSt>, HandshakeAck<AppSt>, Err>,
    cfg: Cfg<AmqpServiceConfig>,
) -> Result<
    (
        IoBoxed,
        AmqpCodec<AmqpFrame>,
        Connection,
        State<AppSt>,
        Millis,
    ),
    ServerError<Err>,
>
where
    St: 'static,
    ReqSt: 'static,
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
                        Handshake::new_plain(req, io, cfg.clone())
                    } else {
                        Handshake::new_sasl(req, io, cfg.clone())
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
