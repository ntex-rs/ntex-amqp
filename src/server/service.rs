use std::{fmt, marker, rc::Rc};

use ntex::io::{Dispatcher as FramedDispatcher, Filter, Io, IoBoxed};
use ntex::service::{IntoServiceFactory, Pipeline, Service, ServiceCtx, ServiceFactory};
use ntex::time::{timeout_checked, Millis};

use crate::codec::{protocol::ProtocolId, AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError};
use crate::{default::DefaultControlService, Configuration, Connection, ControlFrame, State};
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
    config: Configuration,
    _t: marker::PhantomData<St>,
}

pub(super) struct ServerInner<St, Ctl, Pb> {
    control: Ctl,
    publish: Pb,
    config: Rc<Configuration>,
    _t: marker::PhantomData<St>,
}

impl<St> Server<St, (), (), ()>
where
    St: 'static,
{
    /// Start server building process with provided handshake service
    pub fn build<F, H>(handshake: F) -> ServerBuilder<St, H, DefaultControlService<St, H::Error>>
    where
        F: IntoServiceFactory<H, Handshake>,
        H: ServiceFactory<Handshake, Response = HandshakeAck<St>>,
    {
        Server::with_config(Configuration::default(), handshake)
    }

    /// Start server building process with provided handshake service
    pub fn with_config<F, H>(
        config: Configuration,
        handshake: F,
    ) -> ServerBuilder<St, H, DefaultControlService<St, H::Error>>
    where
        F: IntoServiceFactory<H, Handshake>,
        H: ServiceFactory<Handshake, Response = HandshakeAck<St>>,
    {
        ServerBuilder {
            config,
            handshake: handshake.into_factory(),
            control: DefaultControlService::default(),
            _t: marker::PhantomData,
        }
    }
}

impl<St, H, Ctl> ServerBuilder<St, H, Ctl>
where
    St: 'static,
    H: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Error: From<Ctl::Error>,
{
    /// Modify server configuration
    pub fn config<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Configuration),
    {
        f(&mut self.config);
        self
    }

    /// Service to call with control frames
    pub fn control<F, S>(self, service: F) -> ServerBuilder<St, H, S>
    where
        F: IntoServiceFactory<S, ControlFrame, State<St>>,
        S: ServiceFactory<ControlFrame, State<St>, Response = ()> + 'static,
        S::InitError: fmt::Debug,
        Error: From<S::Error>,
    {
        ServerBuilder {
            config: self.config,
            handshake: self.handshake,
            control: service.into_factory(),
            _t: marker::PhantomData,
        }
    }

    /// Set service to execute for incoming links and create service factory
    pub fn finish<S, Pb>(self, service: S) -> Server<St, H, Ctl, Pb>
    where
        S: IntoServiceFactory<Pb, Message, State<St>>,
        Pb: ServiceFactory<Message, State<St>, Response = ()> + 'static,
        Pb::InitError: fmt::Debug,
        Error: From<Pb::Error> + From<Ctl::Error>,
    {
        Server {
            handshake: self.handshake,
            inner: Rc::new(ServerInner {
                config: Rc::new(self.config),
                publish: service.into_factory(),
                control: self.control,
                _t: marker::PhantomData,
            }),
        }
    }
}

impl<F, St, H, Ctl, Pb> ServiceFactory<Io<F>> for Server<St, H, Ctl, Pb>
where
    F: Filter,
    St: 'static,
    H: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Message, State<St>, Response = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Response = ();
    type Error = ServerError<H::Error>;
    type Service = ServerHandler<St, H::Service, Ctl, Pb>;
    type InitError = H::InitError;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        self.handshake
            .pipeline(())
            .await
            .map(move |handshake| ServerHandler {
                handshake,
                inner: self.inner.clone(),
            })
    }
}

impl<St, H, Ctl, Pb> ServiceFactory<IoBoxed> for Server<St, H, Ctl, Pb>
where
    St: 'static,
    H: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Message, State<St>, Response = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Response = ();
    type Error = ServerError<H::Error>;
    type Service = ServerHandler<St, H::Service, Ctl, Pb>;
    type InitError = H::InitError;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        self.handshake
            .pipeline(())
            .await
            .map(move |handshake| ServerHandler {
                handshake,
                inner: self.inner.clone(),
            })
    }
}

/// Amqp connections handler
pub struct ServerHandler<St, H, Ctl, Pb> {
    handshake: Pipeline<H>,
    inner: Rc<ServerInner<St, Ctl, Pb>>,
}

impl<St, H, Ctl, Pb> ServerHandler<St, H, Ctl, Pb>
where
    St: 'static,
    H: Service<Handshake, Response = HandshakeAck<St>> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Message, State<St>, Response = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    async fn create(&self, req: IoBoxed) -> Result<(), ServerError<H::Error>> {
        let fut = handshake(req, &self.handshake, &self.inner);
        let inner = self.inner.clone();

        let (state, codec, sink, st, idle_timeout) =
            timeout_checked(inner.config.handshake_timeout, fut)
                .await
                .map_err(|_| HandshakeError::Timeout)??;

        // create publish service
        let pb_srv = inner.publish.pipeline(st.clone()).await.map_err(|e| {
            log::error!("Publish service init error: {:?}", e);
            ServerError::PublishServiceError
        })?;

        // create control service
        let ctl_srv = inner.control.pipeline(st.clone()).await.map_err(|e| {
            log::error!("Control service init error: {:?}", e);
            ServerError::ControlServiceError
        })?;

        FramedDispatcher::new(
            state,
            codec,
            Dispatcher::new(sink, pb_srv, ctl_srv, idle_timeout),
            &inner.config.disp_config,
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
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Message, State<St>, Response = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Response = ();
    type Error = ServerError<H::Error>;

    ntex::forward_poll_ready!(handshake, ServerError::Service);
    ntex::forward_poll_shutdown!(handshake);

    async fn call(
        &self,
        req: Io<F>,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        self.create(IoBoxed::from(req)).await
    }
}

impl<St, H, Ctl, Pb> Service<IoBoxed> for ServerHandler<St, H, Ctl, Pb>
where
    St: 'static,
    H: Service<Handshake, Response = HandshakeAck<St>> + 'static,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = ()> + 'static,
    Ctl::InitError: fmt::Debug,
    Pb: ServiceFactory<Message, State<St>, Response = ()> + 'static,
    Pb::InitError: fmt::Debug,
    Error: From<Pb::Error> + From<Ctl::Error>,
{
    type Response = ();
    type Error = ServerError<H::Error>;

    ntex::forward_poll_ready!(handshake, ServerError::Service);
    ntex::forward_poll_shutdown!(handshake);

    async fn call(
        &self,
        req: IoBoxed,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        self.create(req).await
    }
}

async fn handshake<St, H, Ctl, Pb>(
    io: IoBoxed,
    handshake: &Pipeline<H>,
    inner: &ServerInner<St, Ctl, Pb>,
) -> Result<(IoBoxed, AmqpCodec<AmqpFrame>, Connection, State<St>, Millis), ServerError<H::Error>>
where
    St: 'static,
    H: Service<Handshake, Response = HandshakeAck<St>>,
    Ctl: ServiceFactory<ControlFrame, State<St>, Response = ()> + 'static,
    Pb: ServiceFactory<Message, State<St>, Response = ()> + 'static,
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
                    Handshake::new_plain(io, inner.config.clone())
                } else {
                    Handshake::new_sasl(io, inner.config.clone())
                })
                .await
                .map_err(ServerError::Service)?;

            let (st, sink, idle_timeout, io) = ack.into_inner();

            let codec = AmqpCodec::new().max_size(inner.config.max_size);

            // confirm Open
            let local = inner.config.to_open();
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
