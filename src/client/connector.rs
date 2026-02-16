use std::marker::PhantomData;

use ntex_bytes::ByteString;
use ntex_io::IoBoxed;
use ntex_net::connect::{self, Address};
use ntex_service::{Service, ServiceCtx, ServiceFactory, cfg::Cfg, cfg::SharedCfg};
use ntex_util::time::timeout_checked;

use crate::codec::protocol::{Frame, ProtocolId, SaslCode, SaslFrameBody, SaslInit};
use crate::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, SaslFrame, types::Symbol};
use crate::{AmqpServiceConfig, Connection, RemoteServiceConfig, error::ProtocolIdError};

use super::{Connect, SaslAuth, connection::Client, error::ConnectError};

/// Amqp client connector
pub struct Connector<A, T = ()> {
    connector: T,
    _t: PhantomData<A>,
}

/// Amqp client connector
pub struct ConnectorService<A, T> {
    connector: T,
    config: Cfg<AmqpServiceConfig>,
    _t: PhantomData<A>,
}

impl<A> Connector<A> {
    /// Create new amqp connector
    pub fn new() -> Connector<A, connect::Connector<A>> {
        Connector {
            connector: connect::Connector::default(),
            _t: PhantomData,
        }
    }
}

impl<A, T> Connector<A, T>
where
    A: Address,
    T: ServiceFactory<connect::Connect<A>, SharedCfg, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    /// Create new amqp connector
    pub fn with(connector: T) -> Connector<A, T> {
        Connector {
            connector,
            _t: PhantomData,
        }
    }
}

impl<A, T> Connector<A, T>
where
    A: Address,
{
    /// Use custom connector
    pub fn connector<U>(self, connector: U) -> Connector<A, U>
    where
        U: ServiceFactory<connect::Connect<A>, SharedCfg, Error = connect::ConnectError>,
        IoBoxed: From<U::Response>,
    {
        Connector {
            connector,
            _t: PhantomData,
        }
    }
}

impl<A, T> ServiceFactory<Connect<A>, SharedCfg> for Connector<A, T>
where
    A: Address,
    T: ServiceFactory<connect::Connect<A>, SharedCfg, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    type Response = Client;
    type Error = ConnectError;
    type Service = ConnectorService<A, T::Service>;
    type InitError = T::InitError;

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        Ok(ConnectorService {
            config: cfg.get(),
            connector: self.connector.create(cfg).await?,
            _t: PhantomData,
        })
    }
}

impl<A, T> Service<Connect<A>> for ConnectorService<A, T>
where
    A: Address,
    T: Service<connect::Connect<A>, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    type Response = Client;
    type Error = ConnectError;

    /// Connect to amqp server
    async fn call(
        &self,
        req: Connect<A>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Client, ConnectError> {
        let fut = async {
            let (addr, sasl, hostname) = req.into_parts();
            let io = ctx
                .call(&self.connector, connect::Connect::new(addr))
                .await?;
            let io = IoBoxed::from(io);

            if let Some(auth) = sasl {
                connect_sasl_inner(io, auth, self.config.clone(), hostname).await
            } else {
                connect_plain_inner(io, self.config.clone(), hostname).await
            }
        };
        timeout_checked(self.config.handshake_timeout, fut)
            .await
            .map_err(|()| ConnectError::HandshakeTimeout)
            .and_then(|res| res)
    }
}

impl<A, T> ConnectorService<A, T>
where
    A: Address,
    T: Service<connect::Connect<A>, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    /// Negotiate amqp protocol over opened socket
    pub async fn negotiate(
        &self,
        io: IoBoxed,
        hostname: Option<ByteString>,
    ) -> Result<Client, ConnectError> {
        log::trace!("{}: Negotiation client protocol id: Amqp", io.tag());

        connect_plain_inner(io, self.config.clone(), hostname).await
    }

    /// Negotiate amqp sasl protocol over opened socket
    pub async fn negotiate_sasl(
        &self,
        io: IoBoxed,
        auth: SaslAuth,
        hostname: Option<ByteString>,
    ) -> Result<Client, ConnectError> {
        log::trace!("{}: Negotiation client protocol id: Amqp", io.tag());

        connect_sasl_inner(io, auth, self.config.clone(), hostname).await
    }
}

async fn connect_sasl_inner(
    io: IoBoxed,
    auth: SaslAuth,
    config: Cfg<AmqpServiceConfig>,
    hostname: Option<ByteString>,
) -> Result<Client, ConnectError> {
    log::trace!("{}: Negotiation client protocol id: AmqpSasl", io.tag());

    io.send(ProtocolId::AmqpSasl, &ProtocolIdCodec).await?;

    let proto = io.recv(&ProtocolIdCodec).await?.ok_or_else(|| {
        log::trace!("{}: Amqp server is disconnected during handshake", io.tag());
        ConnectError::Disconnected
    })?;

    if proto != ProtocolId::AmqpSasl {
        return Err(ConnectError::from(ProtocolIdError::Unexpected {
            exp: ProtocolId::AmqpSasl,
            got: proto,
        }));
    }

    let codec = AmqpCodec::<SaslFrame>::new();

    // processing sasl-mechanisms
    let _ = io.recv(&codec).await?.ok_or(ConnectError::Disconnected)?;

    let initial_response =
        SaslInit::prepare_response(&auth.authz_id, &auth.authn_id, &auth.password);

    let sasl_init = SaslInit {
        hostname: config.hostname.clone(),
        mechanism: Symbol::from("PLAIN"),
        initial_response: Some(initial_response),
    };

    io.send(sasl_init.into(), &codec).await?;

    // processing sasl-outcome
    let sasl_frame = io.recv(&codec).await?.ok_or(ConnectError::Disconnected)?;

    if let SaslFrame {
        body: SaslFrameBody::SaslOutcome(outcome),
    } = sasl_frame
    {
        if outcome.code() != SaslCode::Ok {
            return Err(ConnectError::Sasl(outcome.code()));
        }
    } else {
        return Err(ConnectError::Disconnected);
    }

    connect_plain_inner(io, config, hostname).await
}

async fn connect_plain_inner(
    io: IoBoxed,
    config: Cfg<AmqpServiceConfig>,
    hostname: Option<ByteString>,
) -> Result<Client, ConnectError> {
    log::trace!("{}: Negotiation client protocol id: Amqp", io.tag());

    io.send(ProtocolId::Amqp, &ProtocolIdCodec).await?;

    let proto = io
        .recv(&ProtocolIdCodec)
        .await
        .map_err(ConnectError::from)?
        .ok_or_else(|| {
            log::trace!("{}: Amqp server is disconnected during handshake", io.tag());
            ConnectError::Disconnected
        })?;

    if proto != ProtocolId::Amqp {
        return Err(ConnectError::from(ProtocolIdError::Unexpected {
            exp: ProtocolId::Amqp,
            got: proto,
        }));
    }

    let mut open = config.to_open();
    if let Some(hostname) = hostname {
        *open.hostname_mut() = Some(hostname);
    }
    let codec = AmqpCodec::<AmqpFrame>::new().max_size(config.max_frame_size as usize);

    log::trace!("{}: Open client amqp connection: {:?}", io.tag(), open);
    io.send(AmqpFrame::new(0, Frame::Open(open)), &codec)
        .await?;

    let frame = io.recv(&codec).await?.ok_or_else(|| {
        log::trace!("{}: Amqp server is disconnected during handshake", io.tag());
        ConnectError::Disconnected
    })?;

    if let Frame::Open(open) = frame.performative() {
        log::trace!("{}: Open confirmed: {:?}", io.tag(), open);
        let remote_config = RemoteServiceConfig::new(open);
        let connection = Connection::new(io.get_ref(), config, &remote_config);
        let client = Client::new(io, codec, connection, remote_config);
        Ok(client)
    } else {
        Err(ConnectError::ExpectOpenFrame(Box::new(frame)))
    }
}
