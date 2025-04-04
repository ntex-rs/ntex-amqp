use std::{future::Future, marker::PhantomData};

use ntex::connect::{self, Address, Connect};
use ntex::io::IoBoxed;
use ntex::service::{Pipeline, Service};
use ntex::time::{timeout_checked, Seconds};
use ntex::util::{ByteString, PoolId, PoolRef};

use crate::codec::protocol::{Frame, ProtocolId, SaslCode, SaslFrameBody, SaslInit};
use crate::codec::{types::Symbol, AmqpCodec, AmqpFrame, ProtocolIdCodec, SaslFrame};
use crate::{error::ProtocolIdError, Configuration, Connection};

use super::{connection::Client, error::ConnectError, SaslAuth};

/// Amqp client connector
pub struct Connector<A, T = ()> {
    connector: Pipeline<T>,
    config: Configuration,
    pool: PoolRef,
    _t: PhantomData<A>,
}

impl<A> Connector<A> {
    #[allow(clippy::new_ret_no_self)]
    /// Create new amqp connector
    pub fn new() -> Connector<A, connect::Connector<A>> {
        Connector {
            connector: Pipeline::new(connect::Connector::default().tag("AMQP-CLIENT")),
            config: Configuration::default(),
            pool: PoolId::P6.pool_ref(),
            _t: PhantomData,
        }
    }
}

impl<A, T> Connector<A, T>
where
    A: Address,
{
    /// Modify client configuration
    pub fn config<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut Configuration),
    {
        f(&mut self.config);
        self
    }

    /// The channel-max value is the highest channel number that
    /// may be used on the Connection. This value plus one is the maximum
    /// number of Sessions that can be simultaneously active on the Connection.
    ///
    /// By default channel max value is set to 1024
    pub fn channel_max(&mut self, num: u16) -> &mut Self {
        self.config.channel_max = num;
        self
    }

    /// Set max frame size for the connection.
    ///
    /// By default max size is set to 64kb
    pub fn max_frame_size(&mut self, size: u32) -> &mut Self {
        self.config.max_frame_size = size;
        self
    }

    /// Get max frame size for the connection.
    pub fn get_max_frame_size(&self) -> usize {
        self.config.max_frame_size as usize
    }

    /// Set idle time-out for the connection.
    ///
    /// By default idle time-out is set to 120 seconds
    pub fn idle_timeout(&mut self, timeout: Seconds) -> &mut Self {
        self.config.idle_time_out = (timeout.seconds() * 1000) as u32;
        self
    }

    /// Set connection hostname
    ///
    /// Hostname is not set by default
    pub fn hostname(&mut self, hostname: &str) -> &mut Self {
        self.config.hostname = Some(ByteString::from(hostname));
        self
    }

    /// Set handshake timeout.
    ///
    /// Handshake includes `connect` packet and response `connect-ack`.
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.config.handshake_timeout = timeout;
        self
    }

    /// Set client connection disconnect timeout.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 3 seconds.
    pub fn disconnect_timeout(self, timeout: Seconds) -> Self {
        self.config.disp_config.set_disconnect_timeout(timeout);
        self
    }

    /// Set memory pool.
    ///
    /// Use specified memory pool for memory allocations. By default P6
    /// memory pool is used.
    pub fn memory_pool(mut self, id: PoolId) -> Self {
        self.pool = id.pool_ref();
        self
    }

    /// Use custom connector
    pub fn connector<U>(self, connector: Pipeline<U>) -> Connector<A, U>
    where
        U: Service<Connect<A>, Error = connect::ConnectError>,
        IoBoxed: From<U::Response>,
    {
        Connector {
            connector,
            config: self.config,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    #[doc(hidden)]
    #[deprecated]
    /// Use custom connector
    pub fn boxed_connector<U>(self, connector: Pipeline<U>) -> Connector<A, U>
    where
        U: Service<Connect<A>, Response = IoBoxed, Error = connect::ConnectError>,
    {
        self.connector(connector)
    }
}

impl<A, T> Connector<A, T>
where
    A: Address,
    T: Service<Connect<A>, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    /// Connect to amqp server
    pub async fn connect(&self, address: A) -> Result<Client, ConnectError> {
        timeout_checked(self.config.handshake_timeout, self._connect(address))
            .await
            .map_err(|_| ConnectError::HandshakeTimeout)
            .and_then(|res| res)
    }

    /// Negotiate amqp protocol over opened socket
    pub fn negotiate(&self, io: IoBoxed) -> impl Future<Output = Result<Client, ConnectError>> {
        log::trace!("{}: Negotiation client protocol id: Amqp", io.tag());

        io.set_memory_pool(self.pool);
        _connect_plain(io, self.config.clone())
    }

    async fn _connect(&self, address: A) -> Result<Client, ConnectError> {
        let io = self.connector.call(Connect::new(address)).await?;
        let config = self.config.clone();
        let pool = self.pool;

        let io = IoBoxed::from(io);
        io.set_memory_pool(pool);

        _connect_plain(io, config).await
    }

    /// Connect to amqp server
    pub async fn connect_sasl(&self, addr: A, auth: SaslAuth) -> Result<Client, ConnectError> {
        timeout_checked(
            self.config.handshake_timeout,
            self._connect_sasl(addr, auth),
        )
        .await
        .map_err(|_| ConnectError::HandshakeTimeout)
        .and_then(|res| res)
    }

    /// Negotiate amqp sasl protocol over opened socket
    pub fn negotiate_sasl(
        &self,
        io: IoBoxed,
        auth: SaslAuth,
    ) -> impl Future<Output = Result<Client, ConnectError>> {
        log::trace!("{}: Negotiation client protocol id: Amqp", io.tag());

        let config = self.config.clone();
        io.set_memory_pool(self.pool);

        _connect_sasl(io, auth, config)
    }

    async fn _connect_sasl(&self, addr: A, auth: SaslAuth) -> Result<Client, ConnectError> {
        let io = self.connector.call(Connect::new(addr)).await?;
        let config = self.config.clone();
        let pool = self.pool;

        let io = IoBoxed::from(io);
        io.set_memory_pool(pool);

        _connect_sasl(io, auth, config).await
    }
}

async fn _connect_sasl(
    io: IoBoxed,
    auth: SaslAuth,
    config: Configuration,
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

    _connect_plain(io, config).await
}

async fn _connect_plain(io: IoBoxed, config: Configuration) -> Result<Client, ConnectError> {
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

    let open = config.to_open();
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
        let remote_config = config.from_remote(open);
        let connection = Connection::new(io.get_ref(), &config, &remote_config);
        let client = Client::new(io, codec, connection, remote_config);
        Ok(client)
    } else {
        Err(ConnectError::ExpectOpenFrame(Box::new(frame)))
    }
}
