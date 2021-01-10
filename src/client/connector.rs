use std::time::Duration;

use bytestring::ByteString;
use futures::future::Either;
use futures::{Future, FutureExt};
use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::connect::{self, Address, Connect, Connector};
use ntex::rt::time::delay_for;
use ntex::service::Service;

#[cfg(feature = "openssl")]
use ntex::connect::openssl::{OpensslConnector, SslConnector};

#[cfg(feature = "rustls")]
use ntex::connect::rustls::{ClientConfig, RustlsConnector};

use crate::codec::protocol::{Frame, Milliseconds, ProtocolId, SaslCode, SaslFrameBody, SaslInit};
use crate::codec::{types::Symbol, AmqpCodec, AmqpFrame, ProtocolIdCodec, SaslFrame};
use crate::{error::ProtocolIdError, io::IoState, utils::Select, Configuration, Connection};

use super::{connection::Client, error::ClientError, SaslAuth};

/// Amqp client connector
pub struct AmqpConnector<A, T> {
    address: A,
    connector: T,
    config: Configuration,
    handshake_timeout: u16,
    disconnect_timeout: u16,
}

impl<A> AmqpConnector<A, ()>
where
    A: Address + Clone,
{
    #[allow(clippy::new_ret_no_self)]
    /// Create new amqp connector
    pub fn new(address: A) -> AmqpConnector<A, Connector<A>> {
        AmqpConnector {
            address,
            connector: Connector::default(),
            handshake_timeout: 0,
            disconnect_timeout: 3000,
            config: Configuration::default(),
        }
    }
}

impl<A, T> AmqpConnector<A, T>
where
    A: Address + Clone,
    T: Service<Request = Connect<A>, Error = connect::ConnectError>,
    T::Response: AsyncRead + AsyncWrite + Unpin + 'static,
{
    /// The channel-max value is the highest channel number that
    /// may be used on the Connection. This value plus one is the maximum
    /// number of Sessions that can be simultaneously active on the Connection.
    ///
    /// By default channel max value is set to 1024
    pub fn channel_max(&mut self, num: u16) -> &mut Self {
        self.config.channel_max = num as usize;
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

    /// Set idle time-out for the connection in seconds.
    ///
    /// By default idle time-out is set to 120 seconds
    pub fn idle_timeout(&mut self, timeout: u16) -> &mut Self {
        self.config.idle_time_out = (timeout * 1000) as Milliseconds;
        self
    }

    /// Set connection hostname
    ///
    /// Hostname is not set by default
    pub fn hostname(&mut self, hostname: &str) -> &mut Self {
        self.config.hostname = Some(ByteString::from(hostname));
        self
    }

    /// Set handshake timeout in milliseconds.
    ///
    /// Handshake includes `connect` packet and response `connect-ack`.
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: u16) -> Self {
        self.handshake_timeout = timeout as u16;
        self
    }

    /// Set client connection disconnect timeout in milliseconds.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 3 seconds.
    pub fn disconnect_timeout(mut self, timeout: u16) -> Self {
        self.disconnect_timeout = timeout as u16;
        self
    }

    /// Use custom connector
    pub fn connector<U>(self, connector: U) -> AmqpConnector<A, U>
    where
        U: Service<Request = Connect<A>, Error = connect::ConnectError>,
        U::Response: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        AmqpConnector {
            connector,
            config: self.config,
            address: self.address,
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
        }
    }

    #[cfg(feature = "openssl")]
    /// Use openssl connector
    pub fn openssl(self, connector: SslConnector) -> AmqpConnector<A, OpensslConnector<A>> {
        AmqpConnector {
            config: self.config,
            address: self.address,
            connector: OpensslConnector::new(connector),
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
        }
    }

    #[cfg(feature = "rustls")]
    /// Use rustls connector
    pub fn rustls(self, config: ClientConfig) -> AmqpConnector<A, RustlsConnector<A>> {
        use std::sync::Arc;

        AmqpConnector {
            config: self.config,
            address: self.address,
            connector: RustlsConnector::new(Arc::new(config)),
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
        }
    }

    /// Connect to amqp server
    pub fn connect(&self) -> impl Future<Output = Result<Client<T::Response>, ClientError>> {
        if self.handshake_timeout > 0 {
            Either::Left(
                Select::new(
                    delay_for(Duration::from_millis(self.handshake_timeout as u64)),
                    self._connect(),
                )
                .map(|result| match result {
                    either::Either::Left(_) => Err(ClientError::HandshakeTimeout),
                    either::Either::Right(res) => res.map_err(From::from),
                }),
            )
        } else {
            Either::Right(self._connect())
        }
    }

    fn _connect(&self) -> impl Future<Output = Result<Client<T::Response>, ClientError>> {
        let fut = self.connector.call(Connect::new(self.address.clone()));
        let config = self.config.clone();
        let disconnect_timeout = self.disconnect_timeout;

        async move {
            trace!("Negotiation client protocol id: Amqp");

            let io = fut.await?;
            let state = IoState::new(ProtocolIdCodec);
            _connect_plain(io, state, config, disconnect_timeout).await
        }
    }

    /// Connect to amqp server
    pub fn connect_sasl(
        &self,
        auth: SaslAuth,
    ) -> impl Future<Output = Result<Client<T::Response>, ClientError>> {
        if self.handshake_timeout > 0 {
            Either::Left(
                Select::new(
                    delay_for(Duration::from_millis(self.handshake_timeout as u64)),
                    self._connect_sasl(auth),
                )
                .map(|result| match result {
                    either::Either::Left(_) => Err(ClientError::HandshakeTimeout),
                    either::Either::Right(res) => res.map_err(From::from),
                }),
            )
        } else {
            Either::Right(self._connect_sasl(auth))
        }
    }

    fn _connect_sasl(
        &self,
        auth: SaslAuth,
    ) -> impl Future<Output = Result<Client<T::Response>, ClientError>> {
        let fut = self.connector.call(Connect::new(self.address.clone()));
        let config = self.config.clone();
        let disconnect_timeout = self.disconnect_timeout;

        async move {
            trace!("Negotiation client protocol id: AmqpSasl");

            let mut io = fut.await?;
            let state = IoState::new(ProtocolIdCodec);
            state.send(&mut io, ProtocolId::AmqpSasl).await?;

            let proto = state
                .next(&mut io)
                .await
                .map_err(ClientError::from)
                .and_then(|res| {
                    res.ok_or_else(|| {
                        log::trace!("Amqp server is disconnected during handshake");
                        ClientError::Disconnected
                    })
                })?;
            if proto != ProtocolId::AmqpSasl {
                return Err(ClientError::from(ProtocolIdError::Unexpected {
                    exp: ProtocolId::AmqpSasl,
                    got: proto,
                }));
            }

            let state = state.map_codec(|_| AmqpCodec::<SaslFrame>::new());

            // processing sasl-mechanisms
            let _ = state
                .next(&mut io)
                .await
                .map_err(ClientError::from)
                .and_then(|res| res.ok_or(ClientError::Disconnected))?;

            let initial_response =
                SaslInit::prepare_response(&auth.authz_id, &auth.authn_id, &auth.password);

            let sasl_init = SaslInit {
                hostname: config.hostname.clone(),
                mechanism: Symbol::from("PLAIN"),
                initial_response: Some(initial_response),
            };

            state.send(&mut io, sasl_init.into()).await?;

            // processing sasl-outcome
            let sasl_frame = state
                .next(&mut io)
                .await
                .map_err(ClientError::from)
                .and_then(|res| res.ok_or(ClientError::Disconnected))?;

            if let SaslFrame {
                body: SaslFrameBody::SaslOutcome(outcome),
            } = sasl_frame
            {
                if outcome.code() != SaslCode::Ok {
                    return Err(ClientError::Sasl(outcome.code()));
                }
            } else {
                return Err(ClientError::Disconnected);
            }
            let state = state.map_codec(|_| ProtocolIdCodec);

            _connect_plain(io, state, config, disconnect_timeout).await
        }
    }
}

async fn _connect_plain<T>(
    mut io: T,
    state: IoState<ProtocolIdCodec>,
    config: Configuration,
    disconnect_timeout: u16,
) -> Result<Client<T>, ClientError>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
{
    trace!("Negotiation client protocol id: Amqp");

    state.send(&mut io, ProtocolId::Amqp).await?;

    let proto = state
        .next(&mut io)
        .await
        .map_err(ClientError::from)
        .and_then(|res| {
            res.ok_or_else(|| {
                log::trace!("Amqp server is disconnected during handshake");
                ClientError::Disconnected
            })
        })?;

    if proto != ProtocolId::Amqp {
        return Err(ClientError::from(ProtocolIdError::Unexpected {
            exp: ProtocolId::Amqp,
            got: proto,
        }));
    }

    let state =
        state.map_codec(|_| AmqpCodec::<AmqpFrame>::new().max_size(config.max_frame_size as usize));

    let open = config.to_open();
    trace!("Open client amqp connection: {:?}", open);
    state
        .send(&mut io, AmqpFrame::new(0, Frame::Open(open)))
        .await?;

    let frame = state
        .next(&mut io)
        .await
        .map_err(ClientError::from)
        .and_then(|res| {
            res.ok_or_else(|| {
                log::trace!("Amqp server is disconnected during handshake");
                ClientError::Disconnected
            })
        })?;

    if let Frame::Open(open) = frame.performative() {
        trace!("Open confirmed: {:?}", open);
        let remote_config = open.into();
        let connection = Connection::new(state.clone(), &config, &remote_config);
        let client = Client::new(
            io,
            state,
            connection,
            config.timeout_secs() as u16,
            disconnect_timeout,
            remote_config,
        );
        Ok(client)
    } else {
        Err(ClientError::ExpectOpenFrame(frame))
    }
}
