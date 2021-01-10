use std::num::{NonZeroU16, NonZeroU32};
use std::time::Duration;

use bytes::Bytes;
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

use crate::codec::protocol::Milliseconds;
use crate::{errors::AmqpProtocolError, io::IoState, utils::Select, Configuration};

use super::{connection::Client, error::ClientError};

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
        // let fut = self.connector.call(Connect::new(self.address.clone()));
        // let disconnect_timeout = self.disconnect_timeout;

        // async move {
        //     let mut io = fut.await?;
        //     let state = IoState::new(codec::Codec::new().max_inbound_size(max_packet_size));

        //     state.send(&mut io, codec::Packet::Connect(pkt)).await?;

        //     let packet = state
        //         .next(&mut io)
        //         .await
        //         .map_err(|e| ClientError::from(ProtocolError::from(e)))
        //         .and_then(|res| {
        //             res.ok_or_else(|| {
        //                 log::trace!("Mqtt server is disconnected during handshake");
        //                 ClientError::Disconnected
        //             })
        //         })?;

        //     match packet {
        //         codec::Packet::ConnectAck(pkt) => {
        //             log::trace!("Connect ack response from server: {:#?}", pkt);
        //             if pkt.reason_code == codec::ConnectAckReason::Success {
        //                 // set max outbound (encoder) packet size
        //                 if let Some(size) = pkt.max_packet_size {
        //                     state.with_codec(|codec| codec.set_max_outbound_size(size));
        //                 }
        //                 // server keep-alive
        //                 let keep_alive = pkt.server_keepalive_sec.unwrap_or(keep_alive);

        //                 Ok(Client::new(
        //                     io,
        //                     state,
        //                     pkt,
        //                     max_receive,
        //                     keep_alive,
        //                     disconnect_timeout,
        //                 ))
        //             } else {
        //                 Err(ClientError::Ack(pkt))
        //             }
        //         }
        //         p => Err(ProtocolError::Unexpected(
        //             p.packet_type(),
        //             "Expected CONNECT-ACK packet",
        //         )
        //         .into()),
        //     }
        // }
    }
}
