use ntex_io::IoBoxed;
use ntex_service::cfg::Cfg;
use ntex_util::time::Seconds;

use crate::codec::{AmqpCodec, AmqpFrame, protocol::Frame, protocol::Open};
use crate::{AmqpServiceConfig, RemoteServiceConfig, connection::Connection};

use super::{error::HandshakeError, sasl::Sasl};

#[derive(Debug)]
/// Connection handshake
pub enum Handshake<St = ()> {
    Amqp(HandshakeAmqp<St>),
    Sasl(Sasl<St>),
}

impl<St> Handshake<St> {
    pub(crate) fn new_plain(st: St, io: IoBoxed, local_config: Cfg<AmqpServiceConfig>) -> Self {
        Handshake::Amqp(HandshakeAmqp {
            st,
            io,
            local_config,
        })
    }

    pub(crate) fn new_sasl(st: St, io: IoBoxed, local_config: Cfg<AmqpServiceConfig>) -> Self {
        Handshake::Sasl(Sasl::new(st, io, local_config))
    }

    /// Returns reference to io object
    pub fn io(&self) -> &IoBoxed {
        match self {
            Handshake::Amqp(item) => item.io(),
            Handshake::Sasl(item) => item.io(),
        }
    }
}

#[derive(Debug)]
/// Open new connection
pub struct HandshakeAmqp<St> {
    st: St,
    io: IoBoxed,
    local_config: Cfg<AmqpServiceConfig>,
}

impl<St> HandshakeAmqp<St> {
    /// Returns reference to state object
    pub fn st(&self) -> &St {
        &self.st
    }

    /// Returns reference to io object
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

    /// Wait for connection open frame
    pub async fn open(self) -> Result<HandshakeAmqpOpened<St>, HandshakeError> {
        let HandshakeAmqp {
            st,
            io,
            local_config,
        } = self;

        let codec = AmqpCodec::<AmqpFrame>::new();
        let frame = io.recv(&codec).await?.ok_or_else(|| {
            log::trace!(
                "{}: Server amqp is disconnected during open frame",
                io.tag()
            );
            HandshakeError::Disconnected(None)
        })?;

        let frame = frame.into_parts().1;
        match frame {
            Frame::Open(frame) => {
                log::trace!("{}: Got open frame: {:?}", io.tag(), frame);
                let remote_config = RemoteServiceConfig::new(&frame);
                let sink = Connection::new(io.get_ref(), &local_config, &remote_config);
                Ok(HandshakeAmqpOpened::new(
                    st,
                    io,
                    frame,
                    sink,
                    local_config,
                    remote_config,
                ))
            }
            frame => Err(HandshakeError::Unexpected(frame)),
        }
    }
}

/// Connection is opened
pub struct HandshakeAmqpOpened<St> {
    st: St,
    io: IoBoxed,
    frame: Open,
    sink: Connection,
    local_config: Cfg<AmqpServiceConfig>,
    remote_config: RemoteServiceConfig,
}

impl<St> HandshakeAmqpOpened<St> {
    pub(crate) fn new(
        st: St,
        io: IoBoxed,
        frame: Open,
        sink: Connection,
        local_config: Cfg<AmqpServiceConfig>,
        remote_config: RemoteServiceConfig,
    ) -> Self {
        Self {
            st,
            io,
            frame,
            sink,
            local_config,
            remote_config,
        }
    }

    /// Returns reference to state object
    pub fn st(&self) -> &St {
        &self.st
    }

    /// Returns reference to io object
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

    /// Get reference to remote `Open` frame
    pub fn frame(&self) -> &Open {
        &self.frame
    }

    /// Get local configuration
    pub fn local_config(&self) -> &Cfg<AmqpServiceConfig> {
        &self.local_config
    }

    /// Get remote configuration
    pub fn remote_config(&self) -> &RemoteServiceConfig {
        &self.remote_config
    }

    /// Connection sink
    pub fn sink(&self) -> &Connection {
        &self.sink
    }

    /// Ack connect message and set state
    pub fn ack<AppSt>(self, st: AppSt) -> HandshakeAck<AppSt> {
        HandshakeAck {
            st,
            io: self.io,
            sink: self.sink,
            idle_timeout: self.remote_config.timeout_remote_secs(),
        }
    }
}

/// Handshake ack message
pub struct HandshakeAck<AppSt> {
    st: AppSt,
    io: IoBoxed,
    sink: Connection,
    idle_timeout: Seconds,
}

impl<AppSt> HandshakeAck<AppSt> {
    pub(crate) fn into_inner(self) -> (AppSt, Connection, Seconds, IoBoxed) {
        (self.st, self.sink, self.idle_timeout, self.io)
    }
}
