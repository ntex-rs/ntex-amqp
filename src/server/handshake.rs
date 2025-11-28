use std::rc::Rc;

use ntex::{io::IoBoxed, time::Seconds};

use crate::codec::protocol::{Frame, Open};
use crate::codec::{AmqpCodec, AmqpFrame};
use crate::{Configuration, connection::Connection};

use super::{error::HandshakeError, sasl::Sasl};

#[derive(Debug)]
/// Connection handshake
pub enum Handshake {
    Amqp(HandshakeAmqp),
    Sasl(Sasl),
}

impl Handshake {
    pub(crate) fn new_plain(state: IoBoxed, local_config: Rc<Configuration>) -> Self {
        Handshake::Amqp(HandshakeAmqp {
            state,
            local_config,
        })
    }

    pub(crate) fn new_sasl(state: IoBoxed, local_config: Rc<Configuration>) -> Self {
        Handshake::Sasl(Sasl::new(state, local_config))
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
pub struct HandshakeAmqp {
    state: IoBoxed,
    local_config: Rc<Configuration>,
}

impl HandshakeAmqp {
    /// Returns reference to io object
    pub fn io(&self) -> &IoBoxed {
        &self.state
    }

    /// Wait for connection open frame
    pub async fn open(self) -> Result<HandshakeAmqpOpened, HandshakeError> {
        let state = self.state;
        let local_config = self.local_config;
        let codec = AmqpCodec::<AmqpFrame>::new();

        let frame = state.recv(&codec).await?.ok_or_else(|| {
            log::trace!(
                "{}: Server amqp is disconnected during open frame",
                state.tag()
            );
            HandshakeError::Disconnected(None)
        })?;

        let frame = frame.into_parts().1;
        match frame {
            Frame::Open(frame) => {
                log::trace!("{}: Got open frame: {:?}", state.tag(), frame);
                let remote_config = local_config.from_remote(&frame);
                let sink = Connection::new(state.get_ref(), &local_config, &remote_config);
                Ok(HandshakeAmqpOpened {
                    frame,
                    sink,
                    state,
                    local_config,
                    remote_config,
                })
            }
            frame => Err(HandshakeError::Unexpected(frame)),
        }
    }
}

/// Connection is opened
pub struct HandshakeAmqpOpened {
    frame: Open,
    sink: Connection,
    state: IoBoxed,
    local_config: Rc<Configuration>,
    remote_config: Configuration,
}

impl HandshakeAmqpOpened {
    pub(crate) fn new(
        frame: Open,
        sink: Connection,
        state: IoBoxed,
        local_config: Rc<Configuration>,
        remote_config: Configuration,
    ) -> Self {
        Self {
            frame,
            sink,
            state,
            local_config,
            remote_config,
        }
    }

    /// Returns reference to io object
    pub fn io(&self) -> &IoBoxed {
        &self.state
    }

    /// Get reference to remote `Open` frame
    pub fn frame(&self) -> &Open {
        &self.frame
    }

    /// Get local configuration
    pub fn local_config(&self) -> &Configuration {
        self.local_config.as_ref()
    }

    /// Get remote configuration
    pub fn remote_config(&self) -> &Configuration {
        &self.remote_config
    }

    /// Connection sink
    pub fn sink(&self) -> &Connection {
        &self.sink
    }

    /// Ack connect message and set state
    pub fn ack<St>(self, st: St) -> HandshakeAck<St> {
        HandshakeAck {
            st,
            sink: self.sink,
            state: self.state,
            idle_timeout: self.remote_config.timeout_remote_secs(),
        }
    }
}

/// Handshake ack message
pub struct HandshakeAck<St> {
    st: St,
    sink: Connection,
    state: IoBoxed,
    idle_timeout: Seconds,
}

impl<St> HandshakeAck<St> {
    pub(crate) fn into_inner(self) -> (St, Connection, Seconds, IoBoxed) {
        (self.st, self.sink, self.idle_timeout, self.state)
    }
}
