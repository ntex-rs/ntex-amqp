use std::rc::Rc;

use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::framed::State;

use crate::codec::protocol::{Frame, Open};
use crate::codec::{AmqpCodec, AmqpFrame};
use crate::{connection::Connection, Configuration};

use super::{error::HandshakeError, sasl::Sasl};

/// Connection handshake
pub enum Handshake<Io> {
    Amqp(HandshakeAmqp<Io>),
    Sasl(Sasl<Io>),
}

impl<Io> Handshake<Io> {
    pub(crate) fn new_plain(io: Io, state: State, local_config: Rc<Configuration>) -> Self {
        Handshake::Amqp(HandshakeAmqp {
            io,
            state,
            local_config,
        })
    }

    pub(crate) fn new_sasl(io: Io, state: State, local_config: Rc<Configuration>) -> Self {
        Handshake::Sasl(Sasl::new(io, state, local_config))
    }
}

/// Open new connection
pub struct HandshakeAmqp<Io> {
    io: Io,
    state: State,
    local_config: Rc<Configuration>,
}

impl<Io> HandshakeAmqp<Io> {
    /// Returns reference to io object
    pub fn get_ref(&self) -> &Io {
        &self.io
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        &mut self.io
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> HandshakeAmqp<Io> {
    /// Wait for connection open frame
    pub async fn open(self) -> Result<HandshakeAmqpOpened<Io>, HandshakeError> {
        let mut io = self.io;
        let state = self.state;
        let local_config = self.local_config;
        let codec = AmqpCodec::<AmqpFrame>::new();

        let frame = state
            .next(&mut io, &codec)
            .await
            .map_err(HandshakeError::from)?
            .ok_or_else(|| {
                log::trace!("Server amqp is disconnected during open frame");
                HandshakeError::Disconnected
            })?;

        let frame = frame.into_parts().1;
        match frame {
            Frame::Open(frame) => {
                trace!("Got open frame: {:?}", frame);
                let remote_config = (&frame).into();
                let sink = Connection::new(state.clone(), &local_config, &remote_config);
                Ok(HandshakeAmqpOpened {
                    frame,
                    io,
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
pub struct HandshakeAmqpOpened<Io> {
    frame: Open,
    io: Io,
    sink: Connection,
    state: State,
    local_config: Rc<Configuration>,
    remote_config: Configuration,
}

impl<Io> HandshakeAmqpOpened<Io> {
    pub(crate) fn new(
        frame: Open,
        io: Io,
        sink: Connection,
        state: State,
        local_config: Rc<Configuration>,
        remote_config: Configuration,
    ) -> Self {
        Self {
            frame,
            io,
            sink,
            state,
            local_config,
            remote_config,
        }
    }

    /// Get reference to remote `Open` frame
    pub fn frame(&self) -> &Open {
        &self.frame
    }

    /// Returns reference to io object
    pub fn get_ref(&self) -> &Io {
        &self.io
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        &mut self.io
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
    pub fn ack<St>(self, st: St) -> HandshakeAck<Io, St> {
        HandshakeAck {
            st,
            io: self.io,
            sink: self.sink,
            state: self.state,
            idle_timeout: self.remote_config.timeout_remote_secs(),
        }
    }
}

/// Handshake ack message
pub struct HandshakeAck<Io, St> {
    st: St,
    io: Io,
    sink: Connection,
    state: State,
    idle_timeout: usize,
}

impl<Io, St> HandshakeAck<Io, St> {
    pub(crate) fn into_inner(self) -> (St, Io, Connection, State, usize) {
        (self.st, self.io, self.sink, self.state, self.idle_timeout)
    }
}
