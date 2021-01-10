use std::rc::Rc;

use ntex::codec::{AsyncRead, AsyncWrite};
use ntex_amqp_codec::protocol::{Frame, Open};
use ntex_amqp_codec::{AmqpCodec, AmqpFrame, SaslFrame};

use super::{errors::ServerError, sasl::Sasl};
use crate::{connection::Connection, errors::AmqpProtocolError, io::IoState, Configuration};

/// Connection handshake
pub enum Handshake<Io> {
    Amqp(HandshakeAmqp<Io>),
    Sasl(Sasl<Io>),
}

impl<Io> Handshake<Io> {
    pub(crate) fn new_plain(
        io: Io,
        state: IoState<AmqpCodec<AmqpFrame>>,
        local_config: Rc<Configuration>,
    ) -> Self {
        Handshake::Amqp(HandshakeAmqp {
            io,
            state,
            local_config,
        })
    }

    pub(crate) fn new_sasl(
        io: Io,
        state: IoState<AmqpCodec<SaslFrame>>,
        local_config: Rc<Configuration>,
    ) -> Self {
        Handshake::Sasl(Sasl::new(io, state, local_config))
    }
}

/// Open new connection
pub struct HandshakeAmqp<Io> {
    io: Io,
    state: IoState<AmqpCodec<AmqpFrame>>,
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
    pub async fn open(self) -> Result<HandshakeAmqpOpened<Io>, ServerError<()>> {
        let mut io = self.io;
        let state = self.state;
        let local_config = self.local_config;

        let frame = state
            .next(&mut io)
            .await
            .map_err(ServerError::from)?
            .ok_or_else(|| {
                log::trace!("Server amqp is disconnected during open frame");
                ServerError::Disconnected
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
            frame => Err(AmqpProtocolError::Unexpected(Box::new(frame)).into()),
        }
    }
}

/// Connection is opened
pub struct HandshakeAmqpOpened<Io> {
    frame: Open,
    io: Io,
    sink: Connection,
    state: IoState<AmqpCodec<AmqpFrame>>,
    local_config: Rc<Configuration>,
    remote_config: Configuration,
}

impl<Io> HandshakeAmqpOpened<Io> {
    pub(crate) fn new(
        frame: Open,
        io: Io,
        sink: Connection,
        state: IoState<AmqpCodec<AmqpFrame>>,
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
        }
    }
}

/// Handshake ack message
pub struct HandshakeAck<Io, St> {
    st: St,
    io: Io,
    sink: Connection,
    state: IoState<AmqpCodec<AmqpFrame>>,
}

impl<Io, St> HandshakeAck<Io, St> {
    pub(crate) fn into_inner(self) -> (St, Io, Connection, IoState<AmqpCodec<AmqpFrame>>) {
        (self.st, self.io, self.sink, self.state)
    }
}
