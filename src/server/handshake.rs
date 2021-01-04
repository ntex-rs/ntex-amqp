use futures::StreamExt;
use ntex_amqp_codec::protocol::{Frame, Open};
use ntex_amqp_codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec};
use ntex_codec::{AsyncRead, AsyncWrite, Framed};

use super::{errors::ServerError, sasl::Sasl};
use crate::connection::ConnectionController;

/// Connection handshake
pub enum Handshake<Io> {
    Amqp(HandshakeAmqp<Io>),
    Sasl(Sasl<Io>),
}

impl<Io> Handshake<Io> {
    pub(crate) fn new_plain(
        framed: Framed<Io, ProtocolIdCodec>,
        controller: ConnectionController,
    ) -> Self {
        Handshake::Amqp(HandshakeAmqp::new(framed, controller))
    }

    pub(crate) fn new_sasl(
        framed: Framed<Io, ProtocolIdCodec>,
        controller: ConnectionController,
    ) -> Self {
        Handshake::Sasl(Sasl::new(framed, controller))
    }
}

/// Open new connection
pub struct HandshakeAmqp<Io> {
    conn: Framed<Io, ProtocolIdCodec>,
    controller: ConnectionController,
}

impl<Io> HandshakeAmqp<Io> {
    pub(crate) fn new(conn: Framed<Io, ProtocolIdCodec>, controller: ConnectionController) -> Self {
        Self { conn, controller }
    }

    /// Returns reference to io object
    pub fn get_ref(&self) -> &Io {
        self.conn.get_ref()
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        self.conn.get_mut()
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> HandshakeAmqp<Io> {
    /// Wait for connection open frame
    pub async fn open(self) -> Result<HandshakeAmqpOpened<Io>, ServerError<()>> {
        let mut framed = self.conn.into_framed(AmqpCodec::<AmqpFrame>::new());
        let mut controller = self.controller;

        let frame = framed
            .next()
            .await
            .ok_or(ServerError::Disconnected)?
            .map_err(ServerError::from)?;

        let frame = frame.into_parts().1;
        match frame {
            Frame::Open(frame) => {
                trace!("Got open frame: {:?}", frame);
                controller.set_remote((&frame).into());
                Ok(HandshakeAmqpOpened::new(frame, framed, controller))
            }
            frame => Err(ServerError::Unexpected(Box::new(frame))),
        }
    }
}

/// Connection is opened
pub struct HandshakeAmqpOpened<Io> {
    frame: Open,
    framed: Framed<Io, AmqpCodec<AmqpFrame>>,
    controller: ConnectionController,
}

impl<Io> HandshakeAmqpOpened<Io> {
    pub(crate) fn new(
        frame: Open,
        framed: Framed<Io, AmqpCodec<AmqpFrame>>,
        controller: ConnectionController,
    ) -> Self {
        HandshakeAmqpOpened {
            frame,
            framed,
            controller,
        }
    }

    /// Get reference to remote `Open` frame
    pub fn frame(&self) -> &Open {
        &self.frame
    }

    /// Returns reference to io object
    pub fn get_ref(&self) -> &Io {
        self.framed.get_ref()
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        self.framed.get_mut()
    }

    /// Connection controller
    pub fn connection(&self) -> &ConnectionController {
        &self.controller
    }

    /// Ack connect message and set state
    pub fn ack<St>(self, state: St) -> HandshakeAck<Io, St> {
        HandshakeAck {
            state,
            framed: self.framed,
            controller: self.controller,
        }
    }
}

/// Handshake ack message
pub struct HandshakeAck<Io, St> {
    state: St,
    framed: Framed<Io, AmqpCodec<AmqpFrame>>,
    controller: ConnectionController,
}

impl<Io, St> HandshakeAck<Io, St> {
    pub(crate) fn into_inner(self) -> (St, Framed<Io, AmqpCodec<AmqpFrame>>, ConnectionController) {
        (self.state, self.framed, self.controller)
    }
}
