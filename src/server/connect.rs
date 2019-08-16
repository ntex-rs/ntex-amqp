use actix_codec::{AsyncRead, AsyncWrite, Framed};
use amqp_codec::protocol::{Frame, Open};
use amqp_codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec};
use futures::{Future, Stream};

use super::errors::ServerError;

/// Open new connection
pub struct Connect<Io> {
    conn: Framed<Io, ProtocolIdCodec>,
}

impl<Io> Connect<Io> {
    pub(crate) fn new(conn: Framed<Io, ProtocolIdCodec>) -> Self {
        Self { conn }
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

impl<Io: AsyncRead + AsyncWrite> Connect<Io> {
    /// Wait for connection open frame
    pub fn open(self) -> impl Future<Item = ConnectOpened<Io>, Error = ServerError<()>> {
        let framed = self.conn.into_framed(AmqpCodec::<AmqpFrame>::new());

        framed
            .into_future()
            .map_err(|(err, _)| ServerError::from(err))
            .and_then(|(frame, framed)| {
                if let Some(frame) = frame {
                    let frame = frame.into_parts().1;
                    match frame {
                        Frame::Open(frame) => {
                            trace!("Got open frame: {:?}", frame);
                            Ok(ConnectOpened { frame, framed })
                        }
                        frame => Err(ServerError::Unexpected(frame)),
                    }
                } else {
                    Err(ServerError::Disconnected)
                }
            })
    }
}

/// Connection is opened
pub struct ConnectOpened<Io> {
    frame: Open,
    framed: Framed<Io, AmqpCodec<AmqpFrame>>,
}

impl<Io> ConnectOpened<Io> {
    pub(crate) fn new(frame: Open, framed: Framed<Io, AmqpCodec<AmqpFrame>>) -> Self {
        ConnectOpened { frame, framed }
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

    /// Ack connect message and set state
    pub fn ack<St>(self, state: St) -> ConnectAck<Io, St> {
        ConnectAck {
            state,
            frame: self.frame,
            framed: self.framed,
        }
    }
}

/// Ack connect message
pub struct ConnectAck<Io, St> {
    state: St,
    frame: Open,
    framed: Framed<Io, AmqpCodec<AmqpFrame>>,
}

impl<Io, St> ConnectAck<Io, St> {
    pub(crate) fn into_inner(self) -> (St, Open, Framed<Io, AmqpCodec<AmqpFrame>>) {
        (self.state, self.frame, self.framed)
    }

    pub fn frame(&self) -> &Open {
        &self.frame
    }
}
