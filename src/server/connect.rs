use actix_codec::Framed;
use amqp_codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec};

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

    /// Ack connect message and set state
    pub fn ack<St>(self, st: St) -> ConnectAck<Io, St> {
        ConnectAck::new(st, self.conn.into_framed(AmqpCodec::new()))
    }
}

/// Ack connect message
pub struct ConnectAck<Io, St> {
    state: St,
    conn: Framed<Io, AmqpCodec<AmqpFrame>>,
}

impl<Io, St> ConnectAck<Io, St> {
    /// Create connect ack
    pub(crate) fn new(state: St, conn: Framed<Io, AmqpCodec<AmqpFrame>>) -> Self {
        Self { state, conn }
    }

    pub(crate) fn into_inner(self) -> (St, Framed<Io, AmqpCodec<AmqpFrame>>) {
        (self.state, self.conn)
    }
}
