use std::io;

use bytestring::ByteString;
use derive_more::Display;
use either::Either;
use ntex_amqp_codec::{protocol, AmqpCodecError, ProtocolIdError, SaslFrame};

use crate::errors::{AmqpError, AmqpProtocolError};

/// Errors which can occur when attempting to handle amqp connection.
#[derive(Debug, Display)]
pub enum ServerError<E> {
    #[display(fmt = "Message handler service error")]
    /// Message handler service error
    Service(E),
    #[display(fmt = "Amqp error: {}", _0)]
    /// Amqp error
    Amqp(AmqpError),
    #[display(fmt = "Protocol negotiation error: {}", _0)]
    /// Amqp protocol negotiation error
    Handshake(ProtocolIdError),
    /// Amqp handshake timeout
    HandshakeTimeout,

    /// Amqp codec error
    #[display(fmt = "Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),

    /// Amqp protocol error
    #[display(fmt = "Amqp protocol error: {:?}", _0)]
    Protocol(AmqpProtocolError),

    #[display(fmt = "Unexpected sasl frame: {:?}", _0)]
    UnexpectedSaslFrame(SaslFrame),
    #[display(fmt = "Unexpected sasl frame body: {:?}", _0)]
    UnexpectedSaslBodyFrame(protocol::SaslFrameBody),
    /// Peer disconnect
    Disconnected,
    /// Unexpected io error
    Io(io::Error),
}

impl<E> Into<protocol::Error> for ServerError<E> {
    fn into(self) -> protocol::Error {
        protocol::Error {
            condition: protocol::AmqpError::InternalError.into(),
            description: Some(ByteString::from(format!("{}", self))),
            info: None,
        }
    }
}

impl<E> From<AmqpError> for ServerError<E> {
    fn from(err: AmqpError) -> Self {
        ServerError::Amqp(err)
    }
}

impl<E> From<AmqpCodecError> for ServerError<E> {
    fn from(err: AmqpCodecError) -> Self {
        ServerError::Codec(err)
    }
}

impl<E> From<AmqpProtocolError> for ServerError<E> {
    fn from(err: AmqpProtocolError) -> Self {
        ServerError::Protocol(err)
    }
}

impl<E> From<ProtocolIdError> for ServerError<E> {
    fn from(err: ProtocolIdError) -> Self {
        ServerError::Handshake(err)
    }
}

impl<E> From<SaslFrame> for ServerError<E> {
    fn from(err: SaslFrame) -> Self {
        ServerError::UnexpectedSaslFrame(err)
    }
}

impl<E> From<io::Error> for ServerError<E> {
    fn from(err: io::Error) -> Self {
        ServerError::Io(err)
    }
}

impl<E> From<Either<AmqpCodecError, io::Error>> for ServerError<E> {
    fn from(err: Either<AmqpCodecError, io::Error>) -> Self {
        match err {
            Either::Left(err) => ServerError::Codec(err),
            Either::Right(err) => ServerError::Io(err),
        }
    }
}

impl<E> From<Either<ProtocolIdError, io::Error>> for ServerError<E> {
    fn from(err: Either<ProtocolIdError, io::Error>) -> Self {
        match err {
            Either::Left(err) => ServerError::Handshake(err),
            Either::Right(err) => ServerError::Io(err),
        }
    }
}
