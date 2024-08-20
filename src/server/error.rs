use derive_more::Display;
use ntex::util::Either;

use crate::codec::{protocol, AmqpCodecError, AmqpFrame, ProtocolIdError, SaslFrame};
use crate::error::{AmqpDispatcherError, AmqpProtocolError};

/// Errors which can occur when attempting to handle amqp connection.
#[derive(Debug, Display)]
pub enum ServerError<E> {
    #[display("Message handler service error")]
    /// Message handler service error
    Service(E),
    #[display("Handshake error: {}", _0)]
    /// Amqp handshake error
    Handshake(HandshakeError),
    /// Amqp codec error
    #[display("Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    /// Amqp protocol error
    #[display("Amqp protocol error: {:?}", _0)]
    Protocol(AmqpProtocolError),
    /// Dispatcher error
    Dispatcher(AmqpDispatcherError),
    /// Control service init error
    #[display("Control service init error")]
    ControlServiceError,
    /// Publish service init error
    #[display("Publish service init error")]
    PublishServiceError,
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

impl<E> From<HandshakeError> for ServerError<E> {
    fn from(err: HandshakeError) -> Self {
        ServerError::Handshake(err)
    }
}

/// Errors which can occur when attempting to handle amqp handshake.
#[derive(Debug, Display, From)]
pub enum HandshakeError {
    /// Amqp codec error
    #[display("Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    /// Handshake timeout
    #[display("Handshake timeout")]
    Timeout,
    /// Protocol negotiation error
    #[display("Peer disconnected")]
    ProtocolNegotiation(ProtocolIdError),
    #[from(ignore)]
    /// Expected open frame
    #[display("Expect open frame, got: {:?}", _0)]
    ExpectOpenFrame(AmqpFrame),
    #[display("Unexpected frame, got: {:?}", _0)]
    Unexpected(protocol::Frame),
    #[display("Unexpected sasl frame: {:?}", _0)]
    UnexpectedSaslFrame(Box<SaslFrame>),
    #[display("Unexpected sasl frame body: {:?}", _0)]
    UnexpectedSaslBodyFrame(Box<protocol::SaslFrameBody>),
    #[display("Unsupported sasl mechanism: {}", _0)]
    UnsupportedSaslMechanism(String),
    /// Sasl error code
    #[display("Sasl error code: {:?}", _0)]
    Sasl(protocol::SaslCode),
    /// Unexpected io error, peer disconnected
    #[display("Peer disconnected, with error {:?}", _0)]
    Disconnected(Option<std::io::Error>),
}

impl std::error::Error for HandshakeError {}

impl From<Either<AmqpCodecError, std::io::Error>> for HandshakeError {
    fn from(err: Either<AmqpCodecError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => HandshakeError::Codec(err),
            Either::Right(err) => HandshakeError::Disconnected(Some(err)),
        }
    }
}

impl From<Either<ProtocolIdError, std::io::Error>> for HandshakeError {
    fn from(err: Either<ProtocolIdError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => HandshakeError::ProtocolNegotiation(err),
            Either::Right(err) => HandshakeError::Disconnected(Some(err)),
        }
    }
}
