use ntex::util::Either;

use crate::codec::{protocol, AmqpCodecError, AmqpFrame, ProtocolIdError, SaslFrame};
use crate::error::{AmqpDispatcherError, AmqpProtocolError};

/// Errors which can occur when attempting to handle amqp connection.
#[derive(Debug, thiserror::Error)]
pub enum ServerError<E> {
    #[error("Message handler service error")]
    /// Message handler service error
    Service(E),
    #[error("Handshake error: {}", 0)]
    /// Amqp handshake error
    Handshake(HandshakeError),
    /// Amqp codec error
    #[error("Amqp codec error: {:?}", 0)]
    Codec(AmqpCodecError),
    /// Amqp protocol error
    #[error("Amqp protocol error: {:?}", 0)]
    Protocol(AmqpProtocolError),
    /// Dispatcher error
    #[error("Amqp dispatcher error: {:?}", 0)]
    Dispatcher(AmqpDispatcherError),
    /// Control service init error
    #[error("Control service init error")]
    ControlServiceError,
    /// Publish service init error
    #[error("Publish service init error")]
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
#[derive(Debug, From, thiserror::Error)]
pub enum HandshakeError {
    /// Amqp codec error
    #[error("Amqp codec error: {:?}", 0)]
    Codec(AmqpCodecError),
    /// Handshake timeout
    #[error("Handshake timeout")]
    Timeout,
    /// Protocol negotiation error
    #[error("Peer disconnected")]
    ProtocolNegotiation(ProtocolIdError),
    #[from(ignore)]
    /// Expected open frame
    #[error("Expect open frame, got: {:?}", 0)]
    ExpectOpenFrame(AmqpFrame),
    #[error("Unexpected frame, got: {:?}", 0)]
    Unexpected(protocol::Frame),
    #[error("Unexpected sasl frame: {:?}", 0)]
    UnexpectedSaslFrame(Box<SaslFrame>),
    #[error("Unexpected sasl frame body: {:?}", 0)]
    UnexpectedSaslBodyFrame(Box<protocol::SaslFrameBody>),
    #[error("Unsupported sasl mechanism: {}", 0)]
    UnsupportedSaslMechanism(String),
    /// Sasl error code
    #[error("Sasl error code: {:?}", 0)]
    Sasl(protocol::SaslCode),
    /// Unexpected io error, peer disconnected
    #[error("Peer disconnected, with error {:?}", 0)]
    Disconnected(Option<std::io::Error>),
}

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
