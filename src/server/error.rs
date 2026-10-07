use ntex_error::Failure;
use ntex_util::future::Either;

use crate::codec::{AmqpCodecError, AmqpFrame, ProtocolIdError, SaslFrame, protocol};
use crate::error::{AmqpDispatcherError, AmqpProtocolError};

/// Errors which can occur when attempting to handle amqp connection.
#[derive(Debug, thiserror::Error)]
pub enum ServerError<E> {
    #[error("Message handler service error")]
    /// Message handler service error
    Service(E),
    #[error("Handshake error: {}", _0)]
    /// Amqp handshake error
    Handshake(HandshakeError),
    /// Amqp codec error
    #[error("Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    /// Amqp protocol error
    #[error("Amqp protocol error: {:?}", _0)]
    Protocol(AmqpProtocolError),
    /// Dispatcher error
    #[error("Amqp dispatcher error: {:?}", _0)]
    Dispatcher(AmqpDispatcherError),
    /// Control service init error
    #[error("Control service init error")]
    ControlService(#[source] Failure),
    /// Publish service init error
    #[error("Publish service init error")]
    PublishService(#[source] Failure),
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
    #[error("Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    /// Handshake timeout
    #[error("Handshake timeout")]
    Timeout,
    /// Protocol negotiation error
    #[error("Protocol negotiation error: {0}")]
    ProtocolNegotiation(ProtocolIdError),
    #[from(ignore)]
    /// Expected open frame
    #[error("Expect open frame, got: {:?}", _0)]
    ExpectOpenFrame(AmqpFrame),
    #[error("Unexpected frame, got: {:?}", _0)]
    Unexpected(protocol::Frame),
    #[error("Unexpected sasl frame: {:?}", _0)]
    UnexpectedSaslFrame(Box<SaslFrame>),
    #[error("Unexpected sasl frame body: {:?}", _0)]
    UnexpectedSaslBodyFrame(Box<protocol::SaslFrameBody>),
    #[error("Unsupported sasl mechanism: {}", _0)]
    UnsupportedSaslMechanism(String),
    /// Sasl error code
    #[error("Sasl error code: {:?}", _0)]
    Sasl(protocol::SaslCode),
    /// Remote max frame size is below `MIN_MAX_FRAME_SIZE`
    #[from(ignore)]
    #[error("Invalid remote max frame size: {0}")]
    InvalidMaxFrameSize(u32),
    /// Unexpected io error, peer disconnected
    #[error("Peer disconnected, with error {:?}", _0)]
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
