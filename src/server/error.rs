use derive_more::Display;
use ntex::util::{ByteString, Either};

use crate::codec::{protocol, AmqpCodecError, AmqpFrame, ProtocolIdError, SaslFrame};
use crate::error::AmqpProtocolError;

/// Errors which can occur when attempting to handle amqp connection.
#[derive(Debug, Display)]
pub enum ServerError<E> {
    #[display(fmt = "Message handler service error")]
    /// Message handler service error
    Service(E),
    #[display(fmt = "Handshake error: {}", _0)]
    /// Amqp handshake error
    Handshake(HandshakeError),
    /// Amqp codec error
    #[display(fmt = "Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    /// Amqp protocol error
    #[display(fmt = "Amqp protocol error: {:?}", _0)]
    Protocol(AmqpProtocolError),
    /// Control service init error
    #[display(fmt = "Control service init error")]
    ControlServiceError,
    /// Publish service init error
    #[display(fmt = "Publish service init error")]
    PublishServiceError,
    /// Peer disconnect
    Disconnected,
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
    #[display(fmt = "Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    /// Handshake timeout
    #[display(fmt = "Handshake timeout")]
    Timeout,
    /// Protocol negotiation error
    #[display(fmt = "Peer disconnected")]
    ProtocolNegotiation(ProtocolIdError),
    #[from(ignore)]
    /// Expected open frame
    #[display(fmt = "Expect open frame, got: {:?}", _0)]
    ExpectOpenFrame(Box<AmqpFrame>),
    #[display(fmt = "Unexpected frame, got: {:?}", _0)]
    Unexpected(Box<protocol::Frame>),
    #[display(fmt = "Unexpected sasl frame: {:?}", _0)]
    UnexpectedSaslFrame(SaslFrame),
    #[display(fmt = "Unexpected sasl frame body: {:?}", _0)]
    UnexpectedSaslBodyFrame(protocol::SaslFrameBody),
    #[display(fmt = "Unsupported sasl mechanism: {}", _0)]
    UnsupportedSaslMechanism(String),
    /// Sasl error code
    #[display(fmt = "Sasl error code: {:?}", _0)]
    Sasl(protocol::SaslCode),
    #[display(fmt = "Peer disconnected")]
    Disconnected,
    /// Unexpected io error
    Io(std::io::Error),
}

impl std::error::Error for HandshakeError {}

impl From<Either<AmqpCodecError, std::io::Error>> for HandshakeError {
    fn from(err: Either<AmqpCodecError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => HandshakeError::Codec(err),
            Either::Right(err) => HandshakeError::Io(err),
        }
    }
}

impl From<Either<ProtocolIdError, std::io::Error>> for HandshakeError {
    fn from(err: Either<ProtocolIdError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => HandshakeError::ProtocolNegotiation(err),
            Either::Right(err) => HandshakeError::Io(err),
        }
    }
}

impl From<HandshakeError> for protocol::Error {
    fn from(err: HandshakeError) -> protocol::Error {
        protocol::Error {
            condition: protocol::AmqpError::InternalError.into(),
            description: Some(ByteString::from(format!("{}", err))),
            info: None,
        }
    }
}
