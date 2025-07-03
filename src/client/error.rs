use ntex::util::Either;

use crate::codec::{protocol, AmqpCodecError, AmqpFrame, ProtocolIdError};

/// Errors which can occur when attempting to handle amqp client connection.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// Amqp codec error
    #[error("Amqp codec error: {:?}", _0)]
    Codec(#[from] AmqpCodecError),
    /// Handshake timeout
    #[error("Handshake timeout")]
    HandshakeTimeout,
    /// Protocol negotiation error
    #[error("Peer disconnected")]
    ProtocolNegotiation(#[from] ProtocolIdError),
    /// Expected open frame
    #[error("Expect open frame, got: {:?}", _0)]
    ExpectOpenFrame(Box<AmqpFrame>),
    /// Peer disconnected
    #[error("Sasl error code: {:?}", _0)]
    Sasl(protocol::SaslCode),
    #[error("Peer disconnected")]
    Disconnected,
    /// Connect error
    #[error("Connect error: {}", _0)]
    Connect(#[from] ntex::connect::ConnectError),
    /// Unexpected io error
    #[error("Io error: {}", _0)]
    Io(#[from] std::io::Error),
}

impl Clone for ConnectError {
    fn clone(&self) -> Self {
        match self {
            ConnectError::Codec(err) => ConnectError::Codec(err.clone()),
            ConnectError::HandshakeTimeout => ConnectError::HandshakeTimeout,
            ConnectError::ProtocolNegotiation(err) => {
                ConnectError::ProtocolNegotiation(err.clone())
            }
            ConnectError::ExpectOpenFrame(frame) => ConnectError::ExpectOpenFrame(frame.clone()),
            ConnectError::Sasl(err) => ConnectError::Sasl(*err),
            ConnectError::Disconnected => ConnectError::Disconnected,
            ConnectError::Connect(err) => ConnectError::Connect(err.clone()),
            ConnectError::Io(err) => {
                ConnectError::Io(std::io::Error::new(err.kind(), format!("{err}")))
            }
        }
    }
}

impl From<Either<AmqpCodecError, std::io::Error>> for ConnectError {
    fn from(err: Either<AmqpCodecError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => ConnectError::Codec(err),
            Either::Right(err) => ConnectError::Io(err),
        }
    }
}

impl From<Either<ProtocolIdError, std::io::Error>> for ConnectError {
    fn from(err: Either<ProtocolIdError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => ConnectError::ProtocolNegotiation(err),
            Either::Right(err) => ConnectError::Io(err),
        }
    }
}
