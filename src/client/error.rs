use ntex::util::Either;

use crate::codec::{protocol, AmqpCodecError, AmqpFrame, ProtocolIdError};

/// Errors which can occur when attempting to handle amqp client connection.
#[derive(Debug, Display, From)]
pub enum ConnectError {
    /// Amqp codec error
    #[display("Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    /// Handshake timeout
    #[display("Handshake timeout")]
    HandshakeTimeout,
    /// Protocol negotiation error
    #[display("Peer disconnected")]
    ProtocolNegotiation(ProtocolIdError),
    #[from(ignore)]
    /// Expected open frame
    #[display("Expect open frame, got: {:?}", _0)]
    ExpectOpenFrame(Box<AmqpFrame>),
    /// Peer disconnected
    #[display("Sasl error code: {:?}", _0)]
    Sasl(protocol::SaslCode),
    #[display("Peer disconnected")]
    Disconnected,
    /// Connect error
    #[display("Connect error: {}", _0)]
    Connect(ntex::connect::ConnectError),
    /// Unexpected io error
    Io(std::io::Error),
}

impl std::error::Error for ConnectError {}

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
                ConnectError::Io(std::io::Error::new(err.kind(), format!("{}", err)))
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
