use ntex::util::Either;

use crate::codec::{protocol, AmqpCodecError, AmqpFrame, ProtocolIdError};

/// Errors which can occur when attempting to handle amqp client connection.
#[derive(Debug, Display, From)]
pub enum ConnectError {
    /// Amqp codec error
    #[display(fmt = "Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    /// Handshake timeout
    #[display(fmt = "Handshake timeout")]
    HandshakeTimeout,
    /// Protocol negotiation error
    #[display(fmt = "Peer disconnected")]
    ProtocolNegotiation(ProtocolIdError),
    #[from(ignore)]
    /// Expected open frame
    #[display(fmt = "Expect open frame, got: {:?}", _0)]
    ExpectOpenFrame(Box<AmqpFrame>),
    /// Peer disconnected
    #[display(fmt = "Sasl error code: {:?}", _0)]
    Sasl(protocol::SaslCode),
    #[display(fmt = "Peer disconnected")]
    Disconnected,
    /// Connect error
    #[display(fmt = "Connect error: {}", _0)]
    Connect(ntex::connect::ConnectError),
    /// Unexpected io error
    Io(std::io::Error),
}

impl std::error::Error for ConnectError {}

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
