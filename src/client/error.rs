use either::Either;

use crate::codec::{protocol, AmqpCodecError, AmqpFrame, ProtocolIdError};
use crate::error::AmqpProtocolError;

/// Errors which can occur when attempting to handle amqp client connection.
#[derive(Debug, Display, From)]
pub enum ClientError {
    /// Protocol error
    #[display(fmt = "Protocol error: {:?}", _0)]
    Protocol(AmqpProtocolError),
    /// Handshake timeout
    #[display(fmt = "Handshake timeout")]
    HandshakeTimeout,
    /// Protocol negotiation error
    #[display(fmt = "Peer disconnected")]
    ProtocolNegotiation(ProtocolIdError),
    #[from(ignore)]
    /// Expected open frame
    #[display(fmt = "Expect open frame, got: {:?}", _0)]
    ExpectOpenFrame(AmqpFrame),
    /// Peer disconnected
    #[display(fmt = "Sasl error code: {:?}", _0)]
    Sasl(protocol::SaslCode),
    #[display(fmt = "Peer disconnected")]
    Disconnected,
    /// Connect error
    #[display(fmt = "Connect error: {}", _0)]
    Connect(ntex::connect::ConnectError),
}

impl std::error::Error for ClientError {}

impl From<Either<AmqpCodecError, std::io::Error>> for ClientError {
    fn from(err: Either<AmqpCodecError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => ClientError::Protocol(AmqpProtocolError::Codec(err)),
            Either::Right(err) => ClientError::Protocol(AmqpProtocolError::Io(Some(err))),
        }
    }
}

impl From<Either<ProtocolIdError, std::io::Error>> for ClientError {
    fn from(err: Either<ProtocolIdError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => ClientError::ProtocolNegotiation(err),
            Either::Right(err) => ClientError::Protocol(AmqpProtocolError::Io(Some(err))),
        }
    }
}
