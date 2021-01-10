use crate::codec::AmqpCodecError;

/// Errors which can occur when attempting to handle amqp client connection.
#[derive(Debug, Display, From)]
pub enum ClientError {
    /// Protocol error
    #[display(fmt = "Protocol error: {:?}", _0)]
    Protocol(AmqpProtocolError),
    /// Handshake timeout
    #[display(fmt = "Handshake timeout")]
    HandshakeTimeout,
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected,
    /// Connect error
    #[display(fmt = "Connect error: {}", _0)]
    Connect(ntex::connect::ConnectError),
}

impl std::error::Error for ClientError {}

impl From<Either<AmqpCodecError, io::Error>> for ClientError {
    fn from(err: Either<AmqpCodecError, io::Error>) -> Self {
        match err {
            Either::Left(err) => ClientError::Protocol(AmqpProtocolError::Codec(err)),
            Either::Right(err) => ClientError::Protocol(AmqpProtocolError::Io(err)),
        }
    }
}
