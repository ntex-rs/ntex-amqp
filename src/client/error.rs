use ntex_error::{ErrorDiagnostic, ResultType};
use ntex_util::future::Either;

use crate::codec::{AmqpCodecError, AmqpFrame, ProtocolIdError, protocol};

/// Errors which can occur when attempting to handle amqp client connection.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// Amqp codec error
    #[error("Amqp codec")]
    Codec(
        #[from]
        #[source]
        AmqpCodecError,
    ),
    /// Handshake timeout
    #[error("Handshake timeout")]
    HandshakeTimeout,
    /// Protocol negotiation error
    #[error("Protocol negotiation")]
    ProtocolNegotiation(#[from] ProtocolIdError),
    /// Expected open frame
    #[error("Expect open frame, got: {:?}", _0)]
    ExpectOpenFrame(Box<AmqpFrame>),
    /// Peer disconnected
    #[error("Sasl error code {:?}", _0)]
    Sasl(protocol::SaslCode),
    #[error("Peer disconnected")]
    Disconnected,
    /// Connect error
    #[error("Connect")]
    Connect(
        #[from]
        #[source]
        ntex_net::connect::ConnectError,
    ),
    /// Unexpected io error
    #[error("")]
    Io(
        #[from]
        #[source]
        std::io::Error,
    ),
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

impl ErrorDiagnostic for ConnectError {
    fn typ(&self) -> ResultType {
        if let ConnectError::Sasl(err) = self
            && matches!(err, protocol::SaslCode::Auth | protocol::SaslCode::SysPerm)
        {
            ResultType::ClientError
        } else {
            ResultType::ServiceError
        }
    }

    fn signature(&self) -> &'static str {
        match self {
            ConnectError::Codec(_) => "amqp-client-Codec",
            ConnectError::HandshakeTimeout => "amqp-client-HandshakeTimeout",
            ConnectError::ProtocolNegotiation(_) => "amqp-client-ProtocolNegotiation",
            ConnectError::ExpectOpenFrame(_) => "amqp-client-ExpectOpenFrame",
            ConnectError::Sasl(_) => "amqp-client-Sasl",
            ConnectError::Disconnected => "amqp-client-Disconnected",
            ConnectError::Connect(err) => err.signature(),
            ConnectError::Io(err) => err.signature(),
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
