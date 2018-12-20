use amqp::errors::{AmqpCodecError, ProtocolIdError};
use amqp::protocol::SaslCode;

#[derive(Debug, Display, From, Clone)]
pub enum AmqpTransportError {
    Codec(AmqpCodecError),
    TooManyChannels,
    Disconnected,
    Timeout,
}

#[derive(Debug, From)]
pub enum SaslConnectError {
    Protocol(ProtocolIdError),
    AmqpError(AmqpCodecError),
    Sasl(SaslCode),
    ExpectedOpenFrame,
    Disconnected,
}
