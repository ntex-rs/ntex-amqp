use amqp::errors::{AmqpCodecError, ProtocolIdError};
use amqp::protocol::SaslCode;

#[derive(Debug, Display, From, Clone)]
pub enum AmqpTransportError {
    Codec(AmqpCodecError),
    TooManyChannels,
    Disconnected,
}

#[derive(Debug, From)]
pub enum SaslConnectError {
    Protocol(ProtocolIdError),
    AmqpError(AmqpCodecError),
    Sasl(SaslCode),
    ExpectedOpenFrame,
    Disconnected,
}
