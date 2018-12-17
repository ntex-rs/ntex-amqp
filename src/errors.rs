use amqp::errors::{AmqpCodecError, AmqpParseError, ProtocolIdError};
use amqp::protocol::SaslCode;

#[derive(Debug, Display, From)]
pub enum AmqpTransportError {
    ParseError(AmqpParseError),
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
