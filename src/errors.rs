use amqp::{protocol, AmqpCodecError, ProtocolIdError};

#[derive(Debug, Display, From, Clone)]
pub enum AmqpTransportError {
    Codec(AmqpCodecError),
    TooManyChannels,
    Disconnected,
    Timeout,
    #[display(fmt = "Link detached, error: {:?}", _0)]
    LinkDetached(Option<protocol::Error>),
}

impl From<protocol::Error> for AmqpTransportError {
    fn from(err: protocol::Error) -> Self {
        AmqpTransportError::LinkDetached(Some(err))
    }
}

#[derive(Debug, From)]
pub enum SaslConnectError {
    Protocol(ProtocolIdError),
    AmqpError(AmqpCodecError),
    Sasl(protocol::SaslCode),
    ExpectedOpenFrame,
    Disconnected,
}
