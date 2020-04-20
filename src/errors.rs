use bytestring::ByteString;
use ntex_amqp_codec::{protocol, AmqpCodecError, ProtocolIdError};

#[derive(Debug, Display, Clone)]
pub enum AmqpTransportError {
    Codec(AmqpCodecError),
    TooManyChannels,
    Disconnected,
    Timeout,
    #[display(fmt = "Connection closed, error: {:?}", _0)]
    Closed(Option<protocol::Error>),
    #[display(fmt = "Session ended, error: {:?}", _0)]
    SessionEnded(Option<protocol::Error>),
    #[display(fmt = "Link detached, error: {:?}", _0)]
    LinkDetached(Option<protocol::Error>),
}

impl From<AmqpCodecError> for AmqpTransportError {
    fn from(err: AmqpCodecError) -> Self {
        AmqpTransportError::Codec(err)
    }
}

#[derive(Debug, Display, From)]
pub enum SaslConnectError {
    Protocol(ProtocolIdError),
    AmqpError(AmqpCodecError),
    #[display(fmt = "Sasl error code: {:?}", _0)]
    Sasl(protocol::SaslCode),
    ExpectedOpenFrame,
    Disconnected,
}

#[derive(Debug, Display)]
#[display(fmt = "Link error: {:?} {:?} ({:?})", err, description, info)]
pub struct LinkError {
    err: protocol::LinkError,
    description: Option<ByteString>,
    info: Option<protocol::Fields>,
}

impl LinkError {
    pub fn force_detach() -> Self {
        LinkError {
            err: protocol::LinkError::DetachForced,
            description: None,
            info: None,
        }
    }

    pub fn description<T: AsRef<str>>(mut self, text: T) -> Self {
        self.description = Some(ByteString::from(text.as_ref()));
        self
    }

    pub fn set_description(mut self, text: ByteString) -> Self {
        self.description = Some(text);
        self
    }
}

impl Into<protocol::Error> for LinkError {
    fn into(self) -> protocol::Error {
        protocol::Error {
            condition: self.err.into(),
            description: self.description,
            info: self.info,
        }
    }
}
