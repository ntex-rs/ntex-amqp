use amqp_codec::types::ByteStr;
use amqp_codec::{protocol, AmqpCodecError, ProtocolIdError, SaslFrame};
use bytes::Bytes;
use derive_more::{Display, From};
use string::TryFrom;

#[derive(Debug, Display, From)]
pub enum HandshakeError {
    #[display(fmt = "Protocol negotiation error: {}", _0)]
    Protocol(ProtocolIdError),
    #[display(fmt = "Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    #[display(fmt = "Expected open frame, got: {:?}", _0)]
    Unexpected(protocol::Frame),
    #[display(fmt = "Unexpected sals frame: {:?}", _0)]
    UnexpectedSasl(SaslFrame),
    #[display(fmt = "Disconnected during handshake")]
    Disconnected,
    #[display(fmt = "Service error")]
    Sasl,
    #[display(fmt = "Service error")]
    Service,
}

#[derive(Debug, Display, From)]
pub enum SaslError {
    #[display(fmt = "Protocol negotiation error: {}", _0)]
    Protocol(ProtocolIdError),
    #[display(fmt = "Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    #[display(fmt = "Expected open frame, got: {:?}", _0)]
    Unexpected(protocol::SaslFrameBody),
    #[display(fmt = "Disconnected during handshake")]
    Disconnected,
}

impl Into<protocol::Error> for SaslError {
    fn into(self) -> protocol::Error {
        protocol::Error {
            condition: protocol::AmqpError::InternalError.into(),
            description: Some(string::String::try_from(format!("{}", self).into()).unwrap()),
            info: None,
        }
    }
}

#[derive(Debug, Display)]
#[display(fmt = "Amqp error: {:?} {:?} ({:?})", err, description, info)]
pub struct AmqpError {
    err: protocol::AmqpError,
    description: Option<ByteStr>,
    info: Option<protocol::Fields>,
}

impl AmqpError {
    pub fn new(err: protocol::AmqpError) -> Self {
        AmqpError {
            err,
            description: None,
            info: None,
        }
    }

    pub fn decode_error() -> Self {
        Self::new(protocol::AmqpError::DecodeError)
    }

    pub fn not_allowed() -> Self {
        Self::new(protocol::AmqpError::NotAllowed)
    }

    pub fn internal_error() -> Self {
        Self::new(protocol::AmqpError::InternalError)
    }

    pub fn not_implemented() -> Self {
        Self::new(protocol::AmqpError::NotImplemented)
    }

    pub fn description<T: Into<Bytes>>(mut self, text: T) -> Self {
        self.description = Some(ByteStr::try_from(Bytes::from(text.into())).unwrap());
        self
    }
}

impl Into<protocol::Error> for AmqpError {
    fn into(self) -> protocol::Error {
        protocol::Error {
            condition: self.err.into(),
            description: self.description,
            info: self.info,
        }
    }
}

#[derive(Debug, Display)]
#[display(fmt = "Link error: {:?} {:?} ({:?})", err, description, info)]
pub struct LinkError {
    err: protocol::LinkError,
    description: Option<ByteStr>,
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

    pub fn description<T: Into<Bytes>>(mut self, text: T) -> Self {
        self.description = Some(ByteStr::try_from(Bytes::from(text.into())).unwrap());
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
