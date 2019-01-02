use amqp::types::ByteStr;
use amqp::{protocol, AmqpCodecError, ProtocolIdError};
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
    #[display(fmt = "Disconnected during handshake")]
    Disconnected,
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
