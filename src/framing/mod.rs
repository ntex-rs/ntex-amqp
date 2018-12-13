use super::protocol;
use bytes::Bytes;

/// Length in bytes of the fixed frame header
pub const HEADER_LEN: usize = 8;

/// AMQP Frame type marker (0)
pub const FRAME_TYPE_AMQP: u8 = 0x00;
pub const FRAME_TYPE_SASL: u8 = 0x01;

/// Represents an AMQP Frame
#[derive(Clone, Debug, PartialEq)]
pub struct AmqpFrame {
    channel_id: u16,
    performative: protocol::Frame,
    body: Bytes,
}

impl AmqpFrame {
    pub fn new(channel_id: u16, performative: protocol::Frame, body: Bytes) -> AmqpFrame {
        AmqpFrame {
            channel_id,
            performative,
            body,
        }
    }

    #[inline]
    pub fn channel_id(&self) -> u16 {
        self.channel_id
    }

    #[inline]
    pub fn performative(&self) -> &protocol::Frame {
        &self.performative
    }

    #[inline]
    pub fn body(&self) -> &Bytes {
        &self.body
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SaslFrame {
    pub body: protocol::SaslFrameBody,
}

impl SaslFrame {
    pub fn new(body: protocol::SaslFrameBody) -> SaslFrame {
        SaslFrame { body }
    }
}
