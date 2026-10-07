use super::protocol;

/// Length in bytes of the fixed frame header
pub(crate) const HEADER_LEN: usize = 8;

/// AMQP Frame type marker (0)
pub(crate) const FRAME_TYPE_AMQP: u8 = 0x00;
pub(crate) const FRAME_TYPE_SASL: u8 = 0x01;

/// Represents an AMQP Frame
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AmqpFrame {
    channel_id: u16,
    performative: protocol::Frame,
}

impl AmqpFrame {
    pub fn new(channel_id: u16, performative: protocol::Frame) -> AmqpFrame {
        AmqpFrame {
            channel_id,
            performative,
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
    pub fn into_parts(self) -> (u16, protocol::Frame) {
        (self.channel_id, self.performative)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SaslFrame {
    pub body: protocol::SaslFrameBody,
}

impl SaslFrame {
    pub fn new(body: protocol::SaslFrameBody) -> SaslFrame {
        SaslFrame { body }
    }
}

impl From<protocol::SaslMechanisms> for SaslFrame {
    fn from(item: protocol::SaslMechanisms) -> SaslFrame {
        SaslFrame::new(protocol::SaslFrameBody::SaslMechanisms(item))
    }
}

impl From<protocol::SaslInit> for SaslFrame {
    fn from(item: protocol::SaslInit) -> SaslFrame {
        SaslFrame::new(protocol::SaslFrameBody::SaslInit(item))
    }
}

impl From<protocol::SaslChallenge> for SaslFrame {
    fn from(item: protocol::SaslChallenge) -> SaslFrame {
        SaslFrame::new(protocol::SaslFrameBody::SaslChallenge(item))
    }
}

impl From<protocol::SaslResponse> for SaslFrame {
    fn from(item: protocol::SaslResponse) -> SaslFrame {
        SaslFrame::new(protocol::SaslFrameBody::SaslResponse(item))
    }
}

impl From<protocol::SaslOutcome> for SaslFrame {
    fn from(item: protocol::SaslOutcome) -> SaslFrame {
        SaslFrame::new(protocol::SaslFrameBody::SaslOutcome(item))
    }
}

#[cfg(test)]
mod tests {
    use ntex_bytes::Bytes;

    use super::*;
    use crate::types::Symbol;

    #[test]
    fn amqp_frame_parts() {
        let performative = protocol::Frame::Close(protocol::Close { error: None });
        let frame = AmqpFrame::new(42, performative.clone());

        assert_eq!(frame.channel_id(), 42);
        assert_eq!(frame.performative(), &performative);
        assert_eq!(frame.clone().into_parts(), (42, performative));
    }

    #[test]
    fn sasl_frame_conversions() {
        let mechanisms = protocol::SaslMechanisms {
            sasl_server_mechanisms: protocol::Symbols::default(),
        };
        assert_eq!(
            SaslFrame::from(mechanisms.clone()).body,
            protocol::SaslFrameBody::SaslMechanisms(mechanisms)
        );

        let init = protocol::SaslInit {
            mechanism: Symbol::from("PLAIN"),
            initial_response: None,
            hostname: None,
        };
        assert_eq!(
            SaslFrame::from(init.clone()).body,
            protocol::SaslFrameBody::SaslInit(init)
        );

        let challenge = protocol::SaslChallenge {
            challenge: Bytes::from_static(b"c"),
        };
        assert_eq!(
            SaslFrame::from(challenge.clone()).body,
            protocol::SaslFrameBody::SaslChallenge(challenge)
        );

        let response = protocol::SaslResponse {
            response: Bytes::from_static(b"r"),
        };
        assert_eq!(
            SaslFrame::from(response.clone()).body,
            protocol::SaslFrameBody::SaslResponse(response)
        );

        let outcome = protocol::SaslOutcome {
            code: protocol::SaslCode::Auth,
            additional_data: None,
        };
        assert_eq!(
            SaslFrame::from(outcome.clone()).body,
            protocol::SaslFrameBody::SaslOutcome(outcome.clone())
        );
        assert_eq!(
            SaslFrame::new(protocol::SaslFrameBody::SaslOutcome(outcome.clone())),
            SaslFrame::from(outcome)
        );
    }
}
