use crate::protocol::{AmqpError, Error, ErrorInner, ProtocolId};
use crate::types::Descriptor;

#[derive(Debug, Clone, thiserror::Error)]
pub enum AmqpParseError {
    #[error("Loaded item size is invalid")]
    InvalidSize,
    #[error("More data required during frame parsing, expected {0} bytes")]
    Incomplete(usize),
    #[error("Unexpected format code: {0:#04x}")]
    InvalidFormatCode(u8),
    #[error("Invalid value converting to char: {0:#x}")]
    InvalidChar(u32),
    #[error("Unexpected descriptor: {0:?}")]
    InvalidDescriptor(Box<Descriptor>),
    #[error("Unexpected frame type: {0:#04x}")]
    UnexpectedFrameType(u8),
    #[error("Required field '{0}' was omitted")]
    RequiredFieldOmitted(&'static str),
    #[error("Unknown {0} option")]
    UnknownEnumOption(&'static str),
    #[error("Cannot parse uuid value")]
    UuidParseError,
    #[error("Cannot parse datetime value")]
    DatetimeParseError,
    #[error("Unexpected type: '{0}'")]
    UnexpectedType(&'static str),
    #[error("Value is not valid utf8 string")]
    Utf8Error,
    #[error("Max nesting depth of compound values exceeded")]
    MaxDepthExceeded,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum AmqpCodecError {
    #[error("Parse failed: {:?}", _0)]
    ParseError(#[from] AmqpParseError),
    #[error("Bytes left unparsed at the frame trail")]
    UnparsedBytesLeft,
    #[error("Max inbound frame size exceeded")]
    MaxSizeExceeded,
    #[error("Max outbound frame size exceeded")]
    MaxOutboundSizeExceeded,
    #[error("Invalid inbound frame size")]
    InvalidFrameSize,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ProtocolIdError {
    #[error("Invalid header")]
    InvalidHeader,
    #[error("Incompatible")]
    Incompatible,
    #[error("Unknown protocol")]
    Unknown,
    #[error("Expected {:?} protocol id, seen {:?} instead.", exp, got)]
    Unexpected { exp: ProtocolId, got: ProtocolId },
}

impl From<()> for Error {
    fn from(_: ()) -> Error {
        Error(Box::new(ErrorInner {
            condition: AmqpError::InternalError.into(),
            description: None,
            info: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_converts_to_internal_error() {
        let err = Error::from(());
        assert_eq!(err.condition(), &AmqpError::InternalError.into());
        assert_eq!(err.description(), None);
        assert!(err.info().is_none());
    }

    #[test]
    fn error_messages_with_fields() {
        assert_eq!(
            AmqpParseError::Incomplete(7).to_string(),
            "More data required during frame parsing, expected 7 bytes"
        );
        assert_eq!(
            AmqpParseError::InvalidFormatCode(0x44).to_string(),
            "Unexpected format code: 0x44"
        );
        assert_eq!(
            AmqpParseError::InvalidChar(0xd800).to_string(),
            "Invalid value converting to char: 0xd800"
        );
        assert_eq!(
            AmqpParseError::InvalidDescriptor(Box::new(Descriptor::Ulong(1))).to_string(),
            "Unexpected descriptor: Ulong(1)"
        );
        assert_eq!(
            AmqpParseError::UnexpectedFrameType(2).to_string(),
            "Unexpected frame type: 0x02"
        );
        assert_eq!(
            AmqpParseError::RequiredFieldOmitted("handle").to_string(),
            "Required field 'handle' was omitted"
        );
        assert_eq!(
            AmqpParseError::UnknownEnumOption("Role").to_string(),
            "Unknown Role option"
        );
        assert_eq!(
            AmqpParseError::UnexpectedType("Frame").to_string(),
            "Unexpected type: 'Frame'"
        );
        assert_eq!(
            AmqpCodecError::from(AmqpParseError::Incomplete(7)).to_string(),
            "Parse failed: Incomplete(7)"
        );
    }

    #[test]
    fn error_messages() {
        // messages without interpolated fields
        assert_eq!(
            AmqpParseError::InvalidSize.to_string(),
            "Loaded item size is invalid"
        );
        assert_eq!(
            AmqpParseError::Utf8Error.to_string(),
            "Value is not valid utf8 string"
        );
        assert_eq!(
            AmqpParseError::MaxDepthExceeded.to_string(),
            "Max nesting depth of compound values exceeded"
        );
        assert_eq!(
            AmqpCodecError::from(AmqpParseError::InvalidSize).to_string(),
            "Parse failed: InvalidSize"
        );
        assert_eq!(
            AmqpCodecError::MaxSizeExceeded.to_string(),
            "Max inbound frame size exceeded"
        );
        assert_eq!(
            AmqpCodecError::UnparsedBytesLeft.to_string(),
            "Bytes left unparsed at the frame trail"
        );
        assert_eq!(ProtocolIdError::InvalidHeader.to_string(), "Invalid header");
        assert_eq!(ProtocolIdError::Unknown.to_string(), "Unknown protocol");
        assert_eq!(
            ProtocolIdError::Unexpected {
                exp: ProtocolId::Amqp,
                got: ProtocolId::AmqpSasl,
            }
            .to_string(),
            "Expected Amqp protocol id, seen AmqpSasl instead."
        );
    }

    #[test]
    fn parse_error_is_convertible_to_codec_error() {
        let err: AmqpCodecError = AmqpParseError::Incomplete(7).into();
        assert!(matches!(
            err,
            AmqpCodecError::ParseError(AmqpParseError::Incomplete(7))
        ));
    }
}
