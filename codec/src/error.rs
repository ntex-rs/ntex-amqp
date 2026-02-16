use crate::protocol::{AmqpError, Error, ErrorInner, ProtocolId};
use crate::types::Descriptor;

#[derive(Debug, Clone, thiserror::Error)]
pub enum AmqpParseError {
    #[error("Loaded item size is invalid")]
    InvalidSize,
    #[error("More data required during frame parsing: '{:?}'", "_0")]
    Incomplete(usize),
    #[error("Unexpected format code: '{}'", "_0")]
    InvalidFormatCode(u8),
    #[error("Invalid value converting to char: {}", "_0")]
    InvalidChar(u32),
    #[error("Unexpected descriptor: '{:?}'", "_0")]
    InvalidDescriptor(Box<Descriptor>),
    #[error("Unexpected frame type: '{:?}'", "_0")]
    UnexpectedFrameType(u8),
    #[error("Required field '{:?}' was omitted.", "_0")]
    RequiredFieldOmitted(&'static str),
    #[error("Unknown {:?} option.", "_0")]
    UnknownEnumOption(&'static str),
    #[error("Cannot parse uuid value")]
    UuidParseError,
    #[error("Cannot parse datetime value")]
    DatetimeParseError,
    #[error("Unexpected type: '{:?}'", "_0")]
    UnexpectedType(&'static str),
    #[error("Value is not valid utf8 string")]
    Utf8Error,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum AmqpCodecError {
    #[error("Parse failed: {:?}", _0)]
    ParseError(#[from] AmqpParseError),
    #[error("Bytes left unparsed at the frame trail")]
    UnparsedBytesLeft,
    #[error("Max inbound frame size exceeded")]
    MaxSizeExceeded,
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
