use crate::protocol::{AmqpError, ProtocolId};
pub use crate::protocol::{Error, ErrorInner};
use crate::types::Descriptor;

#[derive(Debug, Display, From, Clone)]
pub enum AmqpParseError {
    #[display(fmt = "Loaded item size is invalid")]
    InvalidSize,
    #[display(fmt = "More data required during frame parsing: '{:?}'", "_0")]
    Incomplete(usize),
    #[from(ignore)]
    #[display(fmt = "Unexpected format code: '{}'", "_0")]
    InvalidFormatCode(u8),
    #[display(fmt = "Invalid value converting to char: {}", "_0")]
    InvalidChar(u32),
    #[display(fmt = "Unexpected descriptor: '{:?}'", "_0")]
    InvalidDescriptor(Box<Descriptor>),
    #[from(ignore)]
    #[display(fmt = "Unexpected frame type: '{:?}'", "_0")]
    UnexpectedFrameType(u8),
    #[from(ignore)]
    #[display(fmt = "Required field '{:?}' was omitted.", "_0")]
    RequiredFieldOmitted(&'static str),
    #[from(ignore)]
    #[display(fmt = "Unknown {:?} option.", "_0")]
    UnknownEnumOption(&'static str),
    UuidParseError,
    DatetimeParseError,
    #[from(ignore)]
    #[display(fmt = "Unexpected type: '{:?}'", "_0")]
    UnexpectedType(&'static str),
    #[display(fmt = "Value is not valid utf8 string")]
    Utf8Error,
}

#[derive(Debug, Display, From, Clone)]
pub enum AmqpCodecError {
    ParseError(AmqpParseError),
    #[display(fmt = "Bytes left unparsed at the frame trail")]
    UnparsedBytesLeft,
    #[display(fmt = "Max inbound frame size exceeded")]
    MaxSizeExceeded,
    #[display(fmt = "Invalid inbound frame size")]
    InvalidFrameSize,
}

#[derive(Debug, Display, From, Clone)]
pub enum ProtocolIdError {
    InvalidHeader,
    Incompatible,
    Unknown,
    #[display(fmt = "Expected {:?} protocol id, seen {:?} instead.", exp, got)]
    Unexpected {
        exp: ProtocolId,
        got: ProtocolId,
    },
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
