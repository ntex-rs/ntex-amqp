use crate::protocol::{AmqpError, ProtocolId};
pub use crate::protocol::{Error, ErrorInner};
use crate::types::Descriptor;

#[derive(Debug, Display, From, Clone)]
pub enum AmqpParseError {
    #[display("Loaded item size is invalid")]
    InvalidSize,
    #[display("More data required during frame parsing: '{:?}'", "_0")]
    Incomplete(usize),
    #[from(ignore)]
    #[display("Unexpected format code: '{}'", "_0")]
    InvalidFormatCode(u8),
    #[display("Invalid value converting to char: {}", "_0")]
    InvalidChar(u32),
    #[display("Unexpected descriptor: '{:?}'", "_0")]
    InvalidDescriptor(Box<Descriptor>),
    #[from(ignore)]
    #[display("Unexpected frame type: '{:?}'", "_0")]
    UnexpectedFrameType(u8),
    #[from(ignore)]
    #[display("Required field '{:?}' was omitted.", "_0")]
    RequiredFieldOmitted(&'static str),
    #[from(ignore)]
    #[display("Unknown {:?} option.", "_0")]
    UnknownEnumOption(&'static str),
    UuidParseError,
    DatetimeParseError,
    #[from(ignore)]
    #[display("Unexpected type: '{:?}'", "_0")]
    UnexpectedType(&'static str),
    #[display("Value is not valid utf8 string")]
    Utf8Error,
}

#[derive(Debug, Display, From, Clone)]
pub enum AmqpCodecError {
    ParseError(AmqpParseError),
    #[display("Bytes left unparsed at the frame trail")]
    UnparsedBytesLeft,
    #[display("Max inbound frame size exceeded")]
    MaxSizeExceeded,
    #[display("Invalid inbound frame size")]
    InvalidFrameSize,
}

#[derive(Debug, Display, From, Clone)]
pub enum ProtocolIdError {
    InvalidHeader,
    Incompatible,
    Unknown,
    #[display("Expected {:?} protocol id, seen {:?} instead.", exp, got)]
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
