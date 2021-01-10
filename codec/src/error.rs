use crate::protocol::ProtocolId;
use crate::types::Descriptor;

#[derive(Debug, Display, From, Clone)]
pub enum AmqpParseError {
    #[display(fmt = "Loaded item size is invalid")]
    InvalidSize,
    #[display(fmt = "More data required during frame parsing: '{:?}'", "_0")]
    Incomplete(Option<usize>),
    #[from(ignore)]
    #[display(fmt = "Unexpected format code: '{}'", "_0")]
    InvalidFormatCode(u8),
    #[display(fmt = "Invalid value converting to char: {}", "_0")]
    InvalidChar(u32),
    #[display(fmt = "Unexpected descriptor: '{:?}'", "_0")]
    InvalidDescriptor(Descriptor),
    #[from(ignore)]
    #[display(fmt = "Unexpected frame type: '{:?}'", "_0")]
    UnexpectedFrameType(u8),
    #[from(ignore)]
    #[display(fmt = "Required field '{:?}' was omitted.", "_0")]
    RequiredFieldOmitted(&'static str),
    #[from(ignore)]
    #[display(fmt = "Unknown {:?} option.", "_0")]
    UnknownEnumOption(&'static str),
    UuidParseError(uuid::Error),
    #[from(ignore)]
    #[display(fmt = "Unexpected type: '{:?}'", "_0")]
    UnexpectedType(&'static str),
    Utf8Error(std::str::Utf8Error),
}

#[derive(Debug, Display, From, Clone)]
pub enum AmqpCodecError {
    ParseError(AmqpParseError),
    #[display(fmt = "bytes left unparsed at the frame trail")]
    UnparsedBytesLeft,
    #[display(fmt = "max inbound frame size exceeded")]
    MaxSizeExceeded,
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
