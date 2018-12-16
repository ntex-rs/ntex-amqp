use uuid;

use crate::protocol::ProtocolId;
use crate::types::Descriptor;

#[derive(Debug, Display, From)]
pub enum AmqpParseError {
    #[display(fmt = "More data required during frame parsing: '{:?}'", "_0")]
    Incomplete(Option<usize>),
    #[display(fmt = "Unexpected format code: '{}'", "_0")]
    InvalidFormatCode(u8),
    #[display(fmt = "Invalid value converting to char: {}", "_0")]
    InvalidChar(u32),
    #[display(fmt = "Unexpected descriptor: '{:?}'", "_0")]
    InvalidDescriptor(Descriptor),
    #[display(fmt = "Unexpected frame type: '{:?}'", "_0")]
    UnexpectedFrameType(u8),
    #[display(fmt = "Required field '{:?}' was omitted.", "_0")]
    RequiredFieldOmitted(&'static str),
    #[display(fmt = "Unknown {:?} option.", "_0")]
    UnknownEnumOption(&'static str),
    UuidParseError(uuid::BytesError),
    Utf8Error(std::str::Utf8Error),
}

#[derive(Debug, Display, From)]
pub enum AmqpCodecError {
    ParseError(AmqpParseError),
    #[display(fmt = "bytes left unparsed at the frame trail")]
    UnparsedBytesLeft,
    Io(std::io::Error),
}

#[derive(Debug, Display, From)]
pub enum ProtocolIdError {
    InvalidHeader,
    Incompatible,
    Unknown,
    #[display(fmt = "Expected {:?} protocol id, seen {:?} instead.", exp, got)]
    Unexpected {
        exp: ProtocolId,
        got: ProtocolId,
    },
    Disconnected,
    #[display(fmt = "io error: {:?}", "_0")]
    Io(std::io::Error),
}
