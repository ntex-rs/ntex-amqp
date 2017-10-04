use bytes::{Bytes, BytesMut, BufMut};
use super::errors::*;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;
use super::codec::{self, DecodeFormatted, Encode};
use super::types::*;
use std::u8;

pub(crate) struct CompoundHeader {
    pub size: u32,
    pub count: u32,
}

impl CompoundHeader {
    pub fn empty() -> CompoundHeader {
        CompoundHeader { size: 0, count: 0 }
    }
}

pub const PROTOCOL_HEADER_LEN: usize = 8;
const PROTOCOL_HEADER_PREFIX: &'static [u8] = b"AMQP";
const PROTOCOL_VERSION: &'static [u8] = &[1, 0, 0];

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum ProtocolId {
    Amqp = 0,
    AmqpTls = 2,
    AmqpSasl = 3,
}

pub fn decode_protocol_header(src: &[u8]) -> Result<ProtocolId> {
    ensure!(&src[0..4] == PROTOCOL_HEADER_PREFIX, "Protocol header is invalid. {:?}", src);
    let protocol_id = src[4];
    ensure!(&src[5..8] == PROTOCOL_VERSION, "Protocol version is incompatible. {:?}", &src[5..8]);
    match protocol_id {
        0 => Ok(ProtocolId::Amqp),
        2 => Ok(ProtocolId::AmqpTls),
        3 => Ok(ProtocolId::AmqpSasl),
        _ => Err("Unknown protocol id.".into()),
    }
}

pub fn encode_protocol_header(protocol_id: ProtocolId) -> BytesMut {
    let mut buf = BytesMut::with_capacity(8);
    buf.put_slice(PROTOCOL_HEADER_PREFIX);
    buf.put_u8(protocol_id as u8);
    buf.put_slice(PROTOCOL_VERSION);
    buf
}

pub type List = Vec<Variant>;
pub type Map = HashMap<Variant, Variant>;
pub type Fields = HashMap<Symbol, Variant>;
pub type FilterSet = HashMap<Symbol, Option<ByteStr>>;
pub type Timestamp = DateTime<Utc>;
pub type Symbols = Multiple<Symbol>;
pub type IetfLanguageTags = Multiple<IetfLanguageTag>;

mod definitions;
pub use self::definitions::*;

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum AnnotationKey {
    Ulong(u64),
    Symbol(Symbol),
}

pub type Annotations = HashMap<Symbol, Variant>;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum MessageId {
    Ulong(u64),
    Uuid(Uuid),
    Binary(Bytes),
    String(ByteStr),
}

impl DecodeFormatted for MessageId {
    fn decode_with_format(input: &[u8], fmt: u8) -> Result<(&[u8], Self)> {
        match fmt {
            codec::FORMATCODE_SMALLULONG | codec::FORMATCODE_ULONG | codec::FORMATCODE_ULONG_0 => u64::decode_with_format(input, fmt).map(|(i, o)| (i, MessageId::Ulong(o))),
            codec::FORMATCODE_UUID => Uuid::decode_with_format(input, fmt).map(|(i, o)| (i, MessageId::Uuid(o))),
            codec::FORMATCODE_BINARY8 | codec::FORMATCODE_BINARY32 => Bytes::decode_with_format(input, fmt).map(|(i, o)| (i, MessageId::Binary(o))),
            codec::FORMATCODE_STRING8 | codec::FORMATCODE_STRING32 => ByteStr::decode_with_format(input, fmt).map(|(i, o)| (i, MessageId::String(o))),
            _ => Err(ErrorKind::InvalidFormatCode(fmt).into()),
        }
    }
}

impl Encode for MessageId {
    fn encoded_size(&self) -> usize {
        match *self {
            MessageId::Ulong(v) => v.encoded_size(),
            MessageId::Uuid(ref v) => v.encoded_size(),
            MessageId::Binary(ref v) => v.encoded_size(),
            MessageId::String(ref v) => v.encoded_size(),
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            MessageId::Ulong(v) => v.encode(buf),
            MessageId::Uuid(ref v) => v.encode(buf),
            MessageId::Binary(ref v) => v.encode(buf),
            MessageId::String(ref v) => v.encode(buf),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ErrorCondition {
    AmqpError(AmqpError),
    ConnectionError(ConnectionError),
    SessionError(SessionError),
    LinkError(LinkError),
    Custom(Symbol),
}

impl DecodeFormatted for ErrorCondition {
    fn decode_with_format(input: &[u8], format: u8) -> Result<(&[u8], Self)> {
        let (input, result) = Symbol::decode_with_format(input, format)?;
        if let Ok(r) = AmqpError::try_from(&result) {
            return Ok((input, ErrorCondition::AmqpError(r)));
        }
        if let Ok(r) = ConnectionError::try_from(&result) {
            return Ok((input, ErrorCondition::ConnectionError(r)));
        }
        if let Ok(r) = SessionError::try_from(&result) {
            return Ok((input, ErrorCondition::SessionError(r)));
        }
        if let Ok(r) = LinkError::try_from(&result) {
            return Ok((input, ErrorCondition::LinkError(r)));
        }
        Ok((input, ErrorCondition::Custom(result)))
    }
}

impl Encode for ErrorCondition {
    fn encoded_size(&self) -> usize {
        match *self {
            ErrorCondition::AmqpError(ref v) => v.encoded_size(),
            ErrorCondition::ConnectionError(ref v) => v.encoded_size(),
            ErrorCondition::SessionError(ref v) => v.encoded_size(),
            ErrorCondition::LinkError(ref v) => v.encoded_size(),
            ErrorCondition::Custom(ref v) => v.encoded_size()
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            ErrorCondition::AmqpError(ref v) => v.encode(buf),
            ErrorCondition::ConnectionError(ref v) => v.encode(buf),
            ErrorCondition::SessionError(ref v) => v.encode(buf),
            ErrorCondition::LinkError(ref v) => v.encode(buf),
            ErrorCondition::Custom(ref v) => v.encode(buf)
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DistributionMode {
    Move,
    Copy,
    Custom(Symbol),
}

impl DecodeFormatted for DistributionMode {
    fn decode_with_format(input: &[u8], format: u8) -> Result<(&[u8], Self)> {
        let (input, result) = Symbol::decode_with_format(input, format)?;
        let result = match result.as_str() {
            "move" => DistributionMode::Move,
            "copy" => DistributionMode::Copy,
            _ => DistributionMode::Custom(result),
        };
        Ok((input, result))
    }
}

impl Encode for DistributionMode {
    fn encoded_size(&self) -> usize {
        match *self {
            DistributionMode::Move => 6,
            DistributionMode::Copy => 6,
            DistributionMode::Custom(ref v) => v.encoded_size(),
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            DistributionMode::Move => Symbol::from_static("move").encode(buf),
            DistributionMode::Copy => Symbol::from_static("copy").encode(buf),
            DistributionMode::Custom(ref v) => v.encode(buf),
        }
    }
}

impl SaslInit {
    pub fn prepare_response(authz_id: &str, authn_id: &str, password: &str) -> Bytes {
        Bytes::from(format!("{}\x00{}\x00{}", authz_id, authn_id, password))
    }
}