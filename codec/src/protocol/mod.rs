#![allow(clippy::derivable_impls)]
use std::fmt;

use chrono::{DateTime, Utc};
use derive_more::From;
use ntex_bytes::{Buf, BufMut, ByteString, Bytes, BytesMut};
use uuid::Uuid;

use crate::codec::{self, Decode, DecodeFormatted, Encode};
use crate::types::{
    Descriptor, List, Multiple, StaticSymbol, Str, Symbol, Variant, VecStringMap, VecSymbolMap,
};
use crate::{error::AmqpParseError, message::Message, HashMap};

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum ProtocolId {
    Amqp = 0,
    AmqpTls = 2,
    AmqpSasl = 3,
}

pub type Map = HashMap<Variant, Variant>;
pub type StringVariantMap = HashMap<Str, Variant>;
pub type Fields = HashMap<Symbol, Variant>;
pub type FilterSet = HashMap<Symbol, Option<ByteString>>;
pub type FieldsVec = VecSymbolMap;
pub type Timestamp = DateTime<Utc>;
pub type Symbols = Multiple<Symbol>;
pub type IetfLanguageTags = Multiple<IetfLanguageTag>;
pub type Annotations = HashMap<Symbol, Variant>;

#[allow(
    clippy::unreadable_literal,
    clippy::match_bool,
    clippy::large_enum_variant
)]
mod definitions;
pub use self::definitions::*;

#[derive(Debug, Eq, PartialEq, Clone, From)]
pub enum MessageId {
    Ulong(u64),
    Uuid(Uuid),
    Binary(Bytes),
    String(ByteString),
}

impl From<usize> for MessageId {
    fn from(id: usize) -> MessageId {
        MessageId::Ulong(id as u64)
    }
}

impl From<i32> for MessageId {
    fn from(id: i32) -> MessageId {
        MessageId::Ulong(id as u64)
    }
}

impl DecodeFormatted for MessageId {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_SMALLULONG | codec::FORMATCODE_ULONG | codec::FORMATCODE_ULONG_0 => {
                u64::decode_with_format(input, fmt).map(MessageId::Ulong)
            }
            codec::FORMATCODE_UUID => Uuid::decode_with_format(input, fmt).map(MessageId::Uuid),
            codec::FORMATCODE_BINARY8 | codec::FORMATCODE_BINARY32 => {
                Bytes::decode_with_format(input, fmt).map(MessageId::Binary)
            }
            codec::FORMATCODE_STRING8 | codec::FORMATCODE_STRING32 => {
                ByteString::decode_with_format(input, fmt).map(MessageId::String)
            }
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
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

#[derive(Clone, Debug, PartialEq, Eq, From)]
pub enum ErrorCondition {
    AmqpError(AmqpError),
    ConnectionError(ConnectionError),
    SessionError(SessionError),
    LinkError(LinkError),
    Custom(Symbol),
}

impl Default for ErrorCondition {
    fn default() -> ErrorCondition {
        ErrorCondition::Custom(Symbol(Str::from("Unknown")))
    }
}

impl DecodeFormatted for ErrorCondition {
    #[inline]
    fn decode_with_format(input: &mut Bytes, format: u8) -> Result<Self, AmqpParseError> {
        let result = Symbol::decode_with_format(input, format)?;
        if let Ok(r) = AmqpError::try_from(&result) {
            return Ok(ErrorCondition::AmqpError(r));
        }
        if let Ok(r) = ConnectionError::try_from(&result) {
            return Ok(ErrorCondition::ConnectionError(r));
        }
        if let Ok(r) = SessionError::try_from(&result) {
            return Ok(ErrorCondition::SessionError(r));
        }
        if let Ok(r) = LinkError::try_from(&result) {
            return Ok(ErrorCondition::LinkError(r));
        }
        Ok(ErrorCondition::Custom(result))
    }
}

impl Encode for ErrorCondition {
    fn encoded_size(&self) -> usize {
        match *self {
            ErrorCondition::AmqpError(ref v) => v.encoded_size(),
            ErrorCondition::ConnectionError(ref v) => v.encoded_size(),
            ErrorCondition::SessionError(ref v) => v.encoded_size(),
            ErrorCondition::LinkError(ref v) => v.encoded_size(),
            ErrorCondition::Custom(ref v) => v.encoded_size(),
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            ErrorCondition::AmqpError(ref v) => v.encode(buf),
            ErrorCondition::ConnectionError(ref v) => v.encode(buf),
            ErrorCondition::SessionError(ref v) => v.encode(buf),
            ErrorCondition::LinkError(ref v) => v.encode(buf),
            ErrorCondition::Custom(ref v) => v.encode(buf),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DistributionMode {
    Move,
    Copy,
    Custom(Symbol),
}

impl DecodeFormatted for DistributionMode {
    fn decode_with_format(input: &mut Bytes, format: u8) -> Result<Self, AmqpParseError> {
        let result = Symbol::decode_with_format(input, format)?;
        let result = match result.as_str() {
            "move" => DistributionMode::Move,
            "copy" => DistributionMode::Copy,
            _ => DistributionMode::Custom(result),
        };
        Ok(result)
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
            DistributionMode::Move => Symbol::from("move").encode(buf),
            DistributionMode::Copy => Symbol::from("copy").encode(buf),
            DistributionMode::Custom(ref v) => v.encode(buf),
        }
    }
}

impl SaslInit {
    pub fn prepare_response(authz_id: &str, authn_id: &str, password: &str) -> Bytes {
        Bytes::from(format!("{authz_id}\x00{authn_id}\x00{password}"))
    }
}

#[derive(Debug, Clone, From, PartialEq, Eq)]
pub enum TransferBody {
    Data(Bytes),
    Message(Message),
}

impl TransferBody {
    #[inline]
    pub fn len(&self) -> usize {
        self.encoded_size()
    }

    #[inline]
    pub fn message_format(&self) -> Option<MessageFormat> {
        match self {
            TransferBody::Data(_) => None,
            TransferBody::Message(ref data) => data.0.message_format,
        }
    }
}

impl Encode for TransferBody {
    #[inline]
    fn encoded_size(&self) -> usize {
        match self {
            TransferBody::Data(ref data) => data.len(),
            TransferBody::Message(ref data) => data.encoded_size(),
        }
    }
    #[inline]
    fn encode(&self, dst: &mut BytesMut) {
        match *self {
            TransferBody::Data(ref data) => dst.put_slice(data),
            TransferBody::Message(ref data) => data.encode(dst),
        }
    }
}

impl Transfer {
    #[inline]
    pub fn get_body(&self) -> Option<&Bytes> {
        match self.body() {
            Some(TransferBody::Data(ref b)) => Some(b),
            _ => None,
        }
    }

    #[inline]
    pub fn load_message<T: Decode>(&self) -> Result<T, AmqpParseError> {
        if let Some(TransferBody::Data(ref b)) = self.body() {
            Ok(T::decode(&mut b.clone())?)
        } else {
            Err(AmqpParseError::UnexpectedType("body"))
        }
    }
}

impl Default for Role {
    fn default() -> Role {
        Role::Sender
    }
}

impl Default for SenderSettleMode {
    fn default() -> SenderSettleMode {
        SenderSettleMode::Mixed
    }
}

impl Default for ReceiverSettleMode {
    fn default() -> ReceiverSettleMode {
        ReceiverSettleMode::First
    }
}

impl Default for TerminusDurability {
    fn default() -> TerminusDurability {
        TerminusDurability::None
    }
}

impl Default for TerminusExpiryPolicy {
    fn default() -> TerminusExpiryPolicy {
        TerminusExpiryPolicy::LinkDetach
    }
}

impl Default for SaslCode {
    fn default() -> SaslCode {
        SaslCode::Ok
    }
}

#[cfg(test)]
mod tests {
    use ntex_bytes::BytesMut;
    use uuid::Uuid;

    use super::*;
    use crate::codec::{Decode, Encode};
    use crate::error::AmqpCodecError;

    #[test]
    fn test_message_id() -> Result<(), AmqpCodecError> {
        let id = MessageId::Uuid(Uuid::new_v4());

        let mut buf = BytesMut::new();
        buf.reserve(id.encoded_size());
        id.encode(&mut buf);

        let new_id = MessageId::decode(&mut buf.freeze())?;
        assert_eq!(id, new_id);
        Ok(())
    }

    #[test]
    fn test_properties() -> Result<(), AmqpCodecError> {
        let id = Uuid::new_v4();
        let mut props = Properties::default();
        props.correlation_id = Some(id.into());

        let mut buf = BytesMut::new();
        buf.reserve(id.encoded_size());
        props.encode(&mut buf);

        let props2 = Properties::decode(&mut buf.freeze())?;
        assert_eq!(props, props2);
        Ok(())
    }
}
