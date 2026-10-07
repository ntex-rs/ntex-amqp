#![allow(clippy::derivable_impls)]
use std::fmt;

use chrono::{DateTime, Utc};
use derive_more::From;
use ntex_bytes::{Buf, BufMut, BytePages, ByteString, Bytes};
use uuid::Uuid;

use crate::codec::{self, Decode, DecodeFormatted, Encode};
use crate::types::{
    Descriptor, List, Multiple, StaticSymbol, Str, Symbol, Variant, VecStringMap, VecSymbolMap,
};
use crate::{HashMap, error::AmqpParseError, message::Message};

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// Smallest max-frame-size value a peer is allowed to advertise
pub const MIN_MAX_FRAME_SIZE: u32 = 512;

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

    fn encode(&self, buf: &mut BytePages) {
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

    fn encode(&self, buf: &mut BytePages) {
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

    fn encode(&self, buf: &mut BytePages) {
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

#[derive(Debug, Clone, From)]
pub enum TransferBody {
    Data(Bytes),
    Pages(BytePages),
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
            TransferBody::Data(_) | TransferBody::Pages(_) => None,
            TransferBody::Message(data) => data.message_format(),
        }
    }
}

impl Encode for TransferBody {
    #[inline]
    fn encoded_size(&self) -> usize {
        match self {
            TransferBody::Data(data) => data.len(),
            TransferBody::Pages(data) => data.len(),
            TransferBody::Message(data) => data.encoded_size(),
        }
    }

    #[inline]
    fn encode(&self, dst: &mut BytePages) {
        match *self {
            TransferBody::Data(ref data) => dst.append(data),
            TransferBody::Pages(ref data) => data.copy_to(dst),
            TransferBody::Message(ref data) => data.encode(dst),
        }
    }
}

impl Eq for TransferBody {}

impl PartialEq for TransferBody {
    fn eq(&self, other: &TransferBody) -> bool {
        match self {
            TransferBody::Data(data) => {
                if let TransferBody::Data(d) = other {
                    data == d
                } else {
                    false
                }
            }
            TransferBody::Message(msg) => {
                if let TransferBody::Message(msg2) = other {
                    msg == msg2
                } else {
                    false
                }
            }
            TransferBody::Pages(_) => false,
        }
    }
}

impl Transfer {
    #[inline]
    pub fn get_body(&self) -> Option<&Bytes> {
        match self.body() {
            Some(TransferBody::Data(b)) => Some(b),
            _ => None,
        }
    }

    #[inline]
    pub fn load_message<T: Decode>(&self) -> Result<T, AmqpParseError> {
        if let Some(TransferBody::Data(b)) = self.body() {
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
    use uuid::Uuid;

    use super::*;
    use crate::codec::{Decode, Encode};
    use crate::error::AmqpCodecError;

    #[test]
    fn test_message_id() -> Result<(), AmqpCodecError> {
        let id = MessageId::Uuid(Uuid::new_v4());

        let mut buf = BytePages::default();
        id.encode(&mut buf);

        let new_id = MessageId::decode(&mut buf.freeze())?;
        assert_eq!(id, new_id);
        Ok(())
    }

    #[test]
    fn test_properties() -> Result<(), AmqpCodecError> {
        let id = Uuid::new_v4();
        let props = Properties {
            correlation_id: Some(id.into()),
            ..Default::default()
        };

        let mut buf = BytePages::default();
        props.encode(&mut buf);

        let props2 = Properties::decode(&mut buf.freeze())?;
        assert_eq!(props, props2);
        Ok(())
    }

    fn encoded<T: Encode>(value: &T) -> Bytes {
        let mut buf = BytePages::default();
        value.encode(&mut buf);
        let buf = buf.freeze();
        assert_eq!(value.encoded_size(), buf.len(), "encoded_size mismatch");
        buf
    }

    #[test]
    fn message_id_roundtrip() {
        let ids = vec![
            MessageId::Ulong(0),
            MessageId::Ulong(42),
            MessageId::Ulong(u64::MAX),
            MessageId::Uuid(Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef)),
            MessageId::Binary(Bytes::from_static(b"binary-id")),
            MessageId::String(ByteString::from("string-id")),
        ];

        for id in ids {
            let buf = encoded(&id);
            assert_eq!(MessageId::decode(&mut buf.clone()).unwrap(), id);
        }
    }

    #[test]
    fn message_id_conversions() {
        assert_eq!(MessageId::from(7usize), MessageId::Ulong(7));
        assert_eq!(MessageId::from(7i32), MessageId::Ulong(7));
        // negative ids are sign-extended rather than rejected
        assert_eq!(MessageId::from(-1i32), MessageId::Ulong(u64::MAX));
        assert_eq!(MessageId::from(7u64), MessageId::Ulong(7));
        assert_eq!(
            MessageId::from(ByteString::from("s")),
            MessageId::String(ByteString::from("s"))
        );
        assert_eq!(
            MessageId::from(Bytes::from_static(b"b")),
            MessageId::Binary(Bytes::from_static(b"b"))
        );
    }

    #[test]
    fn message_id_rejects_other_types() {
        // boolean true
        let res = MessageId::decode(&mut Bytes::from_static(b"\x41"));
        assert!(matches!(res, Err(AmqpParseError::InvalidFormatCode(0x41))));
    }

    #[test]
    fn error_condition_roundtrip() {
        let conditions = vec![
            ErrorCondition::AmqpError(AmqpError::NotFound),
            ErrorCondition::ConnectionError(ConnectionError::ConnectionForced),
            ErrorCondition::SessionError(SessionError::WindowViolation),
            ErrorCondition::LinkError(LinkError::DetachForced),
            ErrorCondition::Custom(Symbol::from("vendor:custom")),
        ];

        for cond in conditions {
            let buf = encoded(&cond);
            assert_eq!(ErrorCondition::decode(&mut buf.clone()).unwrap(), cond);
        }

        assert_eq!(
            ErrorCondition::default(),
            ErrorCondition::Custom(Symbol::from("Unknown"))
        );
        assert_eq!(
            ErrorCondition::from(AmqpError::NotFound),
            ErrorCondition::AmqpError(AmqpError::NotFound)
        );
    }

    #[test]
    fn error_display_matches_debug() {
        let err = Error::build()
            .condition(ErrorCondition::AmqpError(AmqpError::NotFound))
            .description(ByteString::from("missing"))
            .finish();
        assert_eq!(err.to_string(), format!("{err:?}"));
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn distribution_mode_roundtrip() {
        let modes = vec![
            DistributionMode::Move,
            DistributionMode::Copy,
            DistributionMode::Custom(Symbol::from("vendor:mode")),
        ];
        for mode in modes {
            let buf = encoded(&mode);
            assert_eq!(DistributionMode::decode(&mut buf.clone()).unwrap(), mode);
        }

        assert_eq!(encoded(&DistributionMode::Move).as_ref(), b"\xa3\x04move");
        assert_eq!(encoded(&DistributionMode::Copy).as_ref(), b"\xa3\x04copy");
    }

    #[test]
    fn sasl_init_prepare_response() {
        assert_eq!(
            SaslInit::prepare_response("", "user", "pass"),
            Bytes::from_static(b"\x00user\x00pass")
        );
        assert_eq!(
            SaslInit::prepare_response("authz", "user", ""),
            Bytes::from_static(b"authz\x00user\x00")
        );
    }

    #[test]
    fn transfer_body_data() {
        let data = Bytes::from_static(b"hello");
        let body = TransferBody::from(data.clone());

        assert_eq!(body.len(), 5);
        assert_eq!(body.message_format(), None);
        assert_eq!(encoded(&body).as_ref(), b"hello");
        assert_eq!(body, TransferBody::Data(data.clone()));
        assert_ne!(body, TransferBody::Data(Bytes::from_static(b"other")));
        assert_ne!(body, TransferBody::Message(Message::default()));
    }

    #[test]
    fn transfer_body_pages() {
        let mut pages = BytePages::default();
        pages.extend_from_slice(b"hello");
        let body = TransferBody::from(pages);

        assert_eq!(body.len(), 5);
        assert_eq!(body.message_format(), None);
        assert_eq!(encoded(&body).as_ref(), b"hello");
        // pages bodies never compare equal, not even to an identical one
        let mut other = BytePages::default();
        other.extend_from_slice(b"hello");
        assert_ne!(body, TransferBody::from(other));
        assert_ne!(TransferBody::Data(Bytes::from_static(b"hello")), body);
    }

    #[test]
    fn transfer_body_message() {
        let mut msg = Message::default();
        msg.set_body(|b| b.set_data(Bytes::from_static(b"hello")));
        msg.set_format(7);

        let body = TransferBody::from(msg.clone());
        assert_eq!(body.message_format(), Some(7));
        assert_eq!(body.len(), msg.encoded_size());
        assert_eq!(encoded(&body), encoded(&msg));
        assert_eq!(body, TransferBody::Message(msg));
        assert_ne!(body, TransferBody::Data(Bytes::from_static(b"hello")));
    }

    #[test]
    fn transfer_get_body_and_load_message() {
        let payload = encoded(&Variant::from("payload"));
        let transfer = Transfer::build()
            .handle(1)
            .body(TransferBody::Data(payload.clone()))
            .finish();

        assert_eq!(transfer.get_body(), Some(&payload));
        assert_eq!(
            transfer.load_message::<Variant>().unwrap(),
            Variant::from("payload")
        );

        let empty = Transfer::build().handle(1).finish();
        assert_eq!(empty.get_body(), None);
        assert!(matches!(
            empty.load_message::<Variant>(),
            Err(AmqpParseError::UnexpectedType("body"))
        ));

        let msg_body = Transfer::build()
            .handle(1)
            .body(TransferBody::Message(Message::default()))
            .finish();
        assert_eq!(msg_body.get_body(), None);
        assert!(msg_body.load_message::<Variant>().is_err());
    }

    #[test]
    fn protocol_defaults() {
        assert_eq!(Role::default(), Role::Sender);
        assert_eq!(SenderSettleMode::default(), SenderSettleMode::Mixed);
        assert_eq!(ReceiverSettleMode::default(), ReceiverSettleMode::First);
        assert_eq!(TerminusDurability::default(), TerminusDurability::None);
        assert_eq!(
            TerminusExpiryPolicy::default(),
            TerminusExpiryPolicy::LinkDetach
        );
        assert_eq!(SaslCode::default(), SaslCode::Ok);
    }
}
