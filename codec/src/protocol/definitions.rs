#![allow(unused_assignments, unused_variables, unreachable_patterns)]
use super::*;
use crate::codec::{self, decode_format_code, decode_list_header, Decode, DecodeFormatted, Encode};
use crate::error::AmqpParseError;
use derive_more::From;
use ntex_bytes::{BufMut, ByteString, Bytes, BytesMut};
use std::u8;
use uuid::Uuid;
#[derive(Clone, Debug, PartialEq, From)]
pub enum Frame {
    Open(Open),
    Begin(Begin),
    Attach(Attach),
    Flow(Flow),
    Transfer(Transfer),
    Disposition(Disposition),
    Detach(Detach),
    End(End),
    Close(Close),
    Empty,
}
impl Frame {
    pub fn name(&self) -> &'static str {
        match self {
            Frame::Open(_) => "Open",
            Frame::Begin(_) => "Begin",
            Frame::Attach(_) => "Attach",
            Frame::Flow(_) => "Flow",
            Frame::Transfer(_) => "Transfer",
            Frame::Disposition(_) => "Disposition",
            Frame::Detach(_) => "Detach",
            Frame::End(_) => "End",
            Frame::Close(_) => "Close",
            Frame::Empty => "Empty",
        }
    }
}
impl Decode for Frame {
    fn decode(input: &mut Bytes) -> Result<Self, AmqpParseError> {
        if input.is_empty() {
            Ok(Frame::Empty)
        } else {
            let fmt = decode_format_code(input)?;
            validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
            let descriptor = Descriptor::decode(input)?;
            match descriptor {
                Descriptor::Ulong(16) => decode_open_inner(input).map(Frame::Open),
                Descriptor::Ulong(17) => decode_begin_inner(input).map(Frame::Begin),
                Descriptor::Ulong(18) => decode_attach_inner(input).map(Frame::Attach),
                Descriptor::Ulong(19) => decode_flow_inner(input).map(Frame::Flow),
                Descriptor::Ulong(20) => decode_transfer_inner(input).map(Frame::Transfer),
                Descriptor::Ulong(21) => decode_disposition_inner(input).map(Frame::Disposition),
                Descriptor::Ulong(22) => decode_detach_inner(input).map(Frame::Detach),
                Descriptor::Ulong(23) => decode_end_inner(input).map(Frame::End),
                Descriptor::Ulong(24) => decode_close_inner(input).map(Frame::Close),
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:open:list" => {
                    decode_open_inner(input).map(Frame::Open)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:begin:list" => {
                    decode_begin_inner(input).map(Frame::Begin)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:attach:list" => {
                    decode_attach_inner(input).map(Frame::Attach)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:flow:list" => {
                    decode_flow_inner(input).map(Frame::Flow)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:transfer:list" => {
                    decode_transfer_inner(input).map(Frame::Transfer)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:disposition:list" => {
                    decode_disposition_inner(input).map(Frame::Disposition)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:detach:list" => {
                    decode_detach_inner(input).map(Frame::Detach)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:end:list" => {
                    decode_end_inner(input).map(Frame::End)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:close:list" => {
                    decode_close_inner(input).map(Frame::Close)
                }
                _ => Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor))),
            }
        }
    }
}
impl Encode for Frame {
    fn encoded_size(&self) -> usize {
        match *self {
            Frame::Open(ref v) => encoded_size_open_inner(v),
            Frame::Begin(ref v) => encoded_size_begin_inner(v),
            Frame::Attach(ref v) => encoded_size_attach_inner(v),
            Frame::Flow(ref v) => encoded_size_flow_inner(v),
            Frame::Transfer(ref v) => encoded_size_transfer_inner(v),
            Frame::Disposition(ref v) => encoded_size_disposition_inner(v),
            Frame::Detach(ref v) => encoded_size_detach_inner(v),
            Frame::End(ref v) => encoded_size_end_inner(v),
            Frame::Close(ref v) => encoded_size_close_inner(v),
            Frame::Empty => 0,
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            Frame::Open(ref v) => encode_open_inner(v, buf),
            Frame::Begin(ref v) => encode_begin_inner(v, buf),
            Frame::Attach(ref v) => encode_attach_inner(v, buf),
            Frame::Flow(ref v) => encode_flow_inner(v, buf),
            Frame::Transfer(ref v) => encode_transfer_inner(v, buf),
            Frame::Disposition(ref v) => encode_disposition_inner(v, buf),
            Frame::Detach(ref v) => encode_detach_inner(v, buf),
            Frame::End(ref v) => encode_end_inner(v, buf),
            Frame::Close(ref v) => encode_close_inner(v, buf),
            Frame::Empty => (),
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum Section {
    Header(Header),
    DeliveryAnnotations(DeliveryAnnotations),
    MessageAnnotations(MessageAnnotations),
    ApplicationProperties(ApplicationProperties),
    Data(Data),
    AmqpSequence(AmqpSequence),
    AmqpValue(AmqpValue),
    Footer(Footer),
    Properties(Properties),
}
impl DecodeFormatted for Section {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        match descriptor {
            Descriptor::Ulong(112) => decode_header_inner(input).map(Section::Header),
            Descriptor::Ulong(113) => {
                decode_delivery_annotations_inner(input).map(Section::DeliveryAnnotations)
            }
            Descriptor::Ulong(114) => {
                decode_message_annotations_inner(input).map(Section::MessageAnnotations)
            }
            Descriptor::Ulong(116) => {
                decode_application_properties_inner(input).map(Section::ApplicationProperties)
            }
            Descriptor::Ulong(117) => decode_data_inner(input).map(Section::Data),
            Descriptor::Ulong(118) => decode_amqp_sequence_inner(input).map(Section::AmqpSequence),
            Descriptor::Ulong(119) => decode_amqp_value_inner(input).map(Section::AmqpValue),
            Descriptor::Ulong(120) => decode_footer_inner(input).map(Section::Footer),
            Descriptor::Ulong(115) => decode_properties_inner(input).map(Section::Properties),
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:header:list" => {
                decode_header_inner(input).map(Section::Header)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:delivery-annotations:map" => {
                decode_delivery_annotations_inner(input).map(Section::DeliveryAnnotations)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:message-annotations:map" => {
                decode_message_annotations_inner(input).map(Section::MessageAnnotations)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:application-properties:map" => {
                decode_application_properties_inner(input).map(Section::ApplicationProperties)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:data:binary" => {
                decode_data_inner(input).map(Section::Data)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:amqp-sequence:list" => {
                decode_amqp_sequence_inner(input).map(Section::AmqpSequence)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:amqp-value:*" => {
                decode_amqp_value_inner(input).map(Section::AmqpValue)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:footer:map" => {
                decode_footer_inner(input).map(Section::Footer)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:properties:list" => {
                decode_properties_inner(input).map(Section::Properties)
            }
            _ => Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor))),
        }
    }
}
impl Encode for Section {
    fn encoded_size(&self) -> usize {
        match *self {
            Section::Header(ref v) => encoded_size_header_inner(v),
            Section::DeliveryAnnotations(ref v) => encoded_size_delivery_annotations_inner(v),
            Section::MessageAnnotations(ref v) => encoded_size_message_annotations_inner(v),
            Section::ApplicationProperties(ref v) => encoded_size_application_properties_inner(v),
            Section::Data(ref v) => encoded_size_data_inner(v),
            Section::AmqpSequence(ref v) => encoded_size_amqp_sequence_inner(v),
            Section::AmqpValue(ref v) => encoded_size_amqp_value_inner(v),
            Section::Footer(ref v) => encoded_size_footer_inner(v),
            Section::Properties(ref v) => encoded_size_properties_inner(v),
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            Section::Header(ref v) => encode_header_inner(v, buf),
            Section::DeliveryAnnotations(ref v) => encode_delivery_annotations_inner(v, buf),
            Section::MessageAnnotations(ref v) => encode_message_annotations_inner(v, buf),
            Section::ApplicationProperties(ref v) => encode_application_properties_inner(v, buf),
            Section::Data(ref v) => encode_data_inner(v, buf),
            Section::AmqpSequence(ref v) => encode_amqp_sequence_inner(v, buf),
            Section::AmqpValue(ref v) => encode_amqp_value_inner(v, buf),
            Section::Footer(ref v) => encode_footer_inner(v, buf),
            Section::Properties(ref v) => encode_properties_inner(v, buf),
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum SaslFrameBody {
    SaslMechanisms(SaslMechanisms),
    SaslInit(SaslInit),
    SaslChallenge(SaslChallenge),
    SaslResponse(SaslResponse),
    SaslOutcome(SaslOutcome),
}
impl DecodeFormatted for SaslFrameBody {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        match descriptor {
            Descriptor::Ulong(64) => {
                decode_sasl_mechanisms_inner(input).map(SaslFrameBody::SaslMechanisms)
            }
            Descriptor::Ulong(65) => decode_sasl_init_inner(input).map(SaslFrameBody::SaslInit),
            Descriptor::Ulong(66) => {
                decode_sasl_challenge_inner(input).map(SaslFrameBody::SaslChallenge)
            }
            Descriptor::Ulong(67) => {
                decode_sasl_response_inner(input).map(SaslFrameBody::SaslResponse)
            }
            Descriptor::Ulong(68) => {
                decode_sasl_outcome_inner(input).map(SaslFrameBody::SaslOutcome)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:sasl-mechanisms:list" => {
                decode_sasl_mechanisms_inner(input).map(SaslFrameBody::SaslMechanisms)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:sasl-init:list" => {
                decode_sasl_init_inner(input).map(SaslFrameBody::SaslInit)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:sasl-challenge:list" => {
                decode_sasl_challenge_inner(input).map(SaslFrameBody::SaslChallenge)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:sasl-response:list" => {
                decode_sasl_response_inner(input).map(SaslFrameBody::SaslResponse)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:sasl-outcome:list" => {
                decode_sasl_outcome_inner(input).map(SaslFrameBody::SaslOutcome)
            }
            _ => Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor))),
        }
    }
}
impl Encode for SaslFrameBody {
    fn encoded_size(&self) -> usize {
        match *self {
            SaslFrameBody::SaslMechanisms(ref v) => encoded_size_sasl_mechanisms_inner(v),
            SaslFrameBody::SaslInit(ref v) => encoded_size_sasl_init_inner(v),
            SaslFrameBody::SaslChallenge(ref v) => encoded_size_sasl_challenge_inner(v),
            SaslFrameBody::SaslResponse(ref v) => encoded_size_sasl_response_inner(v),
            SaslFrameBody::SaslOutcome(ref v) => encoded_size_sasl_outcome_inner(v),
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            SaslFrameBody::SaslMechanisms(ref v) => encode_sasl_mechanisms_inner(v, buf),
            SaslFrameBody::SaslInit(ref v) => encode_sasl_init_inner(v, buf),
            SaslFrameBody::SaslChallenge(ref v) => encode_sasl_challenge_inner(v, buf),
            SaslFrameBody::SaslResponse(ref v) => encode_sasl_response_inner(v, buf),
            SaslFrameBody::SaslOutcome(ref v) => encode_sasl_outcome_inner(v, buf),
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum DeliveryState {
    Received(Received),
    Accepted(Accepted),
    Rejected(Rejected),
    Released(Released),
    Modified(Modified),
}
impl DecodeFormatted for DeliveryState {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        match descriptor {
            Descriptor::Ulong(35) => decode_received_inner(input).map(DeliveryState::Received),
            Descriptor::Ulong(36) => decode_accepted_inner(input).map(DeliveryState::Accepted),
            Descriptor::Ulong(37) => decode_rejected_inner(input).map(DeliveryState::Rejected),
            Descriptor::Ulong(38) => decode_released_inner(input).map(DeliveryState::Released),
            Descriptor::Ulong(39) => decode_modified_inner(input).map(DeliveryState::Modified),
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:received:list" => {
                decode_received_inner(input).map(DeliveryState::Received)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:accepted:list" => {
                decode_accepted_inner(input).map(DeliveryState::Accepted)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:rejected:list" => {
                decode_rejected_inner(input).map(DeliveryState::Rejected)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:released:list" => {
                decode_released_inner(input).map(DeliveryState::Released)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:modified:list" => {
                decode_modified_inner(input).map(DeliveryState::Modified)
            }
            _ => Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor))),
        }
    }
}
impl Encode for DeliveryState {
    fn encoded_size(&self) -> usize {
        match *self {
            DeliveryState::Received(ref v) => encoded_size_received_inner(v),
            DeliveryState::Accepted(ref v) => encoded_size_accepted_inner(v),
            DeliveryState::Rejected(ref v) => encoded_size_rejected_inner(v),
            DeliveryState::Released(ref v) => encoded_size_released_inner(v),
            DeliveryState::Modified(ref v) => encoded_size_modified_inner(v),
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            DeliveryState::Received(ref v) => encode_received_inner(v, buf),
            DeliveryState::Accepted(ref v) => encode_accepted_inner(v, buf),
            DeliveryState::Rejected(ref v) => encode_rejected_inner(v, buf),
            DeliveryState::Released(ref v) => encode_released_inner(v, buf),
            DeliveryState::Modified(ref v) => encode_modified_inner(v, buf),
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Accepted(Accepted),
    Rejected(Rejected),
    Released(Released),
    Modified(Modified),
}
impl DecodeFormatted for Outcome {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        match descriptor {
            Descriptor::Ulong(36) => decode_accepted_inner(input).map(Outcome::Accepted),
            Descriptor::Ulong(37) => decode_rejected_inner(input).map(Outcome::Rejected),
            Descriptor::Ulong(38) => decode_released_inner(input).map(Outcome::Released),
            Descriptor::Ulong(39) => decode_modified_inner(input).map(Outcome::Modified),
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:accepted:list" => {
                decode_accepted_inner(input).map(Outcome::Accepted)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:rejected:list" => {
                decode_rejected_inner(input).map(Outcome::Rejected)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:released:list" => {
                decode_released_inner(input).map(Outcome::Released)
            }
            Descriptor::Symbol(ref a) if a.as_str() == "amqp:modified:list" => {
                decode_modified_inner(input).map(Outcome::Modified)
            }
            _ => Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor))),
        }
    }
}
impl Encode for Outcome {
    fn encoded_size(&self) -> usize {
        match *self {
            Outcome::Accepted(ref v) => encoded_size_accepted_inner(v),
            Outcome::Rejected(ref v) => encoded_size_rejected_inner(v),
            Outcome::Released(ref v) => encoded_size_released_inner(v),
            Outcome::Modified(ref v) => encoded_size_modified_inner(v),
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            Outcome::Accepted(ref v) => encode_accepted_inner(v, buf),
            Outcome::Rejected(ref v) => encode_rejected_inner(v, buf),
            Outcome::Released(ref v) => encode_released_inner(v, buf),
            Outcome::Modified(ref v) => encode_modified_inner(v, buf),
        }
    }
}
pub type Handle = u32;
pub type Seconds = u32;
pub type Milliseconds = u32;
pub type DeliveryTag = Bytes;
pub type SequenceNo = u32;
pub type DeliveryNumber = SequenceNo;
pub type TransferNumber = SequenceNo;
pub type MessageFormat = u32;
pub type IetfLanguageTag = Symbol;
pub type NodeProperties = Fields;
pub type MessageIdUlong = u64;
pub type MessageIdUuid = Uuid;
pub type MessageIdBinary = Bytes;
pub type MessageIdString = ByteString;
pub type Address = ByteString;
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Role {
    Sender,
    Receiver,
}
impl Role {
    pub fn try_from(v: bool) -> Result<Self, AmqpParseError> {
        match v {
            false => Ok(Role::Sender),
            true => Ok(Role::Receiver),
            _ => Err(AmqpParseError::UnknownEnumOption("Role")),
        }
    }
}
impl DecodeFormatted for Role {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = bool::decode_with_format(input, fmt)?;
        Ok(Self::try_from(base)?)
    }
}
impl Encode for Role {
    fn encoded_size(&self) -> usize {
        match *self {
            Role::Sender => {
                let v: bool = false;
                v.encoded_size()
            }
            Role::Receiver => {
                let v: bool = true;
                v.encoded_size()
            }
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            Role::Sender => {
                let v: bool = false;
                v.encode(buf);
            }
            Role::Receiver => {
                let v: bool = true;
                v.encode(buf);
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SenderSettleMode {
    Unsettled,
    Settled,
    Mixed,
}
impl SenderSettleMode {
    pub fn try_from(v: u8) -> Result<Self, AmqpParseError> {
        match v {
            0 => Ok(SenderSettleMode::Unsettled),
            1 => Ok(SenderSettleMode::Settled),
            2 => Ok(SenderSettleMode::Mixed),
            _ => Err(AmqpParseError::UnknownEnumOption("SenderSettleMode")),
        }
    }
}
impl DecodeFormatted for SenderSettleMode {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = u8::decode_with_format(input, fmt)?;
        Ok(Self::try_from(base)?)
    }
}
impl Encode for SenderSettleMode {
    fn encoded_size(&self) -> usize {
        match *self {
            SenderSettleMode::Unsettled => {
                let v: u8 = 0;
                v.encoded_size()
            }
            SenderSettleMode::Settled => {
                let v: u8 = 1;
                v.encoded_size()
            }
            SenderSettleMode::Mixed => {
                let v: u8 = 2;
                v.encoded_size()
            }
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            SenderSettleMode::Unsettled => {
                let v: u8 = 0;
                v.encode(buf);
            }
            SenderSettleMode::Settled => {
                let v: u8 = 1;
                v.encode(buf);
            }
            SenderSettleMode::Mixed => {
                let v: u8 = 2;
                v.encode(buf);
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReceiverSettleMode {
    First,
    Second,
}
impl ReceiverSettleMode {
    pub fn try_from(v: u8) -> Result<Self, AmqpParseError> {
        match v {
            0 => Ok(ReceiverSettleMode::First),
            1 => Ok(ReceiverSettleMode::Second),
            _ => Err(AmqpParseError::UnknownEnumOption("ReceiverSettleMode")),
        }
    }
}
impl DecodeFormatted for ReceiverSettleMode {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = u8::decode_with_format(input, fmt)?;
        Ok(Self::try_from(base)?)
    }
}
impl Encode for ReceiverSettleMode {
    fn encoded_size(&self) -> usize {
        match *self {
            ReceiverSettleMode::First => {
                let v: u8 = 0;
                v.encoded_size()
            }
            ReceiverSettleMode::Second => {
                let v: u8 = 1;
                v.encoded_size()
            }
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            ReceiverSettleMode::First => {
                let v: u8 = 0;
                v.encode(buf);
            }
            ReceiverSettleMode::Second => {
                let v: u8 = 1;
                v.encode(buf);
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AmqpError {
    InternalError,
    NotFound,
    UnauthorizedAccess,
    DecodeError,
    ResourceLimitExceeded,
    NotAllowed,
    InvalidField,
    NotImplemented,
    ResourceLocked,
    PreconditionFailed,
    ResourceDeleted,
    IllegalState,
    FrameSizeTooSmall,
}
impl AmqpError {
    pub fn try_from(v: &Symbol) -> Result<Self, AmqpParseError> {
        match v.as_str() {
            "amqp:internal-error" => Ok(AmqpError::InternalError),
            "amqp:not-found" => Ok(AmqpError::NotFound),
            "amqp:unauthorized-access" => Ok(AmqpError::UnauthorizedAccess),
            "amqp:decode-error" => Ok(AmqpError::DecodeError),
            "amqp:resource-limit-exceeded" => Ok(AmqpError::ResourceLimitExceeded),
            "amqp:not-allowed" => Ok(AmqpError::NotAllowed),
            "amqp:invalid-field" => Ok(AmqpError::InvalidField),
            "amqp:not-implemented" => Ok(AmqpError::NotImplemented),
            "amqp:resource-locked" => Ok(AmqpError::ResourceLocked),
            "amqp:precondition-failed" => Ok(AmqpError::PreconditionFailed),
            "amqp:resource-deleted" => Ok(AmqpError::ResourceDeleted),
            "amqp:illegal-state" => Ok(AmqpError::IllegalState),
            "amqp:frame-size-too-small" => Ok(AmqpError::FrameSizeTooSmall),
            _ => Err(AmqpParseError::UnknownEnumOption("AmqpError")),
        }
    }
}
impl DecodeFormatted for AmqpError {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = Symbol::decode_with_format(input, fmt)?;
        Ok(Self::try_from(&base)?)
    }
}
impl Encode for AmqpError {
    fn encoded_size(&self) -> usize {
        match *self {
            AmqpError::InternalError => 19 + 2,
            AmqpError::NotFound => 14 + 2,
            AmqpError::UnauthorizedAccess => 24 + 2,
            AmqpError::DecodeError => 17 + 2,
            AmqpError::ResourceLimitExceeded => 28 + 2,
            AmqpError::NotAllowed => 16 + 2,
            AmqpError::InvalidField => 18 + 2,
            AmqpError::NotImplemented => 20 + 2,
            AmqpError::ResourceLocked => 20 + 2,
            AmqpError::PreconditionFailed => 24 + 2,
            AmqpError::ResourceDeleted => 21 + 2,
            AmqpError::IllegalState => 18 + 2,
            AmqpError::FrameSizeTooSmall => 25 + 2,
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            AmqpError::InternalError => StaticSymbol("amqp:internal-error").encode(buf),
            AmqpError::NotFound => StaticSymbol("amqp:not-found").encode(buf),
            AmqpError::UnauthorizedAccess => StaticSymbol("amqp:unauthorized-access").encode(buf),
            AmqpError::DecodeError => StaticSymbol("amqp:decode-error").encode(buf),
            AmqpError::ResourceLimitExceeded => {
                StaticSymbol("amqp:resource-limit-exceeded").encode(buf)
            }
            AmqpError::NotAllowed => StaticSymbol("amqp:not-allowed").encode(buf),
            AmqpError::InvalidField => StaticSymbol("amqp:invalid-field").encode(buf),
            AmqpError::NotImplemented => StaticSymbol("amqp:not-implemented").encode(buf),
            AmqpError::ResourceLocked => StaticSymbol("amqp:resource-locked").encode(buf),
            AmqpError::PreconditionFailed => StaticSymbol("amqp:precondition-failed").encode(buf),
            AmqpError::ResourceDeleted => StaticSymbol("amqp:resource-deleted").encode(buf),
            AmqpError::IllegalState => StaticSymbol("amqp:illegal-state").encode(buf),
            AmqpError::FrameSizeTooSmall => StaticSymbol("amqp:frame-size-too-small").encode(buf),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ConnectionError {
    ConnectionForced,
    FramingError,
    Redirect,
}
impl ConnectionError {
    pub fn try_from(v: &Symbol) -> Result<Self, AmqpParseError> {
        match v.as_str() {
            "amqp:connection:forced" => Ok(ConnectionError::ConnectionForced),
            "amqp:connection:framing-error" => Ok(ConnectionError::FramingError),
            "amqp:connection:redirect" => Ok(ConnectionError::Redirect),
            _ => Err(AmqpParseError::UnknownEnumOption("ConnectionError")),
        }
    }
}
impl DecodeFormatted for ConnectionError {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = Symbol::decode_with_format(input, fmt)?;
        Ok(Self::try_from(&base)?)
    }
}
impl Encode for ConnectionError {
    fn encoded_size(&self) -> usize {
        match *self {
            ConnectionError::ConnectionForced => 22 + 2,
            ConnectionError::FramingError => 29 + 2,
            ConnectionError::Redirect => 24 + 2,
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            ConnectionError::ConnectionForced => StaticSymbol("amqp:connection:forced").encode(buf),
            ConnectionError::FramingError => {
                StaticSymbol("amqp:connection:framing-error").encode(buf)
            }
            ConnectionError::Redirect => StaticSymbol("amqp:connection:redirect").encode(buf),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SessionError {
    WindowViolation,
    ErrantLink,
    HandleInUse,
    UnattachedHandle,
}
impl SessionError {
    pub fn try_from(v: &Symbol) -> Result<Self, AmqpParseError> {
        match v.as_str() {
            "amqp:session:window-violation" => Ok(SessionError::WindowViolation),
            "amqp:session:errant-link" => Ok(SessionError::ErrantLink),
            "amqp:session:handle-in-use" => Ok(SessionError::HandleInUse),
            "amqp:session:unattached-handle" => Ok(SessionError::UnattachedHandle),
            _ => Err(AmqpParseError::UnknownEnumOption("SessionError")),
        }
    }
}
impl DecodeFormatted for SessionError {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = Symbol::decode_with_format(input, fmt)?;
        Ok(Self::try_from(&base)?)
    }
}
impl Encode for SessionError {
    fn encoded_size(&self) -> usize {
        match *self {
            SessionError::WindowViolation => 29 + 2,
            SessionError::ErrantLink => 24 + 2,
            SessionError::HandleInUse => 26 + 2,
            SessionError::UnattachedHandle => 30 + 2,
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            SessionError::WindowViolation => {
                StaticSymbol("amqp:session:window-violation").encode(buf)
            }
            SessionError::ErrantLink => StaticSymbol("amqp:session:errant-link").encode(buf),
            SessionError::HandleInUse => StaticSymbol("amqp:session:handle-in-use").encode(buf),
            SessionError::UnattachedHandle => {
                StaticSymbol("amqp:session:unattached-handle").encode(buf)
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LinkError {
    DetachForced,
    TransferLimitExceeded,
    MessageSizeExceeded,
    Redirect,
    Stolen,
}
impl LinkError {
    pub fn try_from(v: &Symbol) -> Result<Self, AmqpParseError> {
        match v.as_str() {
            "amqp:link:detach-forced" => Ok(LinkError::DetachForced),
            "amqp:link:transfer-limit-exceeded" => Ok(LinkError::TransferLimitExceeded),
            "amqp:link:message-size-exceeded" => Ok(LinkError::MessageSizeExceeded),
            "amqp:link:redirect" => Ok(LinkError::Redirect),
            "amqp:link:stolen" => Ok(LinkError::Stolen),
            _ => Err(AmqpParseError::UnknownEnumOption("LinkError")),
        }
    }
}
impl DecodeFormatted for LinkError {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = Symbol::decode_with_format(input, fmt)?;
        Ok(Self::try_from(&base)?)
    }
}
impl Encode for LinkError {
    fn encoded_size(&self) -> usize {
        match *self {
            LinkError::DetachForced => 23 + 2,
            LinkError::TransferLimitExceeded => 33 + 2,
            LinkError::MessageSizeExceeded => 31 + 2,
            LinkError::Redirect => 18 + 2,
            LinkError::Stolen => 16 + 2,
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            LinkError::DetachForced => StaticSymbol("amqp:link:detach-forced").encode(buf),
            LinkError::TransferLimitExceeded => {
                StaticSymbol("amqp:link:transfer-limit-exceeded").encode(buf)
            }
            LinkError::MessageSizeExceeded => {
                StaticSymbol("amqp:link:message-size-exceeded").encode(buf)
            }
            LinkError::Redirect => StaticSymbol("amqp:link:redirect").encode(buf),
            LinkError::Stolen => StaticSymbol("amqp:link:stolen").encode(buf),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SaslCode {
    Ok,
    Auth,
    Sys,
    SysPerm,
    SysTemp,
}
impl SaslCode {
    pub fn try_from(v: u8) -> Result<Self, AmqpParseError> {
        match v {
            0 => Ok(SaslCode::Ok),
            1 => Ok(SaslCode::Auth),
            2 => Ok(SaslCode::Sys),
            3 => Ok(SaslCode::SysPerm),
            4 => Ok(SaslCode::SysTemp),
            _ => Err(AmqpParseError::UnknownEnumOption("SaslCode")),
        }
    }
}
impl DecodeFormatted for SaslCode {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = u8::decode_with_format(input, fmt)?;
        Ok(Self::try_from(base)?)
    }
}
impl Encode for SaslCode {
    fn encoded_size(&self) -> usize {
        match *self {
            SaslCode::Ok => {
                let v: u8 = 0;
                v.encoded_size()
            }
            SaslCode::Auth => {
                let v: u8 = 1;
                v.encoded_size()
            }
            SaslCode::Sys => {
                let v: u8 = 2;
                v.encoded_size()
            }
            SaslCode::SysPerm => {
                let v: u8 = 3;
                v.encoded_size()
            }
            SaslCode::SysTemp => {
                let v: u8 = 4;
                v.encoded_size()
            }
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            SaslCode::Ok => {
                let v: u8 = 0;
                v.encode(buf);
            }
            SaslCode::Auth => {
                let v: u8 = 1;
                v.encode(buf);
            }
            SaslCode::Sys => {
                let v: u8 = 2;
                v.encode(buf);
            }
            SaslCode::SysPerm => {
                let v: u8 = 3;
                v.encode(buf);
            }
            SaslCode::SysTemp => {
                let v: u8 = 4;
                v.encode(buf);
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TerminusDurability {
    None,
    Configuration,
    UnsettledState,
}
impl TerminusDurability {
    pub fn try_from(v: u32) -> Result<Self, AmqpParseError> {
        match v {
            0 => Ok(TerminusDurability::None),
            1 => Ok(TerminusDurability::Configuration),
            2 => Ok(TerminusDurability::UnsettledState),
            _ => Err(AmqpParseError::UnknownEnumOption("TerminusDurability")),
        }
    }
}
impl DecodeFormatted for TerminusDurability {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = u32::decode_with_format(input, fmt)?;
        Ok(Self::try_from(base)?)
    }
}
impl Encode for TerminusDurability {
    fn encoded_size(&self) -> usize {
        match *self {
            TerminusDurability::None => {
                let v: u32 = 0;
                v.encoded_size()
            }
            TerminusDurability::Configuration => {
                let v: u32 = 1;
                v.encoded_size()
            }
            TerminusDurability::UnsettledState => {
                let v: u32 = 2;
                v.encoded_size()
            }
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            TerminusDurability::None => {
                let v: u32 = 0;
                v.encode(buf);
            }
            TerminusDurability::Configuration => {
                let v: u32 = 1;
                v.encode(buf);
            }
            TerminusDurability::UnsettledState => {
                let v: u32 = 2;
                v.encode(buf);
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TerminusExpiryPolicy {
    LinkDetach,
    SessionEnd,
    ConnectionClose,
    Never,
}
impl TerminusExpiryPolicy {
    pub fn try_from(v: &Symbol) -> Result<Self, AmqpParseError> {
        match v.as_str() {
            "link-detach" => Ok(TerminusExpiryPolicy::LinkDetach),
            "session-end" => Ok(TerminusExpiryPolicy::SessionEnd),
            "connection-close" => Ok(TerminusExpiryPolicy::ConnectionClose),
            "never" => Ok(TerminusExpiryPolicy::Never),
            _ => Err(AmqpParseError::UnknownEnumOption("TerminusExpiryPolicy")),
        }
    }
}
impl DecodeFormatted for TerminusExpiryPolicy {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = Symbol::decode_with_format(input, fmt)?;
        Ok(Self::try_from(&base)?)
    }
}
impl Encode for TerminusExpiryPolicy {
    fn encoded_size(&self) -> usize {
        match *self {
            TerminusExpiryPolicy::LinkDetach => 11 + 2,
            TerminusExpiryPolicy::SessionEnd => 11 + 2,
            TerminusExpiryPolicy::ConnectionClose => 16 + 2,
            TerminusExpiryPolicy::Never => 5 + 2,
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            TerminusExpiryPolicy::LinkDetach => StaticSymbol("link-detach").encode(buf),
            TerminusExpiryPolicy::SessionEnd => StaticSymbol("session-end").encode(buf),
            TerminusExpiryPolicy::ConnectionClose => StaticSymbol("connection-close").encode(buf),
            TerminusExpiryPolicy::Never => StaticSymbol("never").encode(buf),
        }
    }
}
type DeliveryAnnotations = VecSymbolMap;
fn decode_delivery_annotations_inner(
    input: &mut Bytes,
) -> Result<DeliveryAnnotations, AmqpParseError> {
    DeliveryAnnotations::decode(input)
}
fn encoded_size_delivery_annotations_inner(dr: &DeliveryAnnotations) -> usize {
    // descriptor size + actual size
    3 + dr.encoded_size()
}
fn encode_delivery_annotations_inner(dr: &DeliveryAnnotations, buf: &mut BytesMut) {
    Descriptor::Ulong(113).encode(buf);
    dr.encode(buf);
}
type MessageAnnotations = VecSymbolMap;
fn decode_message_annotations_inner(
    input: &mut Bytes,
) -> Result<MessageAnnotations, AmqpParseError> {
    MessageAnnotations::decode(input)
}
fn encoded_size_message_annotations_inner(dr: &MessageAnnotations) -> usize {
    // descriptor size + actual size
    3 + dr.encoded_size()
}
fn encode_message_annotations_inner(dr: &MessageAnnotations, buf: &mut BytesMut) {
    Descriptor::Ulong(114).encode(buf);
    dr.encode(buf);
}
type ApplicationProperties = VecStringMap;
fn decode_application_properties_inner(
    input: &mut Bytes,
) -> Result<ApplicationProperties, AmqpParseError> {
    ApplicationProperties::decode(input)
}
fn encoded_size_application_properties_inner(dr: &ApplicationProperties) -> usize {
    // descriptor size + actual size
    3 + dr.encoded_size()
}
fn encode_application_properties_inner(dr: &ApplicationProperties, buf: &mut BytesMut) {
    Descriptor::Ulong(116).encode(buf);
    dr.encode(buf);
}
type Data = Bytes;
fn decode_data_inner(input: &mut Bytes) -> Result<Data, AmqpParseError> {
    Data::decode(input)
}
fn encoded_size_data_inner(dr: &Data) -> usize {
    // descriptor size + actual size
    3 + dr.encoded_size()
}
fn encode_data_inner(dr: &Data, buf: &mut BytesMut) {
    Descriptor::Ulong(117).encode(buf);
    dr.encode(buf);
}
type AmqpSequence = List;
fn decode_amqp_sequence_inner(input: &mut Bytes) -> Result<AmqpSequence, AmqpParseError> {
    AmqpSequence::decode(input)
}
fn encoded_size_amqp_sequence_inner(dr: &AmqpSequence) -> usize {
    // descriptor size + actual size
    3 + dr.encoded_size()
}
fn encode_amqp_sequence_inner(dr: &AmqpSequence, buf: &mut BytesMut) {
    Descriptor::Ulong(118).encode(buf);
    dr.encode(buf);
}
type AmqpValue = Variant;
fn decode_amqp_value_inner(input: &mut Bytes) -> Result<AmqpValue, AmqpParseError> {
    AmqpValue::decode(input)
}
fn encoded_size_amqp_value_inner(dr: &AmqpValue) -> usize {
    // descriptor size + actual size
    3 + dr.encoded_size()
}
fn encode_amqp_value_inner(dr: &AmqpValue, buf: &mut BytesMut) {
    Descriptor::Ulong(119).encode(buf);
    dr.encode(buf);
}
type Footer = Annotations;
fn decode_footer_inner(input: &mut Bytes) -> Result<Footer, AmqpParseError> {
    Footer::decode(input)
}
fn encoded_size_footer_inner(dr: &Footer) -> usize {
    // descriptor size + actual size
    3 + dr.encoded_size()
}
fn encode_footer_inner(dr: &Footer, buf: &mut BytesMut) {
    Descriptor::Ulong(120).encode(buf);
    dr.encode(buf);
}
#[derive(Clone, Debug, PartialEq)]
pub struct Error(pub Box<ErrorInner>);
#[derive(Clone, Debug, PartialEq)]
pub struct ErrorBuilder(pub Box<ErrorInner>);
#[derive(Clone, Debug, PartialEq, Default)]
pub struct ErrorInner {
    pub condition: ErrorCondition,
    pub description: Option<ByteString>,
    pub info: Option<FieldsVec>,
}
impl Error {
    pub fn build() -> ErrorBuilder {
        ErrorBuilder(Box::new(ErrorInner::default()))
    }
    #[inline]
    pub fn condition(&self) -> &ErrorCondition {
        &self.0.condition
    }
    #[inline]
    pub fn condition_mut(&mut self) -> &mut ErrorCondition {
        &mut self.0.condition
    }
    #[inline]
    pub fn description(&self) -> Option<&ByteString> {
        self.0.description.as_ref()
    }
    #[inline]
    pub fn description_mut(&mut self) -> Option<&mut ByteString> {
        self.0.description.as_mut()
    }
    #[inline]
    pub fn info(&self) -> Option<&FieldsVec> {
        self.0.info.as_ref()
    }
    #[inline]
    pub fn info_mut(&mut self) -> Option<&mut FieldsVec> {
        self.0.info.as_mut()
    }
    pub fn into_inner(self) -> Box<ErrorInner> {
        self.0
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1;
}
impl ErrorBuilder {
    #[inline]
    pub fn condition(mut self, val: ErrorCondition) -> Self {
        self.0.condition = val;
        self
    }
    #[inline]
    pub fn description(mut self, val: ByteString) -> Self {
        self.0.description = Some(val);
        self
    }
    #[inline]
    pub fn info(mut self, val: FieldsVec) -> Self {
        self.0.info = Some(val);
        self
    }
    pub fn finish(self) -> Error {
        Error(self.0)
    }
}
#[allow(unused_mut)]
fn decode_error_inner(input: &mut Bytes) -> Result<Error, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let condition: ErrorCondition;
    if count > 0 {
        let decoded = ErrorCondition::decode(&mut data)?;
        condition = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("condition"));
    }
    let description: Option<ByteString>;
    if count > 0 {
        description = Option::<ByteString>::decode(&mut data)?;
        count -= 1;
    } else {
        description = None;
    }
    let info: Option<FieldsVec>;
    if count > 0 {
        info = Option::<FieldsVec>::decode(&mut data)?;
        count -= 1;
    } else {
        info = None;
    }
    Ok(Error(Box::new(ErrorInner {
        condition,
        description,
        info,
    })))
}
fn encoded_size_error_inner(list: &Error) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.condition.encoded_size()
        + list.0.description.encoded_size()
        + list.0.info.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_error_inner(list: &Error, buf: &mut BytesMut) {
    Descriptor::Ulong(29).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.condition.encoded_size()
        + list.0.description.encoded_size()
        + list.0.info.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Error::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Error::FIELD_COUNT as u8);
    }
    list.0.condition.encode(buf);
    list.0.description.encode(buf);
    list.0.info.encode(buf);
}
impl DecodeFormatted for Error {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 29,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:error:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_error_inner(input)
        }
    }
}
impl Encode for Error {
    fn encoded_size(&self) -> usize {
        encoded_size_error_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_error_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Open(pub Box<OpenInner>);
#[derive(Clone, Debug, PartialEq)]
pub struct OpenBuilder(pub Box<OpenInner>);
#[derive(Clone, Debug, PartialEq, Default)]
pub struct OpenInner {
    pub container_id: ByteString,
    pub hostname: Option<ByteString>,
    pub max_frame_size: u32,
    pub channel_max: u16,
    pub idle_time_out: Option<Milliseconds>,
    pub outgoing_locales: Option<IetfLanguageTags>,
    pub incoming_locales: Option<IetfLanguageTags>,
    pub offered_capabilities: Option<Symbols>,
    pub desired_capabilities: Option<Symbols>,
    pub properties: Option<Fields>,
}
impl Open {
    pub fn build() -> OpenBuilder {
        OpenBuilder(Box::new(OpenInner::default()))
    }
    #[inline]
    pub fn container_id(&self) -> &ByteString {
        &self.0.container_id
    }
    #[inline]
    pub fn container_id_mut(&mut self) -> &mut ByteString {
        &mut self.0.container_id
    }
    #[inline]
    pub fn hostname(&self) -> Option<&ByteString> {
        self.0.hostname.as_ref()
    }
    #[inline]
    pub fn hostname_mut(&mut self) -> Option<&mut ByteString> {
        self.0.hostname.as_mut()
    }
    #[inline]
    pub fn max_frame_size(&self) -> u32 {
        self.0.max_frame_size
    }
    #[inline]
    pub fn max_frame_size_mut(&mut self) -> &mut u32 {
        &mut self.0.max_frame_size
    }
    #[inline]
    pub fn channel_max(&self) -> u16 {
        self.0.channel_max
    }
    #[inline]
    pub fn channel_max_mut(&mut self) -> &mut u16 {
        &mut self.0.channel_max
    }
    #[inline]
    pub fn idle_time_out(&self) -> Option<Milliseconds> {
        self.0.idle_time_out
    }
    #[inline]
    pub fn idle_time_out_mut(&mut self) -> Option<&mut Milliseconds> {
        self.0.idle_time_out.as_mut()
    }
    #[inline]
    pub fn outgoing_locales(&self) -> Option<&IetfLanguageTags> {
        self.0.outgoing_locales.as_ref()
    }
    #[inline]
    pub fn outgoing_locales_mut(&mut self) -> Option<&mut IetfLanguageTags> {
        self.0.outgoing_locales.as_mut()
    }
    #[inline]
    pub fn incoming_locales(&self) -> Option<&IetfLanguageTags> {
        self.0.incoming_locales.as_ref()
    }
    #[inline]
    pub fn incoming_locales_mut(&mut self) -> Option<&mut IetfLanguageTags> {
        self.0.incoming_locales.as_mut()
    }
    #[inline]
    pub fn offered_capabilities(&self) -> Option<&Symbols> {
        self.0.offered_capabilities.as_ref()
    }
    #[inline]
    pub fn offered_capabilities_mut(&mut self) -> Option<&mut Symbols> {
        self.0.offered_capabilities.as_mut()
    }
    #[inline]
    pub fn desired_capabilities(&self) -> Option<&Symbols> {
        self.0.desired_capabilities.as_ref()
    }
    #[inline]
    pub fn desired_capabilities_mut(&mut self) -> Option<&mut Symbols> {
        self.0.desired_capabilities.as_mut()
    }
    #[inline]
    pub fn properties(&self) -> Option<&Fields> {
        self.0.properties.as_ref()
    }
    #[inline]
    pub fn properties_mut(&mut self) -> Option<&mut Fields> {
        self.0.properties.as_mut()
    }
    #[inline]
    pub fn get_properties_mut(&mut self) -> &mut Fields {
        if self.0.properties.is_none() {
            self.0.properties = Some(Fields::default());
        }
        self.0.properties.as_mut().unwrap()
    }
    pub fn into_inner(self) -> Box<OpenInner> {
        self.0
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1;
}
impl OpenBuilder {
    #[inline]
    pub fn container_id(mut self, val: ByteString) -> Self {
        self.0.container_id = val;
        self
    }
    #[inline]
    pub fn hostname(mut self, val: ByteString) -> Self {
        self.0.hostname = Some(val);
        self
    }
    #[inline]
    pub fn max_frame_size(mut self, val: u32) -> Self {
        self.0.max_frame_size = val;
        self
    }
    #[inline]
    pub fn channel_max(mut self, val: u16) -> Self {
        self.0.channel_max = val;
        self
    }
    #[inline]
    pub fn idle_time_out(mut self, val: Milliseconds) -> Self {
        self.0.idle_time_out = Some(val);
        self
    }
    #[inline]
    pub fn outgoing_locales(mut self, val: IetfLanguageTags) -> Self {
        self.0.outgoing_locales = Some(val);
        self
    }
    #[inline]
    pub fn incoming_locales(mut self, val: IetfLanguageTags) -> Self {
        self.0.incoming_locales = Some(val);
        self
    }
    #[inline]
    pub fn offered_capabilities(mut self, val: Symbols) -> Self {
        self.0.offered_capabilities = Some(val);
        self
    }
    #[inline]
    pub fn desired_capabilities(mut self, val: Symbols) -> Self {
        self.0.desired_capabilities = Some(val);
        self
    }
    #[inline]
    pub fn properties(mut self, val: Fields) -> Self {
        self.0.properties = Some(val);
        self
    }
    pub fn finish(self) -> Open {
        Open(self.0)
    }
}
#[allow(unused_mut)]
fn decode_open_inner(input: &mut Bytes) -> Result<Open, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let container_id: ByteString;
    if count > 0 {
        let decoded = ByteString::decode(&mut data)?;
        container_id = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("container_id"));
    }
    let hostname: Option<ByteString>;
    if count > 0 {
        hostname = Option::<ByteString>::decode(&mut data)?;
        count -= 1;
    } else {
        hostname = None;
    }
    let max_frame_size: u32;
    if count > 0 {
        let decoded = Option::<u32>::decode(&mut data)?;
        max_frame_size = decoded.unwrap_or(4294967295);
        count -= 1;
    } else {
        max_frame_size = 4294967295;
    }
    let channel_max: u16;
    if count > 0 {
        let decoded = Option::<u16>::decode(&mut data)?;
        channel_max = decoded.unwrap_or(65535);
        count -= 1;
    } else {
        channel_max = 65535;
    }
    let idle_time_out: Option<Milliseconds>;
    if count > 0 {
        idle_time_out = Option::<Milliseconds>::decode(&mut data)?;
        count -= 1;
    } else {
        idle_time_out = None;
    }
    let outgoing_locales: Option<IetfLanguageTags>;
    if count > 0 {
        outgoing_locales = Option::<IetfLanguageTags>::decode(&mut data)?;
        count -= 1;
    } else {
        outgoing_locales = None;
    }
    let incoming_locales: Option<IetfLanguageTags>;
    if count > 0 {
        incoming_locales = Option::<IetfLanguageTags>::decode(&mut data)?;
        count -= 1;
    } else {
        incoming_locales = None;
    }
    let offered_capabilities: Option<Symbols>;
    if count > 0 {
        offered_capabilities = Option::<Symbols>::decode(&mut data)?;
        count -= 1;
    } else {
        offered_capabilities = None;
    }
    let desired_capabilities: Option<Symbols>;
    if count > 0 {
        desired_capabilities = Option::<Symbols>::decode(&mut data)?;
        count -= 1;
    } else {
        desired_capabilities = None;
    }
    let properties: Option<Fields>;
    if count > 0 {
        properties = Option::<Fields>::decode(&mut data)?;
        count -= 1;
    } else {
        properties = None;
    }
    Ok(Open(Box::new(OpenInner {
        container_id,
        hostname,
        max_frame_size,
        channel_max,
        idle_time_out,
        outgoing_locales,
        incoming_locales,
        offered_capabilities,
        desired_capabilities,
        properties,
    })))
}
fn encoded_size_open_inner(list: &Open) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.container_id.encoded_size()
        + list.0.hostname.encoded_size()
        + list.0.max_frame_size.encoded_size()
        + list.0.channel_max.encoded_size()
        + list.0.idle_time_out.encoded_size()
        + list.0.outgoing_locales.encoded_size()
        + list.0.incoming_locales.encoded_size()
        + list.0.offered_capabilities.encoded_size()
        + list.0.desired_capabilities.encoded_size()
        + list.0.properties.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_open_inner(list: &Open, buf: &mut BytesMut) {
    Descriptor::Ulong(16).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.container_id.encoded_size()
        + list.0.hostname.encoded_size()
        + list.0.max_frame_size.encoded_size()
        + list.0.channel_max.encoded_size()
        + list.0.idle_time_out.encoded_size()
        + list.0.outgoing_locales.encoded_size()
        + list.0.incoming_locales.encoded_size()
        + list.0.offered_capabilities.encoded_size()
        + list.0.desired_capabilities.encoded_size()
        + list.0.properties.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Open::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Open::FIELD_COUNT as u8);
    }
    list.0.container_id.encode(buf);
    list.0.hostname.encode(buf);
    list.0.max_frame_size.encode(buf);
    list.0.channel_max.encode(buf);
    list.0.idle_time_out.encode(buf);
    list.0.outgoing_locales.encode(buf);
    list.0.incoming_locales.encode(buf);
    list.0.offered_capabilities.encode(buf);
    list.0.desired_capabilities.encode(buf);
    list.0.properties.encode(buf);
}
impl DecodeFormatted for Open {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 16,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:open:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_open_inner(input)
        }
    }
}
impl Encode for Open {
    fn encoded_size(&self) -> usize {
        encoded_size_open_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_open_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Begin(pub Box<BeginInner>);
#[derive(Clone, Debug, PartialEq)]
pub struct BeginBuilder(pub Box<BeginInner>);
#[derive(Clone, Debug, PartialEq, Default)]
pub struct BeginInner {
    pub remote_channel: Option<u16>,
    pub next_outgoing_id: TransferNumber,
    pub incoming_window: u32,
    pub outgoing_window: u32,
    pub handle_max: Handle,
    pub offered_capabilities: Option<Symbols>,
    pub desired_capabilities: Option<Symbols>,
    pub properties: Option<Fields>,
}
impl Begin {
    pub fn build() -> BeginBuilder {
        BeginBuilder(Box::new(BeginInner::default()))
    }
    #[inline]
    pub fn remote_channel(&self) -> Option<u16> {
        self.0.remote_channel
    }
    #[inline]
    pub fn remote_channel_mut(&mut self) -> Option<&mut u16> {
        self.0.remote_channel.as_mut()
    }
    #[inline]
    pub fn next_outgoing_id(&self) -> TransferNumber {
        self.0.next_outgoing_id
    }
    #[inline]
    pub fn next_outgoing_id_mut(&mut self) -> &mut TransferNumber {
        &mut self.0.next_outgoing_id
    }
    #[inline]
    pub fn incoming_window(&self) -> u32 {
        self.0.incoming_window
    }
    #[inline]
    pub fn incoming_window_mut(&mut self) -> &mut u32 {
        &mut self.0.incoming_window
    }
    #[inline]
    pub fn outgoing_window(&self) -> u32 {
        self.0.outgoing_window
    }
    #[inline]
    pub fn outgoing_window_mut(&mut self) -> &mut u32 {
        &mut self.0.outgoing_window
    }
    #[inline]
    pub fn handle_max(&self) -> Handle {
        self.0.handle_max
    }
    #[inline]
    pub fn handle_max_mut(&mut self) -> &mut Handle {
        &mut self.0.handle_max
    }
    #[inline]
    pub fn offered_capabilities(&self) -> Option<&Symbols> {
        self.0.offered_capabilities.as_ref()
    }
    #[inline]
    pub fn offered_capabilities_mut(&mut self) -> Option<&mut Symbols> {
        self.0.offered_capabilities.as_mut()
    }
    #[inline]
    pub fn desired_capabilities(&self) -> Option<&Symbols> {
        self.0.desired_capabilities.as_ref()
    }
    #[inline]
    pub fn desired_capabilities_mut(&mut self) -> Option<&mut Symbols> {
        self.0.desired_capabilities.as_mut()
    }
    #[inline]
    pub fn properties(&self) -> Option<&Fields> {
        self.0.properties.as_ref()
    }
    #[inline]
    pub fn properties_mut(&mut self) -> Option<&mut Fields> {
        self.0.properties.as_mut()
    }
    #[inline]
    pub fn get_properties_mut(&mut self) -> &mut Fields {
        if self.0.properties.is_none() {
            self.0.properties = Some(Fields::default());
        }
        self.0.properties.as_mut().unwrap()
    }
    pub fn into_inner(self) -> Box<BeginInner> {
        self.0
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1;
}
impl BeginBuilder {
    #[inline]
    pub fn remote_channel(mut self, val: u16) -> Self {
        self.0.remote_channel = Some(val);
        self
    }
    #[inline]
    pub fn next_outgoing_id(mut self, val: TransferNumber) -> Self {
        self.0.next_outgoing_id = val;
        self
    }
    #[inline]
    pub fn incoming_window(mut self, val: u32) -> Self {
        self.0.incoming_window = val;
        self
    }
    #[inline]
    pub fn outgoing_window(mut self, val: u32) -> Self {
        self.0.outgoing_window = val;
        self
    }
    #[inline]
    pub fn handle_max(mut self, val: Handle) -> Self {
        self.0.handle_max = val;
        self
    }
    #[inline]
    pub fn offered_capabilities(mut self, val: Symbols) -> Self {
        self.0.offered_capabilities = Some(val);
        self
    }
    #[inline]
    pub fn desired_capabilities(mut self, val: Symbols) -> Self {
        self.0.desired_capabilities = Some(val);
        self
    }
    #[inline]
    pub fn properties(mut self, val: Fields) -> Self {
        self.0.properties = Some(val);
        self
    }
    pub fn finish(self) -> Begin {
        Begin(self.0)
    }
}
#[allow(unused_mut)]
fn decode_begin_inner(input: &mut Bytes) -> Result<Begin, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let remote_channel: Option<u16>;
    if count > 0 {
        remote_channel = Option::<u16>::decode(&mut data)?;
        count -= 1;
    } else {
        remote_channel = None;
    }
    let next_outgoing_id: TransferNumber;
    if count > 0 {
        let decoded = TransferNumber::decode(&mut data)?;
        next_outgoing_id = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("next_outgoing_id"));
    }
    let incoming_window: u32;
    if count > 0 {
        let decoded = u32::decode(&mut data)?;
        incoming_window = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("incoming_window"));
    }
    let outgoing_window: u32;
    if count > 0 {
        let decoded = u32::decode(&mut data)?;
        outgoing_window = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("outgoing_window"));
    }
    let handle_max: Handle;
    if count > 0 {
        let decoded = Option::<Handle>::decode(&mut data)?;
        handle_max = decoded.unwrap_or(4294967295);
        count -= 1;
    } else {
        handle_max = 4294967295;
    }
    let offered_capabilities: Option<Symbols>;
    if count > 0 {
        offered_capabilities = Option::<Symbols>::decode(&mut data)?;
        count -= 1;
    } else {
        offered_capabilities = None;
    }
    let desired_capabilities: Option<Symbols>;
    if count > 0 {
        desired_capabilities = Option::<Symbols>::decode(&mut data)?;
        count -= 1;
    } else {
        desired_capabilities = None;
    }
    let properties: Option<Fields>;
    if count > 0 {
        properties = Option::<Fields>::decode(&mut data)?;
        count -= 1;
    } else {
        properties = None;
    }
    Ok(Begin(Box::new(BeginInner {
        remote_channel,
        next_outgoing_id,
        incoming_window,
        outgoing_window,
        handle_max,
        offered_capabilities,
        desired_capabilities,
        properties,
    })))
}
fn encoded_size_begin_inner(list: &Begin) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.remote_channel.encoded_size()
        + list.0.next_outgoing_id.encoded_size()
        + list.0.incoming_window.encoded_size()
        + list.0.outgoing_window.encoded_size()
        + list.0.handle_max.encoded_size()
        + list.0.offered_capabilities.encoded_size()
        + list.0.desired_capabilities.encoded_size()
        + list.0.properties.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_begin_inner(list: &Begin, buf: &mut BytesMut) {
    Descriptor::Ulong(17).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.remote_channel.encoded_size()
        + list.0.next_outgoing_id.encoded_size()
        + list.0.incoming_window.encoded_size()
        + list.0.outgoing_window.encoded_size()
        + list.0.handle_max.encoded_size()
        + list.0.offered_capabilities.encoded_size()
        + list.0.desired_capabilities.encoded_size()
        + list.0.properties.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Begin::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Begin::FIELD_COUNT as u8);
    }
    list.0.remote_channel.encode(buf);
    list.0.next_outgoing_id.encode(buf);
    list.0.incoming_window.encode(buf);
    list.0.outgoing_window.encode(buf);
    list.0.handle_max.encode(buf);
    list.0.offered_capabilities.encode(buf);
    list.0.desired_capabilities.encode(buf);
    list.0.properties.encode(buf);
}
impl DecodeFormatted for Begin {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 17,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:begin:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_begin_inner(input)
        }
    }
}
impl Encode for Begin {
    fn encoded_size(&self) -> usize {
        encoded_size_begin_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_begin_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Attach(pub Box<AttachInner>);
#[derive(Clone, Debug, PartialEq)]
pub struct AttachBuilder(pub Box<AttachInner>);
#[derive(Clone, Debug, PartialEq, Default)]
pub struct AttachInner {
    pub name: ByteString,
    pub handle: Handle,
    pub role: Role,
    pub snd_settle_mode: SenderSettleMode,
    pub rcv_settle_mode: ReceiverSettleMode,
    pub source: Option<Source>,
    pub target: Option<Target>,
    pub unsettled: Option<Map>,
    pub incomplete_unsettled: bool,
    pub initial_delivery_count: Option<SequenceNo>,
    pub max_message_size: Option<u64>,
    pub offered_capabilities: Option<Symbols>,
    pub desired_capabilities: Option<Symbols>,
    pub properties: Option<Fields>,
}
impl Attach {
    pub fn build() -> AttachBuilder {
        AttachBuilder(Box::new(AttachInner::default()))
    }
    #[inline]
    pub fn name(&self) -> &ByteString {
        &self.0.name
    }
    #[inline]
    pub fn name_mut(&mut self) -> &mut ByteString {
        &mut self.0.name
    }
    #[inline]
    pub fn handle(&self) -> Handle {
        self.0.handle
    }
    #[inline]
    pub fn handle_mut(&mut self) -> &mut Handle {
        &mut self.0.handle
    }
    #[inline]
    pub fn role(&self) -> Role {
        self.0.role
    }
    #[inline]
    pub fn role_mut(&mut self) -> &mut Role {
        &mut self.0.role
    }
    #[inline]
    pub fn snd_settle_mode(&self) -> SenderSettleMode {
        self.0.snd_settle_mode
    }
    #[inline]
    pub fn snd_settle_mode_mut(&mut self) -> &mut SenderSettleMode {
        &mut self.0.snd_settle_mode
    }
    #[inline]
    pub fn rcv_settle_mode(&self) -> ReceiverSettleMode {
        self.0.rcv_settle_mode
    }
    #[inline]
    pub fn rcv_settle_mode_mut(&mut self) -> &mut ReceiverSettleMode {
        &mut self.0.rcv_settle_mode
    }
    #[inline]
    pub fn source(&self) -> Option<&Source> {
        self.0.source.as_ref()
    }
    #[inline]
    pub fn source_mut(&mut self) -> Option<&mut Source> {
        self.0.source.as_mut()
    }
    #[inline]
    pub fn target(&self) -> Option<&Target> {
        self.0.target.as_ref()
    }
    #[inline]
    pub fn target_mut(&mut self) -> Option<&mut Target> {
        self.0.target.as_mut()
    }
    #[inline]
    pub fn unsettled(&self) -> Option<&Map> {
        self.0.unsettled.as_ref()
    }
    #[inline]
    pub fn unsettled_mut(&mut self) -> Option<&mut Map> {
        self.0.unsettled.as_mut()
    }
    #[inline]
    pub fn incomplete_unsettled(&self) -> bool {
        self.0.incomplete_unsettled
    }
    #[inline]
    pub fn incomplete_unsettled_mut(&mut self) -> &mut bool {
        &mut self.0.incomplete_unsettled
    }
    #[inline]
    pub fn initial_delivery_count(&self) -> Option<SequenceNo> {
        self.0.initial_delivery_count
    }
    #[inline]
    pub fn initial_delivery_count_mut(&mut self) -> Option<&mut SequenceNo> {
        self.0.initial_delivery_count.as_mut()
    }
    #[inline]
    pub fn max_message_size(&self) -> Option<u64> {
        self.0.max_message_size
    }
    #[inline]
    pub fn max_message_size_mut(&mut self) -> Option<&mut u64> {
        self.0.max_message_size.as_mut()
    }
    #[inline]
    pub fn offered_capabilities(&self) -> Option<&Symbols> {
        self.0.offered_capabilities.as_ref()
    }
    #[inline]
    pub fn offered_capabilities_mut(&mut self) -> Option<&mut Symbols> {
        self.0.offered_capabilities.as_mut()
    }
    #[inline]
    pub fn desired_capabilities(&self) -> Option<&Symbols> {
        self.0.desired_capabilities.as_ref()
    }
    #[inline]
    pub fn desired_capabilities_mut(&mut self) -> Option<&mut Symbols> {
        self.0.desired_capabilities.as_mut()
    }
    #[inline]
    pub fn properties(&self) -> Option<&Fields> {
        self.0.properties.as_ref()
    }
    #[inline]
    pub fn properties_mut(&mut self) -> Option<&mut Fields> {
        self.0.properties.as_mut()
    }
    #[inline]
    pub fn get_properties_mut(&mut self) -> &mut Fields {
        if self.0.properties.is_none() {
            self.0.properties = Some(Fields::default());
        }
        self.0.properties.as_mut().unwrap()
    }
    pub fn into_inner(self) -> Box<AttachInner> {
        self.0
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1;
}
impl AttachBuilder {
    #[inline]
    pub fn name(mut self, val: ByteString) -> Self {
        self.0.name = val;
        self
    }
    #[inline]
    pub fn handle(mut self, val: Handle) -> Self {
        self.0.handle = val;
        self
    }
    #[inline]
    pub fn role(mut self, val: Role) -> Self {
        self.0.role = val;
        self
    }
    #[inline]
    pub fn snd_settle_mode(mut self, val: SenderSettleMode) -> Self {
        self.0.snd_settle_mode = val;
        self
    }
    #[inline]
    pub fn rcv_settle_mode(mut self, val: ReceiverSettleMode) -> Self {
        self.0.rcv_settle_mode = val;
        self
    }
    #[inline]
    pub fn source(mut self, val: Source) -> Self {
        self.0.source = Some(val);
        self
    }
    #[inline]
    pub fn target(mut self, val: Target) -> Self {
        self.0.target = Some(val);
        self
    }
    #[inline]
    pub fn unsettled(mut self, val: Map) -> Self {
        self.0.unsettled = Some(val);
        self
    }
    #[inline]
    pub fn incomplete_unsettled(mut self, val: bool) -> Self {
        self.0.incomplete_unsettled = val;
        self
    }
    #[inline]
    pub fn initial_delivery_count(mut self, val: SequenceNo) -> Self {
        self.0.initial_delivery_count = Some(val);
        self
    }
    #[inline]
    pub fn max_message_size(mut self, val: u64) -> Self {
        self.0.max_message_size = Some(val);
        self
    }
    #[inline]
    pub fn offered_capabilities(mut self, val: Symbols) -> Self {
        self.0.offered_capabilities = Some(val);
        self
    }
    #[inline]
    pub fn desired_capabilities(mut self, val: Symbols) -> Self {
        self.0.desired_capabilities = Some(val);
        self
    }
    #[inline]
    pub fn properties(mut self, val: Fields) -> Self {
        self.0.properties = Some(val);
        self
    }
    pub fn finish(self) -> Attach {
        Attach(self.0)
    }
}
#[allow(unused_mut)]
fn decode_attach_inner(input: &mut Bytes) -> Result<Attach, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let name: ByteString;
    if count > 0 {
        let decoded = ByteString::decode(&mut data)?;
        name = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("name"));
    }
    let handle: Handle;
    if count > 0 {
        let decoded = Handle::decode(&mut data)?;
        handle = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("handle"));
    }
    let role: Role;
    if count > 0 {
        let decoded = Role::decode(&mut data)?;
        role = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("role"));
    }
    let snd_settle_mode: SenderSettleMode;
    if count > 0 {
        let decoded = Option::<SenderSettleMode>::decode(&mut data)?;
        snd_settle_mode = decoded.unwrap_or(SenderSettleMode::Mixed);
        count -= 1;
    } else {
        snd_settle_mode = SenderSettleMode::Mixed;
    }
    let rcv_settle_mode: ReceiverSettleMode;
    if count > 0 {
        let decoded = Option::<ReceiverSettleMode>::decode(&mut data)?;
        rcv_settle_mode = decoded.unwrap_or(ReceiverSettleMode::First);
        count -= 1;
    } else {
        rcv_settle_mode = ReceiverSettleMode::First;
    }
    let source: Option<Source>;
    if count > 0 {
        source = Option::<Source>::decode(&mut data)?;
        count -= 1;
    } else {
        source = None;
    }
    let target: Option<Target>;
    if count > 0 {
        target = Option::<Target>::decode(&mut data)?;
        count -= 1;
    } else {
        target = None;
    }
    let unsettled: Option<Map>;
    if count > 0 {
        unsettled = Option::<Map>::decode(&mut data)?;
        count -= 1;
    } else {
        unsettled = None;
    }
    let incomplete_unsettled: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        incomplete_unsettled = decoded.unwrap_or(false);
        count -= 1;
    } else {
        incomplete_unsettled = false;
    }
    let initial_delivery_count: Option<SequenceNo>;
    if count > 0 {
        initial_delivery_count = Option::<SequenceNo>::decode(&mut data)?;
        count -= 1;
    } else {
        initial_delivery_count = None;
    }
    let max_message_size: Option<u64>;
    if count > 0 {
        max_message_size = Option::<u64>::decode(&mut data)?;
        count -= 1;
    } else {
        max_message_size = None;
    }
    let offered_capabilities: Option<Symbols>;
    if count > 0 {
        offered_capabilities = Option::<Symbols>::decode(&mut data)?;
        count -= 1;
    } else {
        offered_capabilities = None;
    }
    let desired_capabilities: Option<Symbols>;
    if count > 0 {
        desired_capabilities = Option::<Symbols>::decode(&mut data)?;
        count -= 1;
    } else {
        desired_capabilities = None;
    }
    let properties: Option<Fields>;
    if count > 0 {
        properties = Option::<Fields>::decode(&mut data)?;
        count -= 1;
    } else {
        properties = None;
    }
    Ok(Attach(Box::new(AttachInner {
        name,
        handle,
        role,
        snd_settle_mode,
        rcv_settle_mode,
        source,
        target,
        unsettled,
        incomplete_unsettled,
        initial_delivery_count,
        max_message_size,
        offered_capabilities,
        desired_capabilities,
        properties,
    })))
}
fn encoded_size_attach_inner(list: &Attach) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.name.encoded_size()
        + list.0.handle.encoded_size()
        + list.0.role.encoded_size()
        + list.0.snd_settle_mode.encoded_size()
        + list.0.rcv_settle_mode.encoded_size()
        + list.0.source.encoded_size()
        + list.0.target.encoded_size()
        + list.0.unsettled.encoded_size()
        + list.0.incomplete_unsettled.encoded_size()
        + list.0.initial_delivery_count.encoded_size()
        + list.0.max_message_size.encoded_size()
        + list.0.offered_capabilities.encoded_size()
        + list.0.desired_capabilities.encoded_size()
        + list.0.properties.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_attach_inner(list: &Attach, buf: &mut BytesMut) {
    Descriptor::Ulong(18).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.name.encoded_size()
        + list.0.handle.encoded_size()
        + list.0.role.encoded_size()
        + list.0.snd_settle_mode.encoded_size()
        + list.0.rcv_settle_mode.encoded_size()
        + list.0.source.encoded_size()
        + list.0.target.encoded_size()
        + list.0.unsettled.encoded_size()
        + list.0.incomplete_unsettled.encoded_size()
        + list.0.initial_delivery_count.encoded_size()
        + list.0.max_message_size.encoded_size()
        + list.0.offered_capabilities.encoded_size()
        + list.0.desired_capabilities.encoded_size()
        + list.0.properties.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Attach::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Attach::FIELD_COUNT as u8);
    }
    list.0.name.encode(buf);
    list.0.handle.encode(buf);
    list.0.role.encode(buf);
    list.0.snd_settle_mode.encode(buf);
    list.0.rcv_settle_mode.encode(buf);
    list.0.source.encode(buf);
    list.0.target.encode(buf);
    list.0.unsettled.encode(buf);
    list.0.incomplete_unsettled.encode(buf);
    list.0.initial_delivery_count.encode(buf);
    list.0.max_message_size.encode(buf);
    list.0.offered_capabilities.encode(buf);
    list.0.desired_capabilities.encode(buf);
    list.0.properties.encode(buf);
}
impl DecodeFormatted for Attach {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 18,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:attach:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_attach_inner(input)
        }
    }
}
impl Encode for Attach {
    fn encoded_size(&self) -> usize {
        encoded_size_attach_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_attach_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Flow(pub Box<FlowInner>);
#[derive(Clone, Debug, PartialEq)]
pub struct FlowBuilder(pub Box<FlowInner>);
#[derive(Clone, Debug, PartialEq, Default)]
pub struct FlowInner {
    pub next_incoming_id: Option<TransferNumber>,
    pub incoming_window: u32,
    pub next_outgoing_id: TransferNumber,
    pub outgoing_window: u32,
    pub handle: Option<Handle>,
    pub delivery_count: Option<SequenceNo>,
    pub link_credit: Option<u32>,
    pub available: Option<u32>,
    pub drain: bool,
    pub echo: bool,
    pub properties: Option<Fields>,
}
impl Flow {
    pub fn build() -> FlowBuilder {
        FlowBuilder(Box::new(FlowInner::default()))
    }
    #[inline]
    pub fn next_incoming_id(&self) -> Option<TransferNumber> {
        self.0.next_incoming_id
    }
    #[inline]
    pub fn next_incoming_id_mut(&mut self) -> Option<&mut TransferNumber> {
        self.0.next_incoming_id.as_mut()
    }
    #[inline]
    pub fn incoming_window(&self) -> u32 {
        self.0.incoming_window
    }
    #[inline]
    pub fn incoming_window_mut(&mut self) -> &mut u32 {
        &mut self.0.incoming_window
    }
    #[inline]
    pub fn next_outgoing_id(&self) -> TransferNumber {
        self.0.next_outgoing_id
    }
    #[inline]
    pub fn next_outgoing_id_mut(&mut self) -> &mut TransferNumber {
        &mut self.0.next_outgoing_id
    }
    #[inline]
    pub fn outgoing_window(&self) -> u32 {
        self.0.outgoing_window
    }
    #[inline]
    pub fn outgoing_window_mut(&mut self) -> &mut u32 {
        &mut self.0.outgoing_window
    }
    #[inline]
    pub fn handle(&self) -> Option<Handle> {
        self.0.handle
    }
    #[inline]
    pub fn handle_mut(&mut self) -> Option<&mut Handle> {
        self.0.handle.as_mut()
    }
    #[inline]
    pub fn delivery_count(&self) -> Option<SequenceNo> {
        self.0.delivery_count
    }
    #[inline]
    pub fn delivery_count_mut(&mut self) -> Option<&mut SequenceNo> {
        self.0.delivery_count.as_mut()
    }
    #[inline]
    pub fn link_credit(&self) -> Option<u32> {
        self.0.link_credit
    }
    #[inline]
    pub fn link_credit_mut(&mut self) -> Option<&mut u32> {
        self.0.link_credit.as_mut()
    }
    #[inline]
    pub fn available(&self) -> Option<u32> {
        self.0.available
    }
    #[inline]
    pub fn available_mut(&mut self) -> Option<&mut u32> {
        self.0.available.as_mut()
    }
    #[inline]
    pub fn drain(&self) -> bool {
        self.0.drain
    }
    #[inline]
    pub fn drain_mut(&mut self) -> &mut bool {
        &mut self.0.drain
    }
    #[inline]
    pub fn echo(&self) -> bool {
        self.0.echo
    }
    #[inline]
    pub fn echo_mut(&mut self) -> &mut bool {
        &mut self.0.echo
    }
    #[inline]
    pub fn properties(&self) -> Option<&Fields> {
        self.0.properties.as_ref()
    }
    #[inline]
    pub fn properties_mut(&mut self) -> Option<&mut Fields> {
        self.0.properties.as_mut()
    }
    #[inline]
    pub fn get_properties_mut(&mut self) -> &mut Fields {
        if self.0.properties.is_none() {
            self.0.properties = Some(Fields::default());
        }
        self.0.properties.as_mut().unwrap()
    }
    pub fn into_inner(self) -> Box<FlowInner> {
        self.0
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1;
}
impl FlowBuilder {
    #[inline]
    pub fn next_incoming_id(mut self, val: TransferNumber) -> Self {
        self.0.next_incoming_id = Some(val);
        self
    }
    #[inline]
    pub fn incoming_window(mut self, val: u32) -> Self {
        self.0.incoming_window = val;
        self
    }
    #[inline]
    pub fn next_outgoing_id(mut self, val: TransferNumber) -> Self {
        self.0.next_outgoing_id = val;
        self
    }
    #[inline]
    pub fn outgoing_window(mut self, val: u32) -> Self {
        self.0.outgoing_window = val;
        self
    }
    #[inline]
    pub fn handle(mut self, val: Handle) -> Self {
        self.0.handle = Some(val);
        self
    }
    #[inline]
    pub fn delivery_count(mut self, val: SequenceNo) -> Self {
        self.0.delivery_count = Some(val);
        self
    }
    #[inline]
    pub fn link_credit(mut self, val: u32) -> Self {
        self.0.link_credit = Some(val);
        self
    }
    #[inline]
    pub fn available(mut self, val: u32) -> Self {
        self.0.available = Some(val);
        self
    }
    #[inline]
    pub fn drain(mut self, val: bool) -> Self {
        self.0.drain = val;
        self
    }
    #[inline]
    pub fn echo(mut self, val: bool) -> Self {
        self.0.echo = val;
        self
    }
    #[inline]
    pub fn properties(mut self, val: Fields) -> Self {
        self.0.properties = Some(val);
        self
    }
    pub fn finish(self) -> Flow {
        Flow(self.0)
    }
}
#[allow(unused_mut)]
fn decode_flow_inner(input: &mut Bytes) -> Result<Flow, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let next_incoming_id: Option<TransferNumber>;
    if count > 0 {
        next_incoming_id = Option::<TransferNumber>::decode(&mut data)?;
        count -= 1;
    } else {
        next_incoming_id = None;
    }
    let incoming_window: u32;
    if count > 0 {
        let decoded = u32::decode(&mut data)?;
        incoming_window = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("incoming_window"));
    }
    let next_outgoing_id: TransferNumber;
    if count > 0 {
        let decoded = TransferNumber::decode(&mut data)?;
        next_outgoing_id = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("next_outgoing_id"));
    }
    let outgoing_window: u32;
    if count > 0 {
        let decoded = u32::decode(&mut data)?;
        outgoing_window = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("outgoing_window"));
    }
    let handle: Option<Handle>;
    if count > 0 {
        handle = Option::<Handle>::decode(&mut data)?;
        count -= 1;
    } else {
        handle = None;
    }
    let delivery_count: Option<SequenceNo>;
    if count > 0 {
        delivery_count = Option::<SequenceNo>::decode(&mut data)?;
        count -= 1;
    } else {
        delivery_count = None;
    }
    let link_credit: Option<u32>;
    if count > 0 {
        link_credit = Option::<u32>::decode(&mut data)?;
        count -= 1;
    } else {
        link_credit = None;
    }
    let available: Option<u32>;
    if count > 0 {
        available = Option::<u32>::decode(&mut data)?;
        count -= 1;
    } else {
        available = None;
    }
    let drain: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        drain = decoded.unwrap_or(false);
        count -= 1;
    } else {
        drain = false;
    }
    let echo: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        echo = decoded.unwrap_or(false);
        count -= 1;
    } else {
        echo = false;
    }
    let properties: Option<Fields>;
    if count > 0 {
        properties = Option::<Fields>::decode(&mut data)?;
        count -= 1;
    } else {
        properties = None;
    }
    Ok(Flow(Box::new(FlowInner {
        next_incoming_id,
        incoming_window,
        next_outgoing_id,
        outgoing_window,
        handle,
        delivery_count,
        link_credit,
        available,
        drain,
        echo,
        properties,
    })))
}
fn encoded_size_flow_inner(list: &Flow) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.next_incoming_id.encoded_size()
        + list.0.incoming_window.encoded_size()
        + list.0.next_outgoing_id.encoded_size()
        + list.0.outgoing_window.encoded_size()
        + list.0.handle.encoded_size()
        + list.0.delivery_count.encoded_size()
        + list.0.link_credit.encoded_size()
        + list.0.available.encoded_size()
        + list.0.drain.encoded_size()
        + list.0.echo.encoded_size()
        + list.0.properties.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_flow_inner(list: &Flow, buf: &mut BytesMut) {
    Descriptor::Ulong(19).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.next_incoming_id.encoded_size()
        + list.0.incoming_window.encoded_size()
        + list.0.next_outgoing_id.encoded_size()
        + list.0.outgoing_window.encoded_size()
        + list.0.handle.encoded_size()
        + list.0.delivery_count.encoded_size()
        + list.0.link_credit.encoded_size()
        + list.0.available.encoded_size()
        + list.0.drain.encoded_size()
        + list.0.echo.encoded_size()
        + list.0.properties.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Flow::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Flow::FIELD_COUNT as u8);
    }
    list.0.next_incoming_id.encode(buf);
    list.0.incoming_window.encode(buf);
    list.0.next_outgoing_id.encode(buf);
    list.0.outgoing_window.encode(buf);
    list.0.handle.encode(buf);
    list.0.delivery_count.encode(buf);
    list.0.link_credit.encode(buf);
    list.0.available.encode(buf);
    list.0.drain.encode(buf);
    list.0.echo.encode(buf);
    list.0.properties.encode(buf);
}
impl DecodeFormatted for Flow {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 19,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:flow:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_flow_inner(input)
        }
    }
}
impl Encode for Flow {
    fn encoded_size(&self) -> usize {
        encoded_size_flow_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_flow_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Transfer(pub Box<TransferInner>);
#[derive(Clone, Debug, PartialEq)]
pub struct TransferBuilder(pub Box<TransferInner>);
#[derive(Clone, Debug, PartialEq, Default)]
pub struct TransferInner {
    pub handle: Handle,
    pub delivery_id: Option<DeliveryNumber>,
    pub delivery_tag: Option<DeliveryTag>,
    pub message_format: Option<MessageFormat>,
    pub settled: Option<bool>,
    pub more: bool,
    pub rcv_settle_mode: Option<ReceiverSettleMode>,
    pub state: Option<DeliveryState>,
    pub resume: bool,
    pub aborted: bool,
    pub batchable: bool,
    pub body: Option<TransferBody>,
}
impl Transfer {
    pub fn build() -> TransferBuilder {
        TransferBuilder(Box::new(TransferInner::default()))
    }
    #[inline]
    pub fn handle(&self) -> Handle {
        self.0.handle
    }
    #[inline]
    pub fn handle_mut(&mut self) -> &mut Handle {
        &mut self.0.handle
    }
    #[inline]
    pub fn delivery_id(&self) -> Option<DeliveryNumber> {
        self.0.delivery_id
    }
    #[inline]
    pub fn delivery_id_mut(&mut self) -> Option<&mut DeliveryNumber> {
        self.0.delivery_id.as_mut()
    }
    #[inline]
    pub fn delivery_tag(&self) -> Option<&DeliveryTag> {
        self.0.delivery_tag.as_ref()
    }
    #[inline]
    pub fn delivery_tag_mut(&mut self) -> Option<&mut DeliveryTag> {
        self.0.delivery_tag.as_mut()
    }
    #[inline]
    pub fn message_format(&self) -> Option<MessageFormat> {
        self.0.message_format
    }
    #[inline]
    pub fn message_format_mut(&mut self) -> Option<&mut MessageFormat> {
        self.0.message_format.as_mut()
    }
    #[inline]
    pub fn settled(&self) -> Option<bool> {
        self.0.settled
    }
    #[inline]
    pub fn settled_mut(&mut self) -> Option<&mut bool> {
        self.0.settled.as_mut()
    }
    #[inline]
    pub fn more(&self) -> bool {
        self.0.more
    }
    #[inline]
    pub fn more_mut(&mut self) -> &mut bool {
        &mut self.0.more
    }
    #[inline]
    pub fn rcv_settle_mode(&self) -> Option<ReceiverSettleMode> {
        self.0.rcv_settle_mode
    }
    #[inline]
    pub fn rcv_settle_mode_mut(&mut self) -> Option<&mut ReceiverSettleMode> {
        self.0.rcv_settle_mode.as_mut()
    }
    #[inline]
    pub fn state(&self) -> Option<&DeliveryState> {
        self.0.state.as_ref()
    }
    #[inline]
    pub fn state_mut(&mut self) -> Option<&mut DeliveryState> {
        self.0.state.as_mut()
    }
    #[inline]
    pub fn resume(&self) -> bool {
        self.0.resume
    }
    #[inline]
    pub fn resume_mut(&mut self) -> &mut bool {
        &mut self.0.resume
    }
    #[inline]
    pub fn aborted(&self) -> bool {
        self.0.aborted
    }
    #[inline]
    pub fn aborted_mut(&mut self) -> &mut bool {
        &mut self.0.aborted
    }
    #[inline]
    pub fn batchable(&self) -> bool {
        self.0.batchable
    }
    #[inline]
    pub fn batchable_mut(&mut self) -> &mut bool {
        &mut self.0.batchable
    }
    #[inline]
    pub fn body(&self) -> Option<&TransferBody> {
        self.0.body.as_ref()
    }
    pub fn into_inner(self) -> Box<TransferInner> {
        self.0
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1;
}
impl TransferBuilder {
    #[inline]
    pub fn handle(mut self, val: Handle) -> Self {
        self.0.handle = val;
        self
    }
    #[inline]
    pub fn delivery_id(mut self, val: DeliveryNumber) -> Self {
        self.0.delivery_id = Some(val);
        self
    }
    #[inline]
    pub fn delivery_tag(mut self, val: DeliveryTag) -> Self {
        self.0.delivery_tag = Some(val);
        self
    }
    #[inline]
    pub fn message_format(mut self, val: MessageFormat) -> Self {
        self.0.message_format = Some(val);
        self
    }
    #[inline]
    pub fn settled(mut self, val: bool) -> Self {
        self.0.settled = Some(val);
        self
    }
    #[inline]
    pub fn more(mut self, val: bool) -> Self {
        self.0.more = val;
        self
    }
    #[inline]
    pub fn rcv_settle_mode(mut self, val: ReceiverSettleMode) -> Self {
        self.0.rcv_settle_mode = Some(val);
        self
    }
    #[inline]
    pub fn state(mut self, val: DeliveryState) -> Self {
        self.0.state = Some(val);
        self
    }
    #[inline]
    pub fn resume(mut self, val: bool) -> Self {
        self.0.resume = val;
        self
    }
    #[inline]
    pub fn aborted(mut self, val: bool) -> Self {
        self.0.aborted = val;
        self
    }
    #[inline]
    pub fn batchable(mut self, val: bool) -> Self {
        self.0.batchable = val;
        self
    }
    #[inline]
    pub fn body(mut self, body: TransferBody) -> Self {
        self.0.body = Some(body);
        self
    }
    pub fn finish(self) -> Transfer {
        Transfer(self.0)
    }
}
#[allow(unused_mut)]
fn decode_transfer_inner(input: &mut Bytes) -> Result<Transfer, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let handle: Handle;
    if count > 0 {
        let decoded = Handle::decode(&mut data)?;
        handle = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("handle"));
    }
    let delivery_id: Option<DeliveryNumber>;
    if count > 0 {
        delivery_id = Option::<DeliveryNumber>::decode(&mut data)?;
        count -= 1;
    } else {
        delivery_id = None;
    }
    let delivery_tag: Option<DeliveryTag>;
    if count > 0 {
        delivery_tag = Option::<DeliveryTag>::decode(&mut data)?;
        count -= 1;
    } else {
        delivery_tag = None;
    }
    let message_format: Option<MessageFormat>;
    if count > 0 {
        message_format = Option::<MessageFormat>::decode(&mut data)?;
        count -= 1;
    } else {
        message_format = None;
    }
    let settled: Option<bool>;
    if count > 0 {
        settled = Option::<bool>::decode(&mut data)?;
        count -= 1;
    } else {
        settled = None;
    }
    let more: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        more = decoded.unwrap_or(false);
        count -= 1;
    } else {
        more = false;
    }
    let rcv_settle_mode: Option<ReceiverSettleMode>;
    if count > 0 {
        rcv_settle_mode = Option::<ReceiverSettleMode>::decode(&mut data)?;
        count -= 1;
    } else {
        rcv_settle_mode = None;
    }
    let state: Option<DeliveryState>;
    if count > 0 {
        state = Option::<DeliveryState>::decode(&mut data)?;
        count -= 1;
    } else {
        state = None;
    }
    let resume: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        resume = decoded.unwrap_or(false);
        count -= 1;
    } else {
        resume = false;
    }
    let aborted: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        aborted = decoded.unwrap_or(false);
        count -= 1;
    } else {
        aborted = false;
    }
    let batchable: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        batchable = decoded.unwrap_or(false);
        count -= 1;
    } else {
        batchable = false;
    }
    let body = if input.is_empty() {
        None
    } else {
        Some(input.split_to(input.len()).into())
    };
    Ok(Transfer(Box::new(TransferInner {
        handle,
        delivery_id,
        delivery_tag,
        message_format,
        settled,
        more,
        rcv_settle_mode,
        state,
        resume,
        aborted,
        batchable,
        body,
    })))
}
fn encoded_size_transfer_inner(list: &Transfer) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.handle.encoded_size()
        + list.0.delivery_id.encoded_size()
        + list.0.delivery_tag.encoded_size()
        + list.0.message_format.encoded_size()
        + list.0.settled.encoded_size()
        + list.0.more.encoded_size()
        + list.0.rcv_settle_mode.encoded_size()
        + list.0.state.encoded_size()
        + list.0.resume.encoded_size()
        + list.0.aborted.encoded_size()
        + list.0.batchable.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
        + list.0.body.as_ref().map(|b| b.len()).unwrap_or(0)
}
fn encode_transfer_inner(list: &Transfer, buf: &mut BytesMut) {
    Descriptor::Ulong(20).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.handle.encoded_size()
        + list.0.delivery_id.encoded_size()
        + list.0.delivery_tag.encoded_size()
        + list.0.message_format.encoded_size()
        + list.0.settled.encoded_size()
        + list.0.more.encoded_size()
        + list.0.rcv_settle_mode.encoded_size()
        + list.0.state.encoded_size()
        + list.0.resume.encoded_size()
        + list.0.aborted.encoded_size()
        + list.0.batchable.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Transfer::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Transfer::FIELD_COUNT as u8);
    }
    list.0.handle.encode(buf);
    list.0.delivery_id.encode(buf);
    list.0.delivery_tag.encode(buf);
    list.0.message_format.encode(buf);
    list.0.settled.encode(buf);
    list.0.more.encode(buf);
    list.0.rcv_settle_mode.encode(buf);
    list.0.state.encode(buf);
    list.0.resume.encode(buf);
    list.0.aborted.encode(buf);
    list.0.batchable.encode(buf);
    if let Some(body) = list.body() {
        body.encode(buf)
    }
}
impl DecodeFormatted for Transfer {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 20,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:transfer:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_transfer_inner(input)
        }
    }
}
impl Encode for Transfer {
    fn encoded_size(&self) -> usize {
        encoded_size_transfer_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_transfer_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Disposition(pub Box<DispositionInner>);
#[derive(Clone, Debug, PartialEq)]
pub struct DispositionBuilder(pub Box<DispositionInner>);
#[derive(Clone, Debug, PartialEq, Default)]
pub struct DispositionInner {
    pub role: Role,
    pub first: DeliveryNumber,
    pub last: Option<DeliveryNumber>,
    pub settled: bool,
    pub state: Option<DeliveryState>,
    pub batchable: bool,
}
impl Disposition {
    pub fn build() -> DispositionBuilder {
        DispositionBuilder(Box::new(DispositionInner::default()))
    }
    #[inline]
    pub fn role(&self) -> Role {
        self.0.role
    }
    #[inline]
    pub fn role_mut(&mut self) -> &mut Role {
        &mut self.0.role
    }
    #[inline]
    pub fn first(&self) -> DeliveryNumber {
        self.0.first
    }
    #[inline]
    pub fn first_mut(&mut self) -> &mut DeliveryNumber {
        &mut self.0.first
    }
    #[inline]
    pub fn last(&self) -> Option<DeliveryNumber> {
        self.0.last
    }
    #[inline]
    pub fn last_mut(&mut self) -> Option<&mut DeliveryNumber> {
        self.0.last.as_mut()
    }
    #[inline]
    pub fn settled(&self) -> bool {
        self.0.settled
    }
    #[inline]
    pub fn settled_mut(&mut self) -> &mut bool {
        &mut self.0.settled
    }
    #[inline]
    pub fn state(&self) -> Option<&DeliveryState> {
        self.0.state.as_ref()
    }
    #[inline]
    pub fn state_mut(&mut self) -> Option<&mut DeliveryState> {
        self.0.state.as_mut()
    }
    #[inline]
    pub fn batchable(&self) -> bool {
        self.0.batchable
    }
    #[inline]
    pub fn batchable_mut(&mut self) -> &mut bool {
        &mut self.0.batchable
    }
    pub fn into_inner(self) -> Box<DispositionInner> {
        self.0
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1 + 1 + 1 + 1;
}
impl DispositionBuilder {
    #[inline]
    pub fn role(mut self, val: Role) -> Self {
        self.0.role = val;
        self
    }
    #[inline]
    pub fn first(mut self, val: DeliveryNumber) -> Self {
        self.0.first = val;
        self
    }
    #[inline]
    pub fn last(mut self, val: DeliveryNumber) -> Self {
        self.0.last = Some(val);
        self
    }
    #[inline]
    pub fn settled(mut self, val: bool) -> Self {
        self.0.settled = val;
        self
    }
    #[inline]
    pub fn state(mut self, val: DeliveryState) -> Self {
        self.0.state = Some(val);
        self
    }
    #[inline]
    pub fn batchable(mut self, val: bool) -> Self {
        self.0.batchable = val;
        self
    }
    pub fn finish(self) -> Disposition {
        Disposition(self.0)
    }
}
#[allow(unused_mut)]
fn decode_disposition_inner(input: &mut Bytes) -> Result<Disposition, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let role: Role;
    if count > 0 {
        let decoded = Role::decode(&mut data)?;
        role = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("role"));
    }
    let first: DeliveryNumber;
    if count > 0 {
        let decoded = DeliveryNumber::decode(&mut data)?;
        first = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("first"));
    }
    let last: Option<DeliveryNumber>;
    if count > 0 {
        last = Option::<DeliveryNumber>::decode(&mut data)?;
        count -= 1;
    } else {
        last = None;
    }
    let settled: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        settled = decoded.unwrap_or(false);
        count -= 1;
    } else {
        settled = false;
    }
    let state: Option<DeliveryState>;
    if count > 0 {
        state = Option::<DeliveryState>::decode(&mut data)?;
        count -= 1;
    } else {
        state = None;
    }
    let batchable: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        batchable = decoded.unwrap_or(false);
        count -= 1;
    } else {
        batchable = false;
    }
    Ok(Disposition(Box::new(DispositionInner {
        role,
        first,
        last,
        settled,
        state,
        batchable,
    })))
}
fn encoded_size_disposition_inner(list: &Disposition) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.role.encoded_size()
        + list.0.first.encoded_size()
        + list.0.last.encoded_size()
        + list.0.settled.encoded_size()
        + list.0.state.encoded_size()
        + list.0.batchable.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_disposition_inner(list: &Disposition, buf: &mut BytesMut) {
    Descriptor::Ulong(21).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.role.encoded_size()
        + list.0.first.encoded_size()
        + list.0.last.encoded_size()
        + list.0.settled.encoded_size()
        + list.0.state.encoded_size()
        + list.0.batchable.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Disposition::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Disposition::FIELD_COUNT as u8);
    }
    list.0.role.encode(buf);
    list.0.first.encode(buf);
    list.0.last.encode(buf);
    list.0.settled.encode(buf);
    list.0.state.encode(buf);
    list.0.batchable.encode(buf);
}
impl DecodeFormatted for Disposition {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 21,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:disposition:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_disposition_inner(input)
        }
    }
}
impl Encode for Disposition {
    fn encoded_size(&self) -> usize {
        encoded_size_disposition_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_disposition_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Detach(pub Box<DetachInner>);
#[derive(Clone, Debug, PartialEq)]
pub struct DetachBuilder(pub Box<DetachInner>);
#[derive(Clone, Debug, PartialEq, Default)]
pub struct DetachInner {
    pub handle: Handle,
    pub closed: bool,
    pub error: Option<Error>,
}
impl Detach {
    pub fn build() -> DetachBuilder {
        DetachBuilder(Box::new(DetachInner::default()))
    }
    #[inline]
    pub fn handle(&self) -> Handle {
        self.0.handle
    }
    #[inline]
    pub fn handle_mut(&mut self) -> &mut Handle {
        &mut self.0.handle
    }
    #[inline]
    pub fn closed(&self) -> bool {
        self.0.closed
    }
    #[inline]
    pub fn closed_mut(&mut self) -> &mut bool {
        &mut self.0.closed
    }
    #[inline]
    pub fn error(&self) -> Option<&Error> {
        self.0.error.as_ref()
    }
    #[inline]
    pub fn error_mut(&mut self) -> Option<&mut Error> {
        self.0.error.as_mut()
    }
    pub fn into_inner(self) -> Box<DetachInner> {
        self.0
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1;
}
impl DetachBuilder {
    #[inline]
    pub fn handle(mut self, val: Handle) -> Self {
        self.0.handle = val;
        self
    }
    #[inline]
    pub fn closed(mut self, val: bool) -> Self {
        self.0.closed = val;
        self
    }
    #[inline]
    pub fn error(mut self, val: Error) -> Self {
        self.0.error = Some(val);
        self
    }
    pub fn finish(self) -> Detach {
        Detach(self.0)
    }
}
#[allow(unused_mut)]
fn decode_detach_inner(input: &mut Bytes) -> Result<Detach, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let handle: Handle;
    if count > 0 {
        let decoded = Handle::decode(&mut data)?;
        handle = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("handle"));
    }
    let closed: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        closed = decoded.unwrap_or(false);
        count -= 1;
    } else {
        closed = false;
    }
    let error: Option<Error>;
    if count > 0 {
        error = Option::<Error>::decode(&mut data)?;
        count -= 1;
    } else {
        error = None;
    }
    Ok(Detach(Box::new(DetachInner {
        handle,
        closed,
        error,
    })))
}
fn encoded_size_detach_inner(list: &Detach) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.handle.encoded_size()
        + list.0.closed.encoded_size()
        + list.0.error.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_detach_inner(list: &Detach, buf: &mut BytesMut) {
    Descriptor::Ulong(22).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.0.handle.encoded_size()
        + list.0.closed.encoded_size()
        + list.0.error.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Detach::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Detach::FIELD_COUNT as u8);
    }
    list.0.handle.encode(buf);
    list.0.closed.encode(buf);
    list.0.error.encode(buf);
}
impl DecodeFormatted for Detach {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 22,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:detach:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_detach_inner(input)
        }
    }
}
impl Encode for Detach {
    fn encoded_size(&self) -> usize {
        encoded_size_detach_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_detach_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct End {
    pub error: Option<Error>,
}
impl End {
    #[inline]
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }
    #[inline]
    pub fn error_mut(&mut self) -> Option<&mut Error> {
        self.error.as_mut()
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1;
}
#[allow(unused_mut)]
fn decode_end_inner(input: &mut Bytes) -> Result<End, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let error: Option<Error>;
    if count > 0 {
        error = Option::<Error>::decode(&mut data)?;
        count -= 1;
    } else {
        error = None;
    }
    Ok(End { error })
}
fn encoded_size_end_inner(list: &End) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.error.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_end_inner(list: &End, buf: &mut BytesMut) {
    Descriptor::Ulong(23).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.error.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(End::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(End::FIELD_COUNT as u8);
    }
    list.error.encode(buf);
}
impl DecodeFormatted for End {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 23,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:end:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_end_inner(input)
        }
    }
}
impl Encode for End {
    fn encoded_size(&self) -> usize {
        encoded_size_end_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_end_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Close {
    pub error: Option<Error>,
}
impl Close {
    #[inline]
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }
    #[inline]
    pub fn error_mut(&mut self) -> Option<&mut Error> {
        self.error.as_mut()
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1;
}
#[allow(unused_mut)]
fn decode_close_inner(input: &mut Bytes) -> Result<Close, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let error: Option<Error>;
    if count > 0 {
        error = Option::<Error>::decode(&mut data)?;
        count -= 1;
    } else {
        error = None;
    }
    Ok(Close { error })
}
fn encoded_size_close_inner(list: &Close) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.error.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_close_inner(list: &Close, buf: &mut BytesMut) {
    Descriptor::Ulong(24).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.error.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Close::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Close::FIELD_COUNT as u8);
    }
    list.error.encode(buf);
}
impl DecodeFormatted for Close {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 24,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:close:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_close_inner(input)
        }
    }
}
impl Encode for Close {
    fn encoded_size(&self) -> usize {
        encoded_size_close_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_close_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct SaslMechanisms {
    pub sasl_server_mechanisms: Symbols,
}
impl SaslMechanisms {
    #[inline]
    pub fn sasl_server_mechanisms(&self) -> &Symbols {
        &self.sasl_server_mechanisms
    }
    #[inline]
    pub fn sasl_server_mechanisms_mut(&mut self) -> &mut Symbols {
        &mut self.sasl_server_mechanisms
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1;
}
#[allow(unused_mut)]
fn decode_sasl_mechanisms_inner(input: &mut Bytes) -> Result<SaslMechanisms, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let sasl_server_mechanisms: Symbols;
    if count > 0 {
        let decoded = Symbols::decode(&mut data)?;
        sasl_server_mechanisms = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted(
            "sasl_server_mechanisms",
        ));
    }
    Ok(SaslMechanisms {
        sasl_server_mechanisms,
    })
}
fn encoded_size_sasl_mechanisms_inner(list: &SaslMechanisms) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.sasl_server_mechanisms.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_sasl_mechanisms_inner(list: &SaslMechanisms, buf: &mut BytesMut) {
    Descriptor::Ulong(64).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.sasl_server_mechanisms.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(SaslMechanisms::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(SaslMechanisms::FIELD_COUNT as u8);
    }
    list.sasl_server_mechanisms.encode(buf);
}
impl DecodeFormatted for SaslMechanisms {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 64,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:sasl-mechanisms:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_sasl_mechanisms_inner(input)
        }
    }
}
impl Encode for SaslMechanisms {
    fn encoded_size(&self) -> usize {
        encoded_size_sasl_mechanisms_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_sasl_mechanisms_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct SaslInit {
    pub mechanism: Symbol,
    pub initial_response: Option<Bytes>,
    pub hostname: Option<ByteString>,
}
impl SaslInit {
    #[inline]
    pub fn mechanism(&self) -> &Symbol {
        &self.mechanism
    }
    #[inline]
    pub fn mechanism_mut(&mut self) -> &mut Symbol {
        &mut self.mechanism
    }
    #[inline]
    pub fn initial_response(&self) -> Option<&Bytes> {
        self.initial_response.as_ref()
    }
    #[inline]
    pub fn initial_response_mut(&mut self) -> Option<&mut Bytes> {
        self.initial_response.as_mut()
    }
    #[inline]
    pub fn hostname(&self) -> Option<&ByteString> {
        self.hostname.as_ref()
    }
    #[inline]
    pub fn hostname_mut(&mut self) -> Option<&mut ByteString> {
        self.hostname.as_mut()
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1;
}
#[allow(unused_mut)]
fn decode_sasl_init_inner(input: &mut Bytes) -> Result<SaslInit, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let mechanism: Symbol;
    if count > 0 {
        let decoded = Symbol::decode(&mut data)?;
        mechanism = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("mechanism"));
    }
    let initial_response: Option<Bytes>;
    if count > 0 {
        initial_response = Option::<Bytes>::decode(&mut data)?;
        count -= 1;
    } else {
        initial_response = None;
    }
    let hostname: Option<ByteString>;
    if count > 0 {
        hostname = Option::<ByteString>::decode(&mut data)?;
        count -= 1;
    } else {
        hostname = None;
    }
    Ok(SaslInit {
        mechanism,
        initial_response,
        hostname,
    })
}
fn encoded_size_sasl_init_inner(list: &SaslInit) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.mechanism.encoded_size()
        + list.initial_response.encoded_size()
        + list.hostname.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_sasl_init_inner(list: &SaslInit, buf: &mut BytesMut) {
    Descriptor::Ulong(65).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.mechanism.encoded_size()
        + list.initial_response.encoded_size()
        + list.hostname.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(SaslInit::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(SaslInit::FIELD_COUNT as u8);
    }
    list.mechanism.encode(buf);
    list.initial_response.encode(buf);
    list.hostname.encode(buf);
}
impl DecodeFormatted for SaslInit {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 65,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:sasl-init:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_sasl_init_inner(input)
        }
    }
}
impl Encode for SaslInit {
    fn encoded_size(&self) -> usize {
        encoded_size_sasl_init_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_sasl_init_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct SaslChallenge {
    pub challenge: Bytes,
}
impl SaslChallenge {
    #[inline]
    pub fn challenge(&self) -> &Bytes {
        &self.challenge
    }
    #[inline]
    pub fn challenge_mut(&mut self) -> &mut Bytes {
        &mut self.challenge
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1;
}
#[allow(unused_mut)]
fn decode_sasl_challenge_inner(input: &mut Bytes) -> Result<SaslChallenge, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let challenge: Bytes;
    if count > 0 {
        let decoded = Bytes::decode(&mut data)?;
        challenge = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("challenge"));
    }
    Ok(SaslChallenge { challenge })
}
fn encoded_size_sasl_challenge_inner(list: &SaslChallenge) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.challenge.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_sasl_challenge_inner(list: &SaslChallenge, buf: &mut BytesMut) {
    Descriptor::Ulong(66).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.challenge.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(SaslChallenge::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(SaslChallenge::FIELD_COUNT as u8);
    }
    list.challenge.encode(buf);
}
impl DecodeFormatted for SaslChallenge {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 66,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:sasl-challenge:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_sasl_challenge_inner(input)
        }
    }
}
impl Encode for SaslChallenge {
    fn encoded_size(&self) -> usize {
        encoded_size_sasl_challenge_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_sasl_challenge_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct SaslResponse {
    pub response: Bytes,
}
impl SaslResponse {
    #[inline]
    pub fn response(&self) -> &Bytes {
        &self.response
    }
    #[inline]
    pub fn response_mut(&mut self) -> &mut Bytes {
        &mut self.response
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1;
}
#[allow(unused_mut)]
fn decode_sasl_response_inner(input: &mut Bytes) -> Result<SaslResponse, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let response: Bytes;
    if count > 0 {
        let decoded = Bytes::decode(&mut data)?;
        response = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("response"));
    }
    Ok(SaslResponse { response })
}
fn encoded_size_sasl_response_inner(list: &SaslResponse) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.response.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_sasl_response_inner(list: &SaslResponse, buf: &mut BytesMut) {
    Descriptor::Ulong(67).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.response.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(SaslResponse::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(SaslResponse::FIELD_COUNT as u8);
    }
    list.response.encode(buf);
}
impl DecodeFormatted for SaslResponse {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 67,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:sasl-response:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_sasl_response_inner(input)
        }
    }
}
impl Encode for SaslResponse {
    fn encoded_size(&self) -> usize {
        encoded_size_sasl_response_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_sasl_response_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct SaslOutcome {
    pub code: SaslCode,
    pub additional_data: Option<Bytes>,
}
impl SaslOutcome {
    #[inline]
    pub fn code(&self) -> SaslCode {
        self.code
    }
    #[inline]
    pub fn code_mut(&mut self) -> &mut SaslCode {
        &mut self.code
    }
    #[inline]
    pub fn additional_data(&self) -> Option<&Bytes> {
        self.additional_data.as_ref()
    }
    #[inline]
    pub fn additional_data_mut(&mut self) -> Option<&mut Bytes> {
        self.additional_data.as_mut()
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1;
}
#[allow(unused_mut)]
fn decode_sasl_outcome_inner(input: &mut Bytes) -> Result<SaslOutcome, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let code: SaslCode;
    if count > 0 {
        let decoded = SaslCode::decode(&mut data)?;
        code = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("code"));
    }
    let additional_data: Option<Bytes>;
    if count > 0 {
        additional_data = Option::<Bytes>::decode(&mut data)?;
        count -= 1;
    } else {
        additional_data = None;
    }
    Ok(SaslOutcome {
        code,
        additional_data,
    })
}
fn encoded_size_sasl_outcome_inner(list: &SaslOutcome) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.code.encoded_size() + list.additional_data.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_sasl_outcome_inner(list: &SaslOutcome, buf: &mut BytesMut) {
    Descriptor::Ulong(68).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.code.encoded_size() + list.additional_data.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(SaslOutcome::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(SaslOutcome::FIELD_COUNT as u8);
    }
    list.code.encode(buf);
    list.additional_data.encode(buf);
}
impl DecodeFormatted for SaslOutcome {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 68,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:sasl-outcome:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_sasl_outcome_inner(input)
        }
    }
}
impl Encode for SaslOutcome {
    fn encoded_size(&self) -> usize {
        encoded_size_sasl_outcome_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_sasl_outcome_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Source {
    pub address: Option<Address>,
    pub durable: TerminusDurability,
    pub expiry_policy: TerminusExpiryPolicy,
    pub timeout: Seconds,
    pub dynamic: bool,
    pub dynamic_node_properties: Option<NodeProperties>,
    pub distribution_mode: Option<DistributionMode>,
    pub filter: Option<FilterSet>,
    pub default_outcome: Option<Outcome>,
    pub outcomes: Option<Symbols>,
    pub capabilities: Option<Symbols>,
}
impl Source {
    #[inline]
    pub fn address(&self) -> Option<&Address> {
        self.address.as_ref()
    }
    #[inline]
    pub fn address_mut(&mut self) -> Option<&mut Address> {
        self.address.as_mut()
    }
    #[inline]
    pub fn durable(&self) -> TerminusDurability {
        self.durable
    }
    #[inline]
    pub fn durable_mut(&mut self) -> &mut TerminusDurability {
        &mut self.durable
    }
    #[inline]
    pub fn expiry_policy(&self) -> TerminusExpiryPolicy {
        self.expiry_policy
    }
    #[inline]
    pub fn expiry_policy_mut(&mut self) -> &mut TerminusExpiryPolicy {
        &mut self.expiry_policy
    }
    #[inline]
    pub fn timeout(&self) -> Seconds {
        self.timeout
    }
    #[inline]
    pub fn timeout_mut(&mut self) -> &mut Seconds {
        &mut self.timeout
    }
    #[inline]
    pub fn dynamic(&self) -> bool {
        self.dynamic
    }
    #[inline]
    pub fn dynamic_mut(&mut self) -> &mut bool {
        &mut self.dynamic
    }
    #[inline]
    pub fn dynamic_node_properties(&self) -> Option<&NodeProperties> {
        self.dynamic_node_properties.as_ref()
    }
    #[inline]
    pub fn dynamic_node_properties_mut(&mut self) -> Option<&mut NodeProperties> {
        self.dynamic_node_properties.as_mut()
    }
    #[inline]
    pub fn distribution_mode(&self) -> Option<&DistributionMode> {
        self.distribution_mode.as_ref()
    }
    #[inline]
    pub fn distribution_mode_mut(&mut self) -> Option<&mut DistributionMode> {
        self.distribution_mode.as_mut()
    }
    #[inline]
    pub fn filter(&self) -> Option<&FilterSet> {
        self.filter.as_ref()
    }
    #[inline]
    pub fn filter_mut(&mut self) -> Option<&mut FilterSet> {
        self.filter.as_mut()
    }
    #[inline]
    pub fn default_outcome(&self) -> Option<&Outcome> {
        self.default_outcome.as_ref()
    }
    #[inline]
    pub fn default_outcome_mut(&mut self) -> Option<&mut Outcome> {
        self.default_outcome.as_mut()
    }
    #[inline]
    pub fn outcomes(&self) -> Option<&Symbols> {
        self.outcomes.as_ref()
    }
    #[inline]
    pub fn outcomes_mut(&mut self) -> Option<&mut Symbols> {
        self.outcomes.as_mut()
    }
    #[inline]
    pub fn capabilities(&self) -> Option<&Symbols> {
        self.capabilities.as_ref()
    }
    #[inline]
    pub fn capabilities_mut(&mut self) -> Option<&mut Symbols> {
        self.capabilities.as_mut()
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1;
}
#[allow(unused_mut)]
fn decode_source_inner(input: &mut Bytes) -> Result<Source, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let address: Option<Address>;
    if count > 0 {
        address = Option::<Address>::decode(&mut data)?;
        count -= 1;
    } else {
        address = None;
    }
    let durable: TerminusDurability;
    if count > 0 {
        let decoded = Option::<TerminusDurability>::decode(&mut data)?;
        durable = decoded.unwrap_or(TerminusDurability::None);
        count -= 1;
    } else {
        durable = TerminusDurability::None;
    }
    let expiry_policy: TerminusExpiryPolicy;
    if count > 0 {
        let decoded = Option::<TerminusExpiryPolicy>::decode(&mut data)?;
        expiry_policy = decoded.unwrap_or(TerminusExpiryPolicy::SessionEnd);
        count -= 1;
    } else {
        expiry_policy = TerminusExpiryPolicy::SessionEnd;
    }
    let timeout: Seconds;
    if count > 0 {
        let decoded = Option::<Seconds>::decode(&mut data)?;
        timeout = decoded.unwrap_or(0);
        count -= 1;
    } else {
        timeout = 0;
    }
    let dynamic: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        dynamic = decoded.unwrap_or(false);
        count -= 1;
    } else {
        dynamic = false;
    }
    let dynamic_node_properties: Option<NodeProperties>;
    if count > 0 {
        dynamic_node_properties = Option::<NodeProperties>::decode(&mut data)?;
        count -= 1;
    } else {
        dynamic_node_properties = None;
    }
    let distribution_mode: Option<DistributionMode>;
    if count > 0 {
        distribution_mode = Option::<DistributionMode>::decode(&mut data)?;
        count -= 1;
    } else {
        distribution_mode = None;
    }
    let filter: Option<FilterSet>;
    if count > 0 {
        filter = Option::<FilterSet>::decode(&mut data)?;
        count -= 1;
    } else {
        filter = None;
    }
    let default_outcome: Option<Outcome>;
    if count > 0 {
        default_outcome = Option::<Outcome>::decode(&mut data)?;
        count -= 1;
    } else {
        default_outcome = None;
    }
    let outcomes: Option<Symbols>;
    if count > 0 {
        outcomes = Option::<Symbols>::decode(&mut data)?;
        count -= 1;
    } else {
        outcomes = None;
    }
    let capabilities: Option<Symbols>;
    if count > 0 {
        capabilities = Option::<Symbols>::decode(&mut data)?;
        count -= 1;
    } else {
        capabilities = None;
    }
    Ok(Source {
        address,
        durable,
        expiry_policy,
        timeout,
        dynamic,
        dynamic_node_properties,
        distribution_mode,
        filter,
        default_outcome,
        outcomes,
        capabilities,
    })
}
fn encoded_size_source_inner(list: &Source) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.address.encoded_size()
        + list.durable.encoded_size()
        + list.expiry_policy.encoded_size()
        + list.timeout.encoded_size()
        + list.dynamic.encoded_size()
        + list.dynamic_node_properties.encoded_size()
        + list.distribution_mode.encoded_size()
        + list.filter.encoded_size()
        + list.default_outcome.encoded_size()
        + list.outcomes.encoded_size()
        + list.capabilities.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_source_inner(list: &Source, buf: &mut BytesMut) {
    Descriptor::Ulong(40).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.address.encoded_size()
        + list.durable.encoded_size()
        + list.expiry_policy.encoded_size()
        + list.timeout.encoded_size()
        + list.dynamic.encoded_size()
        + list.dynamic_node_properties.encoded_size()
        + list.distribution_mode.encoded_size()
        + list.filter.encoded_size()
        + list.default_outcome.encoded_size()
        + list.outcomes.encoded_size()
        + list.capabilities.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Source::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Source::FIELD_COUNT as u8);
    }
    list.address.encode(buf);
    list.durable.encode(buf);
    list.expiry_policy.encode(buf);
    list.timeout.encode(buf);
    list.dynamic.encode(buf);
    list.dynamic_node_properties.encode(buf);
    list.distribution_mode.encode(buf);
    list.filter.encode(buf);
    list.default_outcome.encode(buf);
    list.outcomes.encode(buf);
    list.capabilities.encode(buf);
}
impl DecodeFormatted for Source {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 40,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:source:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_source_inner(input)
        }
    }
}
impl Encode for Source {
    fn encoded_size(&self) -> usize {
        encoded_size_source_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_source_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Target {
    pub address: Option<Address>,
    pub durable: TerminusDurability,
    pub expiry_policy: TerminusExpiryPolicy,
    pub timeout: Seconds,
    pub dynamic: bool,
    pub dynamic_node_properties: Option<NodeProperties>,
    pub capabilities: Option<Symbols>,
}
impl Target {
    #[inline]
    pub fn address(&self) -> Option<&Address> {
        self.address.as_ref()
    }
    #[inline]
    pub fn address_mut(&mut self) -> Option<&mut Address> {
        self.address.as_mut()
    }
    #[inline]
    pub fn durable(&self) -> TerminusDurability {
        self.durable
    }
    #[inline]
    pub fn durable_mut(&mut self) -> &mut TerminusDurability {
        &mut self.durable
    }
    #[inline]
    pub fn expiry_policy(&self) -> TerminusExpiryPolicy {
        self.expiry_policy
    }
    #[inline]
    pub fn expiry_policy_mut(&mut self) -> &mut TerminusExpiryPolicy {
        &mut self.expiry_policy
    }
    #[inline]
    pub fn timeout(&self) -> Seconds {
        self.timeout
    }
    #[inline]
    pub fn timeout_mut(&mut self) -> &mut Seconds {
        &mut self.timeout
    }
    #[inline]
    pub fn dynamic(&self) -> bool {
        self.dynamic
    }
    #[inline]
    pub fn dynamic_mut(&mut self) -> &mut bool {
        &mut self.dynamic
    }
    #[inline]
    pub fn dynamic_node_properties(&self) -> Option<&NodeProperties> {
        self.dynamic_node_properties.as_ref()
    }
    #[inline]
    pub fn dynamic_node_properties_mut(&mut self) -> Option<&mut NodeProperties> {
        self.dynamic_node_properties.as_mut()
    }
    #[inline]
    pub fn capabilities(&self) -> Option<&Symbols> {
        self.capabilities.as_ref()
    }
    #[inline]
    pub fn capabilities_mut(&mut self) -> Option<&mut Symbols> {
        self.capabilities.as_mut()
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1 + 1 + 1 + 1 + 1;
}
#[allow(unused_mut)]
fn decode_target_inner(input: &mut Bytes) -> Result<Target, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let address: Option<Address>;
    if count > 0 {
        address = Option::<Address>::decode(&mut data)?;
        count -= 1;
    } else {
        address = None;
    }
    let durable: TerminusDurability;
    if count > 0 {
        let decoded = Option::<TerminusDurability>::decode(&mut data)?;
        durable = decoded.unwrap_or(TerminusDurability::None);
        count -= 1;
    } else {
        durable = TerminusDurability::None;
    }
    let expiry_policy: TerminusExpiryPolicy;
    if count > 0 {
        let decoded = Option::<TerminusExpiryPolicy>::decode(&mut data)?;
        expiry_policy = decoded.unwrap_or(TerminusExpiryPolicy::SessionEnd);
        count -= 1;
    } else {
        expiry_policy = TerminusExpiryPolicy::SessionEnd;
    }
    let timeout: Seconds;
    if count > 0 {
        let decoded = Option::<Seconds>::decode(&mut data)?;
        timeout = decoded.unwrap_or(0);
        count -= 1;
    } else {
        timeout = 0;
    }
    let dynamic: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        dynamic = decoded.unwrap_or(false);
        count -= 1;
    } else {
        dynamic = false;
    }
    let dynamic_node_properties: Option<NodeProperties>;
    if count > 0 {
        dynamic_node_properties = Option::<NodeProperties>::decode(&mut data)?;
        count -= 1;
    } else {
        dynamic_node_properties = None;
    }
    let capabilities: Option<Symbols>;
    if count > 0 {
        capabilities = Option::<Symbols>::decode(&mut data)?;
        count -= 1;
    } else {
        capabilities = None;
    }
    Ok(Target {
        address,
        durable,
        expiry_policy,
        timeout,
        dynamic,
        dynamic_node_properties,
        capabilities,
    })
}
fn encoded_size_target_inner(list: &Target) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.address.encoded_size()
        + list.durable.encoded_size()
        + list.expiry_policy.encoded_size()
        + list.timeout.encoded_size()
        + list.dynamic.encoded_size()
        + list.dynamic_node_properties.encoded_size()
        + list.capabilities.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_target_inner(list: &Target, buf: &mut BytesMut) {
    Descriptor::Ulong(41).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.address.encoded_size()
        + list.durable.encoded_size()
        + list.expiry_policy.encoded_size()
        + list.timeout.encoded_size()
        + list.dynamic.encoded_size()
        + list.dynamic_node_properties.encoded_size()
        + list.capabilities.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Target::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Target::FIELD_COUNT as u8);
    }
    list.address.encode(buf);
    list.durable.encode(buf);
    list.expiry_policy.encode(buf);
    list.timeout.encode(buf);
    list.dynamic.encode(buf);
    list.dynamic_node_properties.encode(buf);
    list.capabilities.encode(buf);
}
impl DecodeFormatted for Target {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 41,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:target:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_target_inner(input)
        }
    }
}
impl Encode for Target {
    fn encoded_size(&self) -> usize {
        encoded_size_target_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_target_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Header {
    pub durable: bool,
    pub priority: u8,
    pub ttl: Option<Milliseconds>,
    pub first_acquirer: bool,
    pub delivery_count: u32,
}
impl Header {
    #[inline]
    pub fn durable(&self) -> bool {
        self.durable
    }
    #[inline]
    pub fn durable_mut(&mut self) -> &mut bool {
        &mut self.durable
    }
    #[inline]
    pub fn priority(&self) -> u8 {
        self.priority
    }
    #[inline]
    pub fn priority_mut(&mut self) -> &mut u8 {
        &mut self.priority
    }
    #[inline]
    pub fn ttl(&self) -> Option<Milliseconds> {
        self.ttl
    }
    #[inline]
    pub fn ttl_mut(&mut self) -> Option<&mut Milliseconds> {
        self.ttl.as_mut()
    }
    #[inline]
    pub fn first_acquirer(&self) -> bool {
        self.first_acquirer
    }
    #[inline]
    pub fn first_acquirer_mut(&mut self) -> &mut bool {
        &mut self.first_acquirer
    }
    #[inline]
    pub fn delivery_count(&self) -> u32 {
        self.delivery_count
    }
    #[inline]
    pub fn delivery_count_mut(&mut self) -> &mut u32 {
        &mut self.delivery_count
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1 + 1 + 1;
}
#[allow(unused_mut)]
fn decode_header_inner(input: &mut Bytes) -> Result<Header, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let durable: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        durable = decoded.unwrap_or(false);
        count -= 1;
    } else {
        durable = false;
    }
    let priority: u8;
    if count > 0 {
        let decoded = Option::<u8>::decode(&mut data)?;
        priority = decoded.unwrap_or(4);
        count -= 1;
    } else {
        priority = 4;
    }
    let ttl: Option<Milliseconds>;
    if count > 0 {
        ttl = Option::<Milliseconds>::decode(&mut data)?;
        count -= 1;
    } else {
        ttl = None;
    }
    let first_acquirer: bool;
    if count > 0 {
        let decoded = Option::<bool>::decode(&mut data)?;
        first_acquirer = decoded.unwrap_or(false);
        count -= 1;
    } else {
        first_acquirer = false;
    }
    let delivery_count: u32;
    if count > 0 {
        let decoded = Option::<u32>::decode(&mut data)?;
        delivery_count = decoded.unwrap_or(0);
        count -= 1;
    } else {
        delivery_count = 0;
    }
    Ok(Header {
        durable,
        priority,
        ttl,
        first_acquirer,
        delivery_count,
    })
}
fn encoded_size_header_inner(list: &Header) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.durable.encoded_size()
        + list.priority.encoded_size()
        + list.ttl.encoded_size()
        + list.first_acquirer.encoded_size()
        + list.delivery_count.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_header_inner(list: &Header, buf: &mut BytesMut) {
    Descriptor::Ulong(112).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.durable.encoded_size()
        + list.priority.encoded_size()
        + list.ttl.encoded_size()
        + list.first_acquirer.encoded_size()
        + list.delivery_count.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Header::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Header::FIELD_COUNT as u8);
    }
    list.durable.encode(buf);
    list.priority.encode(buf);
    list.ttl.encode(buf);
    list.first_acquirer.encode(buf);
    list.delivery_count.encode(buf);
}
impl DecodeFormatted for Header {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 112,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:header:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_header_inner(input)
        }
    }
}
impl Encode for Header {
    fn encoded_size(&self) -> usize {
        encoded_size_header_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_header_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Properties {
    pub message_id: Option<MessageId>,
    pub user_id: Option<Bytes>,
    pub to: Option<Address>,
    pub subject: Option<ByteString>,
    pub reply_to: Option<Address>,
    pub correlation_id: Option<MessageId>,
    pub content_type: Option<Symbol>,
    pub content_encoding: Option<Symbol>,
    pub absolute_expiry_time: Option<Timestamp>,
    pub creation_time: Option<Timestamp>,
    pub group_id: Option<ByteString>,
    pub group_sequence: Option<SequenceNo>,
    pub reply_to_group_id: Option<ByteString>,
}
impl Properties {
    #[inline]
    pub fn message_id(&self) -> Option<&MessageId> {
        self.message_id.as_ref()
    }
    #[inline]
    pub fn message_id_mut(&mut self) -> Option<&mut MessageId> {
        self.message_id.as_mut()
    }
    #[inline]
    pub fn user_id(&self) -> Option<&Bytes> {
        self.user_id.as_ref()
    }
    #[inline]
    pub fn user_id_mut(&mut self) -> Option<&mut Bytes> {
        self.user_id.as_mut()
    }
    #[inline]
    pub fn to(&self) -> Option<&Address> {
        self.to.as_ref()
    }
    #[inline]
    pub fn to_mut(&mut self) -> Option<&mut Address> {
        self.to.as_mut()
    }
    #[inline]
    pub fn subject(&self) -> Option<&ByteString> {
        self.subject.as_ref()
    }
    #[inline]
    pub fn subject_mut(&mut self) -> Option<&mut ByteString> {
        self.subject.as_mut()
    }
    #[inline]
    pub fn reply_to(&self) -> Option<&Address> {
        self.reply_to.as_ref()
    }
    #[inline]
    pub fn reply_to_mut(&mut self) -> Option<&mut Address> {
        self.reply_to.as_mut()
    }
    #[inline]
    pub fn correlation_id(&self) -> Option<&MessageId> {
        self.correlation_id.as_ref()
    }
    #[inline]
    pub fn correlation_id_mut(&mut self) -> Option<&mut MessageId> {
        self.correlation_id.as_mut()
    }
    #[inline]
    pub fn content_type(&self) -> Option<&Symbol> {
        self.content_type.as_ref()
    }
    #[inline]
    pub fn content_type_mut(&mut self) -> Option<&mut Symbol> {
        self.content_type.as_mut()
    }
    #[inline]
    pub fn content_encoding(&self) -> Option<&Symbol> {
        self.content_encoding.as_ref()
    }
    #[inline]
    pub fn content_encoding_mut(&mut self) -> Option<&mut Symbol> {
        self.content_encoding.as_mut()
    }
    #[inline]
    pub fn absolute_expiry_time(&self) -> Option<Timestamp> {
        self.absolute_expiry_time
    }
    #[inline]
    pub fn absolute_expiry_time_mut(&mut self) -> Option<&mut Timestamp> {
        self.absolute_expiry_time.as_mut()
    }
    #[inline]
    pub fn creation_time(&self) -> Option<Timestamp> {
        self.creation_time
    }
    #[inline]
    pub fn creation_time_mut(&mut self) -> Option<&mut Timestamp> {
        self.creation_time.as_mut()
    }
    #[inline]
    pub fn group_id(&self) -> Option<&ByteString> {
        self.group_id.as_ref()
    }
    #[inline]
    pub fn group_id_mut(&mut self) -> Option<&mut ByteString> {
        self.group_id.as_mut()
    }
    #[inline]
    pub fn group_sequence(&self) -> Option<SequenceNo> {
        self.group_sequence
    }
    #[inline]
    pub fn group_sequence_mut(&mut self) -> Option<&mut SequenceNo> {
        self.group_sequence.as_mut()
    }
    #[inline]
    pub fn reply_to_group_id(&self) -> Option<&ByteString> {
        self.reply_to_group_id.as_ref()
    }
    #[inline]
    pub fn reply_to_group_id_mut(&mut self) -> Option<&mut ByteString> {
        self.reply_to_group_id.as_mut()
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1;
}
#[allow(unused_mut)]
fn decode_properties_inner(input: &mut Bytes) -> Result<Properties, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let message_id: Option<MessageId>;
    if count > 0 {
        message_id = Option::<MessageId>::decode(&mut data)?;
        count -= 1;
    } else {
        message_id = None;
    }
    let user_id: Option<Bytes>;
    if count > 0 {
        user_id = Option::<Bytes>::decode(&mut data)?;
        count -= 1;
    } else {
        user_id = None;
    }
    let to: Option<Address>;
    if count > 0 {
        to = Option::<Address>::decode(&mut data)?;
        count -= 1;
    } else {
        to = None;
    }
    let subject: Option<ByteString>;
    if count > 0 {
        subject = Option::<ByteString>::decode(&mut data)?;
        count -= 1;
    } else {
        subject = None;
    }
    let reply_to: Option<Address>;
    if count > 0 {
        reply_to = Option::<Address>::decode(&mut data)?;
        count -= 1;
    } else {
        reply_to = None;
    }
    let correlation_id: Option<MessageId>;
    if count > 0 {
        correlation_id = Option::<MessageId>::decode(&mut data)?;
        count -= 1;
    } else {
        correlation_id = None;
    }
    let content_type: Option<Symbol>;
    if count > 0 {
        content_type = Option::<Symbol>::decode(&mut data)?;
        count -= 1;
    } else {
        content_type = None;
    }
    let content_encoding: Option<Symbol>;
    if count > 0 {
        content_encoding = Option::<Symbol>::decode(&mut data)?;
        count -= 1;
    } else {
        content_encoding = None;
    }
    let absolute_expiry_time: Option<Timestamp>;
    if count > 0 {
        absolute_expiry_time = Option::<Timestamp>::decode(&mut data)?;
        count -= 1;
    } else {
        absolute_expiry_time = None;
    }
    let creation_time: Option<Timestamp>;
    if count > 0 {
        creation_time = Option::<Timestamp>::decode(&mut data)?;
        count -= 1;
    } else {
        creation_time = None;
    }
    let group_id: Option<ByteString>;
    if count > 0 {
        group_id = Option::<ByteString>::decode(&mut data)?;
        count -= 1;
    } else {
        group_id = None;
    }
    let group_sequence: Option<SequenceNo>;
    if count > 0 {
        group_sequence = Option::<SequenceNo>::decode(&mut data)?;
        count -= 1;
    } else {
        group_sequence = None;
    }
    let reply_to_group_id: Option<ByteString>;
    if count > 0 {
        reply_to_group_id = Option::<ByteString>::decode(&mut data)?;
        count -= 1;
    } else {
        reply_to_group_id = None;
    }
    Ok(Properties {
        message_id,
        user_id,
        to,
        subject,
        reply_to,
        correlation_id,
        content_type,
        content_encoding,
        absolute_expiry_time,
        creation_time,
        group_id,
        group_sequence,
        reply_to_group_id,
    })
}
fn encoded_size_properties_inner(list: &Properties) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.message_id.encoded_size()
        + list.user_id.encoded_size()
        + list.to.encoded_size()
        + list.subject.encoded_size()
        + list.reply_to.encoded_size()
        + list.correlation_id.encoded_size()
        + list.content_type.encoded_size()
        + list.content_encoding.encoded_size()
        + list.absolute_expiry_time.encoded_size()
        + list.creation_time.encoded_size()
        + list.group_id.encoded_size()
        + list.group_sequence.encoded_size()
        + list.reply_to_group_id.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_properties_inner(list: &Properties, buf: &mut BytesMut) {
    Descriptor::Ulong(115).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.message_id.encoded_size()
        + list.user_id.encoded_size()
        + list.to.encoded_size()
        + list.subject.encoded_size()
        + list.reply_to.encoded_size()
        + list.correlation_id.encoded_size()
        + list.content_type.encoded_size()
        + list.content_encoding.encoded_size()
        + list.absolute_expiry_time.encoded_size()
        + list.creation_time.encoded_size()
        + list.group_id.encoded_size()
        + list.group_sequence.encoded_size()
        + list.reply_to_group_id.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Properties::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Properties::FIELD_COUNT as u8);
    }
    list.message_id.encode(buf);
    list.user_id.encode(buf);
    list.to.encode(buf);
    list.subject.encode(buf);
    list.reply_to.encode(buf);
    list.correlation_id.encode(buf);
    list.content_type.encode(buf);
    list.content_encoding.encode(buf);
    list.absolute_expiry_time.encode(buf);
    list.creation_time.encode(buf);
    list.group_id.encode(buf);
    list.group_sequence.encode(buf);
    list.reply_to_group_id.encode(buf);
}
impl DecodeFormatted for Properties {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 115,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:properties:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_properties_inner(input)
        }
    }
}
impl Encode for Properties {
    fn encoded_size(&self) -> usize {
        encoded_size_properties_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_properties_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Received {
    pub section_number: u32,
    pub section_offset: u64,
}
impl Received {
    #[inline]
    pub fn section_number(&self) -> u32 {
        self.section_number
    }
    #[inline]
    pub fn section_number_mut(&mut self) -> &mut u32 {
        &mut self.section_number
    }
    #[inline]
    pub fn section_offset(&self) -> u64 {
        self.section_offset
    }
    #[inline]
    pub fn section_offset_mut(&mut self) -> &mut u64 {
        &mut self.section_offset
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1;
}
#[allow(unused_mut)]
fn decode_received_inner(input: &mut Bytes) -> Result<Received, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let section_number: u32;
    if count > 0 {
        let decoded = u32::decode(&mut data)?;
        section_number = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("section_number"));
    }
    let section_offset: u64;
    if count > 0 {
        let decoded = u64::decode(&mut data)?;
        section_offset = decoded;
        count -= 1;
    } else {
        return Err(AmqpParseError::RequiredFieldOmitted("section_offset"));
    }
    Ok(Received {
        section_number,
        section_offset,
    })
}
fn encoded_size_received_inner(list: &Received) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.section_number.encoded_size() + list.section_offset.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_received_inner(list: &Received, buf: &mut BytesMut) {
    Descriptor::Ulong(35).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.section_number.encoded_size() + list.section_offset.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Received::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Received::FIELD_COUNT as u8);
    }
    list.section_number.encode(buf);
    list.section_offset.encode(buf);
}
impl DecodeFormatted for Received {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 35,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:received:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_received_inner(input)
        }
    }
}
impl Encode for Received {
    fn encoded_size(&self) -> usize {
        encoded_size_received_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_received_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Accepted {}
impl Accepted {
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0;
}
#[allow(unused_mut)]
fn decode_accepted_inner(input: &mut Bytes) -> Result<Accepted, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    input.split_to(size);
    Ok(Accepted {})
}
fn encoded_size_accepted_inner(list: &Accepted) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0;
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_accepted_inner(list: &Accepted, buf: &mut BytesMut) {
    Descriptor::Ulong(36).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0;
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Accepted::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Accepted::FIELD_COUNT as u8);
    }
}
impl DecodeFormatted for Accepted {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 36,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:accepted:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_accepted_inner(input)
        }
    }
}
impl Encode for Accepted {
    fn encoded_size(&self) -> usize {
        encoded_size_accepted_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_accepted_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Rejected {
    pub error: Option<Error>,
}
impl Rejected {
    #[inline]
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }
    #[inline]
    pub fn error_mut(&mut self) -> Option<&mut Error> {
        self.error.as_mut()
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1;
}
#[allow(unused_mut)]
fn decode_rejected_inner(input: &mut Bytes) -> Result<Rejected, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let error: Option<Error>;
    if count > 0 {
        error = Option::<Error>::decode(&mut data)?;
        count -= 1;
    } else {
        error = None;
    }
    Ok(Rejected { error })
}
fn encoded_size_rejected_inner(list: &Rejected) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.error.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_rejected_inner(list: &Rejected, buf: &mut BytesMut) {
    Descriptor::Ulong(37).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0 + list.error.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Rejected::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Rejected::FIELD_COUNT as u8);
    }
    list.error.encode(buf);
}
impl DecodeFormatted for Rejected {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 37,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:rejected:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_rejected_inner(input)
        }
    }
}
impl Encode for Rejected {
    fn encoded_size(&self) -> usize {
        encoded_size_rejected_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_rejected_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Released {}
impl Released {
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0;
}
#[allow(unused_mut)]
fn decode_released_inner(input: &mut Bytes) -> Result<Released, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    input.split_to(size);
    Ok(Released {})
}
fn encoded_size_released_inner(list: &Released) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0;
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_released_inner(list: &Released, buf: &mut BytesMut) {
    Descriptor::Ulong(38).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0;
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Released::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Released::FIELD_COUNT as u8);
    }
}
impl DecodeFormatted for Released {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 38,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:released:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_released_inner(input)
        }
    }
}
impl Encode for Released {
    fn encoded_size(&self) -> usize {
        encoded_size_released_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_released_inner(self, buf)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Modified {
    pub delivery_failed: Option<bool>,
    pub undeliverable_here: Option<bool>,
    pub message_annotations: Option<FieldsVec>,
}
impl Modified {
    #[inline]
    pub fn delivery_failed(&self) -> Option<bool> {
        self.delivery_failed
    }
    #[inline]
    pub fn delivery_failed_mut(&mut self) -> Option<&mut bool> {
        self.delivery_failed.as_mut()
    }
    #[inline]
    pub fn undeliverable_here(&self) -> Option<bool> {
        self.undeliverable_here
    }
    #[inline]
    pub fn undeliverable_here_mut(&mut self) -> Option<&mut bool> {
        self.undeliverable_here.as_mut()
    }
    #[inline]
    pub fn message_annotations(&self) -> Option<&FieldsVec> {
        self.message_annotations.as_ref()
    }
    #[inline]
    pub fn message_annotations_mut(&mut self) -> Option<&mut FieldsVec> {
        self.message_annotations.as_mut()
    }
    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 + 1 + 1 + 1;
}
#[allow(unused_mut)]
fn decode_modified_inner(input: &mut Bytes) -> Result<Modified, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    let mut data = input.split_to(size);
    let mut count = header.count;
    let delivery_failed: Option<bool>;
    if count > 0 {
        delivery_failed = Option::<bool>::decode(&mut data)?;
        count -= 1;
    } else {
        delivery_failed = None;
    }
    let undeliverable_here: Option<bool>;
    if count > 0 {
        undeliverable_here = Option::<bool>::decode(&mut data)?;
        count -= 1;
    } else {
        undeliverable_here = None;
    }
    let message_annotations: Option<FieldsVec>;
    if count > 0 {
        message_annotations = Option::<FieldsVec>::decode(&mut data)?;
        count -= 1;
    } else {
        message_annotations = None;
    }
    Ok(Modified {
        delivery_failed,
        undeliverable_here,
        message_annotations,
    })
}
fn encoded_size_modified_inner(list: &Modified) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.delivery_failed.encoded_size()
        + list.undeliverable_here.encoded_size()
        + list.message_annotations.encoded_size();
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize {
        12
    } else {
        6
    }) + content_size
}
fn encode_modified_inner(list: &Modified, buf: &mut BytesMut) {
    Descriptor::Ulong(39).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0
        + list.delivery_failed.encoded_size()
        + list.undeliverable_here.encoded_size()
        + list.message_annotations.encoded_size();
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32(Modified::FIELD_COUNT as u32);
    } else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8(Modified::FIELD_COUNT as u8);
    }
    list.delivery_failed.encode(buf);
    list.undeliverable_here.encode(buf);
    list.message_annotations.encode(buf);
}
impl DecodeFormatted for Modified {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == 39,
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"amqp:modified:list",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_modified_inner(input)
        }
    }
}
impl Encode for Modified {
    fn encoded_size(&self) -> usize {
        encoded_size_modified_inner(self)
    }
    fn encode(&self, buf: &mut BytesMut) {
        encode_modified_inner(self, buf)
    }
}
