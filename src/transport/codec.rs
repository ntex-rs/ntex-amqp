use actix_codec::{Decoder, Encoder};
use bytes::{BufMut, BytesMut};

use crate::errors::ProtocolIdError;
use crate::protocol::ProtocolId;

const PROTOCOL_HEADER_LEN: usize = 8;
const PROTOCOL_HEADER_PREFIX: &[u8] = b"AMQP";
const PROTOCOL_VERSION: &[u8] = &[1, 0, 0];

pub struct ProtocolIdCodec;

impl Decoder for ProtocolIdCodec {
    type Item = ProtocolId;
    type Error = ProtocolIdError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < PROTOCOL_HEADER_LEN {
            Ok(None)
        } else {
            if &src[0..4] != PROTOCOL_HEADER_PREFIX {
                Err(ProtocolIdError::InvalidHeader)
            } else if &src[5..8] != PROTOCOL_VERSION {
                Err(ProtocolIdError::Incompatible)
            } else {
                let protocol_id = src[4];
                match protocol_id {
                    0 => Ok(Some(ProtocolId::Amqp)),
                    2 => Ok(Some(ProtocolId::AmqpTls)),
                    3 => Ok(Some(ProtocolId::AmqpSasl)),
                    _ => Err(ProtocolIdError::Unknown),
                }
            }
        }
    }
}

impl Encoder for ProtocolIdCodec {
    type Item = ProtocolId;
    type Error = ProtocolIdError;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(PROTOCOL_HEADER_PREFIX);
        dst.put_u8(item as u8);
        dst.extend_from_slice(PROTOCOL_VERSION);
        Ok(())
    }
}
