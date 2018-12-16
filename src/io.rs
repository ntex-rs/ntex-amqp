use bytes::{BigEndian, BufMut, ByteOrder, BytesMut};
use std::marker::PhantomData;
use tokio_io::codec::{Decoder, Encoder};

use super::errors::{AmqpCodecError, ProtocolIdError};
use super::framing::HEADER_LEN;
use crate::codec::{Decode, Encode};
use crate::protocol::ProtocolId;

pub struct AmqpCodec<T: Decode + Encode> {
    state: DecodeState,
    phantom: PhantomData<T>,
}

#[derive(Debug, Clone, Copy)]
enum DecodeState {
    FrameHeader,
    Frame(usize),
}

impl<T: Decode + Encode> Default for AmqpCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Decode + Encode> AmqpCodec<T> {
    pub fn new() -> AmqpCodec<T> {
        AmqpCodec {
            state: DecodeState::FrameHeader,
            phantom: PhantomData,
        }
    }
}

impl<T: Decode + Encode> Decoder for AmqpCodec<T> {
    type Item = T;
    type Error = AmqpCodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match self.state {
                DecodeState::FrameHeader => {
                    let len = src.len();
                    if len < HEADER_LEN {
                        return Ok(None);
                    }
                    let size = BigEndian::read_u32(src.as_ref()) as usize;
                    // todo: max frame size check
                    self.state = DecodeState::Frame(size);
                    src.split_to(4);
                    if len < size {
                        src.reserve(size); // extend receiving buffer to fit the whole frame -- todo: too eager?
                        return Ok(None);
                    }
                }
                DecodeState::Frame(size) => {
                    if src.len() < size - 4 {
                        return Ok(None);
                    }

                    let frame_buf = src.split_to(size - 4);
                    let (remainder, frame) = T::decode(frame_buf.as_ref())?;
                    if remainder.is_empty() {
                        // todo: could it really happen?
                        return Err(AmqpCodecError::UnparsedBytesLeft);
                    }
                    src.reserve(HEADER_LEN);
                    self.state = DecodeState::FrameHeader;
                    return Ok(Some(frame));
                }
            }
        }
    }
}

impl<T: Decode + Encode + ::std::fmt::Debug> Encoder for AmqpCodec<T> {
    type Item = T;
    type Error = AmqpCodecError;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let size = item.encoded_size();
        if dst.remaining_mut() < size {
            dst.reserve(size);
        }

        item.encode(dst);
        Ok(())
    }
}

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
            let src = src.split_to(8);
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
