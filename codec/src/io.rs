use std::{cell::Cell, fmt, marker::PhantomData};

use byteorder::{BigEndian, ByteOrder};
use ntex_bytes::{Buf, BufMut, BytePages, BytesMut};
use ntex_codec::{Decoder, Encoder};

use super::error::{AmqpCodecError, ProtocolIdError};
use super::framing::HEADER_LEN;
use crate::codec::{Decode, Encode};
use crate::protocol::ProtocolId;

#[derive(Debug)]
pub struct AmqpCodec<T: Decode + Encode> {
    state: Cell<DecodeState>,
    max_size: usize,
    max_encode_size: usize,
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
            state: Cell::new(DecodeState::FrameHeader),
            max_size: 0,
            max_encode_size: 0,
            phantom: PhantomData,
        }
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn set_max_size(&mut self, size: usize) {
        self.max_size = size;
    }

    /// Set max outbound frame size.
    ///
    /// Encoding of larger frame fails with `AmqpCodecError::MaxOutboundSizeExceeded`.
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_encode_size(mut self, size: usize) -> Self {
        self.max_encode_size = size;
        self
    }

    /// Set max outbound frame size.
    ///
    /// Encoding of larger frame fails with `AmqpCodecError::MaxOutboundSizeExceeded`.
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn set_max_encode_size(&mut self, size: usize) {
        self.max_encode_size = size;
    }
}

impl<T: Decode + Encode + fmt::Debug> Decoder for AmqpCodec<T> {
    type Item = T;
    type Error = AmqpCodecError;

    fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match self.state.get() {
                DecodeState::FrameHeader => {
                    let len = src.len();
                    if len < HEADER_LEN {
                        return Ok(None);
                    }

                    // read frame size
                    let size = BigEndian::read_u32(src.as_ref()) as usize;
                    if self.max_size != 0 && size > self.max_size {
                        return Err(AmqpCodecError::MaxSizeExceeded);
                    }
                    if size <= 4 {
                        return Err(AmqpCodecError::InvalidFrameSize);
                    }
                    self.state.set(DecodeState::Frame(size - 4));
                    src.advance(4);

                    if len < size {
                        return Ok(None);
                    }
                }
                DecodeState::Frame(size) => {
                    if src.len() < size {
                        return Ok(None);
                    }

                    let mut frame_buf = src.split_to(size);
                    let frame = T::decode(&mut frame_buf)?;
                    if !frame_buf.is_empty() {
                        // todo: could it really happen?
                        return Err(AmqpCodecError::UnparsedBytesLeft);
                    }
                    self.state.set(DecodeState::FrameHeader);
                    return Ok(Some(frame));
                }
            }
        }
    }
}

impl<T: Decode + Encode + ::std::fmt::Debug> Encoder for AmqpCodec<T> {
    type Item = T;
    type Error = AmqpCodecError;

    fn encode(&self, item: Self::Item, dst: &mut BytePages) -> Result<(), Self::Error> {
        if self.max_encode_size != 0 && item.encoded_size() > self.max_encode_size {
            return Err(AmqpCodecError::MaxOutboundSizeExceeded);
        }
        item.encode(dst);
        Ok(())
    }
}

const PROTOCOL_HEADER_LEN: usize = 8;
const PROTOCOL_HEADER_PREFIX: &[u8] = b"AMQP";
const PROTOCOL_VERSION: &[u8] = &[1, 0, 0];

#[derive(Default, Debug)]
pub struct ProtocolIdCodec;

impl Decoder for ProtocolIdCodec {
    type Item = ProtocolId;
    type Error = ProtocolIdError;

    fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < PROTOCOL_HEADER_LEN {
            Ok(None)
        } else {
            let src = src.split_to(PROTOCOL_HEADER_LEN);
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

    fn encode(&self, item: Self::Item, dst: &mut BytePages) -> Result<(), Self::Error> {
        dst.put_slice(PROTOCOL_HEADER_PREFIX);
        dst.put_u8(item as u8);
        dst.put_slice(PROTOCOL_VERSION);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::AmqpFrame;

    #[test]
    fn test_decode() -> Result<(), AmqpCodecError> {
        let mut data = BytesMut::from(b"\0\0\0\0\0\0\0\0\0\x06AC@A\0S$\xc0\x01\0B".as_ref());

        let codec = AmqpCodec::<AmqpFrame>::new();
        let res = codec.decode(&mut data);
        assert!(matches!(res, Err(AmqpCodecError::InvalidFrameSize)));

        Ok(())
    }

    #[test]
    fn test_max_encode_size() {
        use crate::protocol::{Frame, Open, OpenInner};

        let frame = || {
            let open = Open(Box::new(OpenInner {
                container_id: "a".repeat(100).into(),
                ..Default::default()
            }));
            AmqpFrame::new(0, Frame::Open(open))
        };
        let size = frame().encoded_size();

        let mut buf = BytePages::default();
        let codec = AmqpCodec::<AmqpFrame>::new().max_encode_size(size - 1);
        let res = codec.encode(frame(), &mut buf);
        assert!(matches!(res, Err(AmqpCodecError::MaxOutboundSizeExceeded)));
        assert_eq!(buf.len(), 0);

        let mut codec = AmqpCodec::<AmqpFrame>::new();
        codec.set_max_encode_size(size);
        codec.encode(frame(), &mut buf).unwrap();
        assert_eq!(buf.len(), size);

        // unlimited
        let mut buf = BytePages::default();
        AmqpCodec::<AmqpFrame>::new()
            .encode(frame(), &mut buf)
            .unwrap();
        assert_eq!(buf.len(), size);
    }
}
