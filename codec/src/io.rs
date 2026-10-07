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

    fn amqp_frame() -> AmqpFrame {
        use crate::protocol::{Begin, BeginInner, Frame};

        AmqpFrame::new(
            3,
            Frame::Begin(Begin(Box::new(BeginInner {
                remote_channel: Some(1),
                next_outgoing_id: 2,
                incoming_window: 3,
                outgoing_window: 4,
                ..Default::default()
            }))),
        )
    }

    fn encode_frame(frame: &AmqpFrame) -> BytesMut {
        let mut buf = BytePages::default();
        AmqpCodec::<AmqpFrame>::new()
            .encode(frame.clone(), &mut buf)
            .unwrap();
        BytesMut::from(&buf.freeze()[..])
    }

    #[test]
    fn amqp_codec_roundtrip() {
        let frame = amqp_frame();
        let mut data = encode_frame(&frame);
        assert_eq!(data.len(), frame.encoded_size());

        let codec = AmqpCodec::<AmqpFrame>::default();
        assert_eq!(codec.decode(&mut data).unwrap(), Some(frame));
        assert!(data.is_empty());
        // the codec is reusable after a complete frame
        assert_eq!(codec.decode(&mut data).unwrap(), None);
    }

    #[test]
    fn amqp_codec_partial_frames() {
        let frame = amqp_frame();
        let data = encode_frame(&frame);
        let codec = AmqpCodec::<AmqpFrame>::new();

        // not even a full header
        let mut buf = BytesMut::from(&data[..4]);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);

        // header is complete, body is not: the header is consumed and state kept
        let mut buf = BytesMut::from(&data[..HEADER_LEN]);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);

        // feed the rest
        buf.extend_from_slice(&data[HEADER_LEN..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(frame));
    }

    #[test]
    fn amqp_codec_max_size() {
        let data = encode_frame(&amqp_frame());

        let codec = AmqpCodec::<AmqpFrame>::new().max_size(data.len() - 1);
        let mut buf = data.clone();
        assert!(matches!(
            codec.decode(&mut buf),
            Err(AmqpCodecError::MaxSizeExceeded)
        ));

        let mut codec = AmqpCodec::<AmqpFrame>::new();
        codec.set_max_size(data.len());
        let mut buf = data.clone();
        assert!(codec.decode(&mut buf).unwrap().is_some());
    }

    #[test]
    fn amqp_codec_unparsed_bytes_left() {
        let frame = amqp_frame();
        let mut data = encode_frame(&frame).to_vec();
        data.push(0x42);
        let size = data.len() as u32;
        data[..4].copy_from_slice(&size.to_be_bytes());

        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&data[..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(AmqpCodecError::UnparsedBytesLeft)
        ));
    }

    #[test]
    fn protocol_id_roundtrip() {
        let codec = ProtocolIdCodec;
        for (id, byte) in [
            (ProtocolId::Amqp, 0u8),
            (ProtocolId::AmqpTls, 2),
            (ProtocolId::AmqpSasl, 3),
        ] {
            let mut buf = BytePages::default();
            codec.encode(id, &mut buf).unwrap();
            let encoded = buf.freeze();
            assert_eq!(encoded.as_ref(), &[b'A', b'M', b'Q', b'P', byte, 1, 0, 0]);

            let mut src = BytesMut::from(&encoded[..]);
            assert_eq!(codec.decode(&mut src).unwrap(), Some(id));
            assert!(src.is_empty());
        }
    }

    #[test]
    fn protocol_id_decode_errors() {
        let codec = ProtocolIdCodec;

        // short buffer, nothing consumed
        let mut src = BytesMut::from(b"AMQP\x00\x01\x00".as_ref());
        assert_eq!(codec.decode(&mut src).unwrap(), None);
        assert_eq!(src.len(), 7);

        let cases: Vec<(&[u8], ProtocolIdError)> = vec![
            (b"XMQP\x00\x01\x00\x00", ProtocolIdError::InvalidHeader),
            (b"AMQP\x00\x02\x00\x00", ProtocolIdError::Incompatible),
            (b"AMQP\x01\x01\x00\x00", ProtocolIdError::Unknown),
            (b"AMQP\x04\x01\x00\x00", ProtocolIdError::Unknown),
        ];
        for (input, expected) in cases {
            let mut src = BytesMut::from(input);
            let err = codec.decode(&mut src).unwrap_err();
            assert_eq!(
                std::mem::discriminant(&err),
                std::mem::discriminant(&expected),
                "input {input:?}"
            );
            // the header is always consumed before validation
            assert!(src.is_empty());
        }
    }
}
