use tokio_io::codec::{Decoder, Encoder};
use bytes::{BufMut, BytesMut, ByteOrder, BigEndian};
use super::errors::{Result, Error};
use super::framing::{HEADER_LEN};
use codec::{Decode, Encode};
use std::marker::PhantomData;

pub struct AmqpDecoder<T: Decode> {
    state: DecodeState,
    phantom: PhantomData<T>
}

pub struct AmqpEncoder<T: Encode> {
    phantom: PhantomData<T>
}

#[derive(Debug, Clone, Copy)]
enum DecodeState {
    FrameHeader,
    Frame(usize),
}

impl<T: Decode> AmqpDecoder<T> {
    pub fn new() -> AmqpDecoder<T> {
        AmqpDecoder { state: DecodeState::FrameHeader, phantom: PhantomData }
    }
}

impl<T: Encode> AmqpEncoder<T> {
    pub fn new() -> AmqpEncoder<T> {
        AmqpEncoder { phantom: PhantomData }
    }
}

impl<T: Decode> Decoder for AmqpDecoder<T> {
    type Item = T;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
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
                },
                DecodeState::Frame(size) => {
                    if src.len() < size - 4 {
                        return Ok(None);
                    }

                    let frame_buf = src.split_to(size - 4);
                    let (remainder, frame) = T::decode(frame_buf.as_ref())?;
                    if remainder.len() > 0 { // todo: could it really happen?
                        return Err("bytes left unparsed at the frame trail".into());
                    }
                    src.reserve(HEADER_LEN);
                    self.state = DecodeState::FrameHeader;
                    return Ok(Some(frame));
                }
            }
        }
    }
}

impl<T: Encode> Encoder for AmqpEncoder<T> {
    type Item = T;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<()> {
        let size = item.encoded_size();
        if dst.remaining_mut() < size {
            dst.reserve(size);
        }

        item.encode(dst);
        Ok(())
    }
}
