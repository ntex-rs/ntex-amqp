use ntex_bytes::{BufMut, Bytes, BytesMut};

use crate::codec::{self, decode, ArrayEncode, DecodeFormatted, Encode};
use crate::error::AmqpParseError;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Array {
    len: u32,
    format: u8,
    payload: Bytes,
}

impl Array {
    pub fn new<'a, I, T>(iter: I) -> Array
    where
        I: Iterator<Item = &'a T>,
        T: ArrayEncode + 'a,
    {
        let mut len = 0;
        let mut buf = BytesMut::new();
        for item in iter {
            len += 1;
            item.array_encode(&mut buf);
        }

        Array {
            len,
            payload: buf.freeze(),
            format: T::ARRAY_FORMAT_CODE,
        }
    }

    pub fn decode<T: DecodeFormatted>(&self) -> Result<Vec<T>, AmqpParseError> {
        let mut buf = self.payload.clone();
        let mut result: Vec<T> = Vec::with_capacity(self.len as usize);
        for _ in 0..self.len {
            let decoded = T::decode_with_format(&mut buf, self.format)?;
            result.push(decoded);
        }
        Ok(result)
    }
}

impl<T> From<Vec<T>> for Array
where
    T: ArrayEncode,
{
    fn from(data: Vec<T>) -> Array {
        Array::new(data.iter())
    }
}

impl Encode for Array {
    fn encoded_size(&self) -> usize {
        // format_code + size + count + item constructor -- todo: support described ctor?
        (if self.payload.len() + 1 > u8::MAX as usize {
            10
        } else {
            4
        }) // +1 for 1 byte count and 1 byte format code
            + self.payload.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        if self.payload.len() + 1 > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_ARRAY32);
            buf.put_u32((self.payload.len() + 5) as u32); // +4 for 4 byte count and 1 byte item ctor that follow
            buf.put_u32(self.len);
        } else {
            buf.put_u8(codec::FORMATCODE_ARRAY8);
            buf.put_u8((self.payload.len() + 2) as u8); // +1 for 1 byte count and 1 byte item ctor that follow
            buf.put_u8(self.len as u8);
        }
        buf.put_u8(self.format);
        buf.extend_from_slice(&self.payload[..]);
    }
}

impl DecodeFormatted for Array {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let header = decode::decode_array_header(input, fmt)?;
        decode_check_len!(input, 1);
        let size = header.size as usize - 1;
        let format = input[0]; // todo: support descriptor
        input.split_to(1);
        decode_check_len!(input, size);
        let payload = input.split_to(size);

        Ok(Array {
            format,
            payload,
            len: header.count,
        })
    }
}
