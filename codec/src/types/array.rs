use ntex_bytes::{BufMut, Bytes, BytesMut};

use crate::codec::{self, ArrayEncode, ArrayHeader, Decode, DecodeFormatted, Encode};
use crate::error::AmqpParseError;
use crate::types::Constructor;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Array {
    count: u32,
    element_constructor: Constructor,
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
            count: len,
            payload: buf.freeze(),
            element_constructor: T::ARRAY_CONSTRUCTOR,
        }
    }

    pub fn element_constructor(&self) -> &Constructor {
        &self.element_constructor
    }

    /// Attempts to decode the array into a vector of type `T`. Format code supplied to T::decode_with_format is the format code of the underlying
    /// AMQP type of array's element constructor. Use `Array::element_constructor` to access full constructor if needed.
    pub fn decode<T: DecodeFormatted>(&self) -> Result<Vec<T>, AmqpParseError> {
        let mut buf = self.payload.clone();
        let mut result: Vec<T> = Vec::with_capacity(self.count as usize);
        for _ in 0..self.count {
            let decoded = T::decode_with_format(&mut buf, self.element_constructor.format_code())?;
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
        let ctor_len = self.element_constructor.encoded_size();
        let header_len = if self.payload.len() + ctor_len + 1 > u8::MAX as usize {
            9 // 1 for format code, 4 for size, 4 for count
        } else {
            3 // 1 for format code, 1 for size, 1 for count
        };

        header_len + ctor_len + self.payload.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        let ctor_len = self.element_constructor.encoded_size();
        if self.payload.len() + ctor_len + 1 > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_ARRAY32);
            buf.put_u32((4 + ctor_len + self.payload.len()) as u32); // size. 4 for count
            buf.put_u32(self.count);
        } else {
            buf.put_u8(codec::FORMATCODE_ARRAY8);
            buf.put_u8((1 + ctor_len + self.payload.len()) as u8); // size. 1 for count
            buf.put_u8(self.count as u8);
        }
        self.element_constructor.encode(buf);
        buf.extend_from_slice(self.payload.as_ref());
    }
}

impl DecodeFormatted for Array {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let header = ArrayHeader::decode_with_format(input, fmt)?;
        let size = header.size as usize;
        decode_check_len!(input, size);
        let mut payload = input.split_to(size);
        let element_constructor = Constructor::decode(&mut payload)?;

        Ok(Array {
            element_constructor,
            payload,
            count: header.count,
        })
    }
}
