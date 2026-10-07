use ntex_bytes::{BufMut, BytePages, Bytes};

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
        let mut buf = BytePages::default();
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
        let mut result: Vec<T> = Vec::with_capacity(codec::decode::array_capacity(self.count));
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

impl Array {
    fn is_array32(&self, ctor_len: usize) -> bool {
        // elements of zero-width types take no bytes, count may exceed size
        self.payload.len() + ctor_len + 1 > u8::MAX as usize || self.count > u32::from(u8::MAX)
    }
}

impl Encode for Array {
    fn encoded_size(&self) -> usize {
        let ctor_len = self.element_constructor.encoded_size();
        let header_len = if self.is_array32(ctor_len) {
            9 // 1 for format code, 4 for size, 4 for count
        } else {
            3 // 1 for format code, 1 for size, 1 for count
        };

        header_len + ctor_len + self.payload.len()
    }

    fn encode(&self, buf: &mut BytePages) {
        let ctor_len = self.element_constructor.encoded_size();
        if self.is_array32(ctor_len) {
            buf.put_u8(codec::FORMATCODE_ARRAY32);
            buf.put_u32(codec::size_u32(4 + ctor_len + self.payload.len())); // size. 4 for count
            buf.put_u32(self.count);
        } else {
            buf.put_u8(codec::FORMATCODE_ARRAY8);
            buf.put_u8((1 + ctor_len + self.payload.len()) as u8); // size. 1 for count
            buf.put_u8(self.count as u8);
        }
        self.element_constructor.encode(buf);
        buf.append(self.payload.clone());
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

#[cfg(test)]
mod tests {
    use ntex_bytes::{BytePages, Bytes};

    use super::*;
    use crate::codec::Encode;

    // zero-width elements, count exceeds size
    fn check_header(mut buf: Bytes, count: u32) {
        if count > u32::from(u8::MAX) {
            assert_eq!(buf[0], codec::FORMATCODE_ARRAY32);
            assert_eq!(buf[5..9], count.to_be_bytes());
        } else {
            assert_eq!(buf[0], codec::FORMATCODE_ARRAY8);
            assert_eq!(u32::from(buf[2]), count);
        }
        // decoder rejects arrays with more elements than bytes
        let res = <Array as Decode>::decode(&mut buf);
        assert!(matches!(res, Err(AmqpParseError::InvalidSize)));
    }

    #[test]
    fn encode_count_above_u8() {
        for count in [255, 256, 300] {
            let arr = Array {
                count,
                element_constructor: Constructor::FormatCode(codec::FORMATCODE_BOOLEAN_TRUE),
                payload: Bytes::new(),
            };
            let mut buf = BytePages::default();
            arr.encode(&mut buf);
            let buf = buf.freeze();
            assert_eq!(arr.encoded_size(), buf.len());
            check_header(buf, count);
        }
    }

    struct Null;

    impl ArrayEncode for Null {
        const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_NULL);
        fn array_encoded_size(&self) -> usize {
            0
        }
        fn array_encode(&self, _: &mut BytePages) {}
    }

    #[test]
    fn encode_vec_count_above_u8() {
        for count in [255, 256, 300] {
            let data: Vec<Null> = (0..count).map(|_| Null).collect();
            let mut buf = BytePages::default();
            data.encode(&mut buf);
            let buf = buf.freeze();
            assert_eq!(data.encoded_size(), buf.len());
            check_header(buf, count);
        }
    }

    #[test]
    fn array_new_roundtrip() {
        let items: Vec<u32> = vec![1, 2, 300];
        let arr = Array::new(items.iter());

        assert_eq!(arr.element_constructor(), &u32::ARRAY_CONSTRUCTOR);
        assert_eq!(arr.decode::<u32>().unwrap(), items);

        let mut buf = BytePages::default();
        arr.encode(&mut buf);
        let buf = buf.freeze();
        assert_eq!(arr.encoded_size(), buf.len());
        // array8, size, count, uint ctor, 3 * 4 bytes
        assert_eq!(
            buf[..4],
            [codec::FORMATCODE_ARRAY8, 14, 3, codec::FORMATCODE_UINT]
        );

        let decoded = <Array as Decode>::decode(&mut buf.clone()).unwrap();
        assert_eq!(decoded, arr);
        assert_eq!(decoded.decode::<u32>().unwrap(), items);
    }

    #[test]
    fn array_from_vec_empty() {
        let arr = Array::from(Vec::<u64>::new());
        assert_eq!(arr.decode::<u64>().unwrap(), Vec::<u64>::new());

        let mut buf = BytePages::default();
        arr.encode(&mut buf);
        let buf = buf.freeze();
        assert_eq!(arr.encoded_size(), buf.len());
        assert_eq!(
            buf.as_ref(),
            &[codec::FORMATCODE_ARRAY8, 2, 0, codec::FORMATCODE_ULONG]
        );
        assert_eq!(<Array as Decode>::decode(&mut buf.clone()).unwrap(), arr);
    }

    #[test]
    fn array_decode_wrong_element_type() {
        let arr = Array::from(vec![1u32, 2]);
        // uint payload is not a valid ubyte sequence of the same length
        assert!(matches!(
            arr.decode::<bool>(),
            Err(AmqpParseError::InvalidFormatCode(codec::FORMATCODE_UINT))
        ));
    }

    #[test]
    fn array_decode_truncated_payload() {
        let arr = Array {
            count: 2,
            element_constructor: Constructor::FormatCode(codec::FORMATCODE_UINT),
            payload: Bytes::from_static(b"\x00\x00\x00\x01"),
        };
        assert!(matches!(
            arr.decode::<u32>(),
            Err(AmqpParseError::Incomplete(_))
        ));
    }
}
