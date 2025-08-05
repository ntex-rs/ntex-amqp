use std::{char, collections, convert::TryFrom, hash::BuildHasher, hash::Hash};

use byteorder::{BigEndian, ByteOrder};
use chrono::{DateTime, TimeZone, Utc};
use ntex_bytes::{Buf, ByteString, Bytes};
use ordered_float::OrderedFloat;
use uuid::Uuid;

use crate::codec::{
    self, decode_format_code, ArrayHeader, Decode, DecodeFormatted, ListHeader, MapHeader,
};
use crate::error::AmqpParseError;
use crate::framing::{self, AmqpFrame, SaslFrame, HEADER_LEN};
use crate::protocol;
use crate::types::{
    Array, Constructor, DescribedCompound, Descriptor, List, Multiple, Str, Symbol, Variant,
    VariantMap, VecStringMap, VecSymbolMap,
};
use crate::HashMap;

macro_rules! be_read {
    ($input:ident, $fn:ident, $size:expr) => {{
        decode_check_len!($input, $size);
        let result = BigEndian::$fn(&$input);
        $input.advance($size);
        Ok(result)
    }};
}

fn read_u8(input: &mut Bytes) -> Result<u8, AmqpParseError> {
    decode_check_len!(input, 1);
    let code = input[0];
    input.advance(1);
    Ok(code)
}

fn read_i8(input: &mut Bytes) -> Result<i8, AmqpParseError> {
    decode_check_len!(input, 1);
    let code = input[0] as i8;
    input.advance(1);
    Ok(code)
}

fn read_bytes_u8(input: &mut Bytes) -> Result<Bytes, AmqpParseError> {
    let len = read_u8(input)?;
    let len = len as usize;
    decode_check_len!(input, len);
    Ok(input.split_to(len))
}

fn read_bytes_u32(input: &mut Bytes) -> Result<Bytes, AmqpParseError> {
    let result: Result<u32, AmqpParseError> = be_read!(input, read_u32, 4);
    let len = result?;
    let len = len as usize;
    decode_check_len!(input, len);
    Ok(input.split_to(len))
}

#[macro_export]
macro_rules! validate_code {
    ($fmt:ident, $code:expr) => {
        if $fmt != $code {
            return Err(AmqpParseError::InvalidFormatCode($fmt));
        }
    };
}

impl DecodeFormatted for bool {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_BOOLEAN => read_u8(input).map(|o| o != 0),
            codec::FORMATCODE_BOOLEAN_TRUE => Ok(true),
            codec::FORMATCODE_BOOLEAN_FALSE => Ok(false),
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl DecodeFormatted for u8 {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_UBYTE);
        read_u8(input)
    }
}

impl DecodeFormatted for u16 {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_USHORT);
        be_read!(input, read_u16, 2)
    }
}

impl DecodeFormatted for u32 {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_UINT => be_read!(input, read_u32, 4),
            codec::FORMATCODE_SMALLUINT => read_u8(input).map(u32::from),
            codec::FORMATCODE_UINT_0 => Ok(0),
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl DecodeFormatted for u64 {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_ULONG => be_read!(input, read_u64, 8),
            codec::FORMATCODE_SMALLULONG => read_u8(input).map(u64::from),
            codec::FORMATCODE_ULONG_0 => Ok(0),
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl DecodeFormatted for i8 {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_BYTE);
        read_i8(input)
    }
}

impl DecodeFormatted for i16 {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_SHORT);
        be_read!(input, read_i16, 2)
    }
}

impl DecodeFormatted for i32 {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_INT => be_read!(input, read_i32, 4),
            codec::FORMATCODE_SMALLINT => read_i8(input).map(i32::from),
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl DecodeFormatted for i64 {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_LONG => be_read!(input, read_i64, 8),
            codec::FORMATCODE_SMALLLONG => read_i8(input).map(i64::from),
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl DecodeFormatted for f32 {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_FLOAT);
        be_read!(input, read_f32, 4)
    }
}

impl DecodeFormatted for f64 {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DOUBLE);
        be_read!(input, read_f64, 8)
    }
}

impl DecodeFormatted for char {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_CHAR);
        let result: Result<u32, AmqpParseError> = be_read!(input, read_u32, 4);
        let o = result?;
        if let Some(c) = char::from_u32(o) {
            Ok(c)
        } else {
            Err(AmqpParseError::InvalidChar(o))
        } // todo: replace with CharTryFromError once try_from is stabilized
    }
}

impl DecodeFormatted for DateTime<Utc> {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_TIMESTAMP);
        be_read!(input, read_i64, 8).and_then(datetime_from_millis)
    }
}

impl DecodeFormatted for Uuid {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_UUID);
        decode_check_len!(input, 16);
        let uuid =
            Uuid::from_slice(&input.split_to(16)).map_err(|_| AmqpParseError::UuidParseError)?;
        Ok(uuid)
    }
}

impl DecodeFormatted for Bytes {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_BINARY8 => read_bytes_u8(input),
            codec::FORMATCODE_BINARY32 => read_bytes_u32(input),
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl DecodeFormatted for ByteString {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_STRING8 => {
                let bytes = read_bytes_u8(input)?;
                Ok(ByteString::try_from(bytes).map_err(|_| AmqpParseError::Utf8Error)?)
            }
            codec::FORMATCODE_STRING32 => {
                let bytes = read_bytes_u32(input)?;
                Ok(ByteString::try_from(bytes).map_err(|_| AmqpParseError::Utf8Error)?)
            }
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl DecodeFormatted for Str {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        Ok(Str::from(ByteString::decode_with_format(input, fmt)?))
    }
}

impl DecodeFormatted for Symbol {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_SYMBOL8 => {
                let bytes = read_bytes_u8(input)?;
                Ok(Symbol(Str::from(
                    ByteString::try_from(bytes).map_err(|_| AmqpParseError::Utf8Error)?,
                )))
            }
            codec::FORMATCODE_SYMBOL32 => {
                let bytes = read_bytes_u32(input)?;
                Ok(Symbol(Str::from(
                    ByteString::try_from(bytes).map_err(|_| AmqpParseError::Utf8Error)?,
                )))
            }
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl<K: Decode + Eq + Hash, V: Decode, S: BuildHasher + Default> DecodeFormatted
    for collections::HashMap<K, V, S>
{
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let header = MapHeader::decode_with_format(input, fmt)?;
        decode_check_len!(input, header.size as usize);
        let mut map_input = input.split_to(header.size as usize);
        let count = header.count / 2;
        let mut map: collections::HashMap<K, V, S> =
            collections::HashMap::with_capacity_and_hasher(count as usize, Default::default());
        for _ in 0..count {
            let key = K::decode(&mut map_input)?;
            let value = V::decode(&mut map_input)?;
            map.insert(key, value); // todo: ensure None returned?
        }
        // todo: validate map_input is empty
        Ok(map)
    }
}

impl<T: DecodeFormatted> DecodeFormatted for Vec<T> {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let header = ArrayHeader::decode_with_format(input, fmt)?;
        let item_fmt = decode_format_code(input)?; // todo: mg: support descriptor
        let mut result: Vec<T> = Vec::with_capacity(header.count as usize);
        for _ in 0..header.count {
            let decoded = T::decode_with_format(input, item_fmt)?;
            result.push(decoded);
        }
        Ok(result)
    }
}

impl DecodeFormatted for VecSymbolMap {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let header = MapHeader::decode_with_format(input, fmt)?;
        decode_check_len!(input, header.size as usize);
        let mut map_input = input.split_to(header.size as usize);
        let count = header.count / 2;
        let mut map = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let key = Symbol::decode(&mut map_input)?;
            let value = Variant::decode(&mut map_input)?;
            map.push((key, value)); // todo: ensure None returned?
        }
        // todo: validate map_input is empty
        Ok(VecSymbolMap(map))
    }
}

impl DecodeFormatted for VecStringMap {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let header = MapHeader::decode_with_format(input, fmt)?;
        decode_check_len!(input, header.size as usize);
        let mut map_input = input.split_to(header.size as usize);
        let count = header.count / 2;
        let mut map = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let key = Str::decode(&mut map_input)?;
            let value = Variant::decode(&mut map_input)?;
            map.push((key, value)); // todo: ensure None returned?
        }
        // todo: validate map_input is empty
        Ok(VecStringMap(map))
    }
}

impl<T: DecodeFormatted> DecodeFormatted for Multiple<T> {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_ARRAY8 | codec::FORMATCODE_ARRAY32 => {
                let items = Vec::<T>::decode_with_format(input, fmt)?;
                Ok(Multiple(items))
            }
            _ => {
                let item = T::decode_with_format(input, fmt)?;
                Ok(Multiple(vec![item]))
            }
        }
    }
}

impl DecodeFormatted for List {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let header = ListHeader::decode_with_format(input, fmt)?;
        let mut result: Vec<Variant> = Vec::with_capacity(header.count as usize);
        for _ in 0..header.count {
            let decoded = Variant::decode(input)?;
            result.push(decoded);
        }
        Ok(List(result))
    }
}

impl DecodeFormatted for Variant {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_NULL => Ok(Variant::Null),
            codec::FORMATCODE_BOOLEAN => bool::decode_with_format(input, fmt).map(Variant::Boolean),
            codec::FORMATCODE_BOOLEAN_FALSE => Ok(Variant::Boolean(false)),
            codec::FORMATCODE_BOOLEAN_TRUE => Ok(Variant::Boolean(true)),
            codec::FORMATCODE_UINT_0 => Ok(Variant::Uint(0)),
            codec::FORMATCODE_ULONG_0 => Ok(Variant::Ulong(0)),
            codec::FORMATCODE_UBYTE => u8::decode_with_format(input, fmt).map(Variant::Ubyte),
            codec::FORMATCODE_USHORT => u16::decode_with_format(input, fmt).map(Variant::Ushort),
            codec::FORMATCODE_UINT => u32::decode_with_format(input, fmt).map(Variant::Uint),
            codec::FORMATCODE_ULONG => u64::decode_with_format(input, fmt).map(Variant::Ulong),
            codec::FORMATCODE_BYTE => i8::decode_with_format(input, fmt).map(Variant::Byte),
            codec::FORMATCODE_SHORT => i16::decode_with_format(input, fmt).map(Variant::Short),
            codec::FORMATCODE_INT => i32::decode_with_format(input, fmt).map(Variant::Int),
            codec::FORMATCODE_LONG => i64::decode_with_format(input, fmt).map(Variant::Long),
            codec::FORMATCODE_SMALLUINT => u32::decode_with_format(input, fmt).map(Variant::Uint),
            codec::FORMATCODE_SMALLULONG => u64::decode_with_format(input, fmt).map(Variant::Ulong),
            codec::FORMATCODE_SMALLINT => i32::decode_with_format(input, fmt).map(Variant::Int),
            codec::FORMATCODE_SMALLLONG => i64::decode_with_format(input, fmt).map(Variant::Long),
            codec::FORMATCODE_FLOAT => {
                f32::decode_with_format(input, fmt).map(|o| Variant::Float(OrderedFloat(o)))
            }
            codec::FORMATCODE_DOUBLE => {
                f64::decode_with_format(input, fmt).map(|o| Variant::Double(OrderedFloat(o)))
            }
            codec::FORMATCODE_DECIMAL32 => read_fixed_bytes(input).map(Variant::Decimal32),
            codec::FORMATCODE_DECIMAL64 => read_fixed_bytes(input).map(Variant::Decimal64),
            codec::FORMATCODE_DECIMAL128 => read_fixed_bytes(input).map(Variant::Decimal128),
            codec::FORMATCODE_CHAR => char::decode_with_format(input, fmt).map(Variant::Char),
            codec::FORMATCODE_TIMESTAMP => {
                DateTime::<Utc>::decode_with_format(input, fmt).map(Variant::Timestamp)
            }
            codec::FORMATCODE_UUID => Uuid::decode_with_format(input, fmt).map(Variant::Uuid),
            codec::FORMATCODE_BINARY8 | codec::FORMATCODE_BINARY32 => {
                Bytes::decode_with_format(input, fmt).map(Variant::Binary)
            }
            codec::FORMATCODE_STRING8 | codec::FORMATCODE_STRING32 => {
                ByteString::decode_with_format(input, fmt).map(|o| Variant::String(o.into()))
            }
            codec::FORMATCODE_SYMBOL8 | codec::FORMATCODE_SYMBOL32 => {
                Symbol::decode_with_format(input, fmt).map(Variant::Symbol)
            }
            codec::FORMATCODE_LIST0 => Ok(Variant::List(List(vec![]))),
            codec::FORMATCODE_LIST8 | codec::FORMATCODE_LIST32 => {
                List::decode_with_format(input, fmt).map(Variant::List)
            }
            codec::FORMATCODE_ARRAY8 | codec::FORMATCODE_ARRAY32 => {
                Array::decode_with_format(input, fmt).map(Variant::Array)
            }
            codec::FORMATCODE_MAP8 | codec::FORMATCODE_MAP32 => {
                HashMap::<Variant, Variant>::decode_with_format(input, fmt)
                    .map(|o| Variant::Map(VariantMap::new(o)))
            }
            codec::FORMATCODE_DESCRIBED => {
                let descriptor = Descriptor::decode(input)?;
                let format_code = {
                    decode_check_len!(input, 1);
                    let code = input[0];
                    Ok(code)
                }?;
                match format_code {
                    codec::FORMATCODE_LIST0 => {
                        input.advance(1); // advance past format code
                        Ok(Variant::DescribedCompound(DescribedCompound::new(
                            descriptor,
                            Bytes::from_static(&[codec::FORMATCODE_LIST0]),
                        )))
                    }
                    codec::FORMATCODE_LIST8 | codec::FORMATCODE_MAP8 | codec::FORMATCODE_ARRAY8 => {
                        decode_check_len!(input, 2);
                        let size = input[1] as usize;
                        decode_check_len!(input, 2 + size);
                        let data = input.split_to(2 + size);
                        Ok(Variant::DescribedCompound(DescribedCompound::new(
                            descriptor, data,
                        )))
                    }
                    codec::FORMATCODE_LIST32
                    | codec::FORMATCODE_MAP32
                    | codec::FORMATCODE_ARRAY32 => {
                        decode_check_len!(input, 5);
                        let size = u32::from_be_bytes(input[1..5].try_into().unwrap()) as usize;
                        decode_check_len!(input, 5 + size);
                        let data = input.split_to(5 + size);
                        Ok(Variant::DescribedCompound(DescribedCompound::new(
                            descriptor, data,
                        )))
                    }
                    _ => {
                        input.advance(1); // advance past format code
                        let value = Variant::decode_with_format(input, format_code)?;
                        Ok(Variant::Described((descriptor, Box::new(value))))
                    }
                }
            }
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl<T: DecodeFormatted> DecodeFormatted for Option<T> {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_NULL => Ok(None),
            _ => T::decode_with_format(input, fmt).map(Some),
        }
    }
}

impl DecodeFormatted for Descriptor {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_SMALLULONG => {
                u64::decode_with_format(input, fmt).map(Descriptor::Ulong)
            }
            codec::FORMATCODE_ULONG => u64::decode_with_format(input, fmt).map(Descriptor::Ulong),
            codec::FORMATCODE_SYMBOL8 => {
                Symbol::decode_with_format(input, fmt).map(Descriptor::Symbol)
            }
            codec::FORMATCODE_SYMBOL32 => {
                Symbol::decode_with_format(input, fmt).map(Descriptor::Symbol)
            }
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl DecodeFormatted for Constructor {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_DESCRIBED => {
                let descriptor = Descriptor::decode(input)?;
                let format_code = codec::decode_format_code(input)?;
                Ok(Constructor::Described {
                    descriptor,
                    format_code,
                })
            }
            _ => Ok(Constructor::FormatCode(fmt)),
        }
    }
}

impl Decode for AmqpFrame {
    fn decode(input: &mut Bytes) -> Result<Self, AmqpParseError> {
        let channel_id = decode_frame_header(input, framing::FRAME_TYPE_AMQP)?;
        let performative = protocol::Frame::decode(input)?;
        Ok(AmqpFrame::new(channel_id, performative))
    }
}

impl Decode for SaslFrame {
    fn decode(input: &mut Bytes) -> Result<Self, AmqpParseError> {
        let _ = decode_frame_header(input, framing::FRAME_TYPE_SASL)?;
        let frame = protocol::SaslFrameBody::decode(input)?;
        Ok(SaslFrame { body: frame })
    }
}

impl DecodeFormatted for ListHeader {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_LIST0 => Ok(ListHeader { count: 0, size: 0 }),
            codec::FORMATCODE_LIST8 => {
                decode_compound8(input).map(|(size, count)| ListHeader { count, size })
            }
            codec::FORMATCODE_LIST32 => {
                decode_compound32(input).map(|(size, count)| ListHeader { count, size })
            }
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl DecodeFormatted for MapHeader {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_MAP8 => {
                decode_compound8(input).map(|(size, count)| MapHeader { count, size })
            }
            codec::FORMATCODE_MAP32 => {
                decode_compound32(input).map(|(size, count)| MapHeader { count, size })
            }
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

impl DecodeFormatted for ArrayHeader {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        match fmt {
            codec::FORMATCODE_ARRAY8 => {
                decode_compound8(input).map(|(size, count)| ArrayHeader { count, size })
            }
            codec::FORMATCODE_ARRAY32 => {
                decode_compound32(input).map(|(size, count)| ArrayHeader { count, size })
            }
            _ => Err(AmqpParseError::InvalidFormatCode(fmt)),
        }
    }
}

fn decode_frame_header(input: &mut Bytes, expected_frame_type: u8) -> Result<u16, AmqpParseError> {
    decode_check_len!(input, 4);
    let doff = input[0];
    let frame_type = input[1];
    if frame_type != expected_frame_type {
        return Err(AmqpParseError::UnexpectedFrameType(frame_type));
    }

    let channel_id = BigEndian::read_u16(&input[2..]);
    let doff = doff as usize * 4;
    if doff < HEADER_LEN {
        return Err(AmqpParseError::InvalidSize);
    }
    // skipping remaining two header bytes and ext header
    let ext_header_len = doff - HEADER_LEN + 4;
    decode_check_len!(input, ext_header_len);
    input.advance(ext_header_len);
    Ok(channel_id)
}

fn decode_compound8(input: &mut Bytes) -> Result<(u32, u32), AmqpParseError> {
    decode_check_len!(input, 2);
    let size = input[0] - 1; // -1 for 1 byte count
    let count = input[1];
    input.advance(2);
    Ok((u32::from(size), u32::from(count)))
}

fn decode_compound32(input: &mut Bytes) -> Result<(u32, u32), AmqpParseError> {
    decode_check_len!(input, 8);
    let size = BigEndian::read_u32(input) - 4; // -4 for 4 byte count
    let count = BigEndian::read_u32(&input[4..]);
    input.advance(8);
    Ok((size, count))
}

fn datetime_from_millis(millis: i64) -> Result<DateTime<Utc>, AmqpParseError> {
    let seconds = millis / 1000;
    if seconds < 0 {
        // In order to handle time before 1970 correctly, we need to subtract a second
        // and use the nanoseconds field to add it back. This is a result of the nanoseconds
        // parameter being u32
        let nanoseconds = ((1000 + (millis - (seconds * 1000))) * 1_000_000).unsigned_abs();
        Utc.timestamp_opt(seconds - 1, nanoseconds as u32)
            .earliest()
            .ok_or(AmqpParseError::DatetimeParseError)
    } else {
        let nanoseconds = ((millis - (seconds * 1000)) * 1_000_000).unsigned_abs();
        Utc.timestamp_opt(seconds, nanoseconds as u32)
            .earliest()
            .ok_or(AmqpParseError::DatetimeParseError)
    }
}

fn read_fixed_bytes<const N: usize>(input: &mut Bytes) -> Result<[u8; N], AmqpParseError> {
    decode_check_len!(input, N);
    let mut data = [0u8; N];
    data.copy_from_slice(&input[..N]);
    input.advance(N);
    Ok(data)
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use ntex_bytes::{BufMut, BytesMut};
    use test_case::test_case;

    use super::*;
    use crate::codec::{Decode, Encode};

    const LOREM: &str = include_str!("lorem.txt");

    macro_rules! decode_tests {
        ($($name:ident: $kind:ident, $test:expr, $expected:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let mut b1 = BytesMut::with_capacity(($test).encoded_size());
                ($test).encode(&mut b1);
                assert_eq!($expected, <$kind as Decode>::decode(&mut b1.freeze()).unwrap());
            }
        )*
        }
    }

    decode_tests! {
        ubyte: u8, 255_u8, 255_u8,
        ushort: u16, 350_u16, 350_u16,

        uint_zero: u32, 0_u32, 0_u32,
        uint_small: u32, 128_u32, 128_u32,
        uint_big: u32, 2147483647_u32, 2147483647_u32,

        ulong_zero: u64, 0_u64, 0_u64,
        ulong_small: u64, 128_u64, 128_u64,
        uulong_big: u64, 2147483649_u64, 2147483649_u64,

        byte: i8, -128_i8, -128_i8,
        short: i16, -255_i16, -255_i16,

        int_zero: i32, 0_i32, 0_i32,
        int_small: i32, -50000_i32, -50000_i32,
        int_neg: i32, -128_i32, -128_i32,

        long_zero: i64, 0_i64, 0_i64,
        long_big: i64, -2147483647_i64, -2147483647_i64,
        long_small: i64, -128_i64, -128_i64,

        float: f32, 1.234_f32, 1.234_f32,
        double: f64, 1.234_f64, 1.234_f64,

        test_char: char, '💯', '💯',

        uuid: Uuid, Uuid::from_slice(&[4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87]).expect("parse error"),
        Uuid::parse_str("0436430c2b02624c2032570501212b57").expect("parse error"),

        binary_short: Bytes, Bytes::from(&[4u8, 5u8][..]), Bytes::from(&[4u8, 5u8][..]),
        binary_long: Bytes, Bytes::from(&[4u8; 500][..]), Bytes::from(&[4u8; 500][..]),

        string_short: ByteString, ByteString::from("Hello there"), ByteString::from("Hello there"),
        string_long: ByteString, ByteString::from(LOREM), ByteString::from(LOREM),

        // symbol_short: Symbol, Symbol::from("Hello there"), Symbol::from("Hello there"),
        // symbol_long: Symbol, Symbol::from(LOREM), Symbol::from(LOREM),

        variant_ubyte: Variant, Variant::Ubyte(255_u8), Variant::Ubyte(255_u8),
        variant_ushort: Variant, Variant::Ushort(350_u16), Variant::Ushort(350_u16),

        variant_uint_zero: Variant, Variant::Uint(0_u32), Variant::Uint(0_u32),
        variant_uint_small: Variant, Variant::Uint(128_u32), Variant::Uint(128_u32),
        variant_uint_big: Variant, Variant::Uint(2147483647_u32), Variant::Uint(2147483647_u32),

        variant_ulong_zero: Variant, Variant::Ulong(0_u64), Variant::Ulong(0_u64),
        variant_ulong_small: Variant, Variant::Ulong(128_u64), Variant::Ulong(128_u64),
        variant_ulong_big: Variant, Variant::Ulong(2147483649_u64), Variant::Ulong(2147483649_u64),

        variant_byte: Variant, Variant::Byte(-128_i8), Variant::Byte(-128_i8),
        variant_short: Variant, Variant::Short(-255_i16), Variant::Short(-255_i16),

        variant_int_zero: Variant, Variant::Int(0_i32), Variant::Int(0_i32),
        variant_int_small: Variant, Variant::Int(-50000_i32), Variant::Int(-50000_i32),
        variant_int_neg: Variant, Variant::Int(-128_i32), Variant::Int(-128_i32),

        variant_long_zero: Variant, Variant::Long(0_i64), Variant::Long(0_i64),
        variant_long_big: Variant, Variant::Long(-2147483647_i64), Variant::Long(-2147483647_i64),
        variant_long_small: Variant, Variant::Long(-128_i64), Variant::Long(-128_i64),

        variant_float: Variant, Variant::Float(OrderedFloat(1.234_f32)), Variant::Float(OrderedFloat(1.234_f32)),
        variant_double: Variant, Variant::Double(OrderedFloat(1.234_f64)), Variant::Double(OrderedFloat(1.234_f64)),

        variant_char: Variant, Variant::Char('💯'), Variant::Char('💯'),

        variant_uuid: Variant, Variant::Uuid(Uuid::from_slice(&[4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87]).expect("parse error")),
        Variant::Uuid(Uuid::parse_str("0436430c2b02624c2032570501212b57").expect("parse error")),

        variant_binary_short: Variant, Variant::Binary(Bytes::from(&[4u8, 5u8][..])), Variant::Binary(Bytes::from(&[4u8, 5u8][..])),
        variant_binary_long: Variant, Variant::Binary(Bytes::from(&[4u8; 500][..])), Variant::Binary(Bytes::from(&[4u8; 500][..])),

        variant_string_short: Variant, Variant::String(ByteString::from("Hello there").into()), Variant::String(ByteString::from("Hello there").into()),
        variant_string_long: Variant, Variant::String(ByteString::from(LOREM).into()), Variant::String(ByteString::from(LOREM).into()),

        // variant_symbol_short: Variant, Variant::Symbol(Symbol::from("Hello there")), Variant::Symbol(Symbol::from("Hello there")),
        // variant_symbol_long: Variant, Variant::Symbol(Symbol::from(LOREM)), Variant::Symbol(Symbol::from(LOREM)),
    }

    fn unwrap_value<T>(res: Result<T, AmqpParseError>) -> T {
        assert!(res.is_ok());
        res.unwrap()
    }

    #[test]
    fn test_bool_true() {
        let mut b1 = BytesMut::with_capacity(0);
        b1.put_u8(0x41);
        assert_eq!(true, unwrap_value(bool::decode(&mut b1.freeze())));

        let mut b2 = BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x01);
        assert_eq!(true, unwrap_value(bool::decode(&mut b2.freeze())));
    }

    #[test]
    fn test_bool_false() {
        let mut b1 = BytesMut::with_capacity(0);
        b1.put_u8(0x42u8);
        assert_eq!(false, unwrap_value(bool::decode(&mut b1.freeze())));

        let mut b2 = BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x00);
        assert_eq!(false, unwrap_value(bool::decode(&mut b2.freeze())));
    }

    /// UTC with a precision of milliseconds. For example, 1311704463521
    /// represents the moment 2011-07-26T18:21:03.521Z.
    #[test]
    fn test_timestamp() {
        let mut b1 = BytesMut::with_capacity(0);
        let datetime =
            Utc.with_ymd_and_hms(2011, 7, 26, 18, 21, 3).unwrap() + TimeDelta::milliseconds(521);
        datetime.encode(&mut b1);

        let expected =
            Utc.with_ymd_and_hms(2011, 7, 26, 18, 21, 3).unwrap() + TimeDelta::milliseconds(521);
        assert_eq!(
            expected,
            unwrap_value(DateTime::<Utc>::decode(&mut b1.freeze()))
        );
    }

    #[test]
    fn test_timestamp_pre_unix() {
        let mut b1 = BytesMut::with_capacity(0);
        let datetime =
            Utc.with_ymd_and_hms(1968, 7, 26, 18, 21, 3).unwrap() + TimeDelta::milliseconds(521);
        datetime.encode(&mut b1);

        let expected =
            Utc.with_ymd_and_hms(1968, 7, 26, 18, 21, 3).unwrap() + TimeDelta::milliseconds(521);
        assert_eq!(
            expected,
            unwrap_value(DateTime::<Utc>::decode(&mut b1.freeze()))
        );
    }

    #[test]
    fn variant_null() {
        let mut b = BytesMut::with_capacity(0);
        Variant::Null.encode(&mut b);
        let t = unwrap_value(Variant::decode(&mut b.freeze()));
        assert_eq!(Variant::Null, t);
    }

    #[test]
    fn variant_bool_true() {
        let mut b1 = BytesMut::with_capacity(0);
        b1.put_u8(0x41);
        assert_eq!(
            Variant::Boolean(true),
            unwrap_value(Variant::decode(&mut b1.freeze()))
        );

        let mut b2 = BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x01);
        assert_eq!(
            Variant::Boolean(true),
            unwrap_value(Variant::decode(&mut b2.freeze()))
        );
    }

    #[test]
    fn variant_bool_false() {
        let mut b1 = BytesMut::with_capacity(0);
        b1.put_u8(0x42u8);
        assert_eq!(
            Variant::Boolean(false),
            unwrap_value(Variant::decode(&mut b1.freeze()))
        );

        let mut b2 = BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x00);
        assert_eq!(
            Variant::Boolean(false),
            unwrap_value(Variant::decode(&mut b2.freeze()))
        );
    }

    /// UTC with a precision of milliseconds. For example, 1311704463521
    /// represents the moment 2011-07-26T18:21:03.521Z.
    #[test]
    fn variant_timestamp() {
        let mut b1 = BytesMut::with_capacity(0);
        let datetime =
            Utc.with_ymd_and_hms(2011, 7, 26, 18, 21, 3).unwrap() + TimeDelta::milliseconds(521);
        Variant::Timestamp(datetime).encode(&mut b1);

        let expected =
            Utc.with_ymd_and_hms(2011, 7, 26, 18, 21, 3).unwrap() + TimeDelta::milliseconds(521);
        assert_eq!(
            Variant::Timestamp(expected),
            unwrap_value(Variant::decode(&mut b1.freeze()))
        );
    }

    #[test]
    fn variant_timestamp_pre_unix() {
        let mut b1 = BytesMut::with_capacity(0);
        let datetime =
            Utc.with_ymd_and_hms(1968, 7, 26, 18, 21, 3).unwrap() + TimeDelta::milliseconds(521);
        Variant::Timestamp(datetime).encode(&mut b1);

        let expected =
            Utc.with_ymd_and_hms(1968, 7, 26, 18, 21, 3).unwrap() + TimeDelta::milliseconds(521);
        assert_eq!(
            Variant::Timestamp(expected),
            unwrap_value(Variant::decode(&mut b1.freeze()))
        );
    }

    #[test_case(
        b"\x00\xa3\x07foo:bar\xc0\x03\x01\x50\x03",
        Descriptor::Symbol("foo:bar".into()),
        List(vec![Variant::Ubyte(3)]);
        "described 'foo:bar', list8 w/one u8 field with value 3")]
    #[test_case(
        b"\x00\x80\x00\x00\x01\x37\x00\x00\x03\xe9\x45",
        Descriptor::Ulong((311 << 32) + 1001), List(vec![]); "described 311:1001, list0")]
    #[test_case(
        b"\x00\x80\x00\x01\xd4\xc0\x00\x03\x82\x70\xd0\x00\x00\x00\x0c\x00\x00\x00\x03\x53\x6f\xa1\x03abc\x42",
        Descriptor::Ulong((120_000 << 32) + 230_000),
        List(vec![Variant::Ulong(111), Variant::String("abc".into()), Variant::Boolean(false)]);
        "described 120000:230000, list32 w/3 fields: smallulong: 111, string8: 'abc', booleanfalse")]
    fn decode_described_list(
        input: &'static [u8],
        expected_descriptor: Descriptor,
        expected_list: List,
    ) {
        let mut buf = Bytes::from(input);
        let variant = Variant::decode(&mut buf).unwrap();
        assert!(buf.is_empty(), "Expected no remaining bytes after decoding");
        let dc = match variant {
            Variant::DescribedCompound(dc) => dc,
            _ => panic!("Expected a DescribedCompound variant"),
        };
        assert_eq!(dc.descriptor(), &expected_descriptor);
        println!("{:02x?}", dc.data.as_ref());
        let decoded_list: List = dc.decode().expect("Failed to decode List");
        assert_eq!(decoded_list, expected_list);
    }

    #[test_case(
        b"\x00\xa3\x05a:b:c\xc1\x08\x04\x50\x03\x41\x50\xc8\x56\x00",
        Descriptor::Symbol("a:b:c".into()),
        vec![(Variant::Ubyte(3), Variant::Boolean(true)), (Variant::Ubyte(200), Variant::Boolean(false))];
        "described 'a:b:c', map8 with 2 pairs: (ubyte(3), true), (ubyte(200), false)")]
    #[test_case(
        b"\x00\x80\x00\x01\xd4\xc0\x00\x03\x82\x70\xd1\x00\x00\x00\x0a\x00\x00\x00\x02\x73\x00\x00\x00z\x40",
        Descriptor::Ulong((120_000 << 32) + 230_000),
        vec![(Variant::Char('z'), Variant::Null)];
        "described 120000:230000, map32 with 1 pair: char: 'z', null")]
    fn decode_described_map(
        input: &'static [u8],
        expected_descriptor: Descriptor,
        expected_map: Vec<(Variant, Variant)>,
    ) {
        let mut buf = Bytes::from(input);
        let variant = Variant::decode(&mut buf).unwrap();
        assert!(buf.is_empty(), "Expected no remaining bytes after decoding");
        let dc = match variant {
            Variant::DescribedCompound(dc) => dc,
            _ => panic!("Expected a DescribedCompound variant"),
        };
        assert_eq!(dc.descriptor(), &expected_descriptor);
        println!("{:02x?}", dc.data.as_ref());
        let decoded_map: HashMap<Variant, Variant> = dc.decode().expect("Failed to decode List");
        let expected_map: HashMap<Variant, Variant> = expected_map.into_iter().collect();
        assert_eq!(decoded_map, expected_map);
    }

    #[test_case(
        b"\x00\xa3\x07foo:bar\xe0\x05\x03\x50\x01\x02\x03",
        Descriptor::Symbol("foo:bar".into()),
        Constructor::FormatCode(codec::FORMATCODE_UBYTE),
        vec![Variant::Ubyte(1), Variant::Ubyte(2), Variant::Ubyte(3)];
        "described 'foo:bar', array8 w/3 u8 elements: 1, 2, 3")]
    fn decode_described_array(
        input: &'static [u8],
        expected_descriptor: Descriptor,
        expected_el_ctor: Constructor,
        expected_array: Vec<Variant>,
    ) {
        // todo: mg: array decoding: add check that all bytes are read out according to size when done decoding array elements /
        // list fields / map key-value pairs according to count
        let mut buf = Bytes::from(input);
        let variant = Variant::decode(&mut buf).unwrap();
        assert!(buf.is_empty(), "Expected no remaining bytes after decoding");
        let dc = match variant {
            Variant::DescribedCompound(dc) => dc,
            _ => panic!("Expected a DescribedCompound variant"),
        };
        assert_eq!(dc.descriptor(), &expected_descriptor);
        println!("{:02x?}", dc.data.as_ref());
        let decoded_array: Array = dc.decode().expect("Failed to decode Array");
        assert_eq!(decoded_array.element_constructor(), &expected_el_ctor);
        let array_items: Vec<Variant> = decoded_array
            .decode()
            .expect("Failed to decode Array items using Variant type");
        assert_eq!(array_items, expected_array);
    }

    #[test]
    fn option_i8() {
        let mut b1 = BytesMut::with_capacity(0);
        Some(42i8).encode(&mut b1);

        assert_eq!(
            Some(42),
            unwrap_value(Option::<i8>::decode(&mut b1.freeze()))
        );

        let mut b2 = BytesMut::with_capacity(0);
        let o1: Option<i8> = None;
        o1.encode(&mut b2);

        assert_eq!(None, unwrap_value(Option::<i8>::decode(&mut b2.freeze())));
    }

    #[test]
    fn option_string() {
        let mut b1 = BytesMut::with_capacity(0);
        Some(ByteString::from("hello")).encode(&mut b1);

        assert_eq!(
            Some(ByteString::from("hello")),
            unwrap_value(Option::<ByteString>::decode(&mut b1.freeze()))
        );

        let mut b2 = BytesMut::with_capacity(0);
        let o1: Option<ByteString> = None;
        o1.encode(&mut b2);

        assert_eq!(
            None,
            unwrap_value(Option::<ByteString>::decode(&mut b2.freeze()))
        );
    }
}
