use std::{char, str, u8};

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use nom::{be_i8, be_i16, be_i32, be_i64, be_u8, be_u16, be_u32, be_u64, be_f32, be_f64};
use types::{ByteStr, Null, Symbol};
use uuid::Uuid;

named!(pub decode_null<Null>, map_res!(tag!([0x40u8]), |_| Ok::<Null, ()>(Null)));

named!(pub decode_bool<bool>, alt!(
    map_res!(tag!([0x56, 0x00]), |_| Ok::<bool, ()>(false)) |
    map_res!(tag!([0x56, 0x01]), |_| Ok::<bool, ()>(true)) |
    map_res!(tag!([0x41]), |_| Result::Ok::<bool, ()>(true)) |
    map_res!(tag!([0x42]), |_| Result::Ok::<bool, ()>(false))
));

named!(pub decode_ubyte<u8>, do_parse!(tag!([0x50u8]) >> byte: be_u8 >> (byte)));
named!(pub decode_ushort<u16>, do_parse!(tag!([0x60u8]) >> short: be_u16 >> (short)));
named!(pub decode_uint<u32>, alt!(
    do_parse!(tag!([0x70u8]) >> uint: be_u32 >> (uint)) |
    do_parse!(tag!([0x52u8]) >> uint: be_u8 >> (uint as u32)) |
    do_parse!(tag!([0x43u8]) >> (0))
));
named!(pub decode_ulong<u64>, alt!(
    do_parse!(tag!([0x80u8]) >> uint: be_u64 >> (uint)) |
    do_parse!(tag!([0x53u8]) >> uint: be_u8 >> (uint as u64)) |
    do_parse!(tag!([0x44u8]) >> (0))
));

named!(pub decode_byte<i8>, do_parse!(tag!([0x51u8]) >> byte: be_i8 >> (byte)));
named!(pub decode_short<i16>, do_parse!(tag!([0x61u8]) >> short: be_i16 >> (short)));
named!(pub decode_int<i32>, alt!(
    do_parse!(tag!([0x71u8]) >> int: be_i32 >> (int)) |
    do_parse!(tag!([0x54u8]) >> int: be_i8 >> (int as i32))
));
named!(pub decode_long<i64>, alt!(
    do_parse!(tag!([0x81u8]) >> long: be_i64 >> (long)) |
    do_parse!(tag!([0x55u8]) >> long: be_i8 >> (long as i64))
));

named!(pub decode_float<f32>, do_parse!(tag!([0x72u8]) >> float: be_f32 >> (float)));
named!(pub decode_double<f64>, do_parse!(tag!([0x82u8]) >> double: be_f64 >> (double)));

named!(pub decode_char<char>, map_opt!(do_parse!(tag!([0x73u8]) >> int: be_u32 >> (int)), |c| char::from_u32(c)));

named!(pub decode_timestamp<DateTime<Utc>>, do_parse!(tag!([0x83u8]) >> timestamp: be_i64 >> (datetime_from_millis(timestamp))));

named!(pub decode_uuid<Uuid>, do_parse!(tag!([0x98u8]) >> uuid: map_res!(take!(16), Uuid::from_bytes) >> (uuid)));

named!(pub decode_binary<Bytes>, alt!(
    do_parse!(tag!([0xA0u8]) >> bytes: length_bytes!(be_u8) >> (Bytes::from(bytes))) |
    do_parse!(tag!([0xB0u8]) >> bytes: length_bytes!(be_u32) >> (Bytes::from(bytes)))
));

named!(pub decode_string<ByteStr>, alt!(
    do_parse!(tag!([0xA1u8]) >> string: map_res!(length_bytes!(be_u8), str::from_utf8) >> (ByteStr::from(string))) |
    do_parse!(tag!([0xB1u8]) >> string: map_res!(length_bytes!(be_u32), str::from_utf8) >> (ByteStr::from(string)))
));

named!(pub decode_symbol<Symbol>, alt!(
    do_parse!(tag!([0xA3u8]) >> string: map_res!(length_bytes!(be_u8), str::from_utf8) >> (Symbol::from(string))) |
    do_parse!(tag!([0xB3u8]) >> string: map_res!(length_bytes!(be_u32), str::from_utf8) >> (Symbol::from(string)))
));

fn datetime_from_millis(millis: i64) -> DateTime<Utc> {
    let seconds = millis / 1000;
    if seconds < 0 {
        // In order to handle time before 1970 correctly, we need to subtract a second
        // and use the nanoseconds field to add it back. This is a result of the nanoseconds
        // parameter being u32
        let nanoseconds = ((1000 + (millis - (seconds * 1000))) * 1_000_000).abs() as u32;
        Utc.timestamp(seconds - 1, nanoseconds)
    } else {
        let nanoseconds = ((millis - (seconds * 1000)) * 1_000_000).abs() as u32;
        Utc.timestamp(seconds, nanoseconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use codec::Encode;

    const LOREM: &str = include_str!("lorem.txt");

    #[test]
    fn test_null() {
        let mut b = BytesMut::with_capacity(0);
        Null.encode(&mut b);
        let t = decode_null(&mut b).to_full_result();
        assert_eq!(Ok(Null), t);
    }

    #[test]
    fn test_bool_true() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x41);
        assert_eq!(Ok(true), decode_bool(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x01);
        assert_eq!(Ok(true), decode_bool(b2).to_full_result());
    }

    #[test]
    fn test_bool_false() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x42u8);
        assert_eq!(Ok(false), decode_bool(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x00);
        assert_eq!(Ok(false), decode_bool(b2).to_full_result());
    }

    #[test]
    fn test_ubyte() {
        let b1 = &mut BytesMut::with_capacity(0);
        (255 as u8).encode(b1);
        assert_eq!(Ok(255 as u8), decode_ubyte(b1).to_full_result());
    }

    #[test]
    fn test_ushort() {
        let b1 = &mut BytesMut::with_capacity(0);
        (350 as u16).encode(b1);
        assert_eq!(Ok(350 as u16), decode_ushort(b1).to_full_result());
    }

    #[test]
    fn test_uint() {
        let b1 = &mut BytesMut::with_capacity(0);
        (0 as u32).encode(b1);
        assert_eq!(Ok(0 as u32), decode_uint(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        (128 as u32).encode(b2);
        assert_eq!(Ok(128 as u32), decode_uint(b2).to_full_result());

        let b3 = &mut BytesMut::with_capacity(0);
        (2147483647 as u32).encode(b3);
        assert_eq!(Ok(2147483647 as u32), decode_uint(b3).to_full_result());
    }

    #[test]
    fn test_ulong() {
        let b1 = &mut BytesMut::with_capacity(0);
        (0 as u64).encode(b1);
        assert_eq!(Ok(0 as u64), decode_ulong(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        (128 as u64).encode(b2);
        assert_eq!(Ok(128 as u64), decode_ulong(b2).to_full_result());

        let b3 = &mut BytesMut::with_capacity(0);
        (2147483649 as u64).encode(b3);
        assert_eq!(Ok(2147483649 as u64), decode_ulong(b3).to_full_result());
    }

    #[test]
    fn test_byte() {
        let b1 = &mut BytesMut::with_capacity(0);
        (-128 as i8).encode(b1);
        assert_eq!(Ok(-128 as i8), decode_byte(b1).to_full_result());
    }

    #[test]
    fn test_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        (-255 as i16).encode(b1);
        assert_eq!(Ok(-255 as i16), decode_short(b1).to_full_result());
    }

    #[test]
    fn test_int() {
        let b1 = &mut BytesMut::with_capacity(0);
        0.encode(b1);
        assert_eq!(Ok(0), decode_int(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        (-50000).encode(b2);
        assert_eq!(Ok(-50000), decode_int(b2).to_full_result());

        let b3 = &mut BytesMut::with_capacity(0);
        (-128).encode(b3);
        assert_eq!(Ok(-128), decode_int(b3).to_full_result());
    }

    #[test]
    fn test_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        (0 as i64).encode(b1);
        assert_eq!(Ok(0 as i64), decode_long(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        (-2147483647 as i64).encode(b2);
        assert_eq!(Ok(-2147483647 as i64), decode_long(b2).to_full_result());

        let b3 = &mut BytesMut::with_capacity(0);
        (-128 as i64).encode(b3);
        assert_eq!(Ok(-128 as i64), decode_long(b3).to_full_result());
    }

    #[test]
    fn test_float() {
        let b1 = &mut BytesMut::with_capacity(0);
        (1.234 as f32).encode(b1);
        assert_eq!(Ok(1.234 as f32), decode_float(b1).to_full_result());
    }

    #[test]
    fn test_double() {
        let b1 = &mut BytesMut::with_capacity(0);
        (1.234 as f64).encode(b1);
        assert_eq!(Ok(1.234 as f64), decode_double(b1).to_full_result());
    }

    #[test]
    fn test_char() {
        let b1 = &mut BytesMut::with_capacity(0);
        '💯'.encode(b1);
        assert_eq!(Ok('💯'), decode_char(b1).to_full_result());
    }

    /// UTC with a precision of milliseconds. For example, 1311704463521
    /// represents the moment 2011-07-26T18:21:03.521Z.
    #[test]
    fn test_timestamp() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        datetime.encode(b1);

        let expected = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(Ok(expected), decode_timestamp(b1).to_full_result());
    }

    #[test]
    fn test_timestamp_pre_unix() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        datetime.encode(b1);

        let expected = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(Ok(expected), decode_timestamp(b1).to_full_result());
    }

    #[test]
    fn test_uuid() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        let u1 = Uuid::from_bytes(&bytes).expect("parse error");
        u1.encode(b1);

        let expected = Uuid::parse_str("0436430c2b02624c2032570501212b57").expect("parse error");
        assert_eq!(Ok(expected), decode_uuid(b1).to_full_result());
    }

    #[test]
    fn test_binary_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4u8, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        Bytes::from(&bytes[..]).encode(b1);

        let expected = [4u8, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        assert_eq!(Ok(Bytes::from(&expected[..])),
                   decode_binary(b1).to_full_result());
    }

    #[test]
    fn test_binary_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4u8; 500];
        Bytes::from(&bytes[..]).encode(b1);

        let expected = [4u8; 500];
        assert_eq!(Ok(Bytes::from(&expected[..])),
                   decode_binary(b1).to_full_result());
    }

    #[test]
    fn test_string_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        ByteStr::from("Hello there").encode(b1);

        assert_eq!(Ok(ByteStr::from("Hello there")),
                   decode_string(b1).to_full_result());
    }

    #[test]
    fn test_string_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let s1 = ByteStr::from(LOREM);
        s1.encode(b1);

        let expected = ByteStr::from(LOREM);
        assert_eq!(Ok(expected), decode_string(b1).to_full_result());
    }

    #[test]
    fn test_symbol_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        Symbol::from("Hello there").encode(b1);

        assert_eq!(Ok(Symbol::from("Hello there")),
                   decode_symbol(b1).to_full_result());
    }

    #[test]
    fn test_symbol_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let s1 = Symbol::from(LOREM);
        s1.encode(b1);

        let expected = Symbol::from(LOREM);
        assert_eq!(Ok(expected), decode_symbol(b1).to_full_result());
    }
}
