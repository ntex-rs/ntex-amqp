use std::{char, str, u8};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, TimeZone, Utc};
use nom::{be_i8, be_i16, be_i32, be_i64, be_u8, be_u16, be_u32, be_u64, be_f32, be_f64};
use types::{ByteStr, Symbol, Variant};
use uuid::Uuid;

pub fn decode(buf: &mut BytesMut) -> Result<Variant, ()> {
    value(buf).to_full_result().map_err(|_| ())
}

named!(value<Variant>, alt!(null | bool | ubyte | ushort | uint | ulong | byte | short | int | long | float | double | char | timestamp | uuid | binary | string | symbol));

named!(null<Variant>, map_res!(tag!([0x40u8]), |_| Ok::<Variant, ()>(Variant::Null)));

named!(bool<Variant>, alt!(
    map_res!(tag!([0x56, 0x00]), |_| Ok::<Variant, ()>(Variant::Boolean(false))) |
    map_res!(tag!([0x56, 0x01]), |_| Ok::<Variant, ()>(Variant::Boolean(true))) |
    map_res!(tag!([0x41]), |_| Result::Ok::<Variant, ()>(Variant::Boolean(true))) |
    map_res!(tag!([0x42]), |_| Result::Ok::<Variant, ()>(Variant::Boolean(false)))
));

named!(ubyte<Variant>, do_parse!(tag!([0x50u8]) >> byte: be_u8 >> (Variant::Ubyte(byte))));
named!(ushort<Variant>, do_parse!(tag!([0x60u8]) >> short: be_u16 >> (Variant::Ushort(short))));
named!(uint<Variant>, alt!(
    do_parse!(tag!([0x70u8]) >> uint: be_u32 >> (Variant::Uint(uint))) |
    do_parse!(tag!([0x52u8]) >> uint: be_u8 >> (Variant::Uint(uint as u32))) |
    do_parse!(tag!([0x43u8]) >> (Variant::Uint(0)))
));
named!(ulong<Variant>, alt!(
    do_parse!(tag!([0x80u8]) >> uint: be_u64 >> (Variant::Ulong(uint))) |
    do_parse!(tag!([0x53u8]) >> uint: be_u8 >> (Variant::Ulong(uint as u64))) |
    do_parse!(tag!([0x44u8]) >> (Variant::Ulong(0)))
));

named!(byte<Variant>, do_parse!(tag!([0x51u8]) >> byte: be_i8 >> (Variant::Byte(byte))));
named!(short<Variant>, do_parse!(tag!([0x61u8]) >> short: be_i16 >> (Variant::Short(short))));
named!(int<Variant>, alt!(
    do_parse!(tag!([0x71u8]) >> int: be_i32 >> (Variant::Int(int))) |
    do_parse!(tag!([0x54u8]) >> int: be_i8 >> (Variant::Int(int as i32)))
));
named!(long<Variant>, alt!(
    do_parse!(tag!([0x81u8]) >> long: be_i64 >> (Variant::Long(long))) |
    do_parse!(tag!([0x55u8]) >> long: be_i8 >> (Variant::Long(long as i64)))
));

named!(float<Variant>, do_parse!(tag!([0x72u8]) >> float: be_f32 >> (Variant::Float(float))));
named!(double<Variant>, do_parse!(tag!([0x82u8]) >> double: be_f64 >> (Variant::Double(double))));

named!(char<Variant>, map_opt!(do_parse!(tag!([0x73u8]) >> int: be_u32 >> (int)), |c| char::from_u32(c).map(|c2| Variant::Char(c2))));

named!(timestamp<Variant>, do_parse!(tag!([0x83u8]) >> timestamp: be_i64 >> (Variant::Timestamp(datetime_from_millis(timestamp)))));

named!(uuid<Variant>, do_parse!(tag!([0x98u8]) >> uuid: map_res!(take!(16), Uuid::from_bytes) >> (Variant::Uuid(uuid))));

named!(binary<Variant>, alt!(
    do_parse!(tag!([0xA0u8]) >> bytes: length_bytes!(be_u8) >> (Variant::Binary(Bytes::from(bytes)))) |
    do_parse!(tag!([0xB0u8]) >> bytes: length_bytes!(be_u32) >> (Variant::Binary(Bytes::from(bytes))))
));

named!(string<Variant>, alt!(
    do_parse!(tag!([0xA1u8]) >> string: map_res!(length_bytes!(be_u8), str::from_utf8) >> (Variant::String(ByteStr::from(string)))) |
    do_parse!(tag!([0xB1u8]) >> string: map_res!(length_bytes!(be_u32), str::from_utf8) >> (Variant::String(ByteStr::from(string))))
));

named!(symbol<Variant>, alt!(
    do_parse!(tag!([0xA3u8]) >> string: map_res!(length_bytes!(be_u8), str::from_utf8) >> (Variant::Symbol(Symbol::from(string)))) |
    do_parse!(tag!([0xB3u8]) >> string: map_res!(length_bytes!(be_u32), str::from_utf8) >> (Variant::Symbol(Symbol::from(string))))
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
    use bytes::BufMut;
    use codec::Encodable;

    #[test]
    fn null() {
        let mut b = BytesMut::with_capacity(0);
        Variant::Null.encode(&mut b);
        let t = decode(&mut b);
        assert_eq!(Ok(Variant::Null), t);
    }

    #[test]
    fn bool_true() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x41);
        assert_eq!(Ok(Variant::Boolean(true)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x01);
        assert_eq!(Ok(Variant::Boolean(true)), decode(b2));
    }

    #[test]
    fn bool_false() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x42u8);
        assert_eq!(Ok(Variant::Boolean(false)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x00);
        assert_eq!(Ok(Variant::Boolean(false)), decode(b2));
    }

    #[test]
    fn ubyte() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Ubyte(255).encode(b1);
        assert_eq!(Ok(Variant::Ubyte(255)), decode(b1));
    }

    #[test]
    fn ushort() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Ushort(350).encode(b1);
        assert_eq!(Ok(Variant::Ushort(350)), decode(b1));
    }

    #[test]
    fn uint() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Uint(0).encode(b1);
        assert_eq!(Ok(Variant::Uint(0)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        Variant::Uint(128).encode(b2);
        assert_eq!(Ok(Variant::Uint(128)), decode(b2));

        let b3 = &mut BytesMut::with_capacity(0);
        Variant::Uint(2147483647).encode(b3);
        assert_eq!(Ok(Variant::Uint(2147483647)), decode(b3));
    }

    #[test]
    fn ulong() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Ulong(0).encode(b1);
        assert_eq!(Ok(Variant::Ulong(0)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        Variant::Ulong(128).encode(b2);
        assert_eq!(Ok(Variant::Ulong(128)), decode(b2));

        let b3 = &mut BytesMut::with_capacity(0);
        Variant::Ulong(2147483649).encode(b3);
        assert_eq!(Ok(Variant::Ulong(2147483649)), decode(b3));
    }

    #[test]
    fn byte() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Byte(-128).encode(b1);
        assert_eq!(Ok(Variant::Byte(-128)), decode(b1));
    }

    #[test]
    fn short() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Short(-255).encode(b1);
        assert_eq!(Ok(Variant::Short(-255)), decode(b1));
    }

    #[test]
    fn int() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Int(0).encode(b1);
        assert_eq!(Ok(Variant::Int(0)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        Variant::Int(-50000).encode(b2);
        assert_eq!(Ok(Variant::Int(-50000)), decode(b2));

        let b3 = &mut BytesMut::with_capacity(0);
        Variant::Int(-128).encode(b3);
        assert_eq!(Ok(Variant::Int(-128)), decode(b3));
    }

    #[test]
    fn long() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Ulong(0).encode(b1);
        assert_eq!(Ok(Variant::Ulong(0)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        Variant::Long(-2147483647).encode(b2);
        assert_eq!(Ok(Variant::Long(-2147483647)), decode(b2));

        let b3 = &mut BytesMut::with_capacity(0);
        Variant::Long(-128).encode(b3);
        assert_eq!(Ok(Variant::Long(-128)), decode(b3));
    }

    #[test]
    fn float() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Float(1.234).encode(b1);
        assert_eq!(Ok(Variant::Float(1.234)), decode(b1));
    }

    #[test]
    fn double() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Double(1.234).encode(b1);
        assert_eq!(Ok(Variant::Double(1.234)), decode(b1));
    }

    #[test]
    fn char() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Char('💯').encode(b1);
        assert_eq!(Ok(Variant::Char('💯')), decode(b1));
    }

    /// UTC with a precision of milliseconds. For example, 1311704463521
    /// represents the moment 2011-07-26T18:21:03.521Z.
    #[test]
    fn timestamp() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        Variant::Timestamp(datetime).encode(b1);

        let expected = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(Ok(Variant::Timestamp(expected)), decode(b1));
    }

    #[test]
    fn timestamp_pre_unix() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        Variant::Timestamp(datetime).encode(b1);

        let expected = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(Ok(Variant::Timestamp(expected)), decode(b1));
    }

    #[test]
    fn uuid() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        let u1 = Uuid::from_bytes(&bytes).expect("parse error");
        Variant::Uuid(u1).encode(b1);

        let expected = Variant::Uuid(Uuid::parse_str("0436430c2b02624c2032570501212b57").expect("parse error"));
        assert_eq!(Ok(expected), decode(b1));
    }

    #[test]
    fn binary_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4u8, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        Variant::Binary(Bytes::from(&bytes[..])).encode(b1);

        let expected = [4u8, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        assert_eq!(Ok(Variant::Binary(Bytes::from(&expected[..]))), decode(b1));
    }

    #[test]
    fn binary_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4u8; 500];
        Variant::Binary(Bytes::from(&bytes[..])).encode(b1);

        let expected = [4u8; 500];
        assert_eq!(Ok(Variant::Binary(Bytes::from(&expected[..]))), decode(b1));
    }

    #[test]
    fn string_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::String(ByteStr::from("Hello there")).encode(b1);

        assert_eq!(Ok(Variant::String(ByteStr::from("Hello there"))), decode(b1));
    }

    #[test]
    fn string_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let s1 = ByteStr::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        Variant::String(s1).encode(b1);

        let expected = ByteStr::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        assert_eq!(Ok(Variant::String(expected)), decode(b1));
    }

    #[test]
    fn symbol_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Symbol(Symbol::from("Hello there")).encode(b1);

        assert_eq!(Ok(Variant::Symbol(Symbol::from("Hello there"))), decode(b1));
    }

    #[test]
    fn symbol_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let s1 = Symbol::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        Variant::Symbol(s1).encode(b1);

        let expected = Symbol::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        assert_eq!(Ok(Variant::Symbol(expected)), decode(b1));
    }
}