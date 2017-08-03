use bytes::{BufMut, Bytes, BytesMut, BigEndian};
use chrono::{DateTime, TimeZone, Utc};
use nom::{be_i8, be_i16, be_i32, be_i64, be_u8, be_u16, be_u32, be_u64, be_f32, be_f64};
use std::{char, i8, str, u8};
use types::{ByteStr, Type};
use uuid::Uuid;

pub fn decode(buf: &mut BytesMut) -> Result<Type, ()> {
    value(buf).to_full_result().map_err(|_| ())
}

named!(value<Type>, alt!(null | bool | ubyte | ushort | uint | ulong | byte | short | int | long | float | double | char | timestamp | uuid | binary | string | symbol));

named!(null<Type>, map_res!(tag!([0x40u8]), |_| Ok::<Type, ()>(Type::Null)));

named!(bool<Type>, alt!(
    map_res!(tag!([0x56, 0x00]), |_| Ok::<Type, ()>(Type::Boolean(false))) |
    map_res!(tag!([0x56, 0x01]), |_| Ok::<Type, ()>(Type::Boolean(true))) |
    map_res!(tag!([0x41]), |_| Result::Ok::<Type, ()>(Type::Boolean(true))) |
    map_res!(tag!([0x42]), |_| Result::Ok::<Type, ()>(Type::Boolean(false)))
));

named!(ubyte<Type>, do_parse!(tag!([0x50u8]) >> byte: be_u8 >> (Type::Ubyte(byte))));
named!(ushort<Type>, do_parse!(tag!([0x60u8]) >> short: be_u16 >> (Type::Ushort(short))));
named!(uint<Type>, alt!(
    do_parse!(tag!([0x70u8]) >> uint: be_u32 >> (Type::Uint(uint))) |
    do_parse!(tag!([0x52u8]) >> uint: be_u8 >> (Type::Uint(uint as u32))) |
    do_parse!(tag!([0x43u8]) >> (Type::Uint(0)))
));
named!(ulong<Type>, alt!(
    do_parse!(tag!([0x80u8]) >> uint: be_u64 >> (Type::Ulong(uint))) |
    do_parse!(tag!([0x53u8]) >> uint: be_u8 >> (Type::Ulong(uint as u64))) |
    do_parse!(tag!([0x44u8]) >> (Type::Ulong(0)))
));

named!(byte<Type>, do_parse!(tag!([0x51u8]) >> byte: be_i8 >> (Type::Byte(byte))));
named!(short<Type>, do_parse!(tag!([0x61u8]) >> short: be_i16 >> (Type::Short(short))));
named!(int<Type>, alt!(
    do_parse!(tag!([0x71u8]) >> int: be_i32 >> (Type::Int(int))) |
    do_parse!(tag!([0x54u8]) >> int: be_i8 >> (Type::Int(int as i32)))
));
named!(long<Type>, alt!(
    do_parse!(tag!([0x81u8]) >> long: be_i64 >> (Type::Long(long))) |
    do_parse!(tag!([0x55u8]) >> long: be_i8 >> (Type::Long(long as i64)))
));

named!(float<Type>, do_parse!(tag!([0x72u8]) >> float: be_f32 >> (Type::Float(float))));
named!(double<Type>, do_parse!(tag!([0x82u8]) >> double: be_f64 >> (Type::Double(double))));

named!(char<Type>, map_opt!(do_parse!(tag!([0x73u8]) >> int: be_u32 >> (int)), |c| char::from_u32(c).map(|c2| Type::Char(c2))));

named!(timestamp<Type>, do_parse!(tag!([0x83u8]) >> timestamp: be_i64 >> (Type::Timestamp(datetime_from_millis(timestamp)))));

named!(uuid<Type>, do_parse!(tag!([0x98u8]) >> uuid: map_res!(take!(16), Uuid::from_bytes) >> (Type::Uuid(uuid))));

named!(binary<Type>, alt!(
    do_parse!(tag!([0xA0u8]) >> bytes: length_bytes!(be_u8) >> (Type::Binary(Bytes::from(bytes)))) |
    do_parse!(tag!([0xB0u8]) >> bytes: length_bytes!(be_u32) >> (Type::Binary(Bytes::from(bytes))))
));

named!(string<Type>, alt!(
    do_parse!(tag!([0xA1u8]) >> string: map_res!(length_bytes!(be_u8), str::from_utf8) >> (Type::String(ByteStr::from(string)))) |
    do_parse!(tag!([0xB1u8]) >> string: map_res!(length_bytes!(be_u32), str::from_utf8) >> (Type::String(ByteStr::from(string))))
));

named!(symbol<Type>, alt!(
    do_parse!(tag!([0xA3u8]) >> string: map_res!(length_bytes!(be_u8), str::from_utf8) >> (Type::Symbol(ByteStr::from(string)))) |
    do_parse!(tag!([0xB3u8]) >> string: map_res!(length_bytes!(be_u32), str::from_utf8) >> (Type::Symbol(ByteStr::from(string))))
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

impl Type {
    /// Encodes `Type` into provided `BytesMut`
    pub fn encode(self: &Type, buf: &mut BytesMut) -> () {
        match *self {
            Type::Null => encode_null(buf),
            Type::Boolean(b) => encode_boolean(b, buf),
            Type::Ubyte(b) => encode_ubyte(b, buf),
            Type::Ushort(s) => encode_ushort(s, buf),
            Type::Uint(i) => encode_uint(i, buf),
            Type::Ulong(l) => encode_ulong(l, buf),
            Type::Byte(b) => encode_byte(b, buf),
            Type::Short(s) => encode_short(s, buf),
            Type::Int(i) => encode_int(i, buf),
            Type::Long(l) => encode_long(l, buf),
            Type::Float(f) => encode_float(f, buf),
            Type::Double(d) => encode_double(d, buf),
            Type::Char(c) => encode_char(c, buf),
            Type::Timestamp(ref t) => encode_timestamp(t, buf),
            Type::Uuid(ref u) => encode_uuid(u, buf),
            Type::Binary(ref b) => encode_binary(b, buf),
            Type::String(ref s) => encode_string(s, buf),
            Type::Symbol(ref s) => encode_symbol(s, buf),
        }
    }
}

fn encode_null(buf: &mut BytesMut) {
    if buf.remaining_mut() < 1 {
        buf.reserve(1);
    }
    buf.put_u8(0x40);
}

fn encode_boolean(b: bool, buf: &mut BytesMut) {
    if buf.remaining_mut() < 1 {
        buf.reserve(1);
    }
    if b { buf.put_u8(0x40) } else { buf.put_u8(0x41) }
}

fn encode_ubyte(b: u8, buf: &mut BytesMut) {
    if buf.remaining_mut() < 2 {
        buf.reserve(2);
    }
    buf.put_u8(0x50);
    buf.put_u8(b);
}

fn encode_ushort(b: u16, buf: &mut BytesMut) {
    if buf.remaining_mut() < 3 {
        buf.reserve(3);
    }
    buf.put_u8(0x60);
    buf.put_u16::<BigEndian>(b);
}

fn encode_uint(i: u32, buf: &mut BytesMut) {
    if buf.remaining_mut() < 5 {
        buf.reserve(5);
    }

    if i == 0 {
        buf.put_u8(0x43)
    } else if i > u8::MAX as u32 {
        buf.put_u8(0x70);
        buf.put_u32::<BigEndian>(i);
    } else {
        buf.put_u8(0x52);
        buf.put_u8(i as u8);
    }
}

fn encode_ulong(i: u64, buf: &mut BytesMut) {
    if buf.remaining_mut() < 9 {
        buf.reserve(9);
    }

    if i == 0 {
        buf.put_u8(0x44)
    } else if i > u8::MAX as u64 {
        buf.put_u8(0x80);
        buf.put_u64::<BigEndian>(i);
    } else {
        buf.put_u8(0x53);
        buf.put_u8(i as u8);
    }
}

fn encode_byte(b: i8, buf: &mut BytesMut) {
    if buf.remaining_mut() < 2 {
        buf.reserve(2);
    }
    buf.put_u8(0x51);
    buf.put_i8(b);
}

fn encode_short(b: i16, buf: &mut BytesMut) {
    if buf.remaining_mut() < 3 {
        buf.reserve(3);
    }
    buf.put_u8(0x61);
    buf.put_i16::<BigEndian>(b);
}

fn encode_int(i: i32, buf: &mut BytesMut) {
    if buf.remaining_mut() < 5 {
        buf.reserve(5);
    }

    if i > i8::MAX as i32 || i < i8::MIN as i32 {
        buf.put_u8(0x71);
        buf.put_i32::<BigEndian>(i);
    } else {
        buf.put_u8(0x54);
        buf.put_i8(i as i8);
    }
}

fn encode_long(i: i64, buf: &mut BytesMut) {
    if buf.remaining_mut() < 9 {
        buf.reserve(9);
    }

    if i > i8::MAX as i64 || i < i8::MIN as i64 {
        buf.put_u8(0x81);
        buf.put_i64::<BigEndian>(i);
    } else {
        buf.put_u8(0x55);
        buf.put_i8(i as i8);
    }
}

fn encode_float(f: f32, buf: &mut BytesMut) {
    if buf.remaining_mut() < 5 {
        buf.reserve(5);
    }

    buf.put_u8(0x72);
    buf.put_f32::<BigEndian>(f);
}

fn encode_double(f: f64, buf: &mut BytesMut) {
    if buf.remaining_mut() < 9 {
        buf.reserve(9);
    }

    buf.put_u8(0x82);
    buf.put_f64::<BigEndian>(f);
}

fn encode_char(c: char, buf: &mut BytesMut) {
    if buf.remaining_mut() < 5 {
        buf.reserve(5);
    }

    buf.put_u8(0x73);
    buf.put_u32::<BigEndian>(c as u32);
}

fn encode_timestamp(datetime: &DateTime<Utc>, buf: &mut BytesMut) {
    if buf.remaining_mut() < 9 {
        buf.reserve(9);
    }
    let timestamp = datetime.timestamp() * 1000 + (datetime.timestamp_subsec_millis() as i64);
    buf.put_u8(0x83);
    buf.put_i64::<BigEndian>(timestamp);
}

fn encode_uuid(u: &Uuid, buf: &mut BytesMut) {
    if buf.remaining_mut() < 17 {
        buf.reserve(17);
    }

    buf.put_u8(0x98);
    buf.put_slice(u.as_bytes());
}

fn encode_binary(b: &Bytes, buf: &mut BytesMut) {
    if buf.remaining_mut() < 5 {
        buf.reserve(5);
    }

    let length = b.len();
    if length > u8::MAX as usize || length < u8::MIN as usize {
        buf.put_u8(0xB0);
        buf.put_u32::<BigEndian>(length as u32);
    } else {
        buf.put_u8(0xA0);
        buf.put_u8(length as u8);
    }
    buf.extend_from_slice(b);
}

fn encode_string(s: &ByteStr, buf: &mut BytesMut) {
    if buf.remaining_mut() < 5 {
        buf.reserve(5);
    }

    let length = s.len();
    if length > u8::MAX as usize || length < u8::MIN as usize {
        buf.put_u8(0xB1);
        buf.put_u32::<BigEndian>(length as u32);
    } else {
        buf.put_u8(0xA1);
        buf.put_u8(length as u8);
    }
    buf.extend_from_slice(s.as_bytes());
}

fn encode_symbol(s: &ByteStr, buf: &mut BytesMut) {
    if buf.remaining_mut() < 5 {
        buf.reserve(5);
    }

    let length = s.len();
    if length > u8::MAX as usize || length < u8::MIN as usize {
        buf.put_u8(0xB3);
        buf.put_u32::<BigEndian>(length as u32);
    } else {
        buf.put_u8(0xA3);
        buf.put_u8(length as u8);
    }
    buf.extend_from_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null() {
        let mut b = BytesMut::with_capacity(0);
        Type::Null.encode(&mut b);
        let t = decode(&mut b);
        assert_eq!(Ok(Type::Null), t);
    }

    #[test]
    fn bool_true() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x41);
        assert_eq!(Ok(Type::Boolean(true)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x01);
        assert_eq!(Ok(Type::Boolean(true)), decode(b2));
    }

    #[test]
    fn bool_false() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x42u8);
        assert_eq!(Ok(Type::Boolean(false)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x00);
        assert_eq!(Ok(Type::Boolean(false)), decode(b2));
    }

    #[test]
    fn ubyte() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Ubyte(255).encode(b1);
        assert_eq!(Ok(Type::Ubyte(255)), decode(b1));
    }

    #[test]
    fn ushort() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Ushort(350).encode(b1);
        assert_eq!(Ok(Type::Ushort(350)), decode(b1));
    }

    #[test]
    fn uint() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Uint(0).encode(b1);
        assert_eq!(Ok(Type::Uint(0)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        Type::Uint(128).encode(b2);
        assert_eq!(Ok(Type::Uint(128)), decode(b2));

        let b3 = &mut BytesMut::with_capacity(0);
        Type::Uint(2147483647).encode(b3);
        assert_eq!(Ok(Type::Uint(2147483647)), decode(b3));
    }

    #[test]
    fn ulong() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Ulong(0).encode(b1);
        assert_eq!(Ok(Type::Ulong(0)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        Type::Ulong(128).encode(b2);
        assert_eq!(Ok(Type::Ulong(128)), decode(b2));

        let b3 = &mut BytesMut::with_capacity(0);
        Type::Ulong(2147483649).encode(b3);
        assert_eq!(Ok(Type::Ulong(2147483649)), decode(b3));
    }

    #[test]
    fn byte() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Byte(-128).encode(b1);
        assert_eq!(Ok(Type::Byte(-128)), decode(b1));
    }

    #[test]
    fn short() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Short(-255).encode(b1);
        assert_eq!(Ok(Type::Short(-255)), decode(b1));
    }

    #[test]
    fn int() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Int(0).encode(b1);
        assert_eq!(Ok(Type::Int(0)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        Type::Int(-50000).encode(b2);
        assert_eq!(Ok(Type::Int(-50000)), decode(b2));

        let b3 = &mut BytesMut::with_capacity(0);
        Type::Int(-128).encode(b3);
        assert_eq!(Ok(Type::Int(-128)), decode(b3));
    }

    #[test]
    fn long() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Ulong(0).encode(b1);
        assert_eq!(Ok(Type::Ulong(0)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(0);
        Type::Long(-2147483647).encode(b2);
        assert_eq!(Ok(Type::Long(-2147483647)), decode(b2));

        let b3 = &mut BytesMut::with_capacity(0);
        Type::Long(-128).encode(b3);
        assert_eq!(Ok(Type::Long(-128)), decode(b3));
    }

    #[test]
    fn float() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Float(1.234).encode(b1);
        assert_eq!(Ok(Type::Float(1.234)), decode(b1));
    }

    #[test]
    fn double() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Double(1.234).encode(b1);
        assert_eq!(Ok(Type::Double(1.234)), decode(b1));
    }

    #[test]
    fn char() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Char('💯').encode(b1);
        assert_eq!(Ok(Type::Char('💯')), decode(b1));
    }

    /// UTC with a precision of milliseconds. For example, 1311704463521
    /// represents the moment 2011-07-26T18:21:03.521Z.
    #[test]
    fn timestamp() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        Type::Timestamp(datetime).encode(b1);

        let expected = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(Ok(Type::Timestamp(expected)), decode(b1));
    }

    #[test]
    fn timestamp_pre_unix() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        Type::Timestamp(datetime).encode(b1);

        let expected = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(Ok(Type::Timestamp(expected)), decode(b1));
    }

    #[test]
    fn uuid() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        let u1 = Uuid::from_bytes(&bytes).expect("parse error");
        Type::Uuid(u1).encode(b1);

        let expected = Type::Uuid(Uuid::parse_str("0436430c2b02624c2032570501212b57").expect("parse error"));
        assert_eq!(Ok(expected), decode(b1));
    }

    #[test]
    fn binary_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4u8, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        Type::Binary(Bytes::from(&bytes[..])).encode(b1);

        let expected = [4u8, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        assert_eq!(Ok(Type::Binary(Bytes::from(&expected[..]))), decode(b1));
    }

    #[test]
    fn binary_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4u8; 500];
        Type::Binary(Bytes::from(&bytes[..])).encode(b1);

        let expected = [4u8; 500];
        assert_eq!(Ok(Type::Binary(Bytes::from(&expected[..]))), decode(b1));
    }

    #[test]
    fn string_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::String(ByteStr::from("Hello there")).encode(b1);

        assert_eq!(Ok(Type::String(ByteStr::from("Hello there"))), decode(b1));
    }

    #[test]
    fn string_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let s1 = ByteStr::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        Type::String(s1).encode(b1);

        let expected = ByteStr::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        assert_eq!(Ok(Type::String(expected)), decode(b1));
    }

    #[test]
    fn symbol_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        Type::Symbol(ByteStr::from("Hello there")).encode(b1);

        assert_eq!(Ok(Type::Symbol(ByteStr::from("Hello there"))), decode(b1));
    }

    #[test]
    fn symbol_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let s1 = ByteStr::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        Type::Symbol(s1).encode(b1);

        let expected = ByteStr::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        assert_eq!(Ok(Type::Symbol(expected)), decode(b1));
    }
}