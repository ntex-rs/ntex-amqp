use codec::decode::primitive::*;
use types::Variant;

named!(pub variant<Variant>, alt!(
    map!(null, |_| Variant::Null) |
    map!(bool, Variant::Boolean) |
    map!(ubyte, Variant::Ubyte) |
    map!(ushort, Variant::Ushort) |
    map!(uint, Variant::Uint) |
    map!(ulong, Variant::Ulong) |
    map!(byte, Variant::Byte) |
    map!(short, Variant::Short) |
    map!(int, Variant::Int) |
    map!(long, Variant::Long) |
    map!(float, Variant::Float) |
    map!(double, Variant::Double) |
    map!(char, Variant::Char) |
    map!(timestamp, Variant::Timestamp) |
    map!(uuid, Variant::Uuid) |
    map!(binary, Variant::Binary) |
    map!(string, Variant::String) |
    map!(symbol, Variant::Symbol)
));

#[cfg(test)]
mod tests {
    use super::*;
    use std::{char, str};

    use bytes::{BufMut, BytesMut};
    use codec::Encodable;
    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use types::{ByteStr, Symbol};
    use uuid::Uuid;

    #[test]
    fn null() {
        let mut b = BytesMut::with_capacity(0);
        Variant::Null.encode(&mut b);
        let t = variant(&mut b).to_full_result();
        assert_eq!(Ok(Variant::Null), t);
    }

    #[test]
    fn bool_true() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x41);
        assert_eq!(Ok(Variant::Boolean(true)), variant(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x01);
        assert_eq!(Ok(Variant::Boolean(true)), variant(b2).to_full_result());
    }

    #[test]
    fn bool_false() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x42u8);
        assert_eq!(Ok(Variant::Boolean(false)), variant(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x00);
        assert_eq!(Ok(Variant::Boolean(false)), variant(b2).to_full_result());
    }

    #[test]
    fn ubyte() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Ubyte(255).encode(b1);
        assert_eq!(Ok(Variant::Ubyte(255)), variant(b1).to_full_result());
    }

    #[test]
    fn ushort() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Ushort(350).encode(b1);
        assert_eq!(Ok(Variant::Ushort(350)), variant(b1).to_full_result());
    }

    #[test]
    fn uint() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Uint(0).encode(b1);
        assert_eq!(Ok(Variant::Uint(0)), variant(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        Variant::Uint(128).encode(b2);
        assert_eq!(Ok(Variant::Uint(128)), variant(b2).to_full_result());

        let b3 = &mut BytesMut::with_capacity(0);
        Variant::Uint(2147483647).encode(b3);
        assert_eq!(Ok(Variant::Uint(2147483647)), variant(b3).to_full_result());
    }

    #[test]
    fn ulong() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Ulong(0).encode(b1);
        assert_eq!(Ok(Variant::Ulong(0)), variant(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        Variant::Ulong(128).encode(b2);
        assert_eq!(Ok(Variant::Ulong(128)), variant(b2).to_full_result());

        let b3 = &mut BytesMut::with_capacity(0);
        Variant::Ulong(2147483649).encode(b3);
        assert_eq!(Ok(Variant::Ulong(2147483649)), variant(b3).to_full_result());
    }

    #[test]
    fn byte() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Byte(-128).encode(b1);
        assert_eq!(Ok(Variant::Byte(-128)), variant(b1).to_full_result());
    }

    #[test]
    fn short() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Short(-255).encode(b1);
        assert_eq!(Ok(Variant::Short(-255)), variant(b1).to_full_result());
    }

    #[test]
    fn int() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Int(0).encode(b1);
        assert_eq!(Ok(Variant::Int(0)), variant(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        Variant::Int(-50000).encode(b2);
        assert_eq!(Ok(Variant::Int(-50000)), variant(b2).to_full_result());

        let b3 = &mut BytesMut::with_capacity(0);
        Variant::Int(-128).encode(b3);
        assert_eq!(Ok(Variant::Int(-128)), variant(b3).to_full_result());
    }

    #[test]
    fn long() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Ulong(0).encode(b1);
        assert_eq!(Ok(Variant::Ulong(0)), variant(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        Variant::Long(-2147483647).encode(b2);
        assert_eq!(Ok(Variant::Long(-2147483647)), variant(b2).to_full_result());

        let b3 = &mut BytesMut::with_capacity(0);
        Variant::Long(-128).encode(b3);
        assert_eq!(Ok(Variant::Long(-128)), variant(b3).to_full_result());
    }

    #[test]
    fn float() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Float(1.234).encode(b1);
        assert_eq!(Ok(Variant::Float(1.234)), variant(b1).to_full_result());
    }

    #[test]
    fn double() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Double(1.234).encode(b1);
        assert_eq!(Ok(Variant::Double(1.234)), variant(b1).to_full_result());
    }

    #[test]
    fn char() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Char('💯').encode(b1);
        assert_eq!(Ok(Variant::Char('💯')), variant(b1).to_full_result());
    }

    /// UTC with a precision of milliseconds. For example, 1311704463521
    /// represents the moment 2011-07-26T18:21:03.521Z.
    #[test]
    fn timestamp() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        Variant::Timestamp(datetime).encode(b1);

        let expected = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(Ok(Variant::Timestamp(expected)),
                   variant(b1).to_full_result());
    }

    #[test]
    fn timestamp_pre_unix() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        Variant::Timestamp(datetime).encode(b1);

        let expected = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(Ok(Variant::Timestamp(expected)),
                   variant(b1).to_full_result());
    }

    #[test]
    fn uuid() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        let u1 = Uuid::from_bytes(&bytes).expect("parse error");
        Variant::Uuid(u1).encode(b1);

        let expected = Variant::Uuid(Uuid::parse_str("0436430c2b02624c2032570501212b57").expect("parse error"));
        assert_eq!(Ok(expected), variant(b1).to_full_result());
    }

    #[test]
    fn binary_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4u8, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        Variant::Binary(Bytes::from(&bytes[..])).encode(b1);

        let expected = [4u8, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
        assert_eq!(Ok(Variant::Binary(Bytes::from(&expected[..]))),
                   variant(b1).to_full_result());
    }

    #[test]
    fn binary_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let bytes = [4u8; 500];
        Variant::Binary(Bytes::from(&bytes[..])).encode(b1);

        let expected = [4u8; 500];
        assert_eq!(Ok(Variant::Binary(Bytes::from(&expected[..]))),
                   variant(b1).to_full_result());
    }

    #[test]
    fn string_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::String(ByteStr::from("Hello there")).encode(b1);

        assert_eq!(Ok(Variant::String(ByteStr::from("Hello there"))),
                   variant(b1).to_full_result());
    }

    #[test]
    fn string_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let s1 = ByteStr::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        Variant::String(s1).encode(b1);

        let expected = ByteStr::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        assert_eq!(Ok(Variant::String(expected)), variant(b1).to_full_result());
    }

    #[test]
    fn symbol_short() {
        let b1 = &mut BytesMut::with_capacity(0);
        Variant::Symbol(Symbol::from("Hello there")).encode(b1);

        assert_eq!(Ok(Variant::Symbol(Symbol::from("Hello there"))),
                   variant(b1).to_full_result());
    }

    #[test]
    fn symbol_long() {
        let b1 = &mut BytesMut::with_capacity(0);
        let s1 = Symbol::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        Variant::Symbol(s1).encode(b1);

        let expected = Symbol::from("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec accumsan iaculis ipsum sed convallis. Phasellus consectetur justo et odio maximus, vel vehicula sapien venenatis. Nunc ac viverra risus. Pellentesque elementum, mauris et viverra ultricies, lacus erat varius nulla, eget maximus nisl sed.",);
        assert_eq!(Ok(Variant::Symbol(expected)), variant(b1).to_full_result());
    }
}