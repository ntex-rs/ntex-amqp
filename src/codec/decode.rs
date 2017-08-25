use std::{char, str, u8};

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use codec::Decode;
use framing::{AmqpFrame, Frame, AMQP_TYPE, HEADER_LEN};
use nom::{ErrorKind, IResult, be_f32, be_f64, be_i16, be_i32, be_i64, be_i8, be_u16, be_u32, be_u64, be_u8};
use types::{ByteStr, Null, Symbol, Variant};
use uuid::Uuid;

pub const INVALID_FRAME: u32 = 0x0001;

macro_rules! error_if (
  ($i:expr, $cond:expr, $code:expr) => (
    {
      if $cond {
        IResult::Error(error_code!(ErrorKind::Custom($code)))
      } else {
        IResult::Done($i, ())
      }
    }
  );
  ($i:expr, $cond:expr, $err:expr) => (
    error!($i, $cond, $err);
  );
);

impl Decode for Null {
    named!(decode<Null>, map!(tag!([0x40u8]), |_| Null));
}

impl Decode for bool {
    named!(decode<bool>, alt!(
        map_res!(tag!([0x56, 0x00]), |_| Ok::<bool, ()>(false)) |
        map_res!(tag!([0x56, 0x01]), |_| Ok::<bool, ()>(true)) |
        map_res!(tag!([0x41]), |_| Result::Ok::<bool, ()>(true)) |
        map_res!(tag!([0x42]), |_| Result::Ok::<bool, ()>(false))
    ));
}

impl Decode for u8 {
    named!(decode<u8>, do_parse!(tag!([0x50u8]) >> byte: be_u8 >> (byte)));
}

impl Decode for u16 {
    named!(decode<u16>, do_parse!(tag!([0x60u8]) >> short: be_u16 >> (short)));
}

impl Decode for u32 {
    named!(decode<u32>, alt!(
        do_parse!(tag!([0x70u8]) >> uint: be_u32 >> (uint)) |
        do_parse!(tag!([0x52u8]) >> uint: be_u8 >> (uint as u32)) |
        do_parse!(tag!([0x43u8]) >> (0))
    ));
}

impl Decode for u64 {
    named!(decode<u64>, alt!(
        do_parse!(tag!([0x80u8]) >> uint: be_u64 >> (uint)) |
        do_parse!(tag!([0x53u8]) >> uint: be_u8 >> (uint as u64)) |
        do_parse!(tag!([0x44u8]) >> (0))
    ));
}

impl Decode for i8 {
    named!(decode<i8>, do_parse!(tag!([0x51u8]) >> byte: be_i8 >> (byte)));
}

impl Decode for i16 {
    named!(decode<i16>, do_parse!(tag!([0x61u8]) >> short: be_i16 >> (short)));
}

impl Decode for i32 {
    named!(decode<i32>, alt!(
        do_parse!(tag!([0x71u8]) >> int: be_i32 >> (int)) |
        do_parse!(tag!([0x54u8]) >> int: be_i8 >> (int as i32))
    ));
}

impl Decode for i64 {
    named!(decode<i64>, alt!(
        do_parse!(tag!([0x81u8]) >> long: be_i64 >> (long)) |
        do_parse!(tag!([0x55u8]) >> long: be_i8 >> (long as i64))
    ));
}

impl Decode for f32 {
    named!(decode<f32>, do_parse!(tag!([0x72u8]) >> float: be_f32 >> (float)));
}

impl Decode for f64 {
    named!(decode<f64>, do_parse!(tag!([0x82u8]) >> double: be_f64 >> (double)));
}

impl Decode for char {
    named!(decode<char>, map_opt!(do_parse!(tag!([0x73u8]) >> int: be_u32 >> (int)), |c| char::from_u32(c)));
}

impl Decode for DateTime<Utc> {
    named!(decode<DateTime<Utc>>, do_parse!(tag!([0x83u8]) >> timestamp: be_i64 >> (datetime_from_millis(timestamp))));
}

impl Decode for Uuid {
    named!(decode<Uuid>, do_parse!(tag!([0x98u8]) >> uuid: map_res!(take!(16), Uuid::from_bytes) >> (uuid)));
}

impl Decode for Bytes {
    named!(decode<Bytes>, alt!(
        do_parse!(tag!([0xA0u8]) >> bytes: length_bytes!(be_u8) >> (Bytes::from(bytes))) |
        do_parse!(tag!([0xB0u8]) >> bytes: length_bytes!(be_u32) >> (Bytes::from(bytes)))
    ));
}

impl Decode for ByteStr {
    named!(decode<ByteStr>, alt!(
        do_parse!(tag!([0xA1u8]) >> string: map_res!(length_bytes!(be_u8), str::from_utf8) >> (ByteStr::from(string))) |
        do_parse!(tag!([0xB1u8]) >> string: map_res!(length_bytes!(be_u32), str::from_utf8) >> (ByteStr::from(string)))
    ));
}

impl Decode for Symbol {
    named!(decode<Symbol>, alt!(
        do_parse!(tag!([0xA3u8]) >> string: map_res!(length_bytes!(be_u8), str::from_utf8) >> (Symbol::from(string))) |
        do_parse!(tag!([0xB3u8]) >> string: map_res!(length_bytes!(be_u32), str::from_utf8) >> (Symbol::from(string)))
    ));
}

impl Decode for Variant {
    named!(decode<Variant>, alt!(
        map!(Null::decode, |_| Variant::Null) |
        map!(bool::decode, Variant::Boolean) |
        map!(u8::decode, Variant::Ubyte) |
        map!(u16::decode, Variant::Ushort) |
        map!(u32::decode, Variant::Uint) |
        map!(u64::decode, Variant::Ulong) |
        map!(i8::decode, Variant::Byte) |
        map!(i16::decode, Variant::Short) |
        map!(i32::decode, Variant::Int) |
        map!(i64::decode, Variant::Long) |
        map!(f32::decode, Variant::Float) |
        map!(f64::decode, Variant::Double) |
        map!(char::decode, Variant::Char) |
        map!(DateTime::<Utc>::decode, Variant::Timestamp) |
        map!(Uuid::decode, Variant::Uuid) |
        map!(Bytes::decode, Variant::Binary) |
        map!(ByteStr::decode, Variant::String) |
        map!(Symbol::decode, Variant::Symbol)
    ));
}

impl<T: Decode> Decode for Option<T> {
    named!(decode<Option<T>>, alt!(
        map!(T::decode, |v| Some(v)) |
        map!(Null::decode, |_| None)
    ));
}

impl Decode for Frame {
    named!(decode<Frame>,
        do_parse!(
            size: be_u32 >>
            error_if!(size < HEADER_LEN as u32, INVALID_FRAME) >>

            doff: be_u8 >>
            error_if!(doff < 2, INVALID_FRAME) >>

            frame: alt!(
                // AMQP Frame
                do_parse!(
                    typ:  tag!([AMQP_TYPE]) >>  // Amqp frame Type

                    channel_id: be_u16 >>
                    extended_header: map!(take!(doff as u32 * 4 - 8), Bytes::from)  >>
                    body: map!(take!(size - doff as u32 * 4), Bytes::from) >>

                    (Frame::Amqp(AmqpFrame::new(channel_id, body)))
                )
            ) >>
            (frame)
        )
    );
}

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

    macro_rules! decode_tests {
        ($($name:ident: $kind:ident, $test:expr, $expected:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let b1 = &mut BytesMut::with_capacity(0);
                ($test).encode(b1);
                assert_eq!(Ok($expected), $kind::decode(b1).to_full_result());
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

        uuid: Uuid, Uuid::from_bytes(&[4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87]).expect("parse error"),
            Uuid::parse_str("0436430c2b02624c2032570501212b57").expect("parse error"),

        binary_short: Bytes, Bytes::from(&[4u8, 5u8][..]), Bytes::from(&[4u8, 5u8][..]),
        binary_long: Bytes, Bytes::from(&[4u8; 500][..]), Bytes::from(&[4u8; 500][..]),

        string_short: ByteStr, ByteStr::from("Hello there"), ByteStr::from("Hello there"),
        string_long: ByteStr, ByteStr::from(LOREM), ByteStr::from(LOREM),

        symbol_short: Symbol, Symbol::from("Hello there"), Symbol::from("Hello there"),
        symbol_long: Symbol, Symbol::from(LOREM), Symbol::from(LOREM),

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

        variant_float: Variant, Variant::Float(1.234_f32), Variant::Float(1.234_f32),
        variant_double: Variant, Variant::Double(1.234_f64), Variant::Double(1.234_f64),

        variant_char: Variant, Variant::Char('💯'), Variant::Char('💯'),

        variant_uuid: Variant, Variant::Uuid(Uuid::from_bytes(&[4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87]).expect("parse error")),
            Variant::Uuid(Uuid::parse_str("0436430c2b02624c2032570501212b57").expect("parse error")),

        variant_binary_short: Variant, Variant::Binary(Bytes::from(&[4u8, 5u8][..])), Variant::Binary(Bytes::from(&[4u8, 5u8][..])),
        variant_binary_long: Variant, Variant::Binary(Bytes::from(&[4u8; 500][..])), Variant::Binary(Bytes::from(&[4u8; 500][..])),

        variant_string_short: Variant, Variant::String(ByteStr::from("Hello there")), Variant::String(ByteStr::from("Hello there")),
        variant_string_long: Variant, Variant::String(ByteStr::from(LOREM)), Variant::String(ByteStr::from(LOREM)),

        variant_symbol_short: Variant, Variant::Symbol(Symbol::from("Hello there")), Variant::Symbol(Symbol::from("Hello there")),
        variant_symbol_long: Variant, Variant::Symbol(Symbol::from(LOREM)), Variant::Symbol(Symbol::from(LOREM)),
   }

    #[test]
    fn test_null() {
        let mut b = BytesMut::with_capacity(0);
        Null.encode(&mut b);
        let t = Null::decode(&mut b).to_full_result();
        assert_eq!(Ok(Null), t);
    }

    #[test]
    fn test_bool_true() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x41);
        assert_eq!(Ok(true), bool::decode(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x01);
        assert_eq!(Ok(true), bool::decode(b2).to_full_result());
    }

    #[test]
    fn test_bool_false() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x42u8);
        assert_eq!(Ok(false), bool::decode(b1).to_full_result());

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x00);
        assert_eq!(Ok(false), bool::decode(b2).to_full_result());
    }

    /// UTC with a precision of milliseconds. For example, 1311704463521
    /// represents the moment 2011-07-26T18:21:03.521Z.
    #[test]
    fn test_timestamp() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        datetime.encode(b1);

        let expected = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(Ok(expected), DateTime::<Utc>::decode(b1).to_full_result());
    }

    #[test]
    fn test_timestamp_pre_unix() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        datetime.encode(b1);

        let expected = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(Ok(expected), DateTime::<Utc>::decode(b1).to_full_result());
    }

    #[test]
    fn variant_null() {
        let mut b = BytesMut::with_capacity(0);
        Variant::Null.encode(&mut b);
        let t = Variant::decode(&mut b).to_full_result();
        assert_eq!(Ok(Variant::Null), t);
    }

    #[test]
    fn variant_bool_true() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x41);
        assert_eq!(
            Ok(Variant::Boolean(true)),
            Variant::decode(b1).to_full_result()
        );

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x01);
        assert_eq!(
            Ok(Variant::Boolean(true)),
            Variant::decode(b2).to_full_result()
        );
    }

    #[test]
    fn variant_bool_false() {
        let b1 = &mut BytesMut::with_capacity(0);
        b1.put_u8(0x42u8);
        assert_eq!(
            Ok(Variant::Boolean(false)),
            Variant::decode(b1).to_full_result()
        );

        let b2 = &mut BytesMut::with_capacity(0);
        b2.put_u8(0x56);
        b2.put_u8(0x00);
        assert_eq!(
            Ok(Variant::Boolean(false)),
            Variant::decode(b2).to_full_result()
        );
    }

    /// UTC with a precision of milliseconds. For example, 1311704463521
    /// represents the moment 2011-07-26T18:21:03.521Z.
    #[test]
    fn variant_timestamp() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        Variant::Timestamp(datetime).encode(b1);

        let expected = Utc.ymd(2011, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(
            Ok(Variant::Timestamp(expected)),
            Variant::decode(b1).to_full_result()
        );
    }

    #[test]
    fn variant_timestamp_pre_unix() {
        let b1 = &mut BytesMut::with_capacity(0);
        let datetime = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        Variant::Timestamp(datetime).encode(b1);

        let expected = Utc.ymd(1968, 7, 26).and_hms_milli(18, 21, 3, 521);
        assert_eq!(
            Ok(Variant::Timestamp(expected)),
            Variant::decode(b1).to_full_result()
        );
    }

    #[test]
    fn option_i8() {
        let b1 = &mut BytesMut::with_capacity(0);
        Some(42i8).encode(b1);

        assert_eq!(
            Ok(Some(42)),
            Option::<i8>::decode(b1).to_full_result()
        );

        let b2 = &mut BytesMut::with_capacity(0);
        let o1: Option<i8> = None;
        o1.encode(b2);

        assert_eq!(
            Ok(None),
            Option::<i8>::decode(b2).to_full_result()
        );
    }

    #[test]
    fn option_string() {
        let b1 = &mut BytesMut::with_capacity(0);
        Some(ByteStr::from("hello")).encode(b1);

        assert_eq!(
            Ok(Some(ByteStr::from("hello"))),
            Option::<ByteStr>::decode(b1).to_full_result()
        );

        let b2 = &mut BytesMut::with_capacity(0);
        let o1: Option<ByteStr> = None;
        o1.encode(b2);

        assert_eq!(
            Ok(None),
            Option::<ByteStr>::decode(b2).to_full_result()
        );
    }
}
