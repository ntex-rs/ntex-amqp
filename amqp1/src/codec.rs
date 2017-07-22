use bytes::{BufMut, BytesMut, BigEndian};
use nom::{be_u8, be_u16, be_u32, be_u64};
use std::u8;
use types::Type;

pub fn decode(buf: &mut BytesMut) -> Result<Type, ()> {
    value(buf).to_full_result().map_err(|_| ())
}

named!(value<Type>, alt!(null | bool | ubyte | ushort | uint | ulong));

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

pub fn encode(t: Type, buf: &mut BytesMut) -> () {
    match t {
        Type::Null => encode_null(buf),
        Type::Boolean(b) => encode_boolean(b, buf),
        Type::Ubyte(b) => encode_ubyte(b, buf),
        Type::Ushort(s) => encode_ushort(s, buf),
        Type::Uint(i) => encode_uint(i, buf),
        Type::Ulong(l) => encode_ulong(l, buf),
        _ => (),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null() {
        let mut b = BytesMut::with_capacity(0);
        encode(Type::Null, &mut b);
        let t = decode(&mut b);
        assert_eq!(Ok(Type::Null), t);
    }

    #[test]
    fn bool_true() {
        let b1 = &mut BytesMut::with_capacity(2);
        b1.put_u8(0x41);
        assert_eq!(Ok(Type::Boolean(true)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(2);
        b2.put_u8(0x56);
        b2.put_u8(0x01);
        assert_eq!(Ok(Type::Boolean(true)), decode(b2));
    }

    #[test]
    fn bool_false() {
        let b1 = &mut BytesMut::with_capacity(2);
        b1.put_u8(0x42u8);
        assert_eq!(Ok(Type::Boolean(false)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(2);
        b2.put_u8(0x56);
        b2.put_u8(0x00);
        assert_eq!(Ok(Type::Boolean(false)), decode(b2));
    }

    #[test]
    fn ubyte() {
        let b1 = &mut BytesMut::with_capacity(2);
        encode(Type::Ubyte(255), b1);
        assert_eq!(Ok(Type::Ubyte(255)), decode(b1));
    }

    #[test]
    fn ushort() {
        let b1 = &mut BytesMut::with_capacity(2);
        encode(Type::Ushort(350), b1);
        assert_eq!(Ok(Type::Ushort(350)), decode(b1));
    }

    #[test]
    fn uint() {
        let b1 = &mut BytesMut::with_capacity(2);
        encode(Type::Uint(0), b1);
        assert_eq!(Ok(Type::Uint(0)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(2);
        encode(Type::Uint(128), b2);
        assert_eq!(Ok(Type::Uint(128)), decode(b2));

        let b3 = &mut BytesMut::with_capacity(2);
        encode(Type::Uint(2147483647), b3);
        assert_eq!(Ok(Type::Uint(2147483647)), decode(b3));
    }

    #[test]
    fn ulong() {
        let b1 = &mut BytesMut::with_capacity(2);
        encode(Type::Ulong(0), b1);
        assert_eq!(Ok(Type::Ulong(0)), decode(b1));

        let b2 = &mut BytesMut::with_capacity(2);
        encode(Type::Ulong(128), b2);
        assert_eq!(Ok(Type::Ulong(128)), decode(b2));

        let b3 = &mut BytesMut::with_capacity(2);
        encode(Type::Ulong(2147483649), b3);
        assert_eq!(Ok(Type::Ulong(2147483649)), decode(b3));
    }
}