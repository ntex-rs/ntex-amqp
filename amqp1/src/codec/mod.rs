use bytes::BytesMut;

mod decode;
mod encode;

pub trait Encode {
    fn encoded_size(&self) -> usize;
    fn encode(&self, buf: &mut BytesMut) -> ();
}

pub use self::decode::primitive::{decode_null, decode_ubyte, decode_ushort, decode_uint, decode_ulong, decode_byte, decode_short, decode_int, decode_long, decode_float,
                                  decode_double, decode_timestamp, decode_uuid, decode_binary, decode_string, decode_symbol};

pub use self::decode::framing::decode_frame;
pub use self::decode::variant::decode_variant;
