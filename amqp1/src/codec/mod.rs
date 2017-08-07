use bytes::BytesMut;

mod decode;
mod encode;

pub use self::decode::decode;

pub trait Encodable {
    fn encode(&self, buf: &mut BytesMut) -> ();
}