use std::marker::Sized;

use bytes::BytesMut;
use nom::IResult;

mod decode;
mod encode;

pub trait Encode {
    fn encoded_size(&self) -> usize;
    fn encode(&self, buf: &mut BytesMut) -> ();
}

pub trait Decode
    where Self: Sized
{
    fn decode(bytes: &[u8]) -> IResult<&[u8], Self, u32>;
}
