#![allow(clippy::mutable_key_type, clippy::len_without_is_empty)]

#[macro_use]
extern crate derive_more;

#[macro_use]
mod codec;
mod error;
mod framing;
mod io;
mod message;
pub mod protocol;
pub mod types;

pub use self::codec::{
    ArrayEncode, ArrayHeader, Decode, DecodeFormatted, Encode, ListHeader, MapHeader, format_codes,
};
pub use self::error::{AmqpCodecError, AmqpParseError, ProtocolIdError};
pub use self::framing::{AmqpFrame, SaslFrame};
pub use self::io::{AmqpCodec, ProtocolIdCodec};
pub use self::message::{Message, MessageBody};

type HashMap<K, V> = ntex_util::HashMap<K, V>;
