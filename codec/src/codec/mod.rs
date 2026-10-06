use ntex_bytes::{Buf, BytePages, Bytes};

use crate::{error::AmqpParseError, types::Constructor, types::Descriptor};

macro_rules! decode_check_len {
    ($buf:ident, $size:expr) => {
        if $buf.len() < $size {
            return Err(AmqpParseError::Incomplete($size));
        }
    };
}

#[macro_use]
pub(crate) mod decode;
mod encode;

/// Defines routines to encode the type as an AMQP value. Encoding must include the type constructor (format code or described type definition).
pub trait Encode {
    /// Returns the size of the type when encoded.
    fn encoded_size(&self) -> usize;

    /// Encodes the type into the provided buffer.
    ///
    /// # Panics
    ///
    /// Panics if an encoded size or element count does not fit into AMQP 32-bit
    /// size field (larger than `u32::MAX`), AMQP cannot represent such value.
    fn encode(&self, buf: &mut BytePages);
}

/// Converts encoded size or element count to AMQP 32-bit size field.
///
/// # Panics
///
/// Panics if value is larger than `u32::MAX`.
#[inline]
#[track_caller]
pub(crate) fn size_u32(size: usize) -> u32 {
    match u32::try_from(size) {
        Ok(size) => size,
        Err(_) => panic!("AMQP encoded size {size} exceeds u32::MAX"),
    }
}

/// Defines routines to encode the type as an element of an AMQP array. It's different from Encode in that it omits the type constructor
/// (format code or described type definition) when encoding.
pub trait ArrayEncode {
    const ARRAY_CONSTRUCTOR: Constructor;

    /// Returns the size of the type when encoded as an element of an AMQP array.
    fn array_encoded_size(&self) -> usize;

    /// Encodes the type as an element of an AMQP array.
    fn array_encode(&self, buf: &mut BytePages);
}

pub trait Composite: Encode + Decode {
    fn descriptor() -> Descriptor;
}

/// Defines routines to decode the type from an encoded AMQP value representation. Decoding must handle parsing the type constructor
/// (format code or described type definition).
pub trait Decode
where
    Self: Sized,
{
    /// Decodes the type from the provided buffer.
    fn decode(input: &mut Bytes) -> Result<Self, AmqpParseError>;
}

pub trait DecodeFormatted
where
    Self: Sized,
{
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError>;
}

impl<T: DecodeFormatted> Decode for T {
    fn decode(input: &mut Bytes) -> Result<Self, AmqpParseError> {
        let fmt = decode_format_code(input)?;
        T::decode_with_format(input, fmt)
    }
}

pub(crate) fn decode_format_code(input: &mut Bytes) -> Result<u8, AmqpParseError> {
    decode_check_len!(input, 1);
    let code = input.get_u8();
    Ok(code)
}

pub mod format_codes {
    pub const FORMATCODE_DESCRIBED: u8 = 0x00;
    pub const FORMATCODE_NULL: u8 = 0x40; // fixed width --V
    pub const FORMATCODE_BOOLEAN: u8 = 0x56;
    pub const FORMATCODE_BOOLEAN_TRUE: u8 = 0x41;
    pub const FORMATCODE_BOOLEAN_FALSE: u8 = 0x42;
    pub const FORMATCODE_UINT_0: u8 = 0x43;
    pub const FORMATCODE_ULONG_0: u8 = 0x44;
    pub const FORMATCODE_UBYTE: u8 = 0x50;
    pub const FORMATCODE_USHORT: u8 = 0x60;
    pub const FORMATCODE_UINT: u8 = 0x70;
    pub const FORMATCODE_ULONG: u8 = 0x80;
    pub const FORMATCODE_BYTE: u8 = 0x51;
    pub const FORMATCODE_SHORT: u8 = 0x61;
    pub const FORMATCODE_INT: u8 = 0x71;
    pub const FORMATCODE_LONG: u8 = 0x81;
    pub const FORMATCODE_SMALLUINT: u8 = 0x52;
    pub const FORMATCODE_SMALLULONG: u8 = 0x53;
    pub const FORMATCODE_SMALLINT: u8 = 0x54;
    pub const FORMATCODE_SMALLLONG: u8 = 0x55;
    pub const FORMATCODE_FLOAT: u8 = 0x72;
    pub const FORMATCODE_DOUBLE: u8 = 0x82;
    pub const FORMATCODE_DECIMAL32: u8 = 0x74;
    pub const FORMATCODE_DECIMAL64: u8 = 0x84;
    pub const FORMATCODE_DECIMAL128: u8 = 0x94;
    pub const FORMATCODE_CHAR: u8 = 0x73;
    pub const FORMATCODE_TIMESTAMP: u8 = 0x83;
    pub const FORMATCODE_UUID: u8 = 0x98;
    pub const FORMATCODE_BINARY8: u8 = 0xa0; // variable --V
    pub const FORMATCODE_BINARY32: u8 = 0xb0;
    pub const FORMATCODE_STRING8: u8 = 0xa1;
    pub const FORMATCODE_STRING32: u8 = 0xb1;
    pub const FORMATCODE_SYMBOL8: u8 = 0xa3;
    pub const FORMATCODE_SYMBOL32: u8 = 0xb3;
    pub const FORMATCODE_LIST0: u8 = 0x45; // compound --V
    pub const FORMATCODE_LIST8: u8 = 0xc0;
    pub const FORMATCODE_LIST32: u8 = 0xd0;
    pub const FORMATCODE_MAP8: u8 = 0xc1;
    pub const FORMATCODE_MAP32: u8 = 0xd1;
    pub const FORMATCODE_ARRAY8: u8 = 0xe0;
    pub const FORMATCODE_ARRAY32: u8 = 0xf0;
}

pub(crate) use self::format_codes::*;

#[derive(Copy, Clone, Debug)]
pub struct ListHeader {
    pub size: u32,
    pub count: u32,
}

#[derive(Copy, Clone, Debug)]
pub struct MapHeader {
    pub size: u32,
    pub count: u32,
}

#[derive(Copy, Clone, Debug)]
pub struct ArrayHeader {
    pub size: u32,
    pub count: u32,
}

#[cfg(test)]
mod tests {
    use ntex_bytes::{BytePages, Bytes};

    use crate::codec::{Decode, Encode};
    use crate::error::AmqpCodecError;
    use crate::framing::{AmqpFrame, SaslFrame};
    use crate::protocol::SaslFrameBody;

    #[test]
    fn test_sasl_mechanisms() -> Result<(), AmqpCodecError> {
        let mut data = Bytes::from_static(
            b"\x02\x01\0\0\0S@\xc02\x01\xe0/\x04\xb3\0\0\0\x07MSSBCBS\0\0\0\x05PLAIN\0\0\0\tANONYMOUS\0\0\0\x08EXTERNAL");

        let data2 = data.clone();
        let frame = SaslFrame::decode(&mut data).unwrap();
        assert!(data.is_empty());
        match frame.body {
            SaslFrameBody::SaslMechanisms(_) => (),
            _ => panic!("error"),
        }

        let mut buf = BytePages::default();
        frame.encode(&mut buf);
        let mut buf = buf.freeze();
        buf.advance_to(4);
        assert_eq!(data2, buf);

        Ok(())
    }

    #[test]
    fn test_disposition() -> Result<(), AmqpCodecError> {
        let data = Bytes::from_static(b"\x02\0\0\0\0S\x15\xc0\x0c\x06AC@A\0S$\xc0\x01\0B");

        let frame = AmqpFrame::decode(&mut data.clone())?;
        assert_eq!(frame.performative().name(), "Disposition");

        let mut buf = BytePages::default();
        frame.encode(&mut buf);
        let mut buf = buf.freeze();
        buf.advance_to(4);
        assert_eq!(data, buf);

        Ok(())
    }

    #[test]
    fn size_u32() {
        assert_eq!(super::size_u32(u32::MAX as usize), u32::MAX);
        #[cfg(target_pointer_width = "64")]
        assert!(std::panic::catch_unwind(|| super::size_u32(u32::MAX as usize + 1)).is_err());
    }

    #[cfg(target_pointer_width = "64")]
    mod oversize {
        use std::{panic::AssertUnwindSafe, panic::catch_unwind, sync::OnceLock};

        use ntex_bytes::{BytePages, ByteString, Bytes};

        use crate::codec::{ArrayEncode, Encode};
        use crate::framing::{AmqpFrame, SaslFrame};
        use crate::message::Message;
        use crate::protocol::{
            Frame, MessageId, Properties, SaslFrameBody, SaslInit, Transfer, TransferBody,
            TransferInner,
        };
        use crate::types::{List, Variant};

        /// 4 GiB of zeroes, lazily mapped by allocator and never touched
        fn huge() -> Bytes {
            static HUGE: OnceLock<&'static [u8]> = OnceLock::new();
            Bytes::from_static(
                HUGE.get_or_init(|| Box::leak(vec![0u8; u32::MAX as usize + 1].into_boxed_slice())),
            )
        }

        /// 2 GiB, fits into 32-bit size field, but two of them do not
        fn half() -> Bytes {
            huge().slice(..1 << 31)
        }

        fn huge_str() -> ByteString {
            // zeroes are valid utf-8, avoid scanning 4 GiB
            unsafe { ByteString::from_bytes_unchecked(huge()) }
        }

        #[track_caller]
        fn assert_panics(f: impl FnOnce(&mut BytePages)) {
            let mut buf = BytePages::default();
            let res = catch_unwind(AssertUnwindSafe(|| f(&mut buf)));
            let err = res.expect_err("encoding must panic");
            let msg = err.downcast_ref::<String>().unwrap();
            assert!(msg.contains("exceeds u32::MAX"), "{msg}");
        }

        #[test]
        fn values() {
            assert_panics(|buf| huge().encode(buf));
            assert_panics(|buf| huge().array_encode(buf));
            assert_panics(|buf| huge_str().encode(buf));
            assert_panics(|buf| huge_str().as_str().encode(buf));
            // containers of values that fit individually
            assert_panics(|buf| {
                List(vec![Variant::Binary(half()), Variant::Binary(half())]).encode(buf);
            });
            assert_panics(|buf| vec![half(), half()].encode(buf));
            assert_panics(|buf| {
                Properties {
                    message_id: Some(MessageId::Binary(half())),
                    user_id: Some(half()),
                    ..Default::default()
                }
                .encode(buf);
            });
        }

        #[test]
        fn messages() {
            assert_panics(|buf| Message::with_body(huge()).encode(buf));
            // nested message is encoded as binary
            assert_panics(|buf| {
                let mut msg = Message::with_body(half());
                msg.body_mut().data.push(half());
                Message::with_messages(vec![TransferBody::Message(msg)]).encode(buf);
            });
        }

        #[test]
        fn frames() {
            assert_panics(|buf| {
                let transfer = Transfer(Box::new(TransferInner {
                    body: Some(TransferBody::Data(huge())),
                    ..Default::default()
                }));
                AmqpFrame::new(0, Frame::Transfer(transfer)).encode(buf);
            });
            assert_panics(|buf| {
                SaslFrame::new(SaslFrameBody::SaslInit(SaslInit {
                    mechanism: "PLAIN".into(),
                    initial_response: Some(huge()),
                    hostname: None,
                }))
                .encode(buf);
            });
        }
    }
}
