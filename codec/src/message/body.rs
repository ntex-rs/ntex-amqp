use ntex_bytes::{BufMut, BytePages, Bytes};

use crate::codec::{Encode, FORMATCODE_BINARY8, FORMATCODE_BINARY32};
use crate::protocol::TransferBody;
use crate::types::{Descriptor, List, Variant};

use super::SECTION_PREFIX_LENGTH;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageBody {
    pub data: Vec<Bytes>,
    pub sequence: Vec<List>,
    pub messages: Vec<TransferBody>,
    pub value: Option<Variant>,
}

impl MessageBody {
    pub fn data(&self) -> Option<&Bytes> {
        if self.data.is_empty() {
            None
        } else {
            Some(&self.data[0])
        }
    }

    pub fn value(&self) -> Option<&Variant> {
        self.value.as_ref()
    }

    pub fn set_data(&mut self, data: Bytes) {
        self.data.clear();
        self.data.push(data);
    }
}

impl Encode for MessageBody {
    fn encoded_size(&self) -> usize {
        let mut size = self
            .data
            .iter()
            .fold(0, |a, d| a + d.encoded_size() + SECTION_PREFIX_LENGTH);
        size += self
            .sequence
            .iter()
            .fold(0, |a, seq| a + seq.encoded_size() + SECTION_PREFIX_LENGTH);
        size += self.messages.iter().fold(0, |a, m| {
            let length = m.encoded_size();
            let size = length + if length > u8::MAX as usize { 5 } else { 2 };
            a + size + SECTION_PREFIX_LENGTH
        });

        if let Some(ref val) = self.value {
            size + val.encoded_size() + SECTION_PREFIX_LENGTH
        } else {
            size
        }
    }

    fn encode(&self, dst: &mut BytePages) {
        self.data.iter().for_each(|d| {
            Descriptor::Ulong(117).encode(dst);
            d.encode(dst);
        });
        self.sequence.iter().for_each(|seq| {
            Descriptor::Ulong(118).encode(dst);
            seq.encode(dst)
        });
        if let Some(ref val) = self.value {
            Descriptor::Ulong(119).encode(dst);
            val.encode(dst);
        }
        // encode Message as nested Bytes object
        self.messages.iter().for_each(|m| {
            Descriptor::Ulong(117).encode(dst);

            // Bytes prefix
            let length = m.encoded_size();
            if length > u8::MAX as usize {
                dst.put_u8(FORMATCODE_BINARY32);
                dst.put_u32(crate::codec::size_u32(length));
            } else {
                dst.put_u8(FORMATCODE_BINARY8);
                dst.put_u8(length as u8);
            }
            // encode nested Message
            m.encode(dst);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;
    use crate::types::Str;

    fn encoded(body: &MessageBody) -> Bytes {
        let mut buf = BytePages::default();
        body.encode(&mut buf);
        let buf = buf.freeze();
        assert_eq!(body.encoded_size(), buf.len(), "encoded_size mismatch");
        buf
    }

    #[test]
    fn empty_body() {
        let body = MessageBody::default();
        assert_eq!(body.data(), None);
        assert_eq!(body.value(), None);
        assert_eq!(encoded(&body).len(), 0);
    }

    #[test]
    fn data_sections() {
        let mut body = MessageBody::default();
        body.set_data(Bytes::from_static(b"one"));
        assert_eq!(body.data(), Some(&Bytes::from_static(b"one")));

        // set_data replaces, it does not append
        body.set_data(Bytes::from_static(b"two"));
        assert_eq!(body.data, vec![Bytes::from_static(b"two")]);
        assert_eq!(encoded(&body).as_ref(), b"\x00\x53\x75\xa0\x03two");

        body.data.push(Bytes::from_static(b"three"));
        assert_eq!(body.data(), Some(&Bytes::from_static(b"two")));
        assert_eq!(
            encoded(&body).as_ref(),
            b"\x00\x53\x75\xa0\x03two\x00\x53\x75\xa0\x05three"
        );
    }

    #[test]
    fn long_data_section() {
        let mut body = MessageBody::default();
        body.set_data(Bytes::from(vec![7u8; 300]));
        let buf = encoded(&body);
        assert_eq!(buf[..4], [0x00, 0x53, 0x75, 0xb0]);
        assert_eq!(buf[4..8], 300u32.to_be_bytes());
        assert_eq!(buf.len(), 3 + 5 + 300);
    }

    #[test]
    fn value_section() {
        let body = MessageBody {
            value: Some(Variant::String(Str::from("v"))),
            ..Default::default()
        };
        assert_eq!(body.value(), Some(&Variant::String(Str::from("v"))));
        assert_eq!(encoded(&body).as_ref(), b"\x00\x53\x77\xa1\x01v");
    }

    #[test]
    fn sequence_section() {
        let body = MessageBody {
            sequence: vec![
                List(vec![Variant::Ubyte(1), Variant::Null]),
                List(vec![Variant::Ubyte(2)]),
            ],
            ..Default::default()
        };
        assert_eq!(
            encoded(&body).as_ref(),
            b"\x00\x53\x76\xc0\x04\x02\x50\x01\x40\x00\x53\x76\xc0\x03\x01\x50\x02"
        );
    }

    #[test]
    fn nested_messages() {
        let mut inner = Message::default();
        inner.set_body(|b| b.set_data(Bytes::from_static(b"nested")));
        let inner_size = inner.encoded_size();
        assert!(inner_size <= u8::MAX as usize);

        let body = MessageBody {
            messages: vec![TransferBody::Message(inner)],
            ..Default::default()
        };
        let buf = encoded(&body);
        // descriptor + binary8 prefix + nested message
        assert_eq!(buf[..3], [0x00, 0x53, 0x75]);
        assert_eq!(buf[3], FORMATCODE_BINARY8);
        assert_eq!(buf[4] as usize, inner_size);
        assert_eq!(buf.len(), 3 + 2 + inner_size);
    }

    #[test]
    fn nested_large_message() {
        let mut inner = Message::default();
        inner.set_body(|b| b.set_data(Bytes::from(vec![0u8; 300])));
        let inner_size = inner.encoded_size();
        assert!(inner_size > u8::MAX as usize);

        let body = MessageBody {
            messages: vec![TransferBody::Message(inner)],
            ..Default::default()
        };
        let buf = encoded(&body);
        assert_eq!(buf[3], FORMATCODE_BINARY32);
        assert_eq!(buf[4..8], (inner_size as u32).to_be_bytes());
        assert_eq!(buf.len(), 3 + 5 + inner_size);
    }

    #[test]
    fn all_sections_combined() {
        let mut body = MessageBody {
            sequence: vec![List(vec![Variant::Ubyte(1)])],
            value: Some(Variant::Ubyte(9)),
            ..Default::default()
        };
        body.set_data(Bytes::from_static(b"d"));
        // data, then sequence, then value
        assert_eq!(
            encoded(&body).as_ref(),
            b"\x00\x53\x75\xa0\x01d\x00\x53\x76\xc0\x03\x01\x50\x01\x00\x53\x77\x50\x09"
        );
    }
}
