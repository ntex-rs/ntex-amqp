use std::cell::Cell;
use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::{Decode, Encode, FORMATCODE_BINARY32, FORMATCODE_BINARY8};
use crate::errors::AmqpParseError;
use crate::protocol::{Annotations, Header, MessageFormat, Properties, Section, StringVariantMap};
use crate::types::{Descriptor, List, Str, Variant};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message {
    pub message_format: Option<MessageFormat>,
    header: Option<Header>,
    delivery_annotations: Option<Annotations>,
    message_annotations: Option<Annotations>,
    properties: Option<Properties>,
    application_properties: Option<StringVariantMap>,
    footer: Option<Annotations>,
    body: MessageBody,
    size: Cell<usize>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MessageBody {
    pub data: Vec<Bytes>,
    pub sequence: Vec<List>,
    pub messages: Vec<Message>,
    pub value: Option<Variant>,
}

const SECTION_PREFIX_LENGTH: usize = 3;

impl Message {
    /// Create new message and set body
    pub fn with_body(body: Bytes) -> Message {
        let mut msg = Message::default();
        msg.body.data.push(body);
        msg
    }

    /// Create new message and set messages as body
    pub fn with_messages(messages: Vec<Message>) -> Message {
        let mut msg = Message::default();
        msg.body.messages = messages;
        msg
    }

    /// Header
    pub fn header(&self) -> Option<&Header> {
        self.header.as_ref()
    }

    /// Set message header
    pub fn set_header(mut self, header: Header) -> Self {
        self.header = Some(header);
        self.size.set(0);
        self
    }

    /// Message properties
    pub fn properties(&self) -> Option<&Properties> {
        self.properties.as_ref()
    }

    /// Add property
    pub fn set_properties<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut Properties),
    {
        if let Some(ref mut props) = self.properties {
            f(props);
        } else {
            let mut props = Properties::default();
            f(&mut props);
            self.properties = Some(props);
        }
        self.size.set(0);
        self
    }

    /// Get application property
    pub fn app_property(&self, key: &str) -> Option<&Variant> {
        if let Some(ref props) = self.application_properties {
            props.get(key)
        } else {
            None
        }
    }

    /// Add application property
    pub fn set_app_property<K: Into<Str>, V: Into<Variant>>(mut self, key: K, value: V) -> Self {
        if let Some(ref mut props) = self.application_properties {
            props.insert(key.into(), value.into());
        } else {
            let mut props = HashMap::new();
            props.insert(key.into(), value.into());
            self.application_properties = Some(props);
        }
        self.size.set(0);
        self
    }

    /// Call closure with message reference
    pub fn update<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self,
    {
        self.size.set(0);
        f(self)
    }

    /// Call closure if value is Some value
    pub fn if_some<T, F>(self, value: &Option<T>, f: F) -> Self
    where
        F: Fn(Self, &T) -> Self,
    {
        if let Some(ref val) = value {
            self.size.set(0);
            f(self, val)
        } else {
            self
        }
    }

    /// Message body
    pub fn body(&self) -> &MessageBody {
        &self.body
    }

    /// Message value
    pub fn value(&self) -> Option<&Variant> {
        self.body.value.as_ref()
    }

    /// Set message body value
    pub fn set_value<V: Into<Variant>>(mut self, v: V) -> Self {
        self.body.value = Some(v.into());
        self
    }

    /// Set message body
    pub fn set_body<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut MessageBody),
    {
        f(&mut self.body);
        self.size.set(0);
        self
    }

    /// Create new message and set `correlation_id` property
    pub fn reply_message(&self) -> Message {
        Message::default().if_some(&self.properties, |msg, data| {
            msg.set_properties(|props| props.correlation_id = data.message_id.clone())
        })
    }
}

impl Decode for Message {
    fn decode(mut input: &[u8]) -> Result<(&[u8], Message), AmqpParseError> {
        let mut message = Message::default();

        loop {
            let (buf, sec) = Section::decode(input)?;
            match sec {
                Section::Header(val) => {
                    message.header = Some(val);
                }
                Section::DeliveryAnnotations(val) => {
                    message.delivery_annotations = Some(val);
                }
                Section::MessageAnnotations(val) => {
                    message.message_annotations = Some(val);
                }
                Section::ApplicationProperties(val) => {
                    message.application_properties = Some(val);
                }
                Section::Footer(val) => {
                    message.footer = Some(val);
                }
                Section::Properties(val) => {
                    message.properties = Some(val);
                }

                // body
                Section::AmqpSequence(val) => {
                    message.body.sequence.push(val);
                }
                Section::AmqpValue(val) => {
                    message.body.value = Some(val);
                }
                Section::Data(val) => {
                    message.body.data.push(val);
                }
            }
            if buf.is_empty() {
                break;
            }
            input = buf;
        }
        Ok((input, message))
    }
}

impl Encode for Message {
    fn encoded_size(&self) -> usize {
        let size = self.size.get();
        if size != 0 {
            return size;
        }

        // body size, always add empty body if needed
        let body_size = self.body.encoded_size();
        let mut size = if body_size == 0 {
            // empty bytes
            SECTION_PREFIX_LENGTH + 2
        } else {
            body_size
        };

        if let Some(ref h) = self.header {
            size += h.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref da) = self.delivery_annotations {
            size += da.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref ma) = self.message_annotations {
            size += ma.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref p) = self.properties {
            size += p.encoded_size();
        }
        if let Some(ref ap) = self.application_properties {
            size += ap.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref f) = self.footer {
            size += f.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        self.size.set(size);
        size
    }

    fn encode(&self, dst: &mut BytesMut) {
        if let Some(ref h) = self.header {
            h.encode(dst);
        }
        if let Some(ref da) = self.delivery_annotations {
            Descriptor::Ulong(113).encode(dst);
            da.encode(dst);
        }
        if let Some(ref ma) = self.message_annotations {
            Descriptor::Ulong(114).encode(dst);
            ma.encode(dst);
        }
        if let Some(ref p) = self.properties {
            p.encode(dst);
        }
        if let Some(ref ap) = self.application_properties {
            Descriptor::Ulong(116).encode(dst);
            ap.encode(dst);
        }

        // message body
        if self.body.encoded_size() == 0 {
            // special treatment for empty body
            Descriptor::Ulong(117).encode(dst);
            dst.put_u8(FORMATCODE_BINARY8);
            dst.put_u8(0);
        } else {
            self.body.encode(dst);
        }

        // message footer, always last item
        if let Some(ref f) = self.footer {
            Descriptor::Ulong(120).encode(dst);
            f.encode(dst);
        }
    }
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
            let size = length + if length > std::u8::MAX as usize { 5 } else { 2 };
            a + size + SECTION_PREFIX_LENGTH
        });

        if let Some(ref val) = self.value {
            size + val.encoded_size() + SECTION_PREFIX_LENGTH
        } else {
            size
        }
    }

    fn encode(&self, dst: &mut BytesMut) {
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
            if length > std::u8::MAX as usize {
                dst.put_u8(FORMATCODE_BINARY32);
                dst.put_u32_be(length as u32);
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
    use bytes::{Bytes, BytesMut};

    use crate::codec::{Decode, Encode};
    use crate::errors::AmqpCodecError;
    use crate::protocol::Header;
    use crate::types::Variant;

    use super::Message;

    #[test]
    fn test_properties() -> Result<(), AmqpCodecError> {
        let msg = Message::default().set_properties(|props| props.message_id = Some(1.into()));

        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&buf)?.1;
        let props = msg2.properties.as_ref().unwrap();
        assert_eq!(props.message_id, Some(1.into()));
        Ok(())
    }

    #[test]
    fn test_app_properties() -> Result<(), AmqpCodecError> {
        let msg = Message::default().set_app_property(string::String::from_str("test"), 1);

        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&buf)?.1;
        let props = msg2.application_properties.as_ref().unwrap();
        assert_eq!(*props.get("test").unwrap(), Variant::from(1));
        Ok(())
    }

    #[test]
    fn test_header() -> Result<(), AmqpCodecError> {
        let hdr = Header {
            durable: false,
            priority: 1,
            ttl: None,
            first_acquirer: false,
            delivery_count: 1,
        };

        let msg = Message::default().set_header(hdr.clone());
        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&buf)?.1;
        assert_eq!(msg2.header().unwrap(), &hdr);
        Ok(())
    }

    #[test]
    fn test_data() -> Result<(), AmqpCodecError> {
        let data = Bytes::from_static(b"test data");

        let msg = Message::default().set_body(|body| body.set_data(data.clone()));
        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&buf)?.1;
        assert_eq!(msg2.body.data().unwrap(), &data);
        Ok(())
    }

    #[test]
    fn test_data_empty() -> Result<(), AmqpCodecError> {
        let msg = Message::default();
        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&buf)?.1;
        assert_eq!(msg2.body.data().unwrap(), &Bytes::from_static(b""));
        Ok(())
    }

    #[test]
    fn test_messages() -> Result<(), AmqpCodecError> {
        let msg1 = Message::default().set_properties(|props| props.message_id = Some(1.into()));
        let msg2 = Message::default().set_properties(|props| props.message_id = Some(2.into()));

        let msg = Message::default().set_body(|body| {
            body.messages.push(msg1.clone());
            body.messages.push(msg2.clone());
        });
        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg3 = Message::decode(&buf)?.1;
        let msg4 = Message::decode(&msg3.body.data().unwrap())?.1;
        assert_eq!(msg1.properties, msg4.properties);

        let msg5 = Message::decode(&msg3.body.data[1])?.1;
        assert_eq!(msg2.properties, msg5.properties);
        Ok(())
    }
}
