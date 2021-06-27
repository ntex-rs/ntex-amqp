use std::cell::Cell;

use ntex_bytes::{Bytes, BytesMut};

use crate::codec::{Decode, Encode};
use crate::error::AmqpParseError;
use crate::protocol::{Annotations, Header, MessageFormat, Properties, Section, TransferBody};
use crate::types::{Descriptor, Str, Symbol, Variant, VecStringMap, VecSymbolMap};

use super::body::MessageBody;
use super::SECTION_PREFIX_LENGTH;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message {
    pub message_format: Option<MessageFormat>,
    pub header: Option<Header>,
    pub delivery_annotations: Option<VecSymbolMap>,
    pub message_annotations: Option<VecSymbolMap>,
    pub properties: Option<Properties>,
    pub application_properties: Option<VecStringMap>,
    pub footer: Option<Annotations>,
    pub body: MessageBody,
    size: Cell<usize>,
}

impl Message {
    /// Create new message and set body
    pub fn with_body(body: Bytes) -> Message {
        let mut msg = Message::default();
        msg.body.data.push(body);
        msg.message_format = Some(0);
        msg
    }

    /// Create new message and set messages as body
    pub fn with_messages(messages: Vec<TransferBody>) -> Message {
        let mut msg = Message::default();
        msg.body.messages = messages;
        msg.message_format = Some(0);
        msg
    }

    /// Header
    pub fn header(&self) -> Option<&Header> {
        self.header.as_ref()
    }

    /// Set message header
    pub fn set_header(&mut self, header: Header) -> &mut Self {
        self.header = Some(header);
        self.size.set(0);
        self
    }

    /// Message properties
    pub fn properties(&self) -> Option<&Properties> {
        self.properties.as_ref()
    }

    /// Mutable reference to properties
    pub fn properties_mut(&mut self) -> &mut Properties {
        if self.properties.is_none() {
            self.properties = Some(Properties::default());
        }

        self.size.set(0);
        self.properties.as_mut().unwrap()
    }

    /// Add property
    pub fn set_properties<F>(&mut self, f: F) -> &mut Self
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
    pub fn app_properties(&self) -> Option<&VecStringMap> {
        self.application_properties.as_ref()
    }

    /// Get application property
    pub fn app_property(&self, key: &str) -> Option<&Variant> {
        if let Some(ref props) = self.application_properties {
            props
                .iter()
                .find_map(|item| if &item.0 == key { Some(&item.1) } else { None })
        } else {
            None
        }
    }

    /// Add application property
    pub fn set_app_property<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<Str>,
        V: Into<Variant>,
    {
        if let Some(ref mut props) = self.application_properties {
            props.push((key.into(), value.into()));
        } else {
            let mut props = VecStringMap::default();
            props.push((key.into(), value.into()));
            self.application_properties = Some(props);
        }
        self.size.set(0);
        self
    }

    /// Get message annotation
    pub fn message_annotation(&self, key: &str) -> Option<&Variant> {
        if let Some(ref props) = self.message_annotations {
            props
                .iter()
                .find_map(|item| if &item.0 == key { Some(&item.1) } else { None })
        } else {
            None
        }
    }

    /// Add message annotation
    pub fn add_message_annotation<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<Symbol>,
        V: Into<Variant>,
    {
        if let Some(ref mut props) = self.message_annotations {
            props.push((key.into(), value.into()));
        } else {
            let mut props = VecSymbolMap::default();
            props.push((key.into(), value.into()));
            self.message_annotations = Some(props);
        }
        self.size.set(0);
        self
    }

    /// Delivery annotations
    pub fn delivery_annotations(&self) -> Option<&VecSymbolMap> {
        self.delivery_annotations.as_ref()
    }

    /// Mut reference to delivery annotations
    pub fn delivery_annotations_mut(&mut self) -> Option<&mut VecSymbolMap> {
        self.delivery_annotations.as_mut()
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
    pub fn set_value<V: Into<Variant>>(&mut self, v: V) -> &mut Self {
        self.body.value = Some(v.into());
        self
    }

    /// Set message body
    pub fn set_body<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut MessageBody),
    {
        f(&mut self.body);
        self.size.set(0);
        self
    }

    /// Create new message and set `correlation_id` property
    pub fn reply_message(&self) -> Message {
        Message::default().if_some(&self.properties, |mut msg, data| {
            msg.set_properties(|props| props.correlation_id = data.message_id.clone());
            msg
        })
    }
}

impl Decode for Message {
    fn decode(mut input: &[u8]) -> Result<(&[u8], Message), AmqpParseError> {
        let mut message = Message::default();

        loop {
            if input.is_empty() {
                break;
            }

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

        let mut size = self.body.encoded_size();

        if let Some(ref h) = self.header {
            size += h.encoded_size();
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
        self.body.encode(dst);

        // message footer, always last item
        if let Some(ref f) = self.footer {
            Descriptor::Ulong(120).encode(dst);
            f.encode(dst);
        }
    }
}

#[cfg(test)]
mod tests {
    use ntex_bytes::{ByteString, Bytes, BytesMut};

    use crate::codec::{Decode, Encode};
    use crate::error::AmqpCodecError;
    use crate::protocol::Header;
    use crate::types::Variant;

    use super::Message;

    #[test]
    fn test_properties() -> Result<(), AmqpCodecError> {
        let mut msg = Message::default();
        msg.set_properties(|props| props.message_id = Some(1.into()));

        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&buf)?.1;
        let props = msg2.properties.as_ref().unwrap();
        assert_eq!(props.message_id, Some(1.into()));
        Ok(())
    }

    #[test]
    fn test_app_properties() -> Result<(), AmqpCodecError> {
        let mut msg = Message::default();
        msg.set_app_property(ByteString::from("test"), 1);

        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&buf)?.1;
        let props = msg2.application_properties.as_ref().unwrap();
        assert_eq!(props[0].0.as_str(), "test");
        assert_eq!(props[0].1, Variant::from(1));
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

        let mut msg = Message::default();
        msg.set_header(hdr.clone());
        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&buf)?.1;
        assert_eq!(msg2.header().unwrap(), &hdr);
        Ok(())
    }

    #[test]
    fn test_data() -> Result<(), AmqpCodecError> {
        let data = Bytes::from_static(b"test data");

        let mut msg = Message::default();
        msg.set_body(|body| body.set_data(data.clone()));
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
        assert_eq!(buf, Bytes::from_static(b""));

        let msg2 = Message::decode(&buf)?.1;
        assert!(msg2.body.data().is_none());
        Ok(())
    }

    #[test]
    fn test_messages() -> Result<(), AmqpCodecError> {
        let mut msg1 = Message::default();
        msg1.set_properties(|props| props.message_id = Some(1.into()));
        let mut msg2 = Message::default();
        msg2.set_properties(|props| props.message_id = Some(2.into()));

        let mut msg = Message::default();
        msg.set_body(|body| {
            body.messages.push(msg1.clone().into());
            body.messages.push(msg2.clone().into());
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
