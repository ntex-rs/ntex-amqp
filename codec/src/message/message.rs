use std::cell::Cell;

use ntex_bytes::{Bytes, BytesMut};

use crate::codec::{Decode, Encode};
use crate::error::AmqpParseError;
use crate::protocol::{Annotations, Header, MessageFormat, Properties, Section, TransferBody};
use crate::types::{Descriptor, Str, Symbol, Variant, VecStringMap, VecSymbolMap};

use super::body::MessageBody;
use super::SECTION_PREFIX_LENGTH;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message(pub Box<MessageInner>);

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MessageInner {
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
    #[inline]
    /// Create new message and set body
    pub fn with_body(body: Bytes) -> Message {
        let mut msg = Message::default();
        msg.0.body.data.push(body);
        msg.0.message_format = Some(0);
        msg
    }

    #[inline]
    /// Create new message and set messages as body
    pub fn with_messages(messages: Vec<TransferBody>) -> Message {
        let mut msg = Message::default();
        msg.0.body.messages = messages;
        msg.0.message_format = Some(0);
        msg
    }

    #[inline]
    /// Header
    pub fn header(&self) -> Option<&Header> {
        self.0.header.as_ref()
    }

    #[inline]
    /// Set message header
    pub fn set_header(&mut self, header: Header) -> &mut Self {
        self.0.header = Some(header);
        self.0.size.set(0);
        self
    }

    #[inline]
    /// Set message format
    pub fn set_format(&mut self, format: MessageFormat) -> &mut Self {
        self.0.message_format = Some(format);
        self
    }

    #[inline]
    /// Message properties
    pub fn properties(&self) -> Option<&Properties> {
        self.0.properties.as_ref()
    }

    #[inline]
    /// Mutable reference to properties
    pub fn properties_mut(&mut self) -> &mut Properties {
        if self.0.properties.is_none() {
            self.0.properties = Some(Properties::default());
        }

        self.0.size.set(0);
        self.0.properties.as_mut().unwrap()
    }

    #[inline]
    /// Add property
    pub fn set_properties<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn(&mut Properties),
    {
        if let Some(ref mut props) = self.0.properties {
            f(props);
        } else {
            let mut props = Properties::default();
            f(&mut props);
            self.0.properties = Some(props);
        }
        self.0.size.set(0);
        self
    }

    #[inline]
    /// Get application property
    pub fn app_properties(&self) -> Option<&VecStringMap> {
        self.0.application_properties.as_ref()
    }

    #[inline]
    /// Mut ref tp application property
    pub fn app_properties_mut(&mut self) -> Option<&mut VecStringMap> {
        self.0.application_properties.as_mut()
    }

    #[inline]
    /// Get application property
    pub fn app_property(&self, key: &str) -> Option<&Variant> {
        if let Some(ref props) = self.0.application_properties {
            props
                .iter()
                .find_map(|item| if &item.0 == key { Some(&item.1) } else { None })
        } else {
            None
        }
    }

    #[inline]
    /// Add application property
    pub fn set_app_property<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<Str>,
        V: Into<Variant>,
    {
        if let Some(ref mut props) = self.0.application_properties {
            props.push((key.into(), value.into()));
        } else {
            let mut props = VecStringMap::default();
            props.push((key.into(), value.into()));
            self.0.application_properties = Some(props);
        }
        self.0.size.set(0);
        self
    }

    #[inline]
    /// Get message annotation
    pub fn message_annotation(&self, key: &str) -> Option<&Variant> {
        if let Some(ref props) = self.0.message_annotations {
            props
                .iter()
                .find_map(|item| if &item.0 == key { Some(&item.1) } else { None })
        } else {
            None
        }
    }

    #[inline]
    /// Add message annotation
    pub fn add_message_annotation<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<Symbol>,
        V: Into<Variant>,
    {
        if let Some(ref mut props) = self.0.message_annotations {
            props.push((key.into(), value.into()));
        } else {
            let mut props = VecSymbolMap::default();
            props.push((key.into(), value.into()));
            self.0.message_annotations = Some(props);
        }
        self.0.size.set(0);
        self
    }

    #[inline]
    /// Get message annotations
    pub fn message_annotations(&self) -> Option<&VecSymbolMap> {
        self.0.message_annotations.as_ref()
    }

    #[inline]
    /// Mut reference to message annotations
    pub fn message_annotations_mut(&mut self) -> Option<&mut VecSymbolMap> {
        self.0.message_annotations.as_mut()
    }

    #[inline]
    /// Delivery annotations
    pub fn delivery_annotations(&self) -> Option<&VecSymbolMap> {
        self.0.delivery_annotations.as_ref()
    }

    #[inline]
    /// Mut reference to delivery annotations
    pub fn delivery_annotations_mut(&mut self) -> Option<&mut VecSymbolMap> {
        self.0.delivery_annotations.as_mut()
    }

    #[inline]
    /// Call closure with message reference
    pub fn update<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self,
    {
        self.0.size.set(0);
        f(self)
    }

    #[inline]
    /// Call closure if value is Some value
    pub fn if_some<T, F>(self, value: &Option<T>, f: F) -> Self
    where
        F: Fn(Self, &T) -> Self,
    {
        if let Some(ref val) = value {
            self.0.size.set(0);
            f(self, val)
        } else {
            self
        }
    }

    #[inline]
    /// Message body
    pub fn body(&self) -> &MessageBody {
        &self.0.body
    }

    #[inline]
    /// Mutable message body
    pub fn body_mut(&mut self) -> &mut MessageBody {
        &mut self.0.body
    }

    #[inline]
    /// Message value
    pub fn value(&self) -> Option<&Variant> {
        self.0.body.value.as_ref()
    }

    #[inline]
    /// Set message body value
    pub fn set_value<V: Into<Variant>>(&mut self, v: V) -> &mut Self {
        self.0.body.value = Some(v.into());
        self
    }

    #[inline]
    /// Set message body
    pub fn set_body<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut MessageBody),
    {
        f(&mut self.0.body);
        self.0.size.set(0);
        self
    }

    #[inline]
    /// Create new message and set `correlation_id` property
    pub fn reply_message(&self) -> Message {
        Message::default().if_some(&self.0.properties, |mut msg, data| {
            msg.set_properties(|props| props.correlation_id = data.message_id.clone());
            msg
        })
    }
}

impl Decode for Message {
    fn decode(input: &mut Bytes) -> Result<Message, AmqpParseError> {
        let mut message = Message::default();

        loop {
            if input.is_empty() {
                break;
            }

            let sec = Section::decode(input)?;
            match sec {
                Section::Header(val) => {
                    message.0.header = Some(val);
                }
                Section::DeliveryAnnotations(val) => {
                    message.0.delivery_annotations = Some(val);
                }
                Section::MessageAnnotations(val) => {
                    message.0.message_annotations = Some(val);
                }
                Section::ApplicationProperties(val) => {
                    message.0.application_properties = Some(val);
                }
                Section::Footer(val) => {
                    message.0.footer = Some(val);
                }
                Section::Properties(val) => {
                    message.0.properties = Some(val);
                }

                // body
                Section::AmqpSequence(val) => {
                    message.0.body.sequence.push(val);
                }
                Section::AmqpValue(val) => {
                    message.0.body.value = Some(val);
                }
                Section::Data(val) => {
                    message.0.body.data.push(val);
                }
            }
        }
        Ok(message)
    }
}

impl Encode for Message {
    fn encoded_size(&self) -> usize {
        let size = self.0.size.get();
        if size != 0 {
            return size;
        }

        let mut size = self.0.body.encoded_size();

        if let Some(ref h) = self.0.header {
            size += h.encoded_size();
        }
        if let Some(ref da) = self.0.delivery_annotations {
            size += da.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref ma) = self.0.message_annotations {
            size += ma.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref p) = self.0.properties {
            size += p.encoded_size();
        }
        if let Some(ref ap) = self.0.application_properties {
            size += ap.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref f) = self.0.footer {
            size += f.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        self.0.size.set(size);
        size
    }

    fn encode(&self, dst: &mut BytesMut) {
        if let Some(ref h) = self.0.header {
            h.encode(dst);
        }
        if let Some(ref da) = self.0.delivery_annotations {
            Descriptor::Ulong(113).encode(dst);
            da.encode(dst);
        }
        if let Some(ref ma) = self.0.message_annotations {
            Descriptor::Ulong(114).encode(dst);
            ma.encode(dst);
        }
        if let Some(ref p) = self.0.properties {
            p.encode(dst);
        }
        if let Some(ref ap) = self.0.application_properties {
            Descriptor::Ulong(116).encode(dst);
            ap.encode(dst);
        }

        // message body
        self.0.body.encode(dst);

        // message footer, always last item
        if let Some(ref f) = self.0.footer {
            Descriptor::Ulong(120).encode(dst);
            f.encode(dst);
        }
    }
}

#[cfg(test)]
mod tests {
    use ntex_bytes::{ByteString, Bytes, BytesMut};
    use uuid::Uuid;

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

        let msg2 = Message::decode(&mut buf.freeze())?;
        let props = msg2.properties().unwrap();
        assert_eq!(props.message_id, Some(1.into()));
        Ok(())
    }

    #[test]
    fn test_app_properties() -> Result<(), AmqpCodecError> {
        let mut msg = Message::default();
        msg.set_app_property(ByteString::from("test"), 1);

        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&mut buf.freeze())?;
        let props = msg2.app_properties().unwrap();
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

        let msg2 = Message::decode(&mut buf.freeze())?;
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

        let msg2 = Message::decode(&mut buf.freeze())?;
        assert_eq!(msg2.body().data().unwrap(), &data);
        Ok(())
    }

    #[test]
    fn test_data_empty() -> Result<(), AmqpCodecError> {
        let msg = Message::default();
        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);
        assert_eq!(buf, Bytes::from_static(b""));

        let msg2 = Message::decode(&mut buf.freeze())?;
        assert!(msg2.body().data().is_none());
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

        let msg3 = Message::decode(&mut buf.freeze())?;
        let msg4 = Message::decode(&mut msg3.body().data().unwrap().clone())?;
        assert_eq!(msg1.properties(), msg4.properties());

        let msg5 = Message::decode(&mut msg3.body().data[1].clone())?;
        assert_eq!(msg2.properties(), msg5.properties());
        Ok(())
    }

    #[test]
    fn test_messages_codec() -> Result<(), AmqpCodecError> {
        let mut msg = Message::default();
        msg.set_properties(|props| props.message_id = Some(Uuid::new_v4().into()));

        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&mut buf.freeze())?;
        assert_eq!(msg.properties(), msg2.properties());
        Ok(())
    }
}
