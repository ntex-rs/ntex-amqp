use std::cell::Cell;

use ntex_bytes::{BytePages, Bytes};

use crate::codec::{Decode, Encode};
use crate::error::AmqpParseError;
use crate::protocol::{Annotations, Header, MessageFormat, Properties, Section, TransferBody};
use crate::types::{Descriptor, Str, Symbol, Variant, VecStringMap, VecSymbolMap};

use super::SECTION_PREFIX_LENGTH;
use super::body::MessageBody;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Message(Box<MessageInner>);

#[derive(Debug, Clone, Default, Eq)]
struct MessageInner {
    message_format: Option<MessageFormat>,
    header: Option<Header>,
    delivery_annotations: Option<VecSymbolMap>,
    message_annotations: Option<VecSymbolMap>,
    properties: Option<Properties>,
    application_properties: Option<VecStringMap>,
    footer: Option<Annotations>,
    body: MessageBody,
    size: Cell<usize>,
}

impl PartialEq for MessageInner {
    fn eq(&self, other: &Self) -> bool {
        // cached size is not part of the message
        let Self {
            message_format,
            header,
            delivery_annotations,
            message_annotations,
            properties,
            application_properties,
            footer,
            body,
            size: _,
        } = self;
        *message_format == other.message_format
            && *header == other.header
            && *delivery_annotations == other.delivery_annotations
            && *message_annotations == other.message_annotations
            && *properties == other.properties
            && *application_properties == other.application_properties
            && *footer == other.footer
            && *body == other.body
    }
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
    /// Message format
    pub fn message_format(&self) -> Option<MessageFormat> {
        self.0.message_format
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
        F: FnOnce(&mut Properties),
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
    pub fn app_properties_mut(&mut self) -> &mut Option<VecStringMap> {
        self.0.size.set(0);
        &mut self.0.application_properties
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
    pub fn message_annotations_mut(&mut self) -> &mut Option<VecSymbolMap> {
        self.0.size.set(0);
        &mut self.0.message_annotations
    }

    #[inline]
    /// Delivery annotations
    pub fn delivery_annotations(&self) -> Option<&VecSymbolMap> {
        self.0.delivery_annotations.as_ref()
    }

    #[inline]
    /// Mut reference to delivery annotations
    pub fn delivery_annotations_mut(&mut self) -> &mut Option<VecSymbolMap> {
        self.0.size.set(0);
        &mut self.0.delivery_annotations
    }

    #[inline]
    /// Get delivery annotation
    pub fn delivery_annotation(&self, key: &str) -> Option<&Variant> {
        if let Some(ref props) = self.0.delivery_annotations {
            props
                .iter()
                .find_map(|item| if &item.0 == key { Some(&item.1) } else { None })
        } else {
            None
        }
    }

    #[inline]
    /// Add delivery annotation
    pub fn add_delivery_annotation<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<Symbol>,
        V: Into<Variant>,
    {
        if let Some(ref mut props) = self.0.delivery_annotations {
            props.push((key.into(), value.into()));
        } else {
            let mut props = VecSymbolMap::default();
            props.push((key.into(), value.into()));
            self.0.delivery_annotations = Some(props);
        }
        self.0.size.set(0);
        self
    }

    #[inline]
    /// Message footer
    pub fn footer(&self) -> Option<&Annotations> {
        self.0.footer.as_ref()
    }

    #[inline]
    /// Mut reference to message footer
    pub fn footer_mut(&mut self) -> &mut Option<Annotations> {
        self.0.size.set(0);
        &mut self.0.footer
    }

    #[inline]
    /// Set message footer
    pub fn set_footer(&mut self, footer: Annotations) -> &mut Self {
        self.0.footer = Some(footer);
        self.0.size.set(0);
        self
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
        if let Some(val) = value {
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
        self.0.size.set(0);
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
        self.0.size.set(0);
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
            msg.set_properties(|props| props.correlation_id.clone_from(&data.message_id));
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

    fn encode(&self, dst: &mut BytePages) {
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
    use ntex_bytes::{BytePages, ByteString, Bytes};
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

        let mut buf = BytePages::default();
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

        let mut buf = BytePages::default();
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
        let mut buf = BytePages::default();
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
        let mut buf = BytePages::default();
        msg.encode(&mut buf);

        let msg2 = Message::decode(&mut buf.freeze())?;
        assert_eq!(msg2.body().data().unwrap(), &data);
        Ok(())
    }

    #[test]
    fn test_data_empty() -> Result<(), AmqpCodecError> {
        let msg = Message::default();
        let mut buf = BytePages::default();
        msg.encode(&mut buf);
        assert_eq!(buf.freeze(), Bytes::from_static(b""));

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
        let mut buf = BytePages::default();
        msg.encode(&mut buf);

        let msg3 = Message::decode(&mut buf.freeze())?;
        let msg4 = Message::decode(&mut msg3.body().data().unwrap().clone())?;
        assert_eq!(msg1.properties(), msg4.properties());

        let msg5 = Message::decode(&mut msg3.body().data[1].clone())?;
        assert_eq!(msg2.properties(), msg5.properties());
        Ok(())
    }

    #[test]
    fn test_size_reset() {
        fn check(msg: &mut Message, f: impl FnOnce(&mut Message)) {
            let _ = msg.encoded_size();
            f(msg);
            let mut buf = BytePages::default();
            msg.encode(&mut buf);
            assert_eq!(msg.encoded_size(), buf.len());
        }

        let mut msg = Message::default();
        msg.set_app_property(ByteString::from("a"), 1)
            .add_message_annotation("b", 1);
        msg.add_delivery_annotation("c", 1);

        check(&mut msg, |m| {
            m.set_value("value");
        });
        check(&mut msg, |m| {
            m.body_mut().set_data(Bytes::from_static(b"data"))
        });
        check(&mut msg, |m| {
            let p = m.app_properties_mut().as_mut().unwrap();
            p.push(("c".into(), "app property".into()));
        });
        check(&mut msg, |m| {
            let a = m.message_annotations_mut().as_mut().unwrap();
            a.push(("d".into(), "message annotation".into()));
        });
        check(&mut msg, |m| {
            let a = m.delivery_annotations_mut().as_mut().unwrap();
            a.push(("e".into(), "delivery annotation".into()));
        });
        check(&mut msg, |m| m.properties_mut().message_id = Some(1.into()));
        check(&mut msg, |m| {
            m.set_footer(Default::default());
        });
        check(&mut msg, |m| {
            let f = m.footer_mut().as_mut().unwrap();
            f.insert("f".into(), "footer".into());
        });

        let mut buf = BytePages::default();
        msg.encode(&mut buf);
        let msg2 = Message::decode(&mut buf.freeze()).unwrap();
        assert_ne!(msg.0.size.get(), msg2.0.size.get());
        assert_eq!(msg, msg2);
        assert_eq!(msg2.delivery_annotation("c"), Some(&Variant::from(1)));
        assert_eq!(msg2.footer(), msg.footer());
    }

    #[test]
    fn test_messages_codec() -> Result<(), AmqpCodecError> {
        let mut msg = Message::default();
        msg.set_properties(|props| props.message_id = Some(Uuid::new_v4().into()));

        let mut buf = BytePages::default();
        msg.encode(&mut buf);

        let msg2 = Message::decode(&mut buf.freeze())?;
        assert_eq!(msg.properties(), msg2.properties());
        Ok(())
    }

    #[test]
    fn message_format_and_cached_size() {
        let mut msg = Message::default();
        assert_eq!(msg.message_format(), None);
        msg.set_format(42);
        assert_eq!(msg.message_format(), Some(42));

        // size is cached and invalidated on mutation
        msg.set_value("a");
        let size = msg.encoded_size();
        assert_eq!(msg.encoded_size(), size);
        msg.set_value("much longer value");
        let new_size = msg.encoded_size();
        assert!(new_size > size);

        let mut buf = BytePages::default();
        msg.encode(&mut buf);
        assert_eq!(buf.len(), new_size);
    }

    #[test]
    fn app_properties_lookup() {
        let mut msg = Message::default();
        assert!(msg.app_property("a").is_none());
        assert!(msg.app_properties().is_none());
        assert!(msg.app_properties_mut().is_none());

        msg.set_app_property("a", 1);
        // second insert takes the "existing vec" branch
        msg.set_app_property("b", 2);
        assert_eq!(msg.app_property("a"), Some(&Variant::from(1)));
        assert_eq!(msg.app_property("b"), Some(&Variant::from(2)));
        assert_eq!(msg.app_property("c"), None);
        assert_eq!(msg.app_properties().unwrap().len(), 2);

        msg.app_properties_mut().as_mut().unwrap().clear();
        assert_eq!(msg.app_property("a"), None);
    }

    #[test]
    fn message_annotations_lookup() {
        let mut msg = Message::default();
        assert!(msg.message_annotation("a").is_none());
        assert!(msg.message_annotations().is_none());
        assert!(msg.message_annotations_mut().is_none());

        msg.add_message_annotation("a", 1);
        msg.add_message_annotation("b", 2);
        assert_eq!(msg.message_annotation("a"), Some(&Variant::from(1)));
        assert_eq!(msg.message_annotation("c"), None);
        assert_eq!(msg.message_annotations().unwrap().len(), 2);

        msg.message_annotations_mut().as_mut().unwrap().clear();
        assert_eq!(msg.message_annotation("a"), None);
    }

    #[test]
    fn delivery_annotations_lookup() {
        let mut msg = Message::default();
        assert!(msg.delivery_annotation("a").is_none());
        assert!(msg.delivery_annotations().is_none());
        assert!(msg.delivery_annotations_mut().is_none());

        msg.add_delivery_annotation("a", 1);
        msg.add_delivery_annotation("b", 2);
        assert_eq!(msg.delivery_annotation("a"), Some(&Variant::from(1)));
        assert_eq!(msg.delivery_annotation("c"), None);
        assert_eq!(msg.delivery_annotations().unwrap().len(), 2);

        msg.delivery_annotations_mut().as_mut().unwrap().clear();
        assert_eq!(msg.delivery_annotation("a"), None);
    }

    #[test]
    fn properties_mut_creates_and_reuses() {
        let mut msg = Message::default();
        assert!(msg.properties().is_none());

        msg.properties_mut().subject = Some(ByteString::from("s"));
        assert_eq!(
            msg.properties().unwrap().subject,
            Some(ByteString::from("s"))
        );

        // second call reuses the existing properties
        msg.properties_mut().group_id = Some(ByteString::from("g"));
        let props = msg.properties().unwrap();
        assert_eq!(props.subject, Some(ByteString::from("s")));
        assert_eq!(props.group_id, Some(ByteString::from("g")));

        // set_properties also takes the existing branch
        msg.set_properties(|p| p.subject = Some(ByteString::from("s2")));
        assert_eq!(
            msg.properties().unwrap().subject,
            Some(ByteString::from("s2"))
        );
    }

    #[test]
    fn footer_roundtrip() {
        use crate::types::Symbol;

        let mut msg = Message::default();
        assert!(msg.footer().is_none());
        assert!(msg.footer_mut().is_none());

        let mut footer = crate::protocol::Annotations::default();
        footer.insert(Symbol::from("f"), Variant::from(1));
        msg.set_footer(footer);
        assert_eq!(msg.footer().unwrap().len(), 1);
        msg.footer_mut().as_mut().unwrap().clear();
        assert!(msg.footer().unwrap().is_empty());
    }

    #[test]
    fn update_and_if_some() {
        let msg = Message::default().update(|mut m| {
            m.set_value("v");
            m
        });
        assert_eq!(msg.value(), Some(&Variant::from("v")));

        // `if_some` with Some runs the closure
        let msg = msg.if_some(&Some(7u8), |mut m, v| {
            m.set_app_property("n", *v);
            m
        });
        assert_eq!(msg.app_property("n"), Some(&Variant::Ubyte(7)));

        // `if_some` with None leaves the message untouched
        let none: Option<u8> = None;
        let msg2 = msg.clone().if_some(&none, |mut m, v| {
            m.set_app_property("other", *v);
            m
        });
        assert_eq!(msg2, msg);
    }

    #[test]
    fn reply_message_copies_correlation_id() {
        // no properties -> nothing to copy
        let plain = Message::default();
        assert!(plain.reply_message().properties().is_none());

        let mut msg = Message::default();
        msg.set_properties(|p| p.message_id = Some(7.into()));
        let reply = msg.reply_message();
        assert_eq!(reply.properties().unwrap().correlation_id, Some(7.into()));
        assert_eq!(reply.properties().unwrap().message_id, None);
    }

    #[test]
    fn body_accessors() {
        let mut msg = Message::default();
        assert!(msg.body().data().is_none());
        assert!(msg.value().is_none());

        msg.body_mut().set_data(Bytes::from_static(b"d"));
        assert_eq!(msg.body().data(), Some(&Bytes::from_static(b"d")));

        msg.set_body(|b| b.sequence.push(crate::types::List(vec![Variant::Ubyte(1)])));
        assert_eq!(msg.body().sequence.len(), 1);
    }

    #[test]
    fn full_message_roundtrip() {
        use crate::types::Symbol;

        let mut footer = crate::protocol::Annotations::default();
        footer.insert(Symbol::from("f"), Variant::from(1));

        let mut msg = Message::default();
        msg.set_header(Header {
            durable: true,
            priority: 2,
            ttl: Some(100),
            first_acquirer: true,
            delivery_count: 3,
        })
        .set_properties(|p| {
            p.message_id = Some(Uuid::from_u128(1).into());
            p.subject = Some(ByteString::from("subj"));
        })
        .set_app_property("ap", 1)
        .add_message_annotation("ma", 2)
        .add_delivery_annotation("da", 3)
        .set_footer(footer)
        .set_body(|b| {
            b.set_data(Bytes::from_static(b"payload"));
            b.sequence.push(crate::types::List(vec![Variant::Ubyte(9)]));
            b.value = Some(Variant::from("v"));
        });

        let mut buf = BytePages::default();
        msg.encode(&mut buf);
        let buf = buf.freeze();
        assert_eq!(msg.encoded_size(), buf.len());

        let msg2 = Message::decode(&mut buf.clone()).unwrap();
        assert_eq!(msg2.header(), msg.header());
        assert_eq!(msg2.properties(), msg.properties());
        assert_eq!(msg2.app_property("ap"), Some(&Variant::from(1)));
        assert_eq!(msg2.message_annotation("ma"), Some(&Variant::from(2)));
        assert_eq!(msg2.delivery_annotation("da"), Some(&Variant::from(3)));
        assert_eq!(msg2.footer(), msg.footer());
        assert_eq!(msg2.body(), msg.body());
        assert_eq!(msg2.encoded_size(), buf.len());
    }

    #[test]
    fn decode_empty_input() {
        let msg = Message::decode(&mut Bytes::new()).unwrap();
        assert_eq!(msg, Message::default());
    }
}
