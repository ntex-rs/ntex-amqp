use bytes::{Bytes, BytesMut};
use std::collections::HashMap;

use crate::codec::{Decode, Encode};
use crate::errors::AmqpParseError;
use crate::protocol::{Annotations, Header, MessageFormat, Properties, Section};
use crate::types::{ByteStr, Descriptor, List, Variant};

#[derive(Debug, Clone)]
pub struct Message {
    pub message_format: Option<MessageFormat>,
    pub header: Option<Header>,
    pub delivery_annotations: Option<Annotations>,
    pub message_annotations: Option<Annotations>,
    pub properties: Option<Properties>,
    pub application_properties: Option<HashMap<ByteStr, Variant>>,
    pub application_data: MessageBody,
    pub sequence: Option<List>,
    pub value: Option<Variant>,
    pub data: Option<Bytes>,
    pub footer: Option<Annotations>,
}

#[derive(Debug, Clone)]
pub enum MessageBody {
    Data(Bytes),
    DataVec(Vec<Bytes>),
    SequenceVec(Vec<List>),
    Messages(Vec<Message>),
    Value(Variant),
}

const SECTION_PREFIX_LENGTH: usize = 3;

impl Message {
    /// Add property
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
        self
    }

    /// Add application property
    pub fn set_app_property<V: Into<Variant>>(mut self, key: ByteStr, value: V) -> Self {
        if let Some(ref mut props) = self.application_properties {
            props.insert(key, value.into());
        } else {
            let mut props = HashMap::new();
            props.insert(key, value.into());
            self.application_properties = Some(props);
        }
        self
    }

    /// Execute closure if value is Some value
    pub fn if_some<T, F>(self, value: &Option<T>, f: F) -> Self
    where
        F: Fn(Self, &T) -> Self,
    {
        if let Some(ref val) = value {
            f(self, val)
        } else {
            self
        }
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
                Section::AmqpSequence(val) => {
                    message.sequence = Some(val);
                }
                Section::AmqpValue(val) => {
                    message.value = Some(val);
                }
                Section::Data(val) => {
                    message.data = Some(val);
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
        let mut size = self.application_data.encoded_size();
        if let Some(ref h) = self.header {
            size += h.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref da) = self.delivery_annotations {
            size += SECTION_PREFIX_LENGTH + da.encoded_size();
        }
        if let Some(ref ma) = self.message_annotations {
            size += ma.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref p) = self.properties {
            size += p.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref ap) = self.application_properties {
            size += ap.encoded_size() + SECTION_PREFIX_LENGTH;
        }
        if let Some(ref f) = self.footer {
            size += f.encoded_size() + SECTION_PREFIX_LENGTH;
        }

        size
    }

    fn encode(&self, dst: &mut BytesMut) {
        if let Some(ref h) = self.header {
            Descriptor::Ulong(112).encode(dst);
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
            Descriptor::Ulong(115).encode(dst);
            p.encode(dst);
        }
        if let Some(ref ap) = self.application_properties {
            Descriptor::Ulong(116).encode(dst);
            ap.encode(dst);
        }
        if let Some(ref v) = self.data {
            Descriptor::Ulong(117).encode(dst);
            v.encode(dst);
        }
        if let Some(ref s) = self.sequence {
            Descriptor::Ulong(118).encode(dst);
            s.encode(dst);
        }
        if let Some(ref v) = self.value {
            Descriptor::Ulong(119).encode(dst);
            v.encode(dst);
        }

        self.application_data.encode(dst);

        if let Some(ref f) = self.footer {
            Descriptor::Ulong(120).encode(dst);
            f.encode(dst);
        }
    }
}

impl Default for Message {
    fn default() -> Message {
        Message {
            message_format: None,
            header: None,
            delivery_annotations: None,
            message_annotations: None,
            properties: None,
            application_properties: None,
            application_data: MessageBody::Data(Bytes::new()),
            sequence: None,
            value: None,
            data: None,
            footer: None,
        }
    }
}

impl Encode for MessageBody {
    fn encoded_size(&self) -> usize {
        match *self {
            MessageBody::Data(ref d) => d.encoded_size() + SECTION_PREFIX_LENGTH,
            MessageBody::DataVec(ref ds) => ds
                .iter()
                .fold(0, |a, d| a + d.encoded_size() + SECTION_PREFIX_LENGTH),
            MessageBody::SequenceVec(ref seqs) => seqs
                .iter()
                .fold(0, |a, seq| a + seq.encoded_size() + SECTION_PREFIX_LENGTH),
            MessageBody::Messages(ref msgs) => msgs
                .iter()
                .fold(0, |a, m| a + m.encoded_size() + SECTION_PREFIX_LENGTH),
            MessageBody::Value(ref val) => val.encoded_size() + SECTION_PREFIX_LENGTH,
        }
    }

    fn encode(&self, dst: &mut BytesMut) {
        match self {
            MessageBody::Data(d) => {
                Descriptor::Ulong(117).encode(dst);
                d.encode(dst)
            }
            MessageBody::DataVec(ds) => ds.into_iter().for_each(|d| {
                Descriptor::Ulong(117).encode(dst);
                d.encode(dst);
            }),
            MessageBody::Messages(msgs) => msgs.into_iter().for_each(|m| {
                Descriptor::Ulong(117).encode(dst);
                m.encode(dst)
            }),
            MessageBody::SequenceVec(seqs) => seqs.into_iter().for_each(|seq| {
                Descriptor::Ulong(118).encode(dst);
                seq.encode(dst)
            }),
            MessageBody::Value(val) => {
                Descriptor::Ulong(119).encode(dst);
                val.encode(dst)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::codec::{Decode, Encode};
    use crate::errors::AmqpCodecError;

    use super::Message;

    #[test]
    fn test_message_app_properties() -> Result<(), AmqpCodecError> {
        let msg = Message::default().set_app_property(string::String::from_str("test"), 1);

        let mut buf = BytesMut::with_capacity(msg.encoded_size());
        msg.encode(&mut buf);

        let msg2 = Message::decode(&buf)?.1;
        let props = msg2.application_properties.as_ref().unwrap();
        assert_eq!(
            *props.get(&string::String::from_str("test")).unwrap(),
            1.into()
        );

        Ok(())
    }
}
