use bytes::{Bytes, BytesMut};
use std::collections::HashMap;

use amqp::codec::{Decode, Encode};
use amqp::protocol::{Annotations, Header, MessageFormat, Properties, Section};
use amqp::types::{ByteStr, List, Variant};
use amqp::AmqpParseError;

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
    Value(Variant),
}

const SECTION_PREFIX_LENGTH: usize = 3;

impl Message {
    /// Add property
    pub fn properties<F>(mut self, f: F) -> Self
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
    pub fn app_property<V: Into<Variant>>(mut self, key: ByteStr, value: V) -> Self {
        if let Some(ref mut props) = self.application_properties {
            props.insert(key, value.into());
        } else {
            let mut props = HashMap::new();
            props.insert(key, value.into());
            self.application_properties = Some(props);
        }
        self
    }

    pub(crate) fn serialize(self) -> Bytes {
        let mut dst = BytesMut::with_capacity(self.encoded_size());
        if let Some(h) = self.header {
            Section::Header(h).encode(&mut dst);
        }
        if let Some(da) = self.delivery_annotations {
            Section::DeliveryAnnotations(da).encode(&mut dst);
        }
        if let Some(ma) = self.message_annotations {
            Section::MessageAnnotations(ma).encode(&mut dst);
        }
        if let Some(p) = self.properties {
            Section::Properties(p).encode(&mut dst);
        }
        if let Some(ap) = self.application_properties {
            Section::ApplicationProperties(ap).encode(&mut dst);
        }
        if let Some(s) = self.sequence {
            Section::AmqpSequence(s).encode(&mut dst);
        }
        if let Some(v) = self.value {
            Section::AmqpValue(v).encode(&mut dst);
        }
        if let Some(v) = self.data {
            Section::Data(v).encode(&mut dst);
        }

        self.application_data.encode(&mut dst);

        if let Some(f) = self.footer {
            Section::Footer(f).encode(&mut dst);
        }

        dst.freeze()
    }

    pub(crate) fn deserialize(src: &Bytes) -> Result<Message, AmqpParseError> {
        let mut message = Message::default();

        let mut input: &[u8] = &src;
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
        Ok(message)
    }

    pub(crate) fn encoded_size(&self) -> usize {
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

impl MessageBody {
    pub(crate) fn encoded_size(&self) -> usize {
        match *self {
            MessageBody::Data(ref d) => d.encoded_size() + SECTION_PREFIX_LENGTH,
            MessageBody::DataVec(ref ds) => ds
                .iter()
                .fold(0, |a, d| a + d.encoded_size() + SECTION_PREFIX_LENGTH),
            MessageBody::SequenceVec(ref seqs) => seqs
                .iter()
                .fold(0, |a, seq| a + seq.encoded_size() + SECTION_PREFIX_LENGTH),
            MessageBody::Value(ref val) => val.encoded_size() + SECTION_PREFIX_LENGTH,
        }
    }

    pub(crate) fn encode(self, dst: &mut BytesMut) {
        match self {
            MessageBody::Data(d) => Section::Data(d).encode(dst),
            MessageBody::DataVec(ds) => ds.into_iter().for_each(|d| Section::Data(d).encode(dst)),
            MessageBody::SequenceVec(seqs) => seqs
                .into_iter()
                .for_each(|seq| Section::AmqpSequence(seq).encode(dst)),
            MessageBody::Value(val) => Section::AmqpValue(val).encode(dst),
        }
    }
}
