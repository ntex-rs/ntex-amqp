use std::collections::HashMap;
use bytes::{Bytes, BytesMut};
use protocol::*;
use types::*;
use codec::Encode;

#[derive(Debug, Clone)]
pub struct Message {
    pub message_format: Option<MessageFormat>,
    pub header: Option<Header>,
    pub delivery_annotations: Option<Annotations>,
    pub message_annotations: Option<Annotations>,
    pub properties: Option<Properties>,
    pub application_properties: Option<HashMap<ByteStr, Variant>>,
    pub application_data: MessageBody,
    pub footer: Option<Annotations>
}

#[derive(Debug, Clone)]
pub enum MessageBody {
    Data(Bytes),
    DataVec(Vec<Bytes>),
    SequenceVec(Vec<List>),
    Value(Variant)
}

const SECTION_PREFIX_LENGTH: usize = 3;

impl Message {
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

        self.application_data.encode(&mut dst);

        if let Some(f) = self.footer {
            Section::Footer(f).encode(&mut dst);
        }

        dst.freeze()
    }

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
            footer: None,
        }
    }
}

impl MessageBody {
    pub(crate) fn encoded_size(&self) -> usize {
        match *self {
            MessageBody::Data(ref d) => d.encoded_size() + SECTION_PREFIX_LENGTH,
            MessageBody::DataVec(ref ds) => ds.iter().fold(0, |a, d| a + d.encoded_size() + SECTION_PREFIX_LENGTH),
            MessageBody::SequenceVec(ref seqs) => seqs.iter().fold(0, |a, seq| seq.encoded_size() + SECTION_PREFIX_LENGTH),
            MessageBody::Value(ref val) => val.encoded_size() + SECTION_PREFIX_LENGTH
        }
    }

    pub(crate) fn encode(self, dst: &mut BytesMut) {
        match self {
            MessageBody::Data(d) => Section::Data(d).encode(dst),
            MessageBody::DataVec(ds) => ds.into_iter().for_each(|d| d.encode(dst)),
            MessageBody::SequenceVec(seqs) => seqs.into_iter().for_each(|seq| seq.encode(dst)),
            MessageBody::Value(val) => val.encode(dst)
        }
    }
}