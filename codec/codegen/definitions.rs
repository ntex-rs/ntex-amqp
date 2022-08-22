#![allow(unused_assignments, unused_variables, unreachable_patterns)]

use std::u8;
use derive_more::From;
use ntex_bytes::{BufMut, Bytes, BytesMut, ByteString};
use uuid::Uuid;

use super::*;
use crate::error::AmqpParseError;
use crate::codec::{self, decode_format_code, decode_list_header, Decode, DecodeFormatted, Encode};

#[derive(Clone, Debug, PartialEq, Eq, From)]
pub enum Frame {
    Open(Open),
    Begin(Begin),
    Attach(Attach),
    Flow(Flow),
    Transfer(Transfer),
    Disposition(Disposition),
    Detach(Detach),
    End(End),
    Close(Close),
    Empty,
}

impl Frame {
    pub fn name(&self) -> &'static str {
        match self {
            Frame::Open(_) => "Open",
            Frame::Begin(_) => "Begin",
            Frame::Attach(_) => "Attach",
            Frame::Flow(_) => "Flow",
            Frame::Transfer(_) => "Transfer",
            Frame::Disposition(_) => "Disposition",
            Frame::Detach(_) => "Detach",
            Frame::End(_) => "End",
            Frame::Close(_) => "Close",
            Frame::Empty => "Empty",
        }
    }
}

impl Decode for Frame {
    fn decode(input: &mut Bytes) -> Result<Self, AmqpParseError> {
        if input.is_empty() {
            Ok(Frame::Empty)
        } else {
            let fmt = decode_format_code(input)?;
            validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
            let descriptor = Descriptor::decode(input)?;
            match descriptor {
                Descriptor::Ulong(16) => decode_open_inner(input).map(Frame::Open),
                Descriptor::Ulong(17) => {
                    decode_begin_inner(input).map(Frame::Begin)
                }
                Descriptor::Ulong(18) => {
                    decode_attach_inner(input).map(Frame::Attach)
                }
                Descriptor::Ulong(19) => decode_flow_inner(input).map(Frame::Flow),
                Descriptor::Ulong(20) => {
                    decode_transfer_inner(input).map(Frame::Transfer)
                }
                Descriptor::Ulong(21) => {
                    decode_disposition_inner(input).map(Frame::Disposition)
                }
                Descriptor::Ulong(22) => {
                    decode_detach_inner(input).map(Frame::Detach)
                }
                Descriptor::Ulong(23) => decode_end_inner(input).map(Frame::End),
                Descriptor::Ulong(24) => {
                    decode_close_inner(input).map(Frame::Close)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:open:list" => {
                    decode_open_inner(input).map(Frame::Open)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:begin:list" => {
                    decode_begin_inner(input).map(Frame::Begin)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:attach:list" => {
                    decode_attach_inner(input).map(Frame::Attach)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:flow:list" => {
                    decode_flow_inner(input).map(Frame::Flow)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:transfer:list" => {
                    decode_transfer_inner(input).map(Frame::Transfer)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:disposition:list" => {
                    decode_disposition_inner(input).map(Frame::Disposition)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:detach:list" => {
                    decode_detach_inner(input).map(Frame::Detach)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:end:list" => {
                    decode_end_inner(input).map(Frame::End)
                }
                Descriptor::Symbol(ref a) if a.as_str() == "amqp:close:list" => {
                    decode_close_inner(input).map(Frame::Close)
                }
                _ => Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor))),
            }
        }
    }
}

impl Encode for Frame {
    fn encoded_size(&self) -> usize {
        match *self {
            Frame::Open(ref v) => encoded_size_open_inner(v),
            Frame::Begin(ref v) => encoded_size_begin_inner(v),
            Frame::Attach(ref v) => encoded_size_attach_inner(v),
            Frame::Flow(ref v) => encoded_size_flow_inner(v),
            Frame::Transfer(ref v) => encoded_size_transfer_inner(v),
            Frame::Disposition(ref v) => encoded_size_disposition_inner(v),
            Frame::Detach(ref v) => encoded_size_detach_inner(v),
            Frame::End(ref v) => encoded_size_end_inner(v),
            Frame::Close(ref v) => encoded_size_close_inner(v),
            Frame::Empty => 0,
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            Frame::Open(ref v) => encode_open_inner(v, buf),
            Frame::Begin(ref v) => encode_begin_inner(v, buf),
            Frame::Attach(ref v) => encode_attach_inner(v, buf),
            Frame::Flow(ref v) => encode_flow_inner(v, buf),
            Frame::Transfer(ref v) => encode_transfer_inner(v, buf),
            Frame::Disposition(ref v) => encode_disposition_inner(v, buf),
            Frame::Detach(ref v) => encode_detach_inner(v, buf),
            Frame::End(ref v) => encode_end_inner(v, buf),
            Frame::Close(ref v) => encode_close_inner(v, buf),
            Frame::Empty => (),
        }
    }
}

{{#each defs.provides as |provide|}}
{{#if provide.described}}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum {{provide.name}} {
{{#each provide.options as |option|}}
    {{option.ty}}({{option.ty}}),
{{/each}}
}

impl DecodeFormatted for {{provide.name}} {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        match descriptor {
            {{#each provide.options as |option|}}
            Descriptor::Ulong({{option.descriptor.code}}) => decode_{{snake option.ty}}_inner(input).map({{provide.name}}::{{option.ty}}),
            {{/each}}
            {{#each provide.options as |option|}}
            Descriptor::Symbol(ref a) if a.as_str() == "{{option.descriptor.name}}" => decode_{{snake option.ty}}_inner(input).map({{provide.name}}::{{option.ty}}),
            {{/each}}
            _ => Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        }
    }
}

impl Encode for {{provide.name}} {
    fn encoded_size(&self) -> usize {
        match *self {
            {{#each provide.options as |option|}}
            {{provide.name}}::{{option.ty}}(ref v) => encoded_size_{{snake option.ty}}_inner(v),
            {{/each}}
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            {{#each provide.options as |option|}}
            {{provide.name}}::{{option.ty}}(ref v) => encode_{{snake option.ty}}_inner(v, buf),
            {{/each}}
        }
    }
}
{{/if}}
{{/each}}

{{#each defs.aliases as |alias|}}
pub type {{alias.name}} = {{alias.source}};
{{/each}}

{{#each defs.enums as |enum|}}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum {{enum.name}} {
{{#each enum.items as |item|}}
    {{item.name}},
{{/each}}
}
{{#if enum.is_symbol}}
impl {{enum.name}} {
    pub fn try_from(v: &Symbol) -> Result<Self, AmqpParseError> {
        match v.as_str() {
            {{#each enum.items as |item|}}
            "{{item.value}}" => Ok({{enum.name}}::{{item.name}}),
            {{/each}}
            _ => Err(AmqpParseError::UnknownEnumOption("{{enum.name}}"))
        }
    }
}
impl DecodeFormatted for {{enum.name}} {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = Symbol::decode_with_format(input, fmt)?;
        Self::try_from(&base)
    }
}
impl Encode for {{enum.name}} {
    fn encoded_size(&self) -> usize {
        match *self {
            {{#each enum.items as |item|}}
            {{enum.name}}::{{item.name}} => {{item.value_len}} + 2,
            {{/each}}
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            {{#each enum.items as |item|}}
            {{enum.name}}::{{item.name}} => StaticSymbol("{{item.value}}").encode(buf),
            {{/each}}
        }
    }
}
{{else}}
impl {{enum.name}} {
    pub fn try_from(v: {{enum.ty}}) -> Result<Self, AmqpParseError> {
        match v {
            {{#each enum.items as |item|}}
            {{item.value}} => Ok({{enum.name}}::{{item.name}}),
            {{/each}}
            _ => Err(AmqpParseError::UnknownEnumOption("{{enum.name}}"))
        }
    }
}
impl DecodeFormatted for {{enum.name}} {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        let base = {{enum.ty}}::decode_with_format(input, fmt)?;
        Self::try_from(base)
    }
}
impl Encode for {{enum.name}} {
    fn encoded_size(&self) -> usize {
        match *self {
            {{#each enum.items as |item|}}
            {{enum.name}}::{{item.name}} => {
                let v : {{enum.ty}} = {{item.value}};
                v.encoded_size()
            },
            {{/each}}
        }
    }
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            {{#each enum.items as |item|}}
            {{enum.name}}::{{item.name}} => {
                let v : {{enum.ty}} = {{item.value}};
                v.encode(buf);
            },
            {{/each}}
        }
    }
}
{{/if}}
{{/each}}

{{#each defs.described_restricted as |dr|}}
type {{dr.name}} = {{dr.ty}};
fn decode_{{snake dr.name}}_inner(input: &mut Bytes) -> Result<{{dr.name}}, AmqpParseError> {
    {{dr.name}}::decode(input)
}
fn encoded_size_{{snake dr.name}}_inner(dr: &{{dr.name}}) -> usize {
    // descriptor size + actual size
    3 + dr.encoded_size()
}
fn encode_{{snake dr.name}}_inner(dr: &{{dr.name}}, buf: &mut BytesMut) {
    Descriptor::Ulong({{dr.descriptor.code}}).encode(buf);
    dr.encode(buf);
}
{{/each}}

{{#each defs.lists as |list|}}
{{#if list.boxed}}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct {{list.name}}(pub Box<{{list.name}}Inner>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct {{list.name}}Builder(pub Box<{{list.name}}Inner>);

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct {{list.name}}Inner {
    {{#each list.fields as |field|}}
    {{#if field.optional}}
    pub {{field.name}}: Option<{{{field.ty}}}>,
    {{else}}
    pub {{field.name}}: {{{field.ty}}},
    {{/if}}
    {{/each}}
    {{#if list.transfer}}
    pub body: Option<TransferBody>,
    {{/if}}
}
{{else}}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct {{list.name}} {
    {{#each list.fields as |field|}}
    {{#if field.optional}}
    pub {{field.name}}: Option<{{{field.ty}}}>,
    {{else}}
    pub {{field.name}}: {{{field.ty}}},
    {{/if}}
    {{/each}}
    {{#if list.transfer}}
    pub body: Option<TransferBody>,
    {{/if}}
}
{{/if}}

impl {{list.name}} {
    {{#if list.boxed}}
    pub fn build() -> {{list.name}}Builder {
        {{list.name}}Builder(Box::new({{list.name}}Inner::default()))
    }
    {{/if}}

    {{#each list.fields as |field|}}
        {{#if field.is_str}}
            {{#if field.optional}}
                #[inline]
                pub fn {{field.name}}(&self) -> Option<&str> {
                    match self.{{list.inner}}{{field.name}} {
                        None => None,
                        Some(ref s) => Some(s.as_str())
                    }
                }
            {{else}}
                #[inline]
                pub fn {{field.name}}(&self) -> &str { self.{{list.inner}}{{field.name}}.as_str() }
            {{/if}}
        {{else}}
            {{#if field.is_ref}}
                {{#if field.optional}}
                   #[inline]
                    pub fn {{field.name}}(&self) -> Option<&{{{field.ty}}}> { self.{{list.inner}}{{field.name}}.as_ref() }
                {{else}}
                    #[inline]
                    pub fn {{field.name}}(&self) -> &{{{field.ty}}} { &self.{{list.inner}}{{field.name}} }
                {{/if}}
            {{else}}
                {{#if field.optional}}
                    #[inline]
                    pub fn {{field.name}}(&self) -> Option<{{{field.ty}}}> { self.{{list.inner}}{{field.name}} }
                {{else}}
                   #[inline]
                    pub fn {{field.name}}(&self) -> {{{field.ty}}} { self.{{list.inner}}{{field.name}} }
                {{/if}}
            {{/if}}
        {{/if}}

    {{#if field.optional}}
    #[inline]
    pub fn {{field.name}}_mut(&mut self) -> Option<&mut {{field.ty}}> {
        self.{{list.inner}}{{field.name}}.as_mut()
    }
    {{else}}
    #[inline]
    pub fn {{field.name}}_mut(&mut self) -> &mut {{field.ty}} {
        &mut self.{{list.inner}}{{field.name}}
    }
    {{/if}}
    {{/each}}

    {{#if list.props}}
    #[inline]
    pub fn get_properties_mut(&mut self) -> &mut Fields {
        if self.{{list.inner}}properties.is_none() {
            self.{{list.inner}}properties = Some(Fields::default());
        }
        self.{{list.inner}}properties.as_mut().unwrap()
    }
    {{/if}}

    {{#if list.transfer}}
    #[inline]
    pub fn body(&self) -> Option<&TransferBody> {
        self.{{list.inner}}body.as_ref()
    }
    {{/if}}

    {{#if list.boxed}}
    pub fn into_inner(self) -> Box<{{list.name}}Inner> {
        self.0
    }
    {{/if}}

    #[allow(clippy::identity_op)]
    const FIELD_COUNT: usize = 0 {{#each list.fields as |field|}} + 1{{/each}};
}

{{#if list.boxed}}
impl {{list.name}}Builder {
    {{#each list.fields as |field|}}
    #[inline]
    pub fn {{field.name}}(mut self, val: {{field.ty}}) -> Self {
        {{#if field.optional}}
        self.0.{{field.name}} = Some(val);
        {{else}}
        self.0.{{field.name}} = val;
        {{/if}}
        self
    }
    {{/each}}

    {{#if list.transfer}}
    #[inline]
    pub fn body(mut self, body: TransferBody) -> Self {
        self.0.body = Some(body);
        self
    }
    {{/if}}

    pub fn finish(self) -> {{list.name}} {
        {{list.name}}(self.0)
    }
}
{{/if}}

#[allow(unused_mut)]
fn decode_{{snake list.name}}_inner(input: &mut Bytes) -> Result<{{list.name}}, AmqpParseError> {
    let format = decode_format_code(input)?;
    let header = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    {{#if list.fields}}
    let mut data = input.split_to(size);
    let mut count = header.count;
    {{#each list.fields as |field|}}
    {{#if field.optional}}
    let {{field.name}}: Option<{{{field.ty}}}>;
    if count > 0 {
        {{field.name}} = Option::<{{{field.ty}}}>::decode(&mut data)?;
        count -= 1;
    }
    else {
        {{field.name}} = None;
    }
    {{else}}
    let {{field.name}}: {{{field.ty}}};
    if count > 0 {
        {{#if field.default}}
        let decoded = Option::<{{{field.ty}}}>::decode(&mut data)?;
        {{field.name}} = decoded.unwrap_or({{field.default}});
        {{else}}
        let decoded = {{{field.ty}}}::decode(&mut data)?;
        {{field.name}} = decoded;
        {{/if}}
        count -= 1;
    }
    else {
        {{#if field.default}}
        {{field.name}} = {{field.default}};
        {{else}}
        return Err(AmqpParseError::RequiredFieldOmitted("{{field.name}}"))
        {{/if}}
    }
    {{/if}}
    {{/each}}
    {{else}}
    input.split_to(size);
    {{/if}}

    {{#if list.transfer}}
    let body = if input.is_empty() {
            None
        } else {
            Some(input.split_to(input.len()).into())
        };
    {{/if}}

    {{#if list.boxed}}
    Ok({{list.name}}(Box::new({{list.name}}Inner {
    {{#each list.fields as |field|}}
    {{field.name}},
    {{/each}}
        {{#if list.transfer}}
        body
        {{/if}}
    })))
    {{else}}
    Ok({{list.name}} {
    {{#each list.fields as |field|}}
    {{field.name}},
    {{/each}}
        {{#if list.transfer}}
        body
        {{/if}}
    })
    {{/if}}
}

fn encoded_size_{{snake list.name}}_inner(list: &{{list.name}}) -> usize {
    #[allow(clippy::identity_op)]
    let content_size = 0 {{#each list.fields as |field|}} + list.{{list.inner}}{{field.name}}.encoded_size(){{/each}};
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize { 12 } else { 6 })
        + content_size

    {{#if list.transfer}}
    + list.{{list.inner}}body.as_ref().map(|b| b.len()).unwrap_or(0)
    {{/if}}
}
fn encode_{{snake list.name}}_inner(list: &{{list.name}}, buf: &mut BytesMut) {
    Descriptor::Ulong({{list.descriptor.code}}).encode(buf);
    #[allow(clippy::identity_op)]
    let content_size = 0 {{#each list.fields as |field|}} + list.{{list.inner}}{{field.name}}.encoded_size(){{/each}};
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32({{list.name}}::FIELD_COUNT as u32);
    }
    else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8({{list.name}}::FIELD_COUNT as u8);
    }
    {{#each list.fields as |field|}}
    list.{{list.inner}}{{field.name}}.encode(buf);
    {{/each}}
    {{#if list.transfer}}
    if let Some(body) = list.body() {
        body.encode(buf)
    }
    {{/if}}
}

impl DecodeFormatted for {{list.name}} {
    fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let descriptor = Descriptor::decode(input)?;
        let is_match = match descriptor {
            Descriptor::Ulong(val) => val == {{list.descriptor.code}},
            Descriptor::Symbol(ref sym) => sym.as_bytes() == b"{{list.descriptor.name}}",
        };
        if !is_match {
            Err(AmqpParseError::InvalidDescriptor(Box::new(descriptor)))
        } else {
            decode_{{snake list.name}}_inner(input)
        }
    }
}

impl Encode for {{list.name}} {
    fn encoded_size(&self) -> usize { encoded_size_{{snake list.name}}_inner(self) }

    fn encode(&self, buf: &mut BytesMut) { encode_{{snake list.name}}_inner(self, buf) }
}
{{/each}}
