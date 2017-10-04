#![allow(unused_variables, unused_assignments, unused_mut, unreachable_patterns)]

use bytes::{BigEndian, BufMut, Bytes, BytesMut};
use ::errors::Result;
use uuid::Uuid;
use ::codec::{self, decode_format_code, decode_list_header, Decode, DecodeFormatted, Encode};
use std::u8;
use super::*;

{{#each defs.provides as |provide|}}
{{#if provide.described}}
#[derive(Clone, Debug, PartialEq)]
pub enum {{provide.name}} {
{{#each provide.options as |option|}}
    {{option.ty}}({{option.ty}}),
{{/each}}
}
impl DecodeFormatted for {{provide.name}} {
    fn decode_with_format(input: &[u8], fmt: u8) -> Result<(&[u8], Self)> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let (input, descriptor) = Descriptor::decode(input)?;
        match descriptor {
            {{#each provide.options as |option|}}
            Descriptor::Ulong({{option.descriptor.code}}) => decode_{{snake option.ty}}_inner(input).map(|(i, r)| (i, {{provide.name}}::{{option.ty}}(r))),
            {{/each}}
            {{#each provide.options as |option|}}
            Descriptor::Symbol(ref a) if a.as_str() == "{{option.descriptor.name}}" => decode_{{snake option.ty}}_inner(input).map(|(i, r)| (i, {{provide.name}}::{{option.ty}}(r))),
            {{/each}}
            _ => Err(ErrorKind::InvalidDescriptor(descriptor).into())
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
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum {{enum.name}} {
{{#each enum.items as |item|}}
    {{item.name}},
{{/each}}
}
{{#if enum.is_symbol}}
impl {{enum.name}} {
    pub fn try_from(v: &Symbol) -> Result<Self> {
        match v.as_str() {
            {{#each enum.items as |item|}}
            "{{item.value}}" => Ok({{enum.name}}::{{item.name}}),
            {{/each}}
            _ => Err("unknown {{enum.name}} option.".into())
        }
    }
}
impl DecodeFormatted for {{enum.name}} {
    fn decode_with_format(input: &[u8], fmt: u8) -> Result<(&[u8], Self)> {
        let (input, base) = Symbol::decode_with_format(input, fmt)?;
        Ok((input, Self::try_from(&base)?))
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
            {{enum.name}}::{{item.name}} => Symbol::from_static("{{item.value}}").encode(buf),
            {{/each}}
        }
    }
}
{{else}}
impl {{enum.name}} {
    pub fn try_from(v: {{enum.ty}}) -> Result<Self> {
        match v {
            {{#each enum.items as |item|}}
            {{item.value}} => Ok({{enum.name}}::{{item.name}}),
            {{/each}}
            _ => Err("unknown {{enum.name}} option.".into())
        }
    }
}
impl DecodeFormatted for {{enum.name}} {
    fn decode_with_format(input: &[u8], fmt: u8) -> Result<(&[u8], Self)> {
        let (input, base) = {{enum.ty}}::decode_with_format(input, fmt)?;
        Ok((input, Self::try_from(base)?))
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
fn decode_{{snake dr.name}}_inner(input: &[u8]) -> Result<(&[u8], {{dr.name}})> {
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
#[derive(Clone, Debug, PartialEq)]
pub struct {{list.name}} {
    {{#each list.fields as |field|}}
    {{#if field.optional}}
    pub {{field.name}}: Option<{{{field.ty}}}>,
    {{else}}
    pub {{field.name}}: {{{field.ty}}},
    {{/if}}
    {{/each}}
}

impl {{list.name}} {
    {{#each list.fields as |field|}}
        {{#if field.is_str}}
            {{#if field.optional}}
                pub fn {{field.name}}(&self) -> Option<&str> {
                    match self.{{field.name}} {
                        None => None,
                        Some(ref s) => Some(s.as_str())
                    }
                }
            {{else}}
                pub fn {{field.name}}(&self) -> &str { self.{{field.name}}.as_str() }
            {{/if}}
        {{else}}
            {{#if field.is_ref}}
                {{#if field.optional}}
                    pub fn {{field.name}}(&self) -> Option<&{{{field.ty}}}> { self.{{field.name}}.as_ref() }
                {{else}}
                    pub fn {{field.name}}(&self) -> &{{{field.ty}}} { &self.{{field.name}} }
                {{/if}}
            {{else}}
                {{#if field.optional}}
                    pub fn {{field.name}}(&self) -> Option<{{{field.ty}}}> { self.{{field.name}} }
                {{else}}
                    pub fn {{field.name}}(&self) -> {{{field.ty}}} { self.{{field.name}} }
                {{/if}}
            {{/if}}
        {{/if}}
    {{/each}}

    const FIELD_COUNT: usize = 0 {{#each list.fields as |field|}} + 1{{/each}};
}
fn decode_{{snake list.name}}_inner(input: &[u8]) -> Result<(&[u8], {{list.name}})> {
    let (input, format) = decode_format_code(input)?;
    let (input, header) = decode_list_header(input, format)?;
    let size = header.size as usize;
    decode_check_len!(input, size);
    {{#if list.fields}}
    let (mut input, remainder) = input.split_at(size);
    let mut count = header.count;
    {{#each list.fields as |field|}}
    {{#if field.optional}}
    let {{field.name}}: Option<{{{field.ty}}}>;
    if count > 0 {
        let decoded = Option::<{{{field.ty}}}>::decode(input)?;
        input = decoded.0;
        {{field.name}} = decoded.1;
        count -= 1;
    }
    else {
        {{field.name}} = None;
    }
    {{else}}
    let {{field.name}}: {{{field.ty}}};
    if count > 0 {
        {{#if field.default}}
        let (in1, decoded) = Option::<{{{field.ty}}}>::decode(input)?;
        {{field.name}} = decoded.unwrap_or({{field.default}});
        {{else}}
        let (in1, decoded) = {{{field.ty}}}::decode(input)?;
        {{field.name}} = decoded;
        {{/if}}
        input = in1;
        count -= 1;
    }
    else {
        {{#if field.default}}
        {{field.name}} = {{field.default}};
        {{else}}
        bail!("Required field {{field.name}} was omitted.");
        {{/if}}
    }
    {{/if}}
    {{/each}}
    {{else}}
    let remainder = &input[size..];
    {{/if}}
    Ok((remainder, {{list.name}} {
    {{#each list.fields as |field|}}
    {{field.name}},
    {{/each}}
    }))
}

fn encoded_size_{{snake list.name}}_inner(list: &{{list.name}}) -> usize {
    let content_size = 0 {{#each list.fields as |field|}} + list.{{field.name}}.encoded_size(){{/each}};
    // header: 0x00 0x53 <descriptor code> format_code size count
    (if content_size + 1 > u8::MAX as usize { 12 } else { 6 })
        + content_size
}
fn encode_{{snake list.name}}_inner(list: &{{list.name}}, buf: &mut BytesMut) {
    Descriptor::Ulong({{list.descriptor.code}}).encode(buf);
    let content_size = 0 {{#each list.fields as |field|}} + list.{{field.name}}.encoded_size(){{/each}};
    if content_size + 1 > u8::MAX as usize {
        buf.put_u8(codec::FORMATCODE_LIST32);
        buf.put_u32::<BigEndian>((content_size + 4) as u32); // +4 for 4 byte count
        buf.put_u32::<BigEndian>({{list.name}}::FIELD_COUNT as u32);
    }
    else {
        buf.put_u8(codec::FORMATCODE_LIST8);
        buf.put_u8((content_size + 1) as u8);
        buf.put_u8({{list.name}}::FIELD_COUNT as u8);
    }
    {{#each list.fields as |field|}}
    list.{{field.name}}.encode(buf);
    {{/each}}
}

impl DecodeFormatted for {{list.name}} {
    fn decode_with_format(input: &[u8], fmt: u8) -> Result<(&[u8], Self)> {
        validate_code!(fmt, codec::FORMATCODE_DESCRIBED);
        let (input, descriptor) = Descriptor::decode(input)?;
        if descriptor != Descriptor::Ulong({{list.descriptor.code}})
            && descriptor != Descriptor::Symbol(Symbol::from_static("{{list.descriptor.name}}"))
        {
            bail!("Invalid descriptor.");
        }
        decode_{{snake list.name}}_inner(input)
    }
}

impl Encode for {{list.name}} {
    fn encoded_size(&self) -> usize { encoded_size_{{snake list.name}}_inner(self) }

    fn encode(&self, buf: &mut BytesMut) { encode_{{snake list.name}}_inner(self, buf) }
}
{{/each}}