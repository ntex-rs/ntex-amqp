use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
use ntex_bytes::{ByteString, Bytes, BytesMut};
use ordered_float::OrderedFloat;
use uuid::Uuid;

use crate::types::{Array, Descriptor, List, Str, Symbol};
use crate::{protocol::Annotations, HashMap};
use crate::{AmqpParseError, Decode, Encode};

/// Represents an AMQP type for use in polymorphic collections
#[derive(Debug, Eq, PartialEq, Hash, Clone, From)]
pub enum Variant {
    /// Indicates an empty value.
    Null,

    /// Represents a true or false value.
    Boolean(bool),

    /// Integer in the range 0 to 2^8 - 1 inclusive.
    Ubyte(u8),

    /// Integer in the range 0 to 2^16 - 1 inclusive.
    Ushort(u16),

    /// Integer in the range 0 to 2^32 - 1 inclusive.
    Uint(u32),

    /// Integer in the range 0 to 2^64 - 1 inclusive.
    Ulong(u64),

    /// Integer in the range 0 to 2^7 - 1 inclusive.
    Byte(i8),

    /// Integer in the range 0 to 2^15 - 1 inclusive.
    Short(i16),

    /// Integer in the range 0 to 2^32 - 1 inclusive.
    Int(i32),

    /// Integer in the range 0 to 2^64 - 1 inclusive.
    Long(i64),

    /// 32-bit floating point number (IEEE 754-2008 binary32).
    Float(OrderedFloat<f32>),

    /// 64-bit floating point number (IEEE 754-2008 binary64).
    Double(OrderedFloat<f64>),

    /// 32-bit decimal number, represented per IEEE 754-2008 decimal32 specification.
    Decimal32([u8; 4]),

    /// 64-bit decimal number, represented per IEEE 754-2008 decimal64 specification.
    Decimal64([u8; 8]),

    /// 128-bit decimal number, represented per IEEE 754-2008 decimal128 specification.
    Decimal128([u8; 16]),

    /// A single Unicode character.
    Char(char),

    /// An absolute point in time.
    /// Represents an approximate point in time using the Unix time encoding of
    /// UTC with a precision of milliseconds. For example, 1311704463521
    /// represents the moment 2011-07-26T18:21:03.521Z.
    Timestamp(DateTime<Utc>),

    /// A universally unique identifier as defined by RFC-4122 section 4.1.2
    Uuid(Uuid),

    /// A sequence of octets.
    Binary(Bytes),

    /// A sequence of Unicode characters
    String(Str),

    /// Symbolic values from a constrained domain.
    Symbol(Symbol),

    /// List
    List(List),

    /// Map
    Map(VariantMap),

    /// Array
    Array(Array),

    /// Described value of primitive type. See `Variant::DescribedCompound` for
    Described((Descriptor, Box<Variant>)),

    /// Described value of compound or array type. See `Variant::DescribedCompound` for details.
    DescribedCompound(DescribedCompound),
}

/// Represents a compound value with a descriptor. The value contains data starting with format code for the underlying AMQP type
/// (right after the descriptor).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DescribedCompound {
    descriptor: Descriptor,
    pub(crate) data: Bytes,
}

impl DescribedCompound {
    /// Creates a representation of described value of compound value type based on `T` type's AMQP encoding.
    /// `T`'s implementation of `Encode` is expected to produce the binary representation of the T in an underlying AMQP type value, starting from the format code.
    /// For instance, if the described value is to be represented as an AMQP list with 1 ubyte field with a value of 3:
    /// ```text
    /// 0x00 0xa3 0x07 "foo:bar" 0xc0 0x02 0x01 0x50 0x03
    /// ```
    /// The `T::encode` method is expected to produce the following output:
    /// ```text
    /// 0xc0 0x02 0x01 0x50 0x03
    /// ```
    pub fn create<T: Encode>(descriptor: Descriptor, value: T) -> Self {
        let size = value.encoded_size();
        let mut buf = BytesMut::with_capacity(size);
        value.encode(&mut buf);
        DescribedCompound {
            descriptor,
            data: buf.freeze(),
        }
    }

    pub(crate) fn new(descriptor: Descriptor, data: Bytes) -> Self {
        DescribedCompound { descriptor, data }
    }

    pub fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }

    /// Attempts to decode the described value as `T`.
    /// `T`'s implementation of `Decode` is expected to parse the underlying AMQP type starting from the format code.
    /// For instance, if the value is of described type represented by AMQP list with 1 ubyte field with a value of 3:
    /// ```text
    /// 0x00 0xa3 0x07 "foo:bar" 0xc0 0x02 0x01 0x50 0x03
    /// ```
    /// The `T::decode` method will be called with the following input:
    /// ```text
    /// 0xc0 0x02 0x01 0x50 0x03
    /// ```
    pub fn decode<T: Decode>(self) -> Result<T, AmqpParseError> {
        let mut buf = self.data.clone();
        let result = T::decode(&mut buf)?;
        if buf.is_empty() {
            Ok(result)
        } else {
            Err(AmqpParseError::InvalidSize)
        }
    }
}

impl Encode for DescribedCompound {
    fn encoded_size(&self) -> usize {
        self.descriptor.encoded_size() + self.data.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.descriptor.encode(buf);
        buf.extend_from_slice(&self.data);
    }
}

impl From<HashMap<Variant, Variant>> for Variant {
    fn from(data: HashMap<Variant, Variant>) -> Self {
        Variant::Map(VariantMap { map: data })
    }
}

impl From<ByteString> for Variant {
    fn from(s: ByteString) -> Self {
        Str::from(s).into()
    }
}

impl From<String> for Variant {
    fn from(s: String) -> Self {
        Str::from(ByteString::from(s)).into()
    }
}

impl From<&'static str> for Variant {
    fn from(s: &'static str) -> Self {
        Str::from(s).into()
    }
}

impl PartialEq<str> for Variant {
    fn eq(&self, other: &str) -> bool {
        match self {
            Variant::String(s) => s == other,
            Variant::Symbol(s) => s == other,
            _ => false,
        }
    }
}

impl Variant {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Variant::String(s) => Some(s.as_str()),
            Variant::Symbol(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Expresses integer-typed variant values as i64 value when possible. Notably, does not include ulong.
    /// Returns `None` for variants other than supported integers.
    pub fn as_long(&self) -> Option<i64> {
        match self {
            Variant::Ubyte(v) => Some(*v as i64),
            Variant::Ushort(v) => Some(*v as i64),
            Variant::Uint(v) => Some(*v as i64),
            Variant::Byte(v) => Some(*v as i64),
            Variant::Short(v) => Some(*v as i64),
            Variant::Int(v) => Some(*v as i64),
            Variant::Long(v) => Some(*v),
            _ => None,
        }
    }

    /// Expresses unsigned integer-typed variant values as u64 value. Returns `None` for variants other than unsigned integers.
    pub fn as_ulong(&self) -> Option<u64> {
        match self {
            Variant::Ubyte(v) => Some(*v as u64),
            Variant::Ushort(v) => Some(*v as u64),
            Variant::Uint(v) => Some(*v as u64),
            Variant::Ulong(v) => Some(*v as u64),
            _ => None,
        }
    }

    pub fn to_bytes_str(&self) -> Option<ByteString> {
        match self {
            Variant::String(s) => Some(s.to_bytes_str()),
            Variant::Symbol(s) => Some(s.to_bytes_str()),
            _ => None,
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct VariantMap {
    pub map: HashMap<Variant, Variant>,
}

impl VariantMap {
    pub fn new(map: HashMap<Variant, Variant>) -> VariantMap {
        VariantMap { map }
    }
}

#[allow(clippy::derived_hash_with_manual_eq)]
impl Hash for VariantMap {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        unimplemented!()
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct VecSymbolMap(pub Vec<(Symbol, Variant)>);

impl Default for VecSymbolMap {
    fn default() -> Self {
        VecSymbolMap(Vec::with_capacity(8))
    }
}

impl From<Annotations> for VecSymbolMap {
    fn from(anns: Annotations) -> VecSymbolMap {
        VecSymbolMap(anns.into_iter().collect())
    }
}

impl From<Vec<(Symbol, Variant)>> for VecSymbolMap {
    fn from(data: Vec<(Symbol, Variant)>) -> VecSymbolMap {
        VecSymbolMap(data)
    }
}

impl std::ops::Deref for VecSymbolMap {
    type Target = Vec<(Symbol, Variant)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for VecSymbolMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct VecStringMap(pub Vec<(Str, Variant)>);

impl Default for VecStringMap {
    fn default() -> Self {
        VecStringMap(Vec::with_capacity(8))
    }
}

impl From<Vec<(Str, Variant)>> for VecStringMap {
    fn from(data: Vec<(Str, Variant)>) -> VecStringMap {
        VecStringMap(data)
    }
}

impl From<HashMap<Str, Variant>> for VecStringMap {
    fn from(map: HashMap<Str, Variant>) -> VecStringMap {
        VecStringMap(map.into_iter().collect())
    }
}

impl std::ops::Deref for VecStringMap {
    type Target = Vec<(Str, Variant)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for VecStringMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use ntex_bytes::{Buf, BufMut};

    use crate::{codec::ListHeader, format_codes};

    use super::*;

    #[test]
    fn bytes_eq() {
        let bytes1 = Variant::Binary(Bytes::from(&b"hello"[..]));
        let bytes2 = Variant::Binary(Bytes::from(&b"hello"[..]));
        let bytes3 = Variant::Binary(Bytes::from(&b"world"[..]));

        assert_eq!(bytes1, bytes2);
        assert!(bytes1 != bytes3);
    }

    #[test]
    fn string_eq() {
        let a = Variant::String(ByteString::from("hello").into());
        let b = Variant::String(ByteString::from("world!").into());

        assert_eq!(Variant::String(ByteString::from("hello").into()), a);
        assert!(a != b);
    }

    #[test]
    fn symbol_eq() {
        let a = Variant::Symbol(Symbol::from("hello"));
        let b = Variant::Symbol(Symbol::from("world!"));

        assert_eq!(Variant::Symbol(Symbol::from("hello")), a);
        assert!(a != b);
    }

    // <type name="mqtt-metadata" class="composite" source="list">
    //   <descriptor name="contoso:test"/>
    //   <field name="field1" type="string" mandatory="true"/>
    //   <field name="field2" type="ubyte" mandatory="true"/>
    //   <field name="field3" type="string"/>
    // </type>
    #[derive(Debug, PartialEq, Eq, Clone)]
    struct CustomList {
        field1: ByteString,
        field2: u8,
        field3: Option<ByteString>,
    }

    impl CustomList {
        fn encoded_data_size(&self) -> usize {
            let mut size = self.field1.encoded_size() + self.field2.encoded_size();
            if let Some(ref field3) = self.field3 {
                size += field3.encoded_size();
            }
            size
        }
    }

    impl crate::DecodeFormatted for CustomList {
        fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
            let header = ListHeader::decode_with_format(input, fmt)?;
            if header.count < 2 {
                return Err(AmqpParseError::RequiredFieldOmitted("field2"));
            }
            let field1 = ByteString::decode(input)?;
            let field2 = u8::decode(input)?;
            let field3 = if header.count == 3 {
                Some(ByteString::decode(input)?)
            } else {
                None
            };
            if input.has_remaining() {
                return Err(AmqpParseError::InvalidSize);
            }
            Ok(CustomList {
                field1,
                field2,
                field3,
            })
        }
    }

    impl crate::Encode for CustomList {
        fn encoded_size(&self) -> usize {
            let size = self.encoded_data_size();
            if size + 1 > u8::MAX as usize {
                size + 9 // 1 for format code, 4 for size, 4 for count
            } else {
                size + 3 // 1 for format code, 1 for size, 1 for count
            }
        }

        fn encode(&self, buf: &mut BytesMut) {
            let count = if self.field3.is_some() { 3u8 } else { 2u8 };
            let data_size = self.encoded_data_size();
            if data_size + 1 > u8::MAX as usize {
                buf.put_u8(format_codes::FORMATCODE_LIST32);
                buf.put_u32((4 + data_size) as u32); // size. 4 for count
                buf.put_u32(count as u32); // count
            } else {
                buf.put_u8(format_codes::FORMATCODE_LIST8);
                buf.put_u8((1 + data_size) as u8); // size. 1 for count
                buf.put_u8(count); // count
            }
            self.field1.encode(buf);
            self.field2.encode(buf);
            if let Some(ref field3) = self.field3 {
                field3.encode(buf);
            }
        }
    }

    #[test]
    fn described_custom_list_recoding() {
        let custom_list = CustomList {
            field1: ByteString::from("value1"),
            field2: 115,
            field3: Some(ByteString::from("value3")),
        };
        let value = Variant::DescribedCompound(DescribedCompound::create(
            Descriptor::Symbol("contoso:test".into()),
            custom_list.clone(),
        ));
        let mut buf = BytesMut::with_capacity(value.encoded_size());
        value.encode(&mut buf);
        let data = buf.freeze();
        assert_eq!(
            data.as_ref(),
            &b"\x00\xa3\x0ccontoso:test\xc0\x13\x03\xa1\x06value1\x50\x73\xa1\x06value3"[..]
        );
        let mut input = data.clone();
        let decoded = Variant::decode(&mut input).unwrap();
        assert_eq!(decoded, value);
        let decoded_list = match decoded {
            Variant::DescribedCompound(desc) => desc.decode::<CustomList>().unwrap(),
            _ => panic!("Expected a described compound"),
        };
        assert_eq!(decoded_list, custom_list);
    }
}
