use bytes::Bytes;
use chrono::{DateTime, Utc};
use ordered_float::OrderedFloat;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use types::{ByteStr, Descriptor, List, Symbol};
use uuid::Uuid;

/// Represents an AMQP type for use in polymorphic collections
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
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

    // Decimal32(d32),
    // Decimal64(d64),
    // Decimal128(d128),
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
    String(ByteStr),

    /// Symbolic values from a constrained domain.
    Symbol(Symbol),

    /// List
    List(List),

    /// Map
    Map(VariantMap),

    /// Described value
    Described((Descriptor, Box<Variant>)),
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

impl Hash for VariantMap {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
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
        let a = Variant::String(ByteStr::from("hello"));
        let b = Variant::String(ByteStr::from("world!"));

        assert_eq!(Variant::String(ByteStr::from("hello")), a);
        assert!(a != b);
    }

    #[test]
    fn symbol_eq() {
        let a = Variant::Symbol(Symbol::from("hello"));
        let b = Variant::Symbol(Symbol::from("world!"));

        assert_eq!(Variant::Symbol(Symbol::from("hello")), a);
        assert!(a != b);
    }
}
