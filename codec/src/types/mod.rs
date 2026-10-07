use std::{borrow, fmt, hash, ops, str};

use ntex_bytes::ByteString;

mod array;
mod symbol;
mod variant;

use crate::AmqpParseError;

pub use self::array::Array;
pub use self::symbol::{StaticSymbol, Symbol};
pub use self::variant::{DescribedCompound, Variant, VariantMap, VecStringMap, VecSymbolMap};

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum Descriptor {
    Ulong(u64),
    Symbol(Symbol),
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum Constructor {
    FormatCode(u8),
    Described {
        descriptor: Descriptor,
        format_code: u8,
    },
}

impl Constructor {
    pub fn format_code(&self) -> u8 {
        match self {
            Constructor::FormatCode(code) => *code,
            Constructor::Described { format_code, .. } => *format_code,
        }
    }

    pub fn descriptor(&self) -> Option<&Descriptor> {
        match self {
            Constructor::FormatCode(_) => None,
            Constructor::Described { descriptor, .. } => Some(descriptor),
        }
    }

    pub fn ensure_described(&self, descriptor: &Descriptor) -> Result<(), AmqpParseError> {
        match self {
            Constructor::Described { descriptor: d, .. } if d == descriptor => Ok(()),
            Constructor::Described { descriptor: d, .. } => {
                Err(AmqpParseError::InvalidDescriptor(Box::new(d.clone())))
            }
            Constructor::FormatCode(fmt) => Err(AmqpParseError::InvalidFormatCode(*fmt)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, From)]
pub struct Multiple<T>(pub Vec<T>);

impl<T> Multiple<T> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> ::std::slice::Iter<'_, T> {
        self.0.iter()
    }
}

impl<T> Default for Multiple<T> {
    fn default() -> Multiple<T> {
        Multiple(Vec::new())
    }
}

impl<T> ops::Deref for Multiple<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> ops::DerefMut for Multiple<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct List(pub Vec<Variant>);

impl List {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> ::std::slice::Iter<'_, Variant> {
        self.0.iter()
    }
}

impl From<Vec<Variant>> for List {
    fn from(data: Vec<Variant>) -> List {
        List(data)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ListDescribed<T>(pub Vec<T>);

impl<T> Default for ListDescribed<T> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T> ListDescribed<T> {
    pub fn new(items: Vec<T>) -> Self {
        Self(items)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> ::std::slice::Iter<'_, T> {
        self.0.iter()
    }
}

impl<T> From<Vec<T>> for ListDescribed<T> {
    fn from(data: Vec<T>) -> Self {
        Self(data)
    }
}

#[derive(Clone, Eq, Ord, PartialOrd, PartialEq)]
pub struct Str(ByteString);

impl Str {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Str {
        Str(ByteString::from(s))
    }

    pub const fn from_static(s: &'static str) -> Str {
        Str(ByteString::from_static(s))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes().as_ref()
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn to_bytes_str(&self) -> ByteString {
        self.0.clone()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<&'static str> for Str {
    fn from(s: &'static str) -> Str {
        Str(ByteString::from_static(s))
    }
}

impl From<ByteString> for Str {
    fn from(s: ByteString) -> Str {
        Str(s)
    }
}

impl From<String> for Str {
    fn from(s: String) -> Str {
        Str(ByteString::from(s))
    }
}

impl<'a> From<&'a ByteString> for Str {
    fn from(s: &'a ByteString) -> Str {
        Str(s.clone())
    }
}

impl hash::Hash for Str {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl borrow::Borrow<str> for Str {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for Str {
    fn eq(&self, other: &str) -> bool {
        self.0.eq(&other)
    }
}

impl fmt::Debug for Str {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use std::collections::HashMap;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    use super::*;

    fn hash_of<T: hash::Hash>(v: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        v.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn constructor_accessors() {
        let plain = Constructor::FormatCode(0xc0);
        assert_eq!(plain.format_code(), 0xc0);
        assert_eq!(plain.descriptor(), None);

        let descriptor = Descriptor::Ulong(0x23);
        let described = Constructor::Described {
            descriptor: descriptor.clone(),
            format_code: 0xc0,
        };
        assert_eq!(described.format_code(), 0xc0);
        assert_eq!(described.descriptor(), Some(&descriptor));
    }

    #[test]
    fn constructor_ensure_described() {
        let descriptor = Descriptor::Ulong(0x23);
        let other = Descriptor::Symbol(Symbol::from("a:b"));
        let described = Constructor::Described {
            descriptor: descriptor.clone(),
            format_code: 0xc0,
        };

        assert!(described.ensure_described(&descriptor).is_ok());

        match described.ensure_described(&other) {
            Err(AmqpParseError::InvalidDescriptor(d)) => assert_eq!(*d, descriptor),
            other => panic!("unexpected result: {other:?}"),
        }

        match Constructor::FormatCode(0xc0).ensure_described(&descriptor) {
            Err(AmqpParseError::InvalidFormatCode(code)) => assert_eq!(code, 0xc0),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn multiple_collection() {
        let empty: Multiple<u32> = Multiple::default();
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
        assert_eq!(empty.iter().count(), 0);

        let mut multiple = Multiple::from(vec![1u32, 2, 3]);
        assert_eq!(multiple.len(), 3);
        assert!(!multiple.is_empty());
        assert_eq!(multiple.iter().copied().sum::<u32>(), 6);

        // `Deref` / `DerefMut` expose the underlying vector
        assert_eq!(multiple.first(), Some(&1));
        multiple.push(4);
        assert_eq!(multiple.len(), 4);
        assert_eq!(*multiple, vec![1, 2, 3, 4]);
    }

    #[test]
    fn list_collection() {
        let empty = List::from(vec![]);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());

        let list = List::from(vec![Variant::Ubyte(1), Variant::Null]);
        assert_eq!(list.len(), 2);
        assert!(!list.is_empty());
        assert_eq!(list.iter().next(), Some(&Variant::Ubyte(1)));
        assert_eq!(list.0[1], Variant::Null);
    }

    #[test]
    fn list_described_collection() {
        let empty: ListDescribed<u32> = ListDescribed::default();
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());

        let list = ListDescribed::new(vec![1u32, 2]);
        assert_eq!(list.len(), 2);
        assert!(!list.is_empty());
        assert_eq!(list.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(ListDescribed::from(vec![1u32, 2]), list);
    }

    #[test]
    fn str_constructors() {
        let expected = Str::from_static("hello");

        assert_eq!(Str::from_str("hello"), expected);
        assert_eq!(Str::from("hello"), expected);
        assert_eq!(Str::from(String::from("hello")), expected);
        assert_eq!(Str::from(ByteString::from("hello")), expected);
        assert_eq!(Str::from(&ByteString::from("hello")), expected);

        assert_eq!(expected.as_str(), "hello");
        assert_eq!(expected.as_bytes(), b"hello");
        assert_eq!(expected.to_bytes_str(), ByteString::from("hello"));
        assert_eq!(expected.len(), 5);
    }

    #[test]
    fn str_lookup_and_compare() {
        // `Borrow<str>` allows `&str` lookups in maps keyed by `Str`
        let mut map = HashMap::new();
        map.insert(Str::from("key"), 1u8);
        assert_eq!(map.get("key"), Some(&1));
        assert_eq!(map.get("other"), None);

        let s = Str::from("key");
        assert!(s == *"key");
        assert!(s != *"other");
        assert_eq!(Borrow::<str>::borrow(&s), "key");

        // `Hash` must agree with the underlying `ByteString`
        assert_eq!(hash_of(&s), hash_of(&ByteString::from("key")));
        let (a, b) = (Str::from("a"), Str::from("b"));
        assert!(a < b);
    }

    #[test]
    fn str_debug_delegates_to_inner() {
        assert_eq!(
            format!("{:?}", Str::from("hi")),
            format!("{:?}", ByteString::from("hi"))
        );
    }
}
