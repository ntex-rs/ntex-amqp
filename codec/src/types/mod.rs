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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}
