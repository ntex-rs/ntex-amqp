use std::{borrow, hash, ops};

use bytes::Bytes;
use string::String;

mod symbol;
mod variant;

pub type ByteStr = String<Bytes>;
pub use self::symbol::{StaticSymbol, Symbol};
pub use self::variant::Variant;
pub use self::variant::VariantMap;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum Descriptor {
    Ulong(u64),
    Symbol(Symbol),
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

    pub fn iter(&self) -> ::std::slice::Iter<T> {
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

    pub fn iter(&self) -> ::std::slice::Iter<Variant> {
        self.0.iter()
    }
}

#[derive(Debug, Clone, Eq, Ord, PartialOrd)]
pub enum Str {
    ByteStr(String<Bytes>),
    Static(&'static str),
}

impl Str {
    pub fn from_str(s: &str) -> Str {
        Str::ByteStr(String::from_str(s))
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Str::ByteStr(s) => s.as_ref(),
            Str::Static(s) => s.as_bytes(),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Str::ByteStr(s) => s.as_ref(),
            Str::Static(s) => s,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Str::ByteStr(s) => s.len(),
            Str::Static(s) => s.len(),
        }
    }
}

impl From<&'static str> for Str {
    fn from(s: &'static str) -> Str {
        Str::Static(s)
    }
}

impl From<String<Bytes>> for Str {
    fn from(s: String<Bytes>) -> Str {
        Str::ByteStr(s)
    }
}

impl hash::Hash for Str {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        match self {
            Str::ByteStr(s) => (&*s).hash(state),
            Str::Static(s) => s.hash(state),
        }
    }
}

impl borrow::Borrow<str> for Str {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<Str> for Str {
    fn eq(&self, other: &Str) -> bool {
        match self {
            Str::ByteStr(s) => match other {
                Str::ByteStr(o) => s == o,
                Str::Static(o) => s == *o,
            },
            Str::Static(s) => match other {
                Str::ByteStr(o) => o == *s,
                Str::Static(o) => s == o,
            },
        }
    }
}

impl PartialEq<str> for Str {
    fn eq(&self, other: &str) -> bool {
        match self {
            Str::ByteStr(s) => s == other,
            Str::Static(s) => *s == other,
        }
    }
}
