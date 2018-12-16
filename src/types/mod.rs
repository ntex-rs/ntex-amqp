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

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
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
