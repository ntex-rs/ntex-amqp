use std::str;

use bytes::Bytes;
use string::{String, TryFrom};

#[derive(Debug, Clone, PartialEq, Eq, Hash, From)]
pub struct Symbol(String<Bytes>);

impl Symbol {
    pub fn from_slice(s: &str) -> Symbol {
        Symbol(String::try_from(Bytes::from(s.as_bytes())).unwrap())
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<&'static str> for Symbol {
    fn from(s: &'static str) -> Symbol {
        Symbol(String::try_from(Bytes::from_static(s.as_bytes())).unwrap())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StaticSymbol(pub &'static str);

impl From<&'static str> for StaticSymbol {
    fn from(s: &'static str) -> StaticSymbol {
        StaticSymbol(s)
    }
}
