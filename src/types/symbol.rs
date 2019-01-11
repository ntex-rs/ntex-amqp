use std::{borrow, str};

use bytes::Bytes;
use string::String;

use super::Str;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Symbol(pub Str);

impl Symbol {
    pub fn from_slice(s: &str) -> Symbol {
        Symbol(Str::ByteStr(String::from_str(s)))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_bytes_str(&self) -> String<Bytes> {
        self.0.as_bytes_str()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<&'static str> for Symbol {
    fn from(s: &'static str) -> Symbol {
        Symbol(Str::Static(s))
    }
}

impl From<String<Bytes>> for Symbol {
    fn from(s: String<Bytes>) -> Symbol {
        Symbol(Str::ByteStr(s))
    }
}

impl borrow::Borrow<str> for Symbol {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for Symbol {
    fn eq(&self, other: &str) -> bool {
        self.0 == *other
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StaticSymbol(pub &'static str);

impl From<&'static str> for StaticSymbol {
    fn from(s: &'static str) -> StaticSymbol {
        StaticSymbol(s)
    }
}
