use std::{borrow, str};

use ntex_bytes::ByteString;

use super::Str;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Symbol(pub Str);

impl Symbol {
    pub const fn from_static(s: &'static str) -> Symbol {
        Symbol(Str::from_static(s))
    }

    pub fn from_slice(s: &str) -> Symbol {
        Symbol(Str(ByteString::from(s)))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn to_bytes_str(&self) -> ByteString {
        self.0.to_bytes_str()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Default for Symbol {
    fn default() -> Symbol {
        Symbol::from_static("")
    }
}

impl From<&'static str> for Symbol {
    fn from(s: &'static str) -> Symbol {
        Symbol::from_static(s)
    }
}

impl From<Str> for Symbol {
    fn from(s: Str) -> Symbol {
        Symbol(s)
    }
}

impl From<std::string::String> for Symbol {
    fn from(s: std::string::String) -> Symbol {
        Symbol(Str::from(s))
    }
}

impl From<ByteString> for Symbol {
    fn from(s: ByteString) -> Symbol {
        Symbol(Str(s))
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

impl StaticSymbol {
    pub const fn new(s: &'static str) -> StaticSymbol {
        StaticSymbol(s)
    }
}

impl From<&'static str> for StaticSymbol {
    fn from(s: &'static str) -> StaticSymbol {
        StaticSymbol(s)
    }
}
