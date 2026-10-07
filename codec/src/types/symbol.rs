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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn symbol_constructors() {
        let expected = Symbol::from_static("sym");

        assert_eq!(Symbol::from_slice("sym"), expected);
        assert_eq!(Symbol::from("sym"), expected);
        assert_eq!(Symbol::from(String::from("sym")), expected);
        assert_eq!(Symbol::from(ByteString::from("sym")), expected);
        assert_eq!(Symbol::from(Str::from("sym")), expected);

        assert_eq!(expected.as_str(), "sym");
        assert_eq!(expected.as_bytes(), b"sym");
        assert_eq!(expected.to_bytes_str(), ByteString::from("sym"));
        assert_eq!(expected.len(), 3);
    }

    #[test]
    fn symbol_default_is_empty() {
        let def = Symbol::default();
        assert_eq!(def.len(), 0);
        assert_eq!(def.as_str(), "");
        assert_eq!(def, Symbol::from(""));
    }

    #[test]
    fn symbol_lookup_and_compare() {
        // `Borrow<str>` allows `&str` lookups in maps keyed by `Symbol`
        let mut map = HashMap::new();
        map.insert(Symbol::from("a"), 1u8);
        assert_eq!(map.get("a"), Some(&1));
        assert_eq!(map.get("b"), None);

        let sym = Symbol::from("a");
        assert!(sym == *"a");
        assert!(sym != *"b");
        assert_eq!(borrow::Borrow::<str>::borrow(&sym), "a");
    }

    #[test]
    fn static_symbol() {
        assert_eq!(StaticSymbol::new("s"), StaticSymbol("s"));
        assert_eq!(StaticSymbol::from("s"), StaticSymbol("s"));
        assert_ne!(StaticSymbol::new("s"), StaticSymbol("t"));
        assert_eq!(StaticSymbol::new("s").0, "s");
    }
}
