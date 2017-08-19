use std::str;

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Symbol(Bytes);

impl Symbol {
    pub unsafe fn from_utf8_unchecked(slice: Bytes) -> Symbol {
        Symbol(slice)
    }

    pub fn from_static(s: &'static str) -> Symbol {
        Symbol(Bytes::from_static(s.as_bytes()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn as_str(&self) -> &str {
        unsafe { str::from_utf8_unchecked(self.0.as_ref()) }
    }
}

impl<'a> From<&'a str> for Symbol {
    fn from(s: &'a str) -> Symbol {
        Symbol(Bytes::from(s))
    }
}