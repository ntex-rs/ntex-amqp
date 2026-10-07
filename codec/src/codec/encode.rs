use std::{collections::HashMap, hash::BuildHasher, hash::Hash};

use chrono::{DateTime, Utc};
use ntex_bytes::{BufMut, BytePages, ByteString, Bytes};
use ntex_util::hash_map::HashMap as HashMapBase;
use uuid::Uuid;

use crate::codec::{self, ArrayEncode, Composite, Encode};
use crate::framing::{self, AmqpFrame, SaslFrame};
use crate::types::{
    Constructor, Descriptor, List, ListDescribed, Multiple, StaticSymbol, Str, Symbol, Variant,
    VecStringMap, VecSymbolMap,
};

fn encode_null(buf: &mut BytePages) {
    buf.put_u8(codec::FORMATCODE_NULL);
}

trait FixedEncode {}

impl<T: FixedEncode + ArrayEncode> Encode for T {
    fn encoded_size(&self) -> usize {
        self.array_encoded_size() + 1
    }
    fn encode(&self, buf: &mut BytePages) {
        T::ARRAY_CONSTRUCTOR.encode(buf);
        self.array_encode(buf);
    }
}

impl Encode for bool {
    fn encoded_size(&self) -> usize {
        1
    }
    fn encode(&self, buf: &mut BytePages) {
        buf.put_u8(if *self {
            codec::FORMATCODE_BOOLEAN_TRUE
        } else {
            codec::FORMATCODE_BOOLEAN_FALSE
        });
    }
}
impl ArrayEncode for bool {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_BOOLEAN);
    fn array_encoded_size(&self) -> usize {
        1
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_u8(u8::from(*self));
    }
}

impl FixedEncode for u8 {}
impl ArrayEncode for u8 {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_UBYTE);
    fn array_encoded_size(&self) -> usize {
        1
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_u8(*self);
    }
}

impl FixedEncode for u16 {}
impl ArrayEncode for u16 {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_USHORT);
    fn array_encoded_size(&self) -> usize {
        2
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_u16(*self);
    }
}

impl Encode for u32 {
    fn encoded_size(&self) -> usize {
        if *self == 0 {
            1
        } else if *self > u32::from(u8::MAX) {
            5
        } else {
            2
        }
    }
    fn encode(&self, buf: &mut BytePages) {
        if *self == 0 {
            buf.put_u8(codec::FORMATCODE_UINT_0)
        } else if *self > u32::from(u8::MAX) {
            buf.put_u8(codec::FORMATCODE_UINT);
            buf.put_u32(*self);
        } else {
            buf.put_u8(codec::FORMATCODE_SMALLUINT);
            buf.put_u8(*self as u8);
        }
    }
}
impl ArrayEncode for u32 {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_UINT);
    fn array_encoded_size(&self) -> usize {
        4
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_u32(*self);
    }
}

impl Encode for u64 {
    fn encoded_size(&self) -> usize {
        if *self == 0 {
            1
        } else if *self > u64::from(u8::MAX) {
            9
        } else {
            2
        }
    }

    fn encode(&self, buf: &mut BytePages) {
        if *self == 0 {
            buf.put_u8(codec::FORMATCODE_ULONG_0)
        } else if *self > u64::from(u8::MAX) {
            buf.put_u8(codec::FORMATCODE_ULONG);
            buf.put_u64(*self);
        } else {
            buf.put_u8(codec::FORMATCODE_SMALLULONG);
            buf.put_u8(*self as u8);
        }
    }
}

impl ArrayEncode for u64 {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_ULONG);
    fn array_encoded_size(&self) -> usize {
        8
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_u64(*self);
    }
}

impl FixedEncode for i8 {}

impl ArrayEncode for i8 {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_BYTE);
    fn array_encoded_size(&self) -> usize {
        1
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_i8(*self);
    }
}

impl FixedEncode for i16 {}

impl ArrayEncode for i16 {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_SHORT);
    fn array_encoded_size(&self) -> usize {
        2
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_i16(*self);
    }
}

impl Encode for i32 {
    fn encoded_size(&self) -> usize {
        if *self > i32::from(i8::MAX) || *self < i32::from(i8::MIN) {
            5
        } else {
            2
        }
    }

    fn encode(&self, buf: &mut BytePages) {
        if *self > i32::from(i8::MAX) || *self < i32::from(i8::MIN) {
            buf.put_u8(codec::FORMATCODE_INT);
            buf.put_i32(*self);
        } else {
            buf.put_u8(codec::FORMATCODE_SMALLINT);
            buf.put_i8(*self as i8);
        }
    }
}

impl ArrayEncode for i32 {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_INT);
    fn array_encoded_size(&self) -> usize {
        4
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_i32(*self);
    }
}

impl Encode for i64 {
    fn encoded_size(&self) -> usize {
        if *self > i64::from(i8::MAX) || *self < i64::from(i8::MIN) {
            9
        } else {
            2
        }
    }

    fn encode(&self, buf: &mut BytePages) {
        if *self > i64::from(i8::MAX) || *self < i64::from(i8::MIN) {
            buf.put_u8(codec::FORMATCODE_LONG);
            buf.put_i64(*self);
        } else {
            buf.put_u8(codec::FORMATCODE_SMALLLONG);
            buf.put_i8(*self as i8);
        }
    }
}

impl ArrayEncode for i64 {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_LONG);
    fn array_encoded_size(&self) -> usize {
        8
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_i64(*self);
    }
}

impl FixedEncode for f32 {}

impl ArrayEncode for f32 {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_FLOAT);

    fn array_encoded_size(&self) -> usize {
        4
    }

    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_f32(*self);
    }
}

impl FixedEncode for f64 {}

impl ArrayEncode for f64 {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_DOUBLE);
    fn array_encoded_size(&self) -> usize {
        8
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_f64(*self);
    }
}

impl FixedEncode for char {}

impl ArrayEncode for char {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_CHAR);
    fn array_encoded_size(&self) -> usize {
        4
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_u32(*self as u32);
    }
}

impl FixedEncode for DateTime<Utc> {}

impl ArrayEncode for DateTime<Utc> {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_TIMESTAMP);
    fn array_encoded_size(&self) -> usize {
        8
    }
    fn array_encode(&self, buf: &mut BytePages) {
        let timestamp = self.timestamp() * 1000 + i64::from(self.timestamp_subsec_millis());
        buf.put_i64(timestamp);
    }
}

impl FixedEncode for Uuid {}

impl ArrayEncode for Uuid {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_UUID);
    fn array_encoded_size(&self) -> usize {
        16
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.extend_from_slice(self.as_bytes());
    }
}

impl Encode for Bytes {
    fn encoded_size(&self) -> usize {
        let length = self.len();
        let size = if length > u8::MAX as usize { 5 } else { 2 };
        size + length
    }

    fn encode(&self, buf: &mut BytePages) {
        let length = self.len();
        if length > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_BINARY32);
            buf.put_u32(codec::size_u32(length));
        } else {
            buf.put_u8(codec::FORMATCODE_BINARY8);
            buf.put_u8(length as u8);
        }
        buf.append(self.clone());
    }
}

impl ArrayEncode for Bytes {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_BINARY32);
    fn array_encoded_size(&self) -> usize {
        4 + self.len()
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_u32(codec::size_u32(self.len()));
        buf.append(self.clone());
    }
}

impl Encode for ByteString {
    fn encoded_size(&self) -> usize {
        let length = self.len();
        let size = if length > u8::MAX as usize { 5 } else { 2 };
        size + length
    }

    fn encode(&self, buf: &mut BytePages) {
        let length = self.len();
        if length > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_STRING32);
            buf.put_u32(codec::size_u32(length));
        } else {
            buf.put_u8(codec::FORMATCODE_STRING8);
            buf.put_u8(length as u8);
        }
        buf.append(self.as_bytes().clone());
    }
}
impl ArrayEncode for ByteString {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_STRING32);
    fn array_encoded_size(&self) -> usize {
        4 + self.len()
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_u32(codec::size_u32(self.len()));
        buf.append(self.as_bytes().clone());
    }
}

impl Encode for str {
    fn encoded_size(&self) -> usize {
        let length = self.len();
        let size = if length > u8::MAX as usize { 5 } else { 2 };
        size + length
    }

    fn encode(&self, buf: &mut BytePages) {
        let length = self.len();
        if length > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_STRING32);
            buf.put_u32(codec::size_u32(length));
        } else {
            buf.put_u8(codec::FORMATCODE_STRING8);
            buf.put_u8(length as u8);
        }
        buf.put_slice(self.as_bytes());
    }
}

impl ArrayEncode for str {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_STRING32);
    fn array_encoded_size(&self) -> usize {
        4 + self.len()
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_u32(codec::size_u32(self.len()));
        buf.put_slice(self.as_bytes());
    }
}

impl Encode for Str {
    fn encoded_size(&self) -> usize {
        let length = self.len();
        let size = if length > u8::MAX as usize { 5 } else { 2 };
        size + length
    }

    fn encode(&self, buf: &mut BytePages) {
        let length = self.as_str().len();
        if length > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_STRING32);
            buf.put_u32(codec::size_u32(length));
        } else {
            buf.put_u8(codec::FORMATCODE_STRING8);
            buf.put_u8(length as u8);
        }
        buf.append(self.to_bytes_str());
    }
}

impl Encode for Symbol {
    fn encoded_size(&self) -> usize {
        let length = self.len();
        let size = if length > u8::MAX as usize { 5 } else { 2 };
        size + length
    }

    fn encode(&self, buf: &mut BytePages) {
        let length = self.as_str().len();
        if length > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_SYMBOL32);
            buf.put_u32(codec::size_u32(length));
        } else {
            buf.put_u8(codec::FORMATCODE_SYMBOL8);
            buf.put_u8(length as u8);
        }
        buf.append(self.to_bytes_str());
    }
}

impl ArrayEncode for Symbol {
    const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_SYMBOL32);
    fn array_encoded_size(&self) -> usize {
        4 + self.len()
    }
    fn array_encode(&self, buf: &mut BytePages) {
        buf.put_u32(codec::size_u32(self.len()));
        buf.append(self.to_bytes_str());
    }
}

impl Encode for StaticSymbol {
    fn encoded_size(&self) -> usize {
        let length = self.0.len();
        let size = if length > u8::MAX as usize { 5 } else { 2 };
        size + length
    }

    fn encode(&self, buf: &mut BytePages) {
        let length = self.0.len();
        if length > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_SYMBOL32);
            buf.put_u32(codec::size_u32(length));
        } else {
            buf.put_u8(codec::FORMATCODE_SYMBOL8);
            buf.put_u8(length as u8);
        }
        buf.append(self.0);
    }
}

macro_rules! hashmap {
    ($ty:ident) => {
        impl<K: Eq + Hash + Encode, V: Encode, S: BuildHasher> Encode for $ty<K, V, S> {
            fn encoded_size(&self) -> usize {
                let size = self
                    .iter()
                    .fold(0, |r, (k, v)| r + k.encoded_size() + v.encoded_size());
                // f:1 + s:4 + c:4 vs f:1 + s:1 + c:1
                let preamble = if size + 1 > u8::MAX as usize { 9 } else { 3 };
                preamble + size
            }

            fn encode(&self, buf: &mut BytePages) {
                let count = self.len() * 2; // key-value pair accounts for two items in count
                let size = self
                    .iter()
                    .fold(0, |r, (k, v)| r + k.encoded_size() + v.encoded_size());
                if size + 1 > u8::MAX as usize {
                    buf.put_u8(codec::FORMATCODE_MAP32);
                    buf.put_u32(codec::size_u32(size + 4)); // +4 for 4 byte count that follows
                    buf.put_u32(codec::size_u32(count));
                } else {
                    buf.put_u8(codec::FORMATCODE_MAP8);
                    buf.put_u8((size + 1) as u8); // +1 for 1 byte count that follows
                    buf.put_u8(count as u8);
                }

                for (k, v) in self {
                    k.encode(buf);
                    v.encode(buf);
                }
            }
        }

        impl<K: Eq + Hash + Encode, V: Encode> ArrayEncode for $ty<K, V> {
            const ARRAY_CONSTRUCTOR: Constructor = Constructor::FormatCode(codec::FORMATCODE_MAP32);
            fn array_encoded_size(&self) -> usize {
                8 + self
                    .iter()
                    .fold(0, |r, (k, v)| r + k.encoded_size() + v.encoded_size())
            }

            fn array_encode(&self, buf: &mut BytePages) {
                let count = self.len() * 2;
                let size = 4 + self
                    .iter()
                    .fold(0, |r, (k, v)| r + k.encoded_size() + v.encoded_size());
                buf.put_u32(codec::size_u32(size));
                buf.put_u32(codec::size_u32(count));

                for (k, v) in self {
                    k.encode(buf);
                    v.encode(buf);
                }
            }
        }
    };
}
hashmap!(HashMap);
hashmap!(HashMapBase);

impl Encode for VecSymbolMap {
    fn encoded_size(&self) -> usize {
        let size = self
            .0
            .iter()
            .fold(0, |r, (k, v)| r + k.encoded_size() + v.encoded_size());

        // f:1 + s:4 + c:4 vs f:1 + s:1 + c:1
        let preamble = if size + 1 > u8::MAX as usize { 9 } else { 3 };
        preamble + size
    }

    fn encode(&self, buf: &mut BytePages) {
        let count = self.len() * 2; // key-value pair accounts for two items in count
        let size = self
            .0
            .iter()
            .fold(0, |r, (k, v)| r + k.encoded_size() + v.encoded_size());

        if size + 1 > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_MAP32);
            buf.put_u32(codec::size_u32(size + 4)); // +4 for 4 byte count that follows
            buf.put_u32(codec::size_u32(count));
        } else {
            buf.put_u8(codec::FORMATCODE_MAP8);
            buf.put_u8((size + 1) as u8); // +1 for 1 byte count that follows
            buf.put_u8(count as u8);
        }

        for (k, v) in self.iter() {
            k.encode(buf);
            v.encode(buf);
        }
    }
}

impl Encode for VecStringMap {
    fn encoded_size(&self) -> usize {
        let size = self
            .0
            .iter()
            .fold(0, |r, (k, v)| r + k.encoded_size() + v.encoded_size());

        // f:1 + s:4 + c:4 vs f:1 + s:1 + c:1
        let preamble = if size + 1 > u8::MAX as usize { 9 } else { 3 };
        preamble + size
    }

    fn encode(&self, buf: &mut BytePages) {
        let count = self.len() * 2; // key-value pair accounts for two items in count
        let size = self
            .0
            .iter()
            .fold(0, |r, (k, v)| r + k.encoded_size() + v.encoded_size());

        if size + 1 > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_MAP32);
            buf.put_u32(codec::size_u32(size + 4)); // +4 for 4 byte count that follows
            buf.put_u32(codec::size_u32(count));
        } else {
            buf.put_u8(codec::FORMATCODE_MAP8);
            buf.put_u8((size + 1) as u8); // +1 for 1 byte count that follows
            buf.put_u8(count as u8);
        }

        for (k, v) in self.iter() {
            k.encode(buf);
            v.encode(buf);
        }
    }
}

fn array_encoded_size<T: ArrayEncode>(vec: &[T]) -> usize {
    vec.iter().fold(0, |r, i| r + i.array_encoded_size())
}

impl<T: ArrayEncode> Encode for Vec<T> {
    fn encoded_size(&self) -> usize {
        let ctor_size = T::ARRAY_CONSTRUCTOR.encoded_size();
        let content_size = array_encoded_size(self);
        (if content_size + 1 + ctor_size > u8::MAX as usize || self.len() > u8::MAX as usize {
            9 // 1 for format code, 4 for size, 4 for count
        } else {
            3 // 1 for format code, 1 for size, 1 for count
        }) + ctor_size
            + content_size
    }

    fn encode(&self, buf: &mut BytePages) {
        let size = array_encoded_size(self);
        let ctor_size = T::ARRAY_CONSTRUCTOR.encoded_size();
        if size + 1 + ctor_size > u8::MAX as usize || self.len() > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_ARRAY32);
            buf.put_u32(codec::size_u32(size + 4 + ctor_size)); // +4 for count
            buf.put_u32(codec::size_u32(self.len()));
        } else {
            buf.put_u8(codec::FORMATCODE_ARRAY8);
            buf.put_u8((size + 1 + ctor_size) as u8); // +1 for count
            buf.put_u8(self.len() as u8);
        }
        T::ARRAY_CONSTRUCTOR.encode(buf);
        for i in self {
            i.array_encode(buf);
        }
    }
}

impl<T: Encode + ArrayEncode> Encode for Multiple<T> {
    fn encoded_size(&self) -> usize {
        let count = self.len();
        match count {
            1 => self.0[0].encoded_size(),
            _ => self.0.encoded_size(),
        }
    }

    fn encode(&self, buf: &mut BytePages) {
        let count = self.0.len();
        match count {
            1 => self.0[0].encode(buf),
            _ => self.0.encode(buf),
        }
    }
}

impl Encode for List {
    fn encoded_size(&self) -> usize {
        let content_size = self.iter().fold(0, |r, i| r + i.encoded_size());
        // format_code + size + count
        (if content_size + 1 > u8::MAX as usize {
            9
        } else {
            3
        }) + content_size
    }

    fn encode(&self, buf: &mut BytePages) {
        let size = self.iter().fold(0, |r, i| r + i.encoded_size());
        if size + 1 > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_LIST32);
            buf.put_u32(codec::size_u32(size + 4)); // +4 for 4 byte count that follow
            buf.put_u32(codec::size_u32(self.len()));
        } else {
            buf.put_u8(codec::FORMATCODE_LIST8);
            buf.put_u8((size + 1) as u8); // +1 for 1 byte count that follow
            buf.put_u8(self.len() as u8);
        }
        for i in self.iter() {
            i.encode(buf);
        }
    }
}

impl<T: Composite> Encode for ListDescribed<T> {
    fn encoded_size(&self) -> usize {
        let descr_size = T::descriptor().encoded_size();
        let content_size = self
            .iter()
            .fold(0, |r, i| r + i.encoded_size() + descr_size);

        // format_code + size + count
        (if content_size + 1 > u8::MAX as usize {
            9
        } else {
            3
        }) + content_size
    }

    fn encode(&self, buf: &mut BytePages) {
        let descr = T::descriptor();
        let descr_size = descr.encoded_size();
        let content_size = self
            .iter()
            .fold(0, |r, i| r + i.encoded_size() + descr_size);

        if content_size + 1 > u8::MAX as usize {
            buf.put_u8(codec::FORMATCODE_LIST32);
            buf.put_u32(codec::size_u32(content_size + 4)); // +4 for 4 byte count that follow
            buf.put_u32(codec::size_u32(self.len()));
        } else {
            buf.put_u8(codec::FORMATCODE_LIST8);
            buf.put_u8((content_size + 1) as u8); // +1 for 1 byte count that follow
            buf.put_u8(self.len() as u8);
        }
        for i in self.iter() {
            descr.encode(buf);
            i.encode(buf);
        }
    }
}

impl Encode for Variant {
    fn encoded_size(&self) -> usize {
        match *self {
            Variant::Null => 1,
            Variant::Boolean(b) => b.encoded_size(),
            Variant::Ubyte(b) => b.encoded_size(),
            Variant::Ushort(s) => s.encoded_size(),
            Variant::Uint(i) => i.encoded_size(),
            Variant::Ulong(l) => l.encoded_size(),
            Variant::Byte(b) => b.encoded_size(),
            Variant::Short(s) => s.encoded_size(),
            Variant::Int(i) => i.encoded_size(),
            Variant::Long(l) => l.encoded_size(),
            Variant::Float(f) => f.encoded_size(),
            Variant::Double(d) => d.encoded_size(),
            Variant::Decimal32(_) => 1 + 4,
            Variant::Decimal64(_) => 1 + 8,
            Variant::Decimal128(_) => 1 + 16,
            Variant::Char(c) => c.encoded_size(),
            Variant::Timestamp(ref t) => t.encoded_size(),
            Variant::Uuid(ref u) => u.encoded_size(),
            Variant::Binary(ref b) => b.encoded_size(),
            Variant::String(ref s) => s.encoded_size(),
            Variant::Symbol(ref s) => s.encoded_size(),
            Variant::List(ref l) => l.encoded_size(),
            Variant::Array(ref a) => a.encoded_size(),
            Variant::Map(ref m) => m.map.encoded_size(),
            Variant::Described(ref dv) => dv.0.encoded_size() + dv.1.encoded_size(),
            Variant::DescribedCompound(ref described) => described.encoded_size(),
        }
    }

    /// Encodes `Variant` into provided `BytesMut`
    fn encode(&self, buf: &mut BytePages) {
        match *self {
            Variant::Null => encode_null(buf),
            Variant::Boolean(b) => b.encode(buf),
            Variant::Ubyte(b) => b.encode(buf),
            Variant::Ushort(s) => s.encode(buf),
            Variant::Uint(i) => i.encode(buf),
            Variant::Ulong(l) => l.encode(buf),
            Variant::Byte(b) => b.encode(buf),
            Variant::Short(s) => s.encode(buf),
            Variant::Int(i) => i.encode(buf),
            Variant::Long(l) => l.encode(buf),
            Variant::Float(f) => f.encode(buf),
            Variant::Double(d) => d.encode(buf),
            Variant::Decimal32(ref data) => {
                buf.put_u8(codec::FORMATCODE_DECIMAL32);
                buf.extend_from_slice(data.as_ref());
            }
            Variant::Decimal64(ref data) => {
                buf.put_u8(codec::FORMATCODE_DECIMAL64);
                buf.extend_from_slice(data.as_ref());
            }
            Variant::Decimal128(ref data) => {
                buf.put_u8(codec::FORMATCODE_DECIMAL128);
                buf.extend_from_slice(data.as_ref());
            }
            Variant::Char(c) => c.encode(buf),
            Variant::Timestamp(ref t) => t.encode(buf),
            Variant::Uuid(ref u) => u.encode(buf),
            Variant::Binary(ref b) => b.encode(buf),
            Variant::String(ref s) => s.encode(buf),
            Variant::Symbol(ref s) => s.encode(buf),
            Variant::List(ref l) => l.encode(buf),
            Variant::Map(ref m) => m.map.encode(buf),
            Variant::Array(ref a) => a.encode(buf),
            Variant::Described(ref dv) => {
                dv.0.encode(buf);
                dv.1.encode(buf);
            }
            Variant::DescribedCompound(ref described) => described.encode(buf),
        }
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encoded_size(&self) -> usize {
        self.as_ref().map_or(1, |v| v.encoded_size())
    }

    fn encode(&self, buf: &mut BytePages) {
        match *self {
            Some(ref e) => e.encode(buf),
            None => encode_null(buf),
        }
    }
}

impl Encode for Descriptor {
    fn encoded_size(&self) -> usize {
        // 1 for described type's format code (0x00) + size of the descriptor value itself
        1 + match *self {
            Descriptor::Ulong(v) => v.encoded_size(),
            Descriptor::Symbol(ref v) => v.encoded_size(),
        }
    }

    fn encode(&self, buf: &mut BytePages) {
        buf.put_u8(codec::FORMATCODE_DESCRIBED);
        match *self {
            Descriptor::Ulong(v) => v.encode(buf),
            Descriptor::Symbol(ref v) => v.encode(buf),
        }
    }
}

impl Encode for Constructor {
    fn encoded_size(&self) -> usize {
        match self {
            Constructor::FormatCode(_) => 1,
            Constructor::Described {
                descriptor,
                format_code: _,
            } => 1 + descriptor.encoded_size(),
        }
    }

    fn encode(&self, buf: &mut BytePages) {
        match self {
            Constructor::FormatCode(format_code) => buf.put_u8(*format_code),
            Constructor::Described {
                descriptor,
                format_code,
            } => {
                descriptor.encode(buf);
                buf.put_u8(*format_code);
            }
        }
    }
}

const WORD_LEN: usize = 4;

impl Encode for AmqpFrame {
    fn encoded_size(&self) -> usize {
        framing::HEADER_LEN + self.performative().encoded_size()
    }

    fn encode(&self, buf: &mut BytePages) {
        let doff: u8 = (framing::HEADER_LEN / WORD_LEN) as u8;
        buf.put_u32(codec::size_u32(self.encoded_size()));
        buf.put_u8(doff);
        buf.put_u8(framing::FRAME_TYPE_AMQP);
        buf.put_u16(self.channel_id());
        self.performative().encode(buf);
    }
}

impl Encode for SaslFrame {
    fn encoded_size(&self) -> usize {
        framing::HEADER_LEN + self.body.encoded_size()
    }

    fn encode(&self, buf: &mut BytePages) {
        let doff: u8 = (framing::HEADER_LEN / WORD_LEN) as u8;
        buf.put_u32(codec::size_u32(self.encoded_size()));
        buf.put_u8(doff);
        buf.put_u8(framing::FRAME_TYPE_SASL);
        buf.put_u16(0);
        self.body.encode(buf);
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use chrono::TimeZone;
    use ordered_float::OrderedFloat;
    use test_case::test_case;

    use super::*;
    use crate::codec::{Decode, DecodeFormatted, ListHeader};
    use crate::error::AmqpParseError;
    use crate::types::{Array, Variant, VariantMap};

    const LOREM: &str = include_str!("lorem.txt");

    /// Encode a value and assert that `encoded_size()` matches the real encoded length.
    #[track_caller]
    fn encoded<T: Encode + ?Sized>(value: &T) -> Bytes {
        let mut buf = BytePages::default();
        value.encode(&mut buf);
        let buf = buf.freeze();
        assert_eq!(
            value.encoded_size(),
            buf.len(),
            "encoded_size() does not match the encoded length"
        );
        buf
    }

    /// Encode `value`, assert the exact byte representation and decode it back.
    #[track_caller]
    fn roundtrip<T>(value: T, expected: &[u8])
    where
        T: Encode + Decode + PartialEq + Debug,
    {
        let buf = encoded(&value);
        assert_eq!(buf.as_ref(), expected, "unexpected encoding");

        let mut input = buf.clone();
        let decoded = T::decode(&mut input).unwrap();
        assert!(input.is_empty(), "decode left {} bytes", input.len());
        assert_eq!(decoded, value);
    }

    /// Encode `value` and decode it back without asserting the exact bytes.
    #[track_caller]
    fn roundtrip_value<T>(value: T) -> Bytes
    where
        T: Encode + Decode + PartialEq + Debug,
    {
        let buf = encoded(&value);
        let mut input = buf.clone();
        let decoded = T::decode(&mut input).unwrap();
        assert!(input.is_empty(), "decode left {} bytes", input.len());
        assert_eq!(decoded, value);
        buf
    }

    #[test_case(false, &[0x42]; "false")]
    #[test_case(true, &[0x41]; "true")]
    fn encode_bool(v: bool, expected: &[u8]) {
        roundtrip(v, expected);
    }

    #[test_case(0, &[0x50, 0x00]; "zero")]
    #[test_case(255, &[0x50, 0xff]; "max")]
    fn encode_u8(v: u8, expected: &[u8]) {
        roundtrip(v, expected);
    }

    #[test_case(0, &[0x60, 0x00, 0x00]; "zero")]
    #[test_case(0x1234, &[0x60, 0x12, 0x34]; "value")]
    #[test_case(u16::MAX, &[0x60, 0xff, 0xff]; "max")]
    fn encode_u16(v: u16, expected: &[u8]) {
        roundtrip(v, expected);
    }

    #[test_case(0, &[0x43]; "uint0 shortcut")]
    #[test_case(1, &[0x52, 0x01]; "smalluint low")]
    #[test_case(255, &[0x52, 0xff]; "smalluint boundary")]
    #[test_case(256, &[0x70, 0x00, 0x00, 0x01, 0x00]; "uint above boundary")]
    #[test_case(u32::MAX, &[0x70, 0xff, 0xff, 0xff, 0xff]; "uint max")]
    fn encode_u32(v: u32, expected: &[u8]) {
        roundtrip(v, expected);
    }

    #[test_case(0, &[0x44]; "ulong0 shortcut")]
    #[test_case(1, &[0x53, 0x01]; "smallulong low")]
    #[test_case(255, &[0x53, 0xff]; "smallulong boundary")]
    #[test_case(256, &[0x80, 0, 0, 0, 0, 0, 0, 0x01, 0x00]; "ulong above boundary")]
    #[test_case(u64::MAX, &[0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]; "ulong max")]
    fn encode_u64(v: u64, expected: &[u8]) {
        roundtrip(v, expected);
    }

    #[test_case(0, &[0x51, 0x00]; "zero")]
    #[test_case(-1, &[0x51, 0xff]; "minus one")]
    #[test_case(i8::MIN, &[0x51, 0x80]; "min")]
    fn encode_i8(v: i8, expected: &[u8]) {
        roundtrip(v, expected);
    }

    #[test_case(0, &[0x61, 0x00, 0x00]; "zero")]
    #[test_case(-2, &[0x61, 0xff, 0xfe]; "negative")]
    #[test_case(i16::MAX, &[0x61, 0x7f, 0xff]; "max")]
    fn encode_i16(v: i16, expected: &[u8]) {
        roundtrip(v, expected);
    }

    // there is no "int0" shortcut, zero still uses the small form
    #[test_case(0, &[0x54, 0x00]; "zero is small")]
    #[test_case(127, &[0x54, 0x7f]; "smallint upper boundary")]
    #[test_case(128, &[0x71, 0x00, 0x00, 0x00, 0x80]; "int above upper boundary")]
    #[test_case(-128, &[0x54, 0x80]; "smallint lower boundary")]
    #[test_case(-129, &[0x71, 0xff, 0xff, 0xff, 0x7f]; "int below lower boundary")]
    #[test_case(i32::MIN, &[0x71, 0x80, 0x00, 0x00, 0x00]; "int min")]
    fn encode_i32(v: i32, expected: &[u8]) {
        roundtrip(v, expected);
    }

    #[test_case(0, &[0x55, 0x00]; "zero is small")]
    #[test_case(127, &[0x55, 0x7f]; "smalllong upper boundary")]
    #[test_case(128, &[0x81, 0, 0, 0, 0, 0, 0, 0x00, 0x80]; "long above upper boundary")]
    #[test_case(-128, &[0x55, 0x80]; "smalllong lower boundary")]
    #[test_case(-129, &[0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]; "long below boundary")]
    #[test_case(i64::MAX, &[0x81, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]; "long max")]
    fn encode_i64(v: i64, expected: &[u8]) {
        roundtrip(v, expected);
    }

    #[test_case(0.0; "zero")]
    #[test_case(-1.5; "negative")]
    #[test_case(f32::MIN; "min")]
    #[test_case(f32::MAX; "max")]
    fn encode_f32(v: f32) {
        let buf = encoded(&v);
        assert_eq!(buf[0], codec::FORMATCODE_FLOAT);
        assert_eq!(buf.len(), 5);
        assert_eq!(f32::decode(&mut buf.clone()).unwrap(), v);
    }

    #[test_case(0.0; "zero")]
    #[test_case(-1.5; "negative")]
    #[test_case(f64::MIN; "min")]
    #[test_case(f64::MAX; "max")]
    fn encode_f64(v: f64) {
        let buf = encoded(&v);
        assert_eq!(buf[0], codec::FORMATCODE_DOUBLE);
        assert_eq!(buf.len(), 9);
        assert_eq!(f64::decode(&mut buf.clone()).unwrap(), v);
    }

    #[test_case('a', &[0x73, 0x00, 0x00, 0x00, 0x61]; "ascii")]
    #[test_case('\u{0}', &[0x73, 0x00, 0x00, 0x00, 0x00]; "nul")]
    #[test_case('\u{10ffff}', &[0x73, 0x00, 0x10, 0xff, 0xff]; "max codepoint")]
    fn encode_char(v: char, expected: &[u8]) {
        roundtrip(v, expected);
    }

    #[test]
    fn encode_timestamp() {
        let ts = Utc.timestamp_millis_opt(1_539_753_448_735).unwrap();
        roundtrip(ts, &[0x83, 0x00, 0x00, 0x01, 0x66, 0x80, 0x75, 0x15, 0x1f]);

        // negative (pre-epoch) timestamps keep millisecond precision
        let ts = Utc.timestamp_millis_opt(-500).unwrap();
        let buf = roundtrip_value(ts);
        assert_eq!(&buf[1..], (-500i64).to_be_bytes());
    }

    #[test]
    fn encode_uuid() {
        let uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        let mut expected = vec![0x98];
        expected.extend_from_slice(&uuid.as_u128().to_be_bytes());
        roundtrip(uuid, &expected);
    }

    /// Variable width types switch from the 8 bit to the 32 bit form above 255 bytes.
    #[test_case(0; "empty")]
    #[test_case(1; "single byte")]
    #[test_case(254; "below boundary")]
    #[test_case(255; "at boundary")]
    #[test_case(256; "above boundary")]
    #[test_case(1000; "long")]
    fn variable_width_boundary(len: usize) {
        let short = len <= u8::MAX as usize;
        let prefix = if short { 2 } else { 5 };
        let text = "x".repeat(len);
        let bytes = Bytes::from(text.clone());
        let bstr = ByteString::from(text.clone());

        let buf = roundtrip_value(bytes.clone());
        assert_eq!(buf[0], if short { 0xa0 } else { 0xb0 });
        assert_eq!(buf.len(), len + prefix);

        let buf = roundtrip_value(bstr.clone());
        assert_eq!(buf[0], if short { 0xa1 } else { 0xb1 });
        assert_eq!(buf.len(), len + prefix);

        let buf = roundtrip_value(Str::from(bstr.clone()));
        assert_eq!(buf[0], if short { 0xa1 } else { 0xb1 });
        assert_eq!(buf.len(), len + prefix);

        let buf = roundtrip_value(Symbol::from(bstr.clone()));
        assert_eq!(buf[0], if short { 0xa3 } else { 0xb3 });
        assert_eq!(buf.len(), len + prefix);

        // `str` has no `Decode` impl, but must encode exactly like `ByteString`
        let str_buf = encoded(text.as_str());
        assert_eq!(str_buf, encoded(&bstr));
    }

    #[test]
    fn encode_static_symbol() {
        // short form
        let buf = encoded(&StaticSymbol("short"));
        assert_eq!(buf.as_ref(), b"\xa3\x05short");
        assert_eq!(
            Symbol::decode(&mut buf.clone()).unwrap(),
            Symbol::from("short")
        );

        // long form, lorem.txt is longer than 255 bytes
        assert!(LOREM.len() > u8::MAX as usize);
        let buf = encoded(&StaticSymbol(LOREM));
        assert_eq!(buf[0], 0xb3);
        assert_eq!(buf.len(), LOREM.len() + 5);
        assert_eq!(Symbol::decode(&mut buf.clone()).unwrap().as_str(), LOREM);
    }

    #[test]
    fn encode_option() {
        roundtrip(Some(1u32), &[0x52, 0x01]);
        roundtrip(None::<u32>, &[0x40]);
        roundtrip(Some(Symbol::from("a")), &[0xa3, 0x01, b'a']);
        // `None` is encoded as a single null format code regardless of the inner type
        assert_eq!(encoded(&None::<Bytes>).as_ref(), &[0x40]);
    }

    #[test_case(Descriptor::Ulong(0x23), &[0x00, 0x53, 0x23]; "smallulong")]
    #[test_case(Descriptor::Ulong(0x1_0000), &[0x00, 0x80, 0, 0, 0, 0, 0, 0x01, 0x00, 0x00]; "ulong")]
    #[test_case(Descriptor::Symbol(Symbol::from("a:b")), &[0x00, 0xa3, 0x03, b'a', b':', b'b']; "symbol")]
    fn encode_descriptor(d: Descriptor, expected: &[u8]) {
        let buf = encoded(&d);
        assert_eq!(buf.as_ref(), expected);

        // `Encode for Descriptor` emits the `described` constructor byte, while
        // `Descriptor::decode` expects it to have been consumed already.
        assert_eq!(buf[0], codec::FORMATCODE_DESCRIBED);
        let mut input = buf.slice(1..);
        assert_eq!(Descriptor::decode(&mut input).unwrap(), d);
        assert!(input.is_empty());
    }

    #[test]
    fn encode_descriptor_ulong_zero() {
        let buf = encoded(&Descriptor::Ulong(0));
        assert_eq!(buf.as_ref(), &[0x00, 0x44]);
        let mut input = buf.slice(1..);
        assert_eq!(
            Descriptor::decode(&mut input).unwrap(),
            Descriptor::Ulong(0)
        );
        assert!(input.is_empty());
    }

    #[test]
    fn encode_constructor() {
        let ctor = Constructor::FormatCode(codec::FORMATCODE_BOOLEAN);
        assert_eq!(encoded(&ctor).as_ref(), &[0x56]);

        let ctor = Constructor::Described {
            descriptor: Descriptor::Ulong(0x23),
            format_code: codec::FORMATCODE_LIST8,
        };
        assert_eq!(encoded(&ctor).as_ref(), &[0x00, 0x53, 0x23, 0xc0]);
    }

    #[test]
    fn encode_empty_list() {
        // `List::encode` never emits the `list0` format code, it always writes a list8 header
        roundtrip(List(vec![]), &[0xc0, 0x01, 0x00]);
    }

    #[test]
    fn encode_small_list() {
        roundtrip(
            List(vec![
                Variant::Null,
                Variant::Boolean(true),
                Variant::Ubyte(7),
            ]),
            &[0xc0, 0x05, 0x03, 0x40, 0x41, 0x50, 0x07],
        );
    }

    #[test]
    fn encode_large_list() {
        // 60 ulongs of 9 bytes each exceed the 8 bit size limit
        let list = List((0..60).map(|_| Variant::Ulong(u64::MAX)).collect());
        let buf = roundtrip_value(list);
        assert_eq!(buf[0], codec::FORMATCODE_LIST32);
        assert_eq!(&buf[1..5], (60u32 * 9 + 4).to_be_bytes());
        assert_eq!(&buf[5..9], 60u32.to_be_bytes());
    }

    #[test]
    fn encode_map_boundary() {
        // single entry, 8 bit form
        let mut map = HashMap::new();
        map.insert(Symbol::from("k"), Variant::Ubyte(1));
        let buf = roundtrip_value(map);
        assert_eq!(
            buf.as_ref(),
            &[0xc1, 0x06, 0x02, 0xa3, 0x01, b'k', 0x50, 0x01]
        );

        // large map switches to the 32 bit form
        let mut map = HashMap::new();
        for i in 0..40u32 {
            map.insert(Symbol::from(format!("key-{i}")), Variant::Uint(i));
        }
        let buf = roundtrip_value(map.clone());
        assert_eq!(buf[0], codec::FORMATCODE_MAP32);
        assert_eq!(&buf[5..9], 80u32.to_be_bytes());

        // the ntex-util hash map encodes identically
        let base: HashMapBase<Symbol, Variant> =
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(base.encoded_size(), buf.len());
    }

    #[test]
    fn encode_empty_map() {
        let map: HashMap<Symbol, Variant> = HashMap::new();
        roundtrip(map, &[0xc1, 0x01, 0x00]);
    }

    #[test]
    fn encode_vec_symbol_map() {
        let map = VecSymbolMap(vec![
            (Symbol::from("a"), Variant::Ubyte(1)),
            (Symbol::from("b"), Variant::Null),
        ]);
        roundtrip(
            map,
            &[
                0xc1, 0x0a, 0x04, 0xa3, 0x01, b'a', 0x50, 0x01, 0xa3, 0x01, b'b', 0x40,
            ],
        );

        let large = VecSymbolMap(
            (0..40u32)
                .map(|i| (Symbol::from(format!("key-{i}")), Variant::Uint(i)))
                .collect(),
        );
        let buf = roundtrip_value(large);
        assert_eq!(buf[0], codec::FORMATCODE_MAP32);
        assert_eq!(&buf[5..9], 80u32.to_be_bytes());
    }

    #[test]
    fn encode_vec_string_map() {
        let map = VecStringMap(vec![
            (Str::from("a"), Variant::Ubyte(1)),
            (Str::from("b"), Variant::Null),
        ]);
        roundtrip(
            map,
            &[
                0xc1, 0x0a, 0x04, 0xa1, 0x01, b'a', 0x50, 0x01, 0xa1, 0x01, b'b', 0x40,
            ],
        );

        let large = VecStringMap(
            (0..40u32)
                .map(|i| (Str::from(format!("key-{i}")), Variant::Uint(i)))
                .collect(),
        );
        let buf = roundtrip_value(large);
        assert_eq!(buf[0], codec::FORMATCODE_MAP32);
        assert_eq!(&buf[5..9], 80u32.to_be_bytes());
    }

    #[test]
    fn encode_empty_array() {
        // an empty array still carries the element constructor
        roundtrip(Vec::<bool>::new(), &[0xe0, 0x02, 0x00, 0x56]);
        roundtrip(Vec::<u64>::new(), &[0xe0, 0x02, 0x00, 0x80]);
    }

    #[test]
    fn encode_small_array() {
        roundtrip(
            vec![true, false, true],
            &[0xe0, 0x05, 0x03, 0x56, 0x01, 0x00, 0x01],
        );
        roundtrip(vec![1u8, 2, 3], &[0xe0, 0x05, 0x03, 0x50, 0x01, 0x02, 0x03]);
    }

    /// Arrays switch to the 32 bit form when the payload or the element count exceeds 255.
    #[test]
    fn encode_large_array() {
        let data: Vec<u64> = (0..40).collect();
        let buf = roundtrip_value(data);
        assert_eq!(buf[0], codec::FORMATCODE_ARRAY32);
        assert_eq!(&buf[1..5], (40u32 * 8 + 4 + 1).to_be_bytes());
        assert_eq!(&buf[5..9], 40u32.to_be_bytes());

        // small payload but more than 255 elements also forces the 32 bit form
        let data: Vec<u8> = (0..300).map(|i| i as u8).collect();
        let buf = roundtrip_value(data);
        assert_eq!(buf[0], codec::FORMATCODE_ARRAY32);
        assert_eq!(&buf[5..9], 300u32.to_be_bytes());
    }

    /// Every `ArrayEncode` implementation must round trip through `Vec<T>`.
    #[test]
    fn encode_array_element_types() {
        roundtrip_value(vec![1u8, 2]);
        roundtrip_value(vec![1u16, 2]);
        roundtrip_value(vec![1u32, 300]);
        roundtrip_value(vec![1u64, 300]);
        roundtrip_value(vec![-1i8, 2]);
        roundtrip_value(vec![-1i16, 2]);
        roundtrip_value(vec![-1i32, 300]);
        roundtrip_value(vec![-1i64, 300]);
        roundtrip_value(vec![true, false]);
        roundtrip_value(vec!['a', '\u{10ffff}']);
        roundtrip_value(vec![Uuid::from_u128(1), Uuid::from_u128(2)]);
        roundtrip_value(vec![
            Utc.timestamp_millis_opt(0).unwrap(),
            Utc.timestamp_millis_opt(1_539_753_448_735).unwrap(),
        ]);
        roundtrip_value(vec![Bytes::from_static(b"ab"), Bytes::new()]);
        roundtrip_value(vec![ByteString::from("ab"), ByteString::new()]);
        roundtrip_value(vec![Symbol::from("ab"), Symbol::from("")]);

        let buf = roundtrip_value(vec![1.5f32, -1.5]);
        assert_eq!(buf[3], codec::FORMATCODE_FLOAT);
        let buf = roundtrip_value(vec![1.5f64, -1.5]);
        assert_eq!(buf[3], codec::FORMATCODE_DOUBLE);

        // `str` is `ArrayEncode` but unsized, so it cannot go through `Vec<T>`
        let mut buf = BytePages::default();
        "ab".array_encode(&mut buf);
        let buf = buf.freeze();
        assert_eq!("ab".array_encoded_size(), buf.len());
        assert_eq!(buf.as_ref(), &[0, 0, 0, 2, b'a', b'b']);
        assert_eq!(
            <str as ArrayEncode>::ARRAY_CONSTRUCTOR,
            ByteString::ARRAY_CONSTRUCTOR
        );

        let mut map = HashMap::new();
        map.insert(Symbol::from("k"), Variant::Ubyte(1));
        let buf = roundtrip_value(vec![map.clone(), HashMap::new()]);
        assert_eq!(buf[3], codec::FORMATCODE_MAP32);
    }

    #[test]
    fn encode_multiple() {
        // a single element is encoded bare, not as an array
        roundtrip(Multiple(vec![Symbol::from("a")]), &[0xa3, 0x01, b'a']);
        // zero or more than one element use the array encoding
        roundtrip(Multiple(Vec::<Symbol>::new()), &[0xe0, 0x02, 0x00, 0xb3]);
        roundtrip(
            Multiple(vec![Symbol::from("a"), Symbol::from("b")]),
            &[0xe0, 0x0c, 0x02, 0xb3, 0, 0, 0, 1, b'a', 0, 0, 0, 1, b'b'],
        );
    }

    #[test]
    fn encode_array_type() {
        let array = Array::from(vec![1u32, 2, 3]);
        let buf = roundtrip_value(array.clone());
        assert_eq!(buf[0], codec::FORMATCODE_ARRAY8);
        assert_eq!(array.decode::<u32>().unwrap(), vec![1, 2, 3]);

        let array = Array::from((0..40u64).collect::<Vec<_>>());
        let buf = roundtrip_value(array.clone());
        assert_eq!(buf[0], codec::FORMATCODE_ARRAY32);
        assert_eq!(
            array.decode::<u64>().unwrap(),
            (0..40u64).collect::<Vec<_>>()
        );
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Pair(u8, u8);

    impl Composite for Pair {
        fn descriptor() -> Descriptor {
            Descriptor::Ulong(0x77)
        }
    }

    impl Encode for Pair {
        fn encoded_size(&self) -> usize {
            3 + self.0.encoded_size() + self.1.encoded_size()
        }

        fn encode(&self, buf: &mut BytePages) {
            buf.put_u8(codec::FORMATCODE_LIST8);
            buf.put_u8((self.0.encoded_size() + self.1.encoded_size() + 1) as u8);
            buf.put_u8(2);
            self.0.encode(buf);
            self.1.encode(buf);
        }
    }

    impl DecodeFormatted for Pair {
        fn decode_with_format(input: &mut Bytes, fmt: u8) -> Result<Self, AmqpParseError> {
            let header = ListHeader::decode_with_format(input, fmt)?;
            if header.count != 2 {
                return Err(AmqpParseError::InvalidSize);
            }
            Ok(Pair(u8::decode(input)?, u8::decode(input)?))
        }
    }

    #[test]
    fn encode_list_described() {
        roundtrip(ListDescribed(Vec::<Pair>::new()), &[0xc0, 0x01, 0x00]);

        // descriptor (3 bytes) + pair (7 bytes) per element
        roundtrip(
            ListDescribed(vec![Pair(1, 2)]),
            &[
                0xc0, 0x0b, 0x01, 0x00, 0x53, 0x77, 0xc0, 0x05, 0x02, 0x50, 0x01, 0x50, 0x02,
            ],
        );

        // 26 elements * 10 bytes > 255, so the list32 form is used
        let items: Vec<Pair> = (0..26u8).map(|i| Pair(i, i)).collect();
        let buf = roundtrip_value(ListDescribed(items));
        assert_eq!(buf[0], codec::FORMATCODE_LIST32);
        assert_eq!(&buf[1..5], (26u32 * 10 + 4).to_be_bytes());
        assert_eq!(&buf[5..9], 26u32.to_be_bytes());
    }

    #[test]
    fn encode_variant_all() {
        let cases = vec![
            Variant::Null,
            Variant::Boolean(true),
            Variant::Ubyte(1),
            Variant::Ushort(2),
            Variant::Uint(300),
            Variant::Ulong(300),
            Variant::Byte(-1),
            Variant::Short(-2),
            Variant::Int(-300),
            Variant::Long(-300),
            Variant::Float(OrderedFloat(1.5)),
            Variant::Double(OrderedFloat(2.5)),
            Variant::Decimal32([1, 2, 3, 4]),
            Variant::Decimal64([1, 2, 3, 4, 5, 6, 7, 8]),
            Variant::Decimal128([9; 16]),
            Variant::Char('x'),
            Variant::Timestamp(Utc.timestamp_millis_opt(1_539_753_448_735).unwrap()),
            Variant::Uuid(Uuid::from_u128(42)),
            Variant::Binary(Bytes::from_static(b"bin")),
            Variant::String(Str::from("str")),
            Variant::Symbol(Symbol::from("sym")),
            Variant::List(List(vec![Variant::Ubyte(1), Variant::Null])),
            Variant::Array(Array::from(vec![1u32, 2])),
        ];

        for case in cases {
            roundtrip_value(case);
        }
    }

    #[test]
    fn encode_variant_decimals() {
        assert_eq!(
            encoded(&Variant::Decimal32([1, 2, 3, 4])).as_ref(),
            &[0x74, 1, 2, 3, 4]
        );
        assert_eq!(
            encoded(&Variant::Decimal64([1, 2, 3, 4, 5, 6, 7, 8])).as_ref(),
            &[0x84, 1, 2, 3, 4, 5, 6, 7, 8]
        );
        let mut expected = vec![0x94];
        expected.extend_from_slice(&[7u8; 16]);
        assert_eq!(encoded(&Variant::Decimal128([7; 16])).as_ref(), expected);
    }

    #[test]
    fn encode_variant_map() {
        let mut map = HashMapBase::default();
        map.insert(Variant::Symbol(Symbol::from("k")), Variant::Ubyte(1));
        let variant = Variant::Map(VariantMap::new(map));
        let buf = roundtrip_value(variant);
        assert_eq!(
            buf.as_ref(),
            &[0xc1, 0x06, 0x02, 0xa3, 0x01, b'k', 0x50, 0x01]
        );
    }

    #[test]
    fn encode_variant_described() {
        // a simple (non compound) described value stays a `Described` variant
        let variant = Variant::Described((Descriptor::Ulong(0x12), Box::new(Variant::Ubyte(5))));
        roundtrip(variant, &[0x00, 0x53, 0x12, 0x50, 0x05]);

        let variant = Variant::Described((
            Descriptor::Symbol(Symbol::from("d")),
            Box::new(Variant::Null),
        ));
        roundtrip(variant, &[0x00, 0xa3, 0x01, b'd', 0x40]);

        // a described compound value decodes into `DescribedCompound`
        let mut buf = BytePages::default();
        Descriptor::Ulong(0x12).encode(&mut buf);
        List(vec![Variant::Ubyte(1)]).encode(&mut buf);
        let bytes = buf.freeze();

        let decoded = Variant::decode(&mut bytes.clone()).unwrap();
        let Variant::DescribedCompound(ref compound) = decoded else {
            panic!("expected a described compound, got {decoded:?}");
        };
        assert_eq!(compound.descriptor(), &Descriptor::Ulong(0x12));
        assert_eq!(
            compound.decode::<List>().unwrap(),
            List(vec![Variant::Ubyte(1)])
        );
        assert_eq!(encoded(&decoded), bytes);
    }

    #[test]
    fn encode_amqp_frame() {
        use crate::protocol::{Begin, Frame};

        let begin = Begin::build()
            .next_outgoing_id(1)
            .incoming_window(2)
            .outgoing_window(3)
            .finish();
        let frame = AmqpFrame::new(7, Frame::Begin(begin));
        let buf = encoded(&frame);

        assert_eq!(&buf[0..4], (buf.len() as u32).to_be_bytes());
        assert_eq!(buf[4], 2); // doff
        assert_eq!(buf[5], framing::FRAME_TYPE_AMQP);
        assert_eq!(&buf[6..8], 7u16.to_be_bytes());

        // the 4 byte frame size is written by `Encode` but consumed by `AmqpCodec`
        let mut input = buf.slice(4..);
        let decoded = AmqpFrame::decode(&mut input).unwrap();
        assert!(input.is_empty());
        assert_eq!(decoded, frame);
    }

    #[test]
    fn encode_sasl_frame() {
        use crate::protocol::{SaslCode, SaslOutcome};

        let frame = SaslFrame::from(SaslOutcome {
            code: SaslCode::Ok,
            additional_data: None,
        });
        let buf = encoded(&frame);

        assert_eq!(&buf[0..4], (buf.len() as u32).to_be_bytes());
        assert_eq!(buf[4], 2); // doff
        assert_eq!(buf[5], framing::FRAME_TYPE_SASL);
        assert_eq!(&buf[6..8], 0u16.to_be_bytes());

        let mut input = buf.slice(4..);
        let decoded = SaslFrame::decode(&mut input).unwrap();
        assert!(input.is_empty());
        assert_eq!(decoded, frame);
    }
}
