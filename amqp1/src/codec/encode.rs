use std::{i8, u8};

use bytes::{BigEndian, BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use codec::Encodable;
use types::{ByteStr, Symbol, Variant};
use uuid::Uuid;

impl Encodable for bool {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 1 {
            buf.reserve(1);
        }
        if *self { buf.put_u8(0x40) } else { buf.put_u8(0x41) }
    }
}

impl Encodable for u8 {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 2 {
            buf.reserve(2);
        }
        buf.put_u8(0x50);
        buf.put_u8(*self);
    }
}

impl Encodable for u16 {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 3 {
            buf.reserve(3);
        }
        buf.put_u8(0x60);
        buf.put_u16::<BigEndian>(*self);
    }
}

impl Encodable for u32 {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 5 {
            buf.reserve(5);
        }

        if *self == 0 {
            buf.put_u8(0x43)
        } else if *self > u8::MAX as u32 {
            buf.put_u8(0x70);
            buf.put_u32::<BigEndian>(*self);
        } else {
            buf.put_u8(0x52);
            buf.put_u8(*self as u8);
        }
    }
}

impl Encodable for u64 {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 9 {
            buf.reserve(9);
        }

        if *self == 0 {
            buf.put_u8(0x44)
        } else if *self > u8::MAX as u64 {
            buf.put_u8(0x80);
            buf.put_u64::<BigEndian>(*self);
        } else {
            buf.put_u8(0x53);
            buf.put_u8(*self as u8);
        }
    }
}

impl Encodable for i8 {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 2 {
            buf.reserve(2);
        }
        buf.put_u8(0x51);
        buf.put_i8(*self);
    }
}

impl Encodable for i16 {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 3 {
            buf.reserve(3);
        }
        buf.put_u8(0x61);
        buf.put_i16::<BigEndian>(*self);
    }
}

impl Encodable for i32 {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 5 {
            buf.reserve(5);
        }

        if *self > i8::MAX as i32 || *self < i8::MIN as i32 {
            buf.put_u8(0x71);
            buf.put_i32::<BigEndian>(*self);
        } else {
            buf.put_u8(0x54);
            buf.put_i8(*self as i8);
        }
    }
}

impl Encodable for i64 {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 9 {
            buf.reserve(9);
        }

        if *self > i8::MAX as i64 || *self < i8::MIN as i64 {
            buf.put_u8(0x81);
            buf.put_i64::<BigEndian>(*self);
        } else {
            buf.put_u8(0x55);
            buf.put_i8(*self as i8);
        }
    }
}

impl Encodable for f32 {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 5 {
            buf.reserve(5);
        }

        buf.put_u8(0x72);
        buf.put_f32::<BigEndian>(*self);
    }
}

impl Encodable for f64 {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 9 {
            buf.reserve(9);
        }

        buf.put_u8(0x82);
        buf.put_f64::<BigEndian>(*self);
    }
}

impl Encodable for char {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 5 {
            buf.reserve(5);
        }

        buf.put_u8(0x73);
        buf.put_u32::<BigEndian>(*self as u32);
    }
}

impl Encodable for DateTime<Utc> {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 9 {
            buf.reserve(9);
        }
        let timestamp = self.timestamp() * 1000 + (self.timestamp_subsec_millis() as i64);
        buf.put_u8(0x83);
        buf.put_i64::<BigEndian>(timestamp);
    }
}

impl Encodable for Uuid {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 17 {
            buf.reserve(17);
        }

        buf.put_u8(0x98);
        buf.put_slice(self.as_bytes());
    }
}

impl Encodable for Bytes {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 5 {
            buf.reserve(5);
        }

        let length = self.len();
        if length > u8::MAX as usize || length < u8::MIN as usize {
            buf.put_u8(0xB0);
            buf.put_u32::<BigEndian>(length as u32);
        } else {
            buf.put_u8(0xA0);
            buf.put_u8(length as u8);
        }
        buf.extend_from_slice(self);
    }
}

impl Encodable for ByteStr {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 5 {
            buf.reserve(5);
        }

        let length = self.len();
        if length > u8::MAX as usize || length < u8::MIN as usize {
            buf.put_u8(0xB1);
            buf.put_u32::<BigEndian>(length as u32);
        } else {
            buf.put_u8(0xA1);
            buf.put_u8(length as u8);
        }
        buf.extend_from_slice(self.as_bytes());
    }
}

impl Encodable for str {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 5 {
            buf.reserve(5);
        }

        let length = self.len();
        if length > u8::MAX as usize || length < u8::MIN as usize {
            buf.put_u8(0xB1);
            buf.put_u32::<BigEndian>(length as u32);
        } else {
            buf.put_u8(0xA1);
            buf.put_u8(length as u8);
        }
        buf.extend_from_slice(self.as_bytes());
    }
}

impl Encodable for Symbol {
    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < 5 {
            buf.reserve(5);
        }

        let length = self.len();
        if length > u8::MAX as usize || length < u8::MIN as usize {
            buf.put_u8(0xB3);
            buf.put_u32::<BigEndian>(length as u32);
        } else {
            buf.put_u8(0xA3);
            buf.put_u8(length as u8);
        }
        buf.extend_from_slice(self.as_bytes());
    }
}

impl Encodable for Variant {
    /// Encodes `Variant` into provided `BytesMut`
    fn encode(&self, buf: &mut BytesMut) -> () {
        match *self {
            Variant::Null => {
                if buf.remaining_mut() < 1 {
                    buf.reserve(1);
                }
                buf.put_u8(0x40);
            }
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
            Variant::Char(c) => c.encode(buf),
            Variant::Timestamp(ref t) => t.encode(buf),
            Variant::Uuid(ref u) => u.encode(buf),
            Variant::Binary(ref b) => b.encode(buf),
            Variant::String(ref s) => s.encode(buf),
            Variant::Symbol(ref s) => s.encode(buf),
        }
    }
}