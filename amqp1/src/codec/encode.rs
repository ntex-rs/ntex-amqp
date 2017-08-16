use std::{i8, u8};

use bytes::{BigEndian, BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use codec::Encodable;
use framing;
use framing::AmqpFrame;
use types::{ByteStr, Null, Symbol, Variant};
use uuid::Uuid;

fn ensure_capacity(encodable: &Encodable, buf: &mut BytesMut) {
    if buf.remaining_mut() < encodable.encoded_size() {
        buf.reserve(encodable.encoded_size());
    }
}

impl Encodable for bool {
    fn encoded_size(&self) -> usize { 1 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        if *self {
            buf.put_u8(0x40)
        } else {
            buf.put_u8(0x41)
        }
    }
}

impl Encodable for u8 {
    fn encoded_size(&self) -> usize { 2 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        buf.put_u8(0x50);
        buf.put_u8(*self);
    }
}

impl Encodable for u16 {
    fn encoded_size(&self) -> usize { 3 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        buf.put_u8(0x60);
        buf.put_u16::<BigEndian>(*self);
    }
}

impl Encodable for u32 {
    fn encoded_size(&self) -> usize {
        if *self == 0 {
            1
        } else if *self > u8::MAX as u32 {
            5
        } else {
            2
        }

    }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

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
    fn encoded_size(&self) -> usize {
        if *self == 0 {
            1
        } else if *self > u8::MAX as u64 {
            9
        } else {
            2
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

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
    fn encoded_size(&self) -> usize { 2 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        buf.put_u8(0x51);
        buf.put_i8(*self);
    }
}

impl Encodable for i16 {
    fn encoded_size(&self) -> usize { 3 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        buf.put_u8(0x61);
        buf.put_i16::<BigEndian>(*self);
    }
}

impl Encodable for i32 {
    fn encoded_size(&self) -> usize {
        if *self > i8::MAX as i32 || *self < i8::MIN as i32 {
            5
        } else {
            2
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

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
    fn encoded_size(&self) -> usize {
        if *self > i8::MAX as i64 || *self < i8::MIN as i64 {
            9
        } else {
            2
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

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
    fn encoded_size(&self) -> usize { 5 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        buf.put_u8(0x72);
        buf.put_f32::<BigEndian>(*self);
    }
}

impl Encodable for f64 {
    fn encoded_size(&self) -> usize { 9 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        buf.put_u8(0x82);
        buf.put_f64::<BigEndian>(*self);
    }
}

impl Encodable for char {
    fn encoded_size(&self) -> usize { 5 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        buf.put_u8(0x73);
        buf.put_u32::<BigEndian>(*self as u32);
    }
}

impl Encodable for DateTime<Utc> {
    fn encoded_size(&self) -> usize { 9 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        let timestamp = self.timestamp() * 1000 + (self.timestamp_subsec_millis() as i64);
        buf.put_u8(0x83);
        buf.put_i64::<BigEndian>(timestamp);
    }
}

impl Encodable for Uuid {
    fn encoded_size(&self) -> usize { 17 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        buf.put_u8(0x98);
        buf.put_slice(self.as_bytes());
    }
}

impl Encodable for Bytes {
    fn encoded_size(&self) -> usize {
        let length = self.len();
        let size = if length > u8::MAX as usize || length < u8::MIN as usize {
            5
        } else {
            2
        };
        size + length
    }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        let length = self.len();
        if length > u8::MAX as usize || length < u8::MIN as usize {
            buf.put_u8(0xB0);
            buf.put_u32::<BigEndian>(length as u32);
        } else {
            buf.put_u8(0xA0);
            buf.put_u8(length as u8);
        }
        buf.put(self);
    }
}

impl Encodable for ByteStr {
    fn encoded_size(&self) -> usize {
        let length = self.len();
        let size = if length > u8::MAX as usize || length < u8::MIN as usize {
            5
        } else {
            2
        };
        size + length
    }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        let length = self.len();
        if length > u8::MAX as usize || length < u8::MIN as usize {
            buf.put_u8(0xB1);
            buf.put_u32::<BigEndian>(length as u32);
        } else {
            buf.put_u8(0xA1);
            buf.put_u8(length as u8);
        }
        buf.put(self.as_bytes());
    }
}

impl Encodable for Null {
    fn encoded_size(&self) -> usize { 1 }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);
        buf.put_u8(0x40);
    }
}

impl Encodable for str {
    fn encoded_size(&self) -> usize {
        let length = self.len();
        let size = if length > u8::MAX as usize || length < u8::MIN as usize {
            5
        } else {
            2
        };
        size + length
    }

    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < self.encoded_size() {
            buf.reserve(self.encoded_size());
        }

        let length = self.len();
        if length > u8::MAX as usize || length < u8::MIN as usize {
            buf.put_u8(0xB1);
            buf.put_u32::<BigEndian>(length as u32);
        } else {
            buf.put_u8(0xA1);
            buf.put_u8(length as u8);
        }
        buf.put(self.as_bytes());
    }
}

impl Encodable for Symbol {
    fn encoded_size(&self) -> usize {
        let length = self.len();
        let size = if length > u8::MAX as usize || length < u8::MIN as usize {
            5
        } else {
            2
        };
        size + length
    }

    fn encode(&self, buf: &mut BytesMut) {
        ensure_capacity(self, buf);

        let length = self.len();
        if length > u8::MAX as usize || length < u8::MIN as usize {
            buf.put_u8(0xB3);
            buf.put_u32::<BigEndian>(length as u32);
        } else {
            buf.put_u8(0xA3);
            buf.put_u8(length as u8);
        }
        buf.put(self.as_bytes());
    }
}

impl Encodable for Variant {
    fn encoded_size(&self) -> usize {
        match *self {
            Variant::Null => Null.encoded_size(),
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
            Variant::Char(c) => c.encoded_size(),
            Variant::Timestamp(ref t) => t.encoded_size(),
            Variant::Uuid(ref u) => u.encoded_size(),
            Variant::Binary(ref b) => b.encoded_size(),
            Variant::String(ref s) => s.encoded_size(),
            Variant::Symbol(ref s) => s.encoded_size(),
        }
    }

    /// Encodes `Variant` into provided `BytesMut`
    fn encode(&self, buf: &mut BytesMut) -> () {
        match *self {
            Variant::Null => Null.encode(buf),
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

const WORD_LEN: usize = 4;
impl Encodable for AmqpFrame {
    fn encoded_size(&self) -> usize {
        framing::HEADER_LEN + self.body().len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        if buf.remaining_mut() < self.encoded_size() {
            buf.reserve(self.encoded_size());
        }

        let doff: u8 = (framing::HEADER_LEN / WORD_LEN) as u8;
        buf.put_u32::<BigEndian>(self.encoded_size() as u32);
        buf.put_u8(doff);
        buf.put_u8(framing::AMQP_TYPE);
        buf.put_u16::<BigEndian>(self.channel_id());
        buf.put(self.body());
    }
}