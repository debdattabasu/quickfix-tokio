//! Conversions between Rust types and FIX wire representations of field values.

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};

use crate::error::ConversionError;
use crate::message::Tag;

/// A value that can be written as a FIX field value.
pub trait FixEncode {
    fn encode(&self, buf: &mut Vec<u8>);
}

/// A value that can be parsed from a FIX field value.
pub trait FixDecode: Sized {
    fn decode(tag: Tag, bytes: &[u8]) -> Result<Self, ConversionError>;
}

fn invalid(tag: Tag, bytes: &[u8]) -> ConversionError {
    ConversionError::InvalidValue { tag, value: String::from_utf8_lossy(bytes).into_owned() }
}

impl FixEncode for &str {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }
}

impl FixEncode for String {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }
}

impl FixEncode for &[u8] {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }
}

impl FixEncode for Vec<u8> {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }
}

impl FixEncode for char {
    fn encode(&self, buf: &mut Vec<u8>) {
        let mut b = [0u8; 4];
        buf.extend_from_slice(self.encode_utf8(&mut b).as_bytes());
    }
}

impl FixEncode for bool {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.push(if *self { b'Y' } else { b'N' });
    }
}

macro_rules! impl_int {
    ($($t:ty),*) => {$(
        impl FixEncode for $t {
            fn encode(&self, buf: &mut Vec<u8>) {
                buf.extend_from_slice(itoa_buf(*self as i64).as_bytes());
            }
        }
        impl FixDecode for $t {
            fn decode(tag: Tag, bytes: &[u8]) -> Result<Self, ConversionError> {
                let s = std::str::from_utf8(bytes).map_err(|_| invalid(tag, bytes))?;
                s.parse::<$t>().map_err(|_| invalid(tag, bytes))
            }
        }
    )*};
}
impl_int!(i32, i64, u32, u64, usize);

fn itoa_buf(v: i64) -> String {
    v.to_string()
}

impl FixEncode for f64 {
    fn encode(&self, buf: &mut Vec<u8>) {
        // FIX float: decimal notation, no exponent.
        let s = format!("{}", self);
        debug_assert!(!s.contains('e') && !s.contains('E'));
        buf.extend_from_slice(s.as_bytes());
    }
}

impl FixDecode for f64 {
    fn decode(tag: Tag, bytes: &[u8]) -> Result<Self, ConversionError> {
        let s = std::str::from_utf8(bytes).map_err(|_| invalid(tag, bytes))?;
        // Only digits, '.', and a leading '-' are valid on the FIX wire
        // (no '+', no exponents) — matches the C++ DoubleConvertor.
        if s.bytes().any(|b| !matches!(b, b'0'..=b'9' | b'.' | b'-')) {
            return Err(invalid(tag, bytes));
        }
        s.parse::<f64>().map_err(|_| invalid(tag, bytes))
    }
}

impl FixDecode for String {
    fn decode(tag: Tag, bytes: &[u8]) -> Result<Self, ConversionError> {
        String::from_utf8(bytes.to_vec()).map_err(|_| invalid(tag, bytes))
    }
}

impl FixDecode for Vec<u8> {
    fn decode(_tag: Tag, bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(bytes.to_vec())
    }
}

impl FixDecode for bool {
    fn decode(tag: Tag, bytes: &[u8]) -> Result<Self, ConversionError> {
        match bytes {
            b"Y" => Ok(true),
            b"N" => Ok(false),
            _ => Err(invalid(tag, bytes)),
        }
    }
}

impl FixDecode for char {
    fn decode(tag: Tag, bytes: &[u8]) -> Result<Self, ConversionError> {
        let s = std::str::from_utf8(bytes).map_err(|_| invalid(tag, bytes))?;
        let mut chars = s.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) => Ok(c),
            _ => Err(invalid(tag, bytes)),
        }
    }
}

/// Precision used when encoding UTCTimestamp values (tag 52 et al).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampPrecision {
    Seconds,
    #[default]
    Millis,
    Micros,
    Nanos,
}

/// A FIX UTCTimestamp: `YYYYMMDD-HH:MM:SS[.sss[sss[sss]]]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtcTimestamp {
    pub time: DateTime<Utc>,
    pub precision: TimestampPrecision,
}

impl UtcTimestamp {
    pub fn now() -> Self {
        Self { time: Utc::now(), precision: TimestampPrecision::default() }
    }

    pub fn new(time: DateTime<Utc>, precision: TimestampPrecision) -> Self {
        Self { time, precision }
    }
}

impl FixEncode for UtcTimestamp {
    fn encode(&self, buf: &mut Vec<u8>) {
        let fmt = match self.precision {
            TimestampPrecision::Seconds => "%Y%m%d-%H:%M:%S",
            TimestampPrecision::Millis => "%Y%m%d-%H:%M:%S%.3f",
            TimestampPrecision::Micros => "%Y%m%d-%H:%M:%S%.6f",
            TimestampPrecision::Nanos => "%Y%m%d-%H:%M:%S%.9f",
        };
        buf.extend_from_slice(self.time.format(fmt).to_string().as_bytes());
    }
}

impl FixDecode for UtcTimestamp {
    fn decode(tag: Tag, bytes: &[u8]) -> Result<Self, ConversionError> {
        let s = std::str::from_utf8(bytes).map_err(|_| invalid(tag, bytes))?;
        let (fmt, precision) = match s.len() {
            17 => ("%Y%m%d-%H:%M:%S", TimestampPrecision::Seconds),
            21 => ("%Y%m%d-%H:%M:%S%.3f", TimestampPrecision::Millis),
            24 => ("%Y%m%d-%H:%M:%S%.6f", TimestampPrecision::Micros),
            27 => ("%Y%m%d-%H:%M:%S%.9f", TimestampPrecision::Nanos),
            _ => return Err(invalid(tag, bytes)),
        };
        let naive = NaiveDateTime::parse_from_str(s, fmt).map_err(|_| invalid(tag, bytes))?;
        Ok(Self { time: Utc.from_utc_datetime(&naive), precision })
    }
}

/// A FIX UTCDateOnly / LocalMktDate: `YYYYMMDD`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixDate(pub NaiveDate);

impl FixEncode for FixDate {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.0.format("%Y%m%d").to_string().as_bytes());
    }
}

impl FixDecode for FixDate {
    fn decode(tag: Tag, bytes: &[u8]) -> Result<Self, ConversionError> {
        let s = std::str::from_utf8(bytes).map_err(|_| invalid(tag, bytes))?;
        NaiveDate::parse_from_str(s, "%Y%m%d").map(FixDate).map_err(|_| invalid(tag, bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: impl FixEncode) -> Vec<u8> {
        let mut buf = Vec::new();
        v.encode(&mut buf);
        buf
    }

    #[test]
    fn roundtrip_scalars() {
        assert_eq!(enc(42i64), b"42");
        assert_eq!(enc(-7i32), b"-7");
        assert_eq!(enc(true), b"Y");
        assert_eq!(enc(false), b"N");
        assert_eq!(enc(1.5f64), b"1.5");
        assert_eq!(enc("ABC"), b"ABC");
        assert_eq!(i64::decode(1, b"42").unwrap(), 42);
        assert!(i64::decode(1, b"4x2").is_err());
        assert!(bool::decode(1, b"X").is_err());
        assert!(f64::decode(1, b"1e5").is_err());
        assert_eq!(f64::decode(1, b"-1.25").unwrap(), -1.25);
    }

    #[test]
    fn roundtrip_timestamp() {
        let ts = UtcTimestamp::decode(52, b"20140515-19:49:56.659").unwrap();
        assert_eq!(ts.precision, TimestampPrecision::Millis);
        assert_eq!(enc(ts), b"20140515-19:49:56.659");

        let ts = UtcTimestamp::decode(52, b"20140515-19:49:56").unwrap();
        assert_eq!(ts.precision, TimestampPrecision::Seconds);
        assert_eq!(enc(ts), b"20140515-19:49:56");

        assert!(UtcTimestamp::decode(52, b"2014-05-15").is_err());
    }
}
