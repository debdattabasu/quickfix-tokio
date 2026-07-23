//! The FIX message: standard header, body, and trailer, with wire-format
//! parsing and serialization.

use crate::error::{Error, Result};
use crate::field_map::{FieldMap, TagValue, write_tag_value};
use crate::tags;
use crate::value::{FixEncode, UtcTimestamp};

/// Signed, like the reference engines' `int` tags: a negative wire tag
/// (e.g. `-1=x`) must parse so it can be *rejected* as an invalid tag
/// number rather than garbling the whole message.
pub type Tag = i32;
pub const SOH: u8 = 0x01;

#[derive(Debug, Clone, Default)]
pub struct Message {
    pub header: FieldMap,
    pub body: FieldMap,
    pub trailer: FieldMap,
    /// False when a header field appeared after the body began or a body
    /// field after the trailer began; records the first offending tag.
    /// Checked by dictionary validation (ValidateFieldsOutOfOrder).
    structure_error: Option<Tag>,
}

impl Message {
    pub fn new() -> Self {
        Self::default()
    }

    /// A skeleton admin/app message with MsgType set. The session layer fills
    /// in BeginString, CompIDs, MsgSeqNum, and SendingTime before sending.
    pub fn with_type(msg_type: &str) -> Self {
        let mut m = Self::default();
        m.header.set(tags::MSG_TYPE, msg_type);
        m
    }

    pub fn msg_type(&self) -> Result<String> {
        Ok(self.header.get_string(tags::MSG_TYPE)?)
    }

    pub fn is_admin(&self) -> bool {
        self.header
            .get_raw(tags::MSG_TYPE)
            .map(|t| matches!(t, b"0" | b"1" | b"2" | b"3" | b"4" | b"5" | b"A" | b"n"))
            .unwrap_or(false)
    }

    pub fn seq_num(&self) -> Result<u64> {
        Ok(self.header.get::<u64>(tags::MSG_SEQ_NUM)?)
    }

    pub fn poss_dup(&self) -> bool {
        self.header.get_raw(tags::POSS_DUP_FLAG) == Some(b"Y")
    }

    /// First tag found out of section order during parse, if any.
    pub fn structure_error(&self) -> Option<Tag> {
        self.structure_error
    }

    // ----- parsing -----

    /// Parse a single complete FIX message (as framed by [`crate::parser`]).
    ///
    /// `validate_length_checksum` enforces BodyLength(9) and CheckSum(10)
    /// correctness; per the spec a failure means the message is garbled and
    /// must be ignored (no Reject, no seqnum increment).
    pub fn parse(raw: &[u8], validate_length_checksum: bool) -> Result<Self> {
        let mut msg = Self::default();
        let mut pos = 0usize;
        // Section: 0 = header, 1 = body, 2 = trailer.
        let mut section = 0u8;
        let mut field_index = 0usize;
        let mut body_start = None;
        let mut checksum_field_start = None;
        let mut pending_data: Option<(Tag, usize)> = None;

        while pos < raw.len() {
            let field_start = pos;
            // tag
            let eq = raw[pos..]
                .iter()
                .position(|&b| b == b'=')
                .ok_or_else(|| Error::Parse("field without '='".into()))?
                + pos;
            let tag: Tag = std::str::from_utf8(&raw[pos..eq])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    Error::Parse(format!(
                        "invalid tag {:?}",
                        String::from_utf8_lossy(&raw[pos..eq.min(pos + 16)])
                    ))
                })?;

            // value: length-prefixed data fields may contain SOH bytes
            let val_start = eq + 1;
            let val_end = match pending_data.take() {
                Some((data_tag, len)) if data_tag == tag => {
                    let end = val_start + len;
                    if end > raw.len() || raw.get(end) != Some(&SOH) {
                        return Err(Error::Parse(format!(
                            "data field {tag} shorter than its declared length {len}"
                        )));
                    }
                    end
                }
                _ => raw[val_start..]
                    .iter()
                    .position(|&b| b == SOH)
                    .map(|p| val_start + p)
                    .ok_or_else(|| Error::Parse(format!("field {tag} not SOH-terminated")))?,
            };
            let value = raw[val_start..val_end].to_vec();
            pos = val_end + 1;

            // Peek: if this is a Length tag, the next field is unframed data.
            if let Some(data_tag) = tags::data_tag_for_length_tag(tag) {
                if let Ok(len) = std::str::from_utf8(&value)
                    .map_err(|_| ())
                    .and_then(|s| s.parse::<usize>().map_err(|_| ()))
                {
                    pending_data = Some((data_tag, len));
                }
            }

            // Enforce leading 8, 9, 35 — anything else is garbled.
            match field_index {
                0 if tag != tags::BEGIN_STRING => {
                    return Err(Error::Parse("first field is not BeginString(8)".into()));
                }
                1 if tag != tags::BODY_LENGTH => {
                    return Err(Error::Parse("second field is not BodyLength(9)".into()));
                }
                2 if tag != tags::MSG_TYPE => {
                    return Err(Error::Parse("third field is not MsgType(35)".into()));
                }
                _ => {}
            }
            field_index += 1;

            // Fields are routed to their section by tag class even when out
            // of position (like the C++ engine); the violation is recorded
            // in structure_error for the dictionary's out-of-order check.
            let tv = TagValue { tag, value };
            if tag == tags::CHECK_SUM {
                checksum_field_start = Some(field_start);
                section = 2;
                msg.trailer.push_tag_value(tv);
            } else if tags::is_header_tag(tag) {
                if section != 0 {
                    msg.structure_error.get_or_insert(tag);
                }
                msg.header.push_tag_value(tv);
            } else if tags::is_trailer_tag(tag) {
                section = 2;
                msg.trailer.push_tag_value(tv);
            } else {
                if section == 0 {
                    section = 1;
                    body_start = Some(field_start);
                } else if section == 2 {
                    // Body field after the trailer began.
                    msg.structure_error.get_or_insert(tag);
                }
                if body_start.is_none() {
                    body_start = Some(field_start);
                }
                msg.body.push_tag_value(tv);
            }
        }

        if validate_length_checksum {
            let checksum_start = checksum_field_start
                .ok_or_else(|| Error::Parse("message has no CheckSum(10)".into()))?;
            let declared_len: usize = msg
                .header
                .get(tags::BODY_LENGTH)
                .map_err(|_| Error::Parse("missing/invalid BodyLength(9)".into()))?;
            // BodyLength counts bytes after the SOH of field 9 up to the
            // start of "10=".
            let body_from = field_end_of(raw, 1)?;
            let actual_len = checksum_start - body_from;
            if declared_len != actual_len {
                return Err(Error::Parse(format!(
                    "BodyLength mismatch: declared {declared_len}, actual {actual_len}"
                )));
            }
            let declared_sum: u32 = msg
                .trailer
                .get(tags::CHECK_SUM)
                .map_err(|_| Error::Parse("missing/invalid CheckSum(10)".into()))?;
            let actual_sum = checksum(&raw[..checksum_start]);
            if declared_sum != actual_sum {
                return Err(Error::Parse(format!(
                    "CheckSum mismatch: declared {declared_sum:03}, actual {actual_sum:03}"
                )));
            }
        }

        Ok(msg)
    }

    // ----- serialization -----

    /// Serialize, computing BodyLength(9) and CheckSum(10).
    ///
    /// Header order matches the reference engines: 8, 9, 35, then remaining
    /// header fields in ascending tag order. Body keeps insertion (wire)
    /// order. Trailer: descending tag order (SignatureLength before
    /// Signature), CheckSum last.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut inner = Vec::with_capacity(self.header.wire_len() + self.body.wire_len() + 64);

        // 35 first among the remaining header fields, then ascending.
        if let Some(v) = self.header.get_raw(tags::MSG_TYPE) {
            write_tag_value(&mut inner, tags::MSG_TYPE, v);
        }
        let mut header: Vec<_> = self
            .header
            .fields()
            .iter()
            .filter(|f| !matches!(f.tag, tags::BEGIN_STRING | tags::BODY_LENGTH | tags::MSG_TYPE))
            .collect();
        header.sort_by_key(|f| f.tag);
        for f in header {
            write_tag_value(&mut inner, f.tag, &f.value);
        }
        self.body.write_to(&mut inner);
        let mut trailer: Vec<_> =
            self.trailer.fields().iter().filter(|f| f.tag != tags::CHECK_SUM).collect();
        trailer.sort_by_key(|f| std::cmp::Reverse(f.tag));
        for f in trailer {
            write_tag_value(&mut inner, f.tag, &f.value);
        }

        let begin_string = self.header.get_raw(tags::BEGIN_STRING).unwrap_or(b"FIX.4.4");
        let mut out = Vec::with_capacity(inner.len() + 32);
        write_tag_value(&mut out, tags::BEGIN_STRING, begin_string);
        write_tag_value(&mut out, tags::BODY_LENGTH, inner.len().to_string().as_bytes());
        out.extend_from_slice(&inner);
        let sum = checksum(&out);
        write_tag_value(&mut out, tags::CHECK_SUM, format!("{sum:03}").as_bytes());
        out
    }

    /// Set a header field (helper).
    pub fn set_header(&mut self, tag: Tag, value: impl FixEncode) {
        self.header.set(tag, value);
    }

    /// Set a body field (helper).
    pub fn set(&mut self, tag: Tag, value: impl FixEncode) {
        self.body.set(tag, value);
    }

    /// Stamp SendingTime(52) with now at the given precision.
    pub fn stamp_sending_time(&mut self, ts: UtcTimestamp) {
        self.header.set(tags::SENDING_TIME, ts);
    }
}

/// Byte offset just past the SOH terminating the nth field (0-indexed).
fn field_end_of(raw: &[u8], n: usize) -> Result<usize> {
    let mut seen = 0usize;
    for (i, &b) in raw.iter().enumerate() {
        if b == SOH {
            if seen == n {
                return Ok(i + 1);
            }
            seen += 1;
        }
    }
    Err(Error::Parse(format!("message has fewer than {} fields", n + 1)))
}

/// FIX checksum: byte sum mod 256.
pub fn checksum(bytes: &[u8]) -> u32 {
    bytes.iter().map(|&b| b as u32).sum::<u32>() % 256
}

#[cfg(test)]
pub(crate) fn build_raw(fields: &[(Tag, &str)]) -> Vec<u8> {
    // Test helper: assemble tag=value|... computing 9 and 10.
    let mut m = Message::new();
    for &(tag, val) in fields {
        if tags::is_header_tag(tag) {
            m.header.push(tag, val);
        } else if tags::is_trailer_tag(tag) {
            m.trailer.push(tag, val);
        } else {
            m.body.push(tag, val);
        }
    }
    m.to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<u8> {
        build_raw(&[
            (8, "FIX.4.2"),
            (35, "D"),
            (34, "2"),
            (49, "TW"),
            (52, "20240515-19:49:56.659"),
            (56, "ISLD"),
            (11, "100"),
            (21, "1"),
            (40, "1"),
            (54, "1"),
            (55, "TSLA"),
        ])
    }

    #[test]
    fn parse_roundtrip() {
        let raw = sample();
        let msg = Message::parse(&raw, true).unwrap();
        assert_eq!(msg.msg_type().unwrap(), "D");
        assert_eq!(msg.seq_num().unwrap(), 2);
        assert_eq!(msg.header.get_string(49).unwrap(), "TW");
        assert_eq!(msg.body.get_string(55).unwrap(), "TSLA");
        assert!(msg.structure_error().is_none());
        assert!(!msg.is_admin());
        assert_eq!(msg.to_bytes(), raw);
    }

    #[test]
    fn bad_checksum_rejected() {
        let mut raw = sample();
        let n = raw.len();
        raw[n - 3] = b'9'; // corrupt checksum digits
        assert!(Message::parse(&raw, true).is_err());
        // ...but tolerated when validation is off
        assert!(Message::parse(&raw, false).is_ok());
    }

    #[test]
    fn bad_body_length_rejected() {
        let raw = sample();
        let s = String::from_utf8(raw).unwrap();
        let tampered = s.replacen("9=", "9=9", 1); // 9=x -> 9=9x
        assert!(Message::parse(tampered.as_bytes(), true).is_err());
    }

    #[test]
    fn leading_field_order_enforced() {
        // 35 before 9 is garbled
        let raw = b"8=FIX.4.2\x0135=D\x019=5\x0110=000\x01";
        assert!(Message::parse(raw, false).is_err());
    }

    #[test]
    fn data_field_with_soh_survives() {
        let mut m = Message::new();
        m.header.push(8, "FIX.4.2");
        m.header.push(35, "B");
        m.body.push(95, 5usize);
        m.body.set_raw(96, b"a\x01b\x01c".to_vec());
        m.body.push(58, "after");
        let raw = m.to_bytes();

        let parsed = Message::parse(&raw, true).unwrap();
        assert_eq!(parsed.body.get_raw(96).unwrap(), b"a\x01b\x01c");
        assert_eq!(parsed.body.get_string(58).unwrap(), "after");
    }

    #[test]
    fn structure_error_recorded() {
        // Header tag 49 appearing after body fields
        let raw = build_raw(&[(8, "FIX.4.2"), (35, "D"), (55, "TSLA")]);
        let s = String::from_utf8(raw).unwrap();
        // splice 49=LATE after 55=TSLA, before checksum; rebuild via parse w/o validation
        let spliced = s.replace("55=TSLA\x01", "55=TSLA\x0149=LATE\x01");
        let msg = Message::parse(spliced.as_bytes(), false).unwrap();
        assert_eq!(msg.structure_error(), Some(49));
    }
}
