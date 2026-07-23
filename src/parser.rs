//! Stream framing: carve complete FIX messages out of a raw byte stream.
//!
//! Mirrors the reference engines' resync behavior: discard garbage before
//! `8=`, use BodyLength(9) to jump the body, then locate `<SOH>10=...<SOH>`.
//! A frame that violates this structure is skipped without dropping the
//! connection.

use bytes::{Buf, Bytes, BytesMut};

use crate::message::SOH;

#[derive(Debug)]
pub enum Frame {
    /// A complete message: bytes from `8=` through the SOH after CheckSum.
    Message(Bytes),
    /// More bytes are needed.
    Incomplete,
}

/// Extract the next complete FIX message from `buf`, consuming it (and any
/// preceding garbage). Returns `Incomplete` when the buffer holds no full
/// message yet. Garbled frames (bad BodyLength structure) are discarded and
/// scanning resumes at the next `8=`.
pub fn extract_frame(buf: &mut BytesMut) -> Frame {
    loop {
        // Resync: drop everything before "8=".
        match find(buf, b"8=") {
            Some(start) => {
                if start > 0 {
                    buf.advance(start);
                }
            }
            None => {
                // Keep a trailing '8' in case '=' arrives next.
                let keep = if buf.last() == Some(&b'8') { 1 } else { 0 };
                let drop = buf.len() - keep;
                buf.advance(drop);
                return Frame::Incomplete;
            }
        }

        // Need "...<SOH>9=<len><SOH>" next.
        let Some(soh1) = find(buf, &[SOH]) else { return Frame::Incomplete };
        let after_begin = soh1 + 1;
        if buf.len() < after_begin + 2 {
            return Frame::Incomplete;
        }
        if &buf[after_begin..after_begin + 2] != b"9=" {
            // Garbled: skip this "8=" and resync.
            buf.advance(2);
            continue;
        }
        let len_start = after_begin + 2;
        let Some(rel_soh2) = buf[len_start..].iter().position(|&b| b == SOH) else {
            return Frame::Incomplete;
        };
        let body_len: usize = match std::str::from_utf8(&buf[len_start..len_start + rel_soh2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(n) => n,
            None => {
                buf.advance(2);
                continue;
            }
        };
        let body_start = len_start + rel_soh2 + 1;

        // The body ends where "10=" begins; expect "10=xyz<SOH>".
        let checksum_start = body_start + body_len;
        // 3 for "10=", at least 1 digit, 1 SOH -> look for the SOH.
        if buf.len() < checksum_start + 4 {
            return Frame::Incomplete;
        }
        if &buf[checksum_start..checksum_start + 3] != b"10=" {
            // BodyLength lied; treat frame as garbled and resync.
            buf.advance(2);
            continue;
        }
        let Some(rel_end) = buf[checksum_start + 3..].iter().position(|&b| b == SOH) else {
            // Bounded field: if no SOH within a few bytes it's garbled.
            if buf.len() > checksum_start + 3 + 8 {
                buf.advance(2);
                continue;
            }
            return Frame::Incomplete;
        };
        let end = checksum_start + 3 + rel_end + 1;
        return Frame::Message(buf.split_to(end).freeze());
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::build_raw;

    fn msg() -> Vec<u8> {
        build_raw(&[(8, "FIX.4.2"), (35, "0"), (34, "2"), (49, "A"), (56, "B")])
    }

    #[test]
    fn extracts_single_message() {
        let raw = msg();
        let mut buf = BytesMut::from(&raw[..]);
        match extract_frame(&mut buf) {
            Frame::Message(m) => assert_eq!(&m[..], &raw[..]),
            _ => panic!("expected message"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn incomplete_returns_incomplete() {
        let raw = msg();
        let mut buf = BytesMut::from(&raw[..raw.len() - 5]);
        assert!(matches!(extract_frame(&mut buf), Frame::Incomplete));
        buf.extend_from_slice(&raw[raw.len() - 5..]);
        assert!(matches!(extract_frame(&mut buf), Frame::Message(_)));
    }

    #[test]
    fn skips_leading_garbage() {
        let raw = msg();
        let mut buf = BytesMut::from(&b"garbage\x01noise"[..]);
        buf.extend_from_slice(&raw);
        match extract_frame(&mut buf) {
            Frame::Message(m) => assert_eq!(&m[..], &raw[..]),
            _ => panic!("expected message"),
        }
    }

    #[test]
    fn skips_garbled_frame_and_recovers() {
        // A frame whose BodyLength points somewhere that is not "10=".
        // (A BodyLength pointing past the buffered bytes is indistinguishable
        // from an incomplete message and correctly returns Incomplete.)
        let garbled = b"8=FIX.4.2\x019=3\x0135=D\x0158=hi\x0110=000\x01";
        let raw = msg();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(garbled);
        buf.extend_from_slice(&raw);
        match extract_frame(&mut buf) {
            Frame::Message(m) => assert_eq!(&m[..], &raw[..]),
            _ => panic!("expected recovery after garbled frame"),
        }
    }

    #[test]
    fn two_messages_back_to_back() {
        let raw = msg();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&raw);
        buf.extend_from_slice(&raw);
        assert!(matches!(extract_frame(&mut buf), Frame::Message(_)));
        assert!(matches!(extract_frame(&mut buf), Frame::Message(_)));
        assert!(matches!(extract_frame(&mut buf), Frame::Incomplete));
    }
}
