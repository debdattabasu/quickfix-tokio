//! The classic QuickFIX acceptance test suite, ported from QuickFIX/n's
//! runner (`AcceptanceTest/Runner.cs` + `ReflectorClient.cs`).
//!
//! Each `.def` script in `acceptance/definitions/<version>/` drives a raw
//! TCP client against a running engine:
//!   `iCONNECT` / `iDISCONNECT`   connect / close (optional id: `i2,CONNECT`)
//!   `I<msg>`                     send a FIX message (SOH bytes literal)
//!   `E<msg>`                     expect the next engine message to match
//!   `eDISCONNECT`                expect the engine to close the socket
//!   `sleep(n)`                   pause n seconds
//!
//! `<TIME±n>` placeholders and missing BodyLength/CheckSum are filled in;
//! expected values for tags 10/42/52/60/122 match by shape, all else
//! byte-for-byte and position-for-position.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use quickfix_tokio::parser::{Frame, extract_frame};
use quickfix_tokio::{
    Application, ApplicationError, Engine, MemoryStoreFactory, Message, NullLogFactory, SessionId,
    Settings,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

const SOH: u8 = 0x01;

// ----- the AT application (mirrors QuickFIX/n ATApplication) -----

struct ATApp {
    begin_string: String,
    echo_tx: mpsc::UnboundedSender<(SessionId, Message)>,
    cl_ord_ids: Mutex<HashSet<String>>,
}

#[async_trait::async_trait]
impl Application for ATApp {
    async fn on_logout(&self, _session_id: &SessionId) {
        self.cl_ord_ids.lock().unwrap().clear();
    }

    async fn from_admin(
        &self,
        msg: &Message,
        session_id: &SessionId,
    ) -> Result<(), ApplicationError> {
        // XMLnonFIX (35=n) gets a News acknowledgment, like QF/n's AT app.
        if msg.msg_type().unwrap_or_default() == "n" {
            let seq = msg.seq_num().unwrap_or_default();
            let mut reply = Message::with_type("B");
            reply.set(148, format!("Successfully received 'n' message with seqNo={seq}").as_str());
            let _ = self.echo_tx.send((session_id.clone(), reply));
        }
        Ok(())
    }

    async fn from_app(
        &self,
        msg: &Message,
        session_id: &SessionId,
    ) -> Result<(), ApplicationError> {
        let bs = self.begin_string.as_str();
        let mt = msg.msg_type().unwrap_or_default();
        match mt.as_str() {
            // NewOrderSingle: echo, deduping PossResend by ClOrdID.
            "D" => {
                let poss_resend = msg.header.get_raw(97) == Some(b"Y");
                let cl_ord_id = msg.body.get_string(11).unwrap_or_default();
                if poss_resend && !self.cl_ord_ids.lock().unwrap().insert(cl_ord_id.clone()) {
                    return Ok(());
                }
                self.cl_ord_ids.lock().unwrap().insert(cl_ord_id);
                let _ = self.echo_tx.send((session_id.clone(), msg.clone()));
                Ok(())
            }
            // SecurityDefinition: echo (FIX 4.2+).
            "d" if bs >= "FIX.4.2" => {
                let _ = self.echo_tx.send((session_id.clone(), msg.clone()));
                Ok(())
            }
            // QuoteRequest: echo (FIX 4.4 handler only, like QF/n).
            "R" if bs == "FIX.4.4" => {
                let _ = self.echo_tx.send((session_id.clone(), msg.clone()));
                Ok(())
            }
            // TradeCaptureReportRequest: swallow (FIX 4.4).
            "AD" if bs == "FIX.4.4" => Ok(()),
            // News: reply to "echo: xyz" headlines (FIX 4.1+).
            "B" if bs >= "FIX.4.1" => {
                if let Ok(headline) = msg.body.get_string(148) {
                    if let Some(text) = headline.strip_prefix("echo:") {
                        let mut reply = Message::with_type("B");
                        reply.set(148, text.trim_start());
                        if let Some(enc) = msg.header.get_raw(347) {
                            reply.header.set_raw(347, enc.to_vec());
                        }
                        if let Some(v) = msg.body.get_raw(359) {
                            reply.body.set_raw(359, v.to_vec());
                        }
                        let _ = self.echo_tx.send((session_id.clone(), reply));
                    }
                }
                Ok(())
            }
            _ => Err(ApplicationError::UnsupportedMessageType),
        }
    }
}

// ----- decoration (ReflectorClient.Decorate) -----

fn decorate(mut msg: Vec<u8>) -> Vec<u8> {
    let now = chrono::Utc::now();
    // <TIME> / <TIME+n> / <TIME-n> (offsets in days)
    loop {
        let s = String::from_utf8_lossy(&msg).into_owned();
        let Some(start) = s.find("<TIME") else { break };
        let Some(rel_end) = s[start..].find('>') else { break };
        let end = start + rel_end;
        let inner = &s[start + 5..end];
        let time = if inner.is_empty() {
            now
        } else {
            now + chrono::Duration::days(inner.parse::<i64>().unwrap_or(0))
        };
        let formatted = time.format("%Y%m%d-%H:%M:%S").to_string();
        msg.splice(start..end + 1, formatted.into_bytes());
    }

    // Insert 9= / 10= when omitted.
    let Some(begin_len) = find(&msg, b"8=").filter(|&i| i == 0).and_then(|_| {
        msg.iter().position(|&b| b == SOH).map(|i| i + 1)
    }) else {
        return msg;
    };
    let has_body_length = msg[begin_len..].starts_with(b"9=");
    let has_checksum = checksum_field_start(&msg).is_some();

    match (has_body_length, has_checksum) {
        (true, true) => msg,
        (true, false) => {
            let sum = checksum(&msg);
            msg.extend_from_slice(format!("10={sum:03}\x01").as_bytes());
            msg
        }
        (false, true) => {
            let cs_start = checksum_field_start(&msg).unwrap();
            let body_len = cs_start - begin_len;
            let mut out = msg[..begin_len].to_vec();
            out.extend_from_slice(format!("9={body_len}\x01").as_bytes());
            out.extend_from_slice(&msg[begin_len..]);
            out
        }
        (false, false) => {
            let body_len = msg.len() - begin_len;
            let mut out = msg[..begin_len].to_vec();
            out.extend_from_slice(format!("9={body_len}\x01").as_bytes());
            out.extend_from_slice(&msg[begin_len..]);
            let sum = checksum(&out);
            out.extend_from_slice(format!("10={sum:03}\x01").as_bytes());
            out
        }
    }
}

/// Byte offset of the trailing `10=...` field, if present at the end.
fn checksum_field_start(msg: &[u8]) -> Option<usize> {
    if msg.last() != Some(&SOH) {
        return None;
    }
    let body = &msg[..msg.len() - 1];
    let field_start = body.iter().rposition(|&b| b == SOH)? + 1;
    let field = &body[field_start..];
    (field.starts_with(b"10=") && field[3..].iter().all(|b| b.is_ascii_digit()))
        .then_some(field_start)
}

fn checksum(bytes: &[u8]) -> u8 {
    bytes.iter().fold(0u8, |acc, &b| acc.wrapping_add(b))
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

// ----- expect matching (ReflectorClient.Expect) -----

fn value_matches(tag: &[u8], expected: &[u8], actual: &[u8]) -> bool {
    fn is_datetime(v: &[u8]) -> bool {
        v.len() >= 17
            && v[..8].iter().all(|b| b.is_ascii_digit())
            && v[8] == b'-'
            && v[9..17].chunks(3).all(|c| {
                c[0].is_ascii_digit() && c[1].is_ascii_digit() && c.get(2).is_none_or(|&b| b == b':')
            })
    }
    match tag {
        b"10" => actual.len() >= 3 && actual.iter().filter(|b| b.is_ascii_digit()).count() >= 3,
        b"42" | b"60" | b"122" => is_datetime(actual),
        b"52" => {
            is_datetime(actual)
                && (actual.len() == 17
                    || (actual.len() == 21
                        && actual[17] == b'.'
                        && actual[18..].iter().all(|b| b.is_ascii_digit())))
        }
        _ => expected == actual,
    }
}

fn printable(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).replace('\x01', "|")
}

/// Positional, tag-by-tag comparison. Returns a description on mismatch.
fn match_message(expected: &[u8], actual: &[u8]) -> Result<(), String> {
    let strip = |m: &[u8]| -> Vec<Vec<u8>> {
        let m = m.strip_suffix(&[SOH]).unwrap_or(m);
        m.split(|&b| b == SOH).map(|f| f.to_vec()).collect()
    };
    let exp_fields = strip(expected);
    let act_fields = strip(actual);

    let fail = |why: String| {
        Err(format!(
            "{why}\n  expected: {}\n  actual:   {}",
            printable(expected),
            printable(actual)
        ))
    };

    for i in 0..exp_fields.len().min(act_fields.len()) {
        let (etag, evalue) = split_field(&exp_fields[i]);
        let (atag, avalue) = split_field(&act_fields[i]);
        if etag != atag {
            return fail(format!(
                "field {} tag mismatch: expected {}, got {}",
                i,
                printable(etag),
                printable(atag)
            ));
        }
        if !value_matches(etag, evalue, avalue) {
            return fail(format!(
                "tag {} value mismatch: expected {:?}, got {:?}",
                printable(etag),
                printable(evalue),
                printable(avalue)
            ));
        }
    }
    if exp_fields.len() != act_fields.len() {
        return fail(format!(
            "field count mismatch: expected {}, got {}",
            exp_fields.len(),
            act_fields.len()
        ));
    }
    Ok(())
}

fn split_field(field: &[u8]) -> (&[u8], &[u8]) {
    match field.iter().position(|&b| b == b'=') {
        Some(i) => (&field[..i], &field[i + 1..]),
        None => (field, &[][..]),
    }
}

// ----- the script runner (Runner.cs) -----

struct Connection {
    stream: TcpStream,
    buf: BytesMut,
}

impl Connection {
    async fn read_message(&mut self) -> Result<Vec<u8>, String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Frame::Message(raw) = extract_frame(&mut self.buf) {
                return Ok(raw.to_vec());
            }
            let n = tokio::time::timeout_at(deadline, self.stream.read_buf(&mut self.buf))
                .await
                .map_err(|_| "timed out waiting for a message".to_string())?
                .map_err(|e| format!("read error: {e}"))?;
            if n == 0 {
                return Err("engine closed the connection while a message was expected".into());
            }
        }
    }

    async fn expect_disconnect(&mut self) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Frame::Message(m) = extract_frame(&mut self.buf) {
                return Err(format!("expected disconnect, got message: {}", printable(&m)));
            }
            let n = tokio::time::timeout_at(deadline, self.stream.read_buf(&mut self.buf))
                .await
                .map_err(|_| "timed out waiting for disconnect".to_string())?
                .map_err(|_| String::new()) // reset-by-peer counts as closed
                .unwrap_or(0);
            if n == 0 {
                return Ok(());
            }
        }
    }
}

/// Parse a directive prefix like `I`, `I2,`, `i1,CONNECT` — returns
/// (connection id, rest-of-line offset).
fn connection_id(line: &[u8], keyword_len: usize) -> (u32, usize) {
    // Digits after the first char, optionally followed by ','.
    let mut i = 1;
    while i < line.len() && line[i].is_ascii_digit() {
        i += 1;
    }
    if i > 1 && i < line.len() && line[i] == b',' {
        let id: u32 = std::str::from_utf8(&line[1..i]).unwrap().parse().unwrap();
        (id, i + 1)
    } else {
        (1, 1.max(keyword_len))
    }
}

async fn run_def(path: &Path, port: u16) -> Result<(), String> {
    let script = std::fs::read(path).map_err(|e| e.to_string())?;
    let mut connections: HashMap<u32, Connection> = HashMap::new();

    for (line_no, line) in script.split(|&b| b == b'\n').enumerate() {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let with_line = |e: String| format!("line {}: {e}", line_no + 1);
        if line.is_empty() || line[0] == b'#' {
            continue;
        }
        if line.starts_with(b"sleep(") && line.ends_with(b")") {
            let secs: f64 = std::str::from_utf8(&line[6..line.len() - 1])
                .map_err(|e| with_line(e.to_string()))?
                .parse()
                .map_err(|_| with_line("bad sleep".into()))?;
            tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            continue;
        }
        match line[0] {
            b'i' if line.ends_with(b"CONNECT") && !line.ends_with(b"DISCONNECT") => {
                let (id, _) = connection_id(line, 1);
                let stream = TcpStream::connect(("127.0.0.1", port))
                    .await
                    .map_err(|e| with_line(format!("connect failed: {e}")))?;
                stream.set_nodelay(true).ok();
                connections.insert(id, Connection { stream, buf: BytesMut::new() });
            }
            b'i' if line.ends_with(b"DISCONNECT") => {
                let (id, _) = connection_id(line, 1);
                connections.remove(&id);
            }
            b'e' if line.ends_with(b"DISCONNECT") => {
                let (id, _) = connection_id(line, 1);
                let conn = connections
                    .get_mut(&id)
                    .ok_or_else(|| with_line(format!("no connection {id}")))?;
                conn.expect_disconnect().await.map_err(with_line)?;
                connections.remove(&id);
            }
            b'I' => {
                let (id, offset) = connection_id(line, 1);
                let msg = decorate(line[offset..].to_vec());
                let conn = connections
                    .get_mut(&id)
                    .ok_or_else(|| with_line(format!("no connection {id}")))?;
                conn.stream
                    .write_all(&msg)
                    .await
                    .map_err(|e| with_line(format!("send failed: {e}")))?;
            }
            b'E' => {
                let (id, offset) = connection_id(line, 1);
                let expected = decorate(line[offset..].to_vec());
                let conn = connections
                    .get_mut(&id)
                    .ok_or_else(|| with_line(format!("no connection {id}")))?;
                let actual = conn.read_message().await.map_err(&with_line)?;
                match_message(&expected, &actual).map_err(with_line)?;
            }
            _ => {
                return Err(with_line(format!(
                    "unrecognized directive: {}",
                    String::from_utf8_lossy(line)
                )));
            }
        }
    }
    Ok(())
}

// ----- per-version fixtures -----

async fn run_version(dir_name: &str, begin_string: &str, spec_file: &str) {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    // The defs were authored against QuickFIX/n's spec XMLs, which differ
    // slightly from quickfix-go's (e.g. FIX44 News group requirements).
    let spec = format!("{}/acceptance/spec/{spec_file}", env!("CARGO_MANIFEST_DIR"));
    let settings = Settings::parse(&format!(
        "[SESSION]\n\
         ConnectionType=acceptor\n\
         BeginString={begin_string}\n\
         SenderCompID=ISLD\n\
         TargetCompID=TW\n\
         SocketAcceptPort={port}\n\
         HeartBtInt=30\n\
         ResetOnLogon=Y\n\
         LogonTimeout=2\n\
         LogoutTimeout=1\n\
         DataDictionary={spec}\n"
    ))
    .unwrap();

    let (echo_tx, mut echo_rx) = mpsc::unbounded_channel::<(SessionId, Message)>();
    let app = Arc::new(ATApp {
        begin_string: begin_string.to_owned(),
        echo_tx,
        cl_ord_ids: Mutex::new(HashSet::new()),
    });
    let engine = Engine::start(
        &settings,
        app,
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();
    let session = engine.session(begin_string, "ISLD", "TW").unwrap();
    tokio::spawn(async move {
        while let Some((_, msg)) = echo_rx.recv().await {
            let _ = session.send(msg).await;
        }
    });

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("acceptance/definitions")
        .join(dir_name);
    let mut defs: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {dir:?}: {e}"))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "def"))
        .collect();
    defs.sort();
    assert!(!defs.is_empty(), "no .def files in {dir:?}");

    let mut failures = Vec::new();
    for def in &defs {
        let name = def.file_name().unwrap().to_string_lossy().into_owned();
        let result = tokio::time::timeout(Duration::from_secs(60), run_def(def, port))
            .await
            .unwrap_or_else(|_| Err("test timed out".into()));
        if let Err(e) = result {
            failures.push(format!("{name}: {e}"));
        }
        // Let the engine notice the connection teardown between scripts.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    engine.stop().await;

    if !failures.is_empty() {
        panic!(
            "{}/{} acceptance tests failed for {begin_string}:\n\n{}",
            failures.len(),
            defs.len(),
            failures.join("\n\n")
        );
    }
}

#[tokio::test]
async fn acceptance_fix40() {
    run_version("fix40", "FIX.4.0", "FIX40.xml").await;
}

#[tokio::test]
async fn acceptance_fix41() {
    run_version("fix41", "FIX.4.1", "FIX41.xml").await;
}

#[tokio::test]
async fn acceptance_fix42() {
    run_version("fix42", "FIX.4.2", "FIX42.xml").await;
}

#[tokio::test]
async fn acceptance_fix43() {
    run_version("fix43", "FIX.4.3", "FIX43.xml").await;
}

#[tokio::test]
async fn acceptance_fix44() {
    run_version("fix44", "FIX.4.4", "FIX44.xml").await;
}
