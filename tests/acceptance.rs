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
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

const SOH: u8 = 0x01;

// ----- the AT application (mirrors QuickFIX/n ATApplication) -----

struct ATApp {
    /// Application-level FIX version (differs from BeginString for FIXT
    /// sessions), used to mirror QF/n's per-version message handlers.
    app_ver: String,
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
        let bs = self.app_ver.as_str();
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
        // Generous: some defs wait out real heartbeat/test-request timers
        // (e.g. misc/FIX42TestRequest waits ~36s between messages).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
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
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
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

/// Handle `iSET_SESSION <bs>:<sender>-><target> NEXT{SENDER,TARGET}SEQNUM=n`.
async fn set_session(line: &[u8], engine: &Engine) -> Result<(), String> {
    let text = std::str::from_utf8(line).map_err(|e| e.to_string())?;
    let mut parts = text.split_whitespace();
    parts.next(); // "iSET_SESSION"
    let sid = parts.next().ok_or("SET_SESSION missing session id")?;
    let assign = parts.next().ok_or("SET_SESSION missing assignment")?;

    // Parse "BeginString:Sender->Target".
    let (bs, comps) = sid.split_once(':').ok_or("bad session id")?;
    let (sender, target) = comps.split_once("->").ok_or("bad session id")?;
    let handle = engine
        .session(bs, sender, target)
        .ok_or_else(|| format!("no session {sid}"))?;

    let (key, val) = assign.split_once('=').ok_or("bad assignment")?;
    let n: u64 = val.parse().map_err(|_| format!("bad seqnum {val}"))?;
    match key {
        "NEXTSENDERSEQNUM" => handle.set_next_sender_seq_num(n).await,
        "NEXTTARGETSEQNUM" => handle.set_next_target_seq_num(n).await,
        other => return Err(format!("unknown SET_SESSION key {other}")),
    }
    .map_err(|e| e.to_string())
}

async fn run_def(path: &Path, port: u16, engine: &Engine) -> Result<(), String> {
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
        // `iSET_SESSION <bs>:<sender>-><target> NEXT{SENDER,TARGET}SEQNUM=n`
        // presets a session's sequence numbers (go's test directive).
        if line.starts_with(b"iSET_SESSION") {
            set_session(line, engine).await.map_err(with_line)?;
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

// ----- suite fixtures -----

struct Suite {
    /// Directory under acceptance/definitions/.
    dir: &'static str,
    begin_string: &'static str,
    /// Application-level FIX version for the AT app's handler table.
    app_ver: &'static str,
    /// Extra config lines appended to the `[SESSION]` block (dictionaries,
    /// feature toggles, ...). `{spec}` expands to the acceptance/spec
    /// directory and `{port}` to the acceptor port, so a fixture may append
    /// an entire additional `[SESSION]` block.
    extra_settings: &'static str,
}

async fn run_suite(suite: Suite) {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    // The defs were authored against QuickFIX/n's spec XMLs, which differ
    // slightly from quickfix-go's (e.g. FIX44 News group requirements).
    let spec_dir = format!("{}/acceptance/spec", env!("CARGO_MANIFEST_DIR"));
    let begin_string = suite.begin_string;
    let settings = Settings::parse(&format!(
        "[SESSION]\n\
         ConnectionType=acceptor\n\
         BeginString={begin_string}\n\
         SenderCompID=ISLD\n\
         TargetCompID=TW\n\
         SocketAcceptPort={port}\n\
         HeartBtInt=30\n\
         LogonTimeout=2\n\
         LogoutTimeout=1\n\
         {}\n",
        suite.extra_settings.replace("{spec}", &spec_dir).replace("{port}", &port.to_string())
    ))
    .unwrap();

    let (echo_tx, mut echo_rx) = mpsc::unbounded_channel::<(SessionId, Message)>();
    let app = Arc::new(ATApp {
        app_ver: suite.app_ver.to_owned(),
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
        .join(suite.dir);
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
        let result = tokio::time::timeout(Duration::from_secs(150), run_def(def, port, &engine))
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

/// The standard per-version suite: ResetOnLogon so each def starts fresh.
fn classic(dir: &'static str, begin_string: &'static str, spec: &'static str) -> Suite {
    let extra = match spec {
        "FIX40.xml" => "ResetOnLogon=Y\nDataDictionary={spec}/FIX40.xml",
        "FIX41.xml" => "ResetOnLogon=Y\nDataDictionary={spec}/FIX41.xml",
        "FIX42.xml" => "ResetOnLogon=Y\nDataDictionary={spec}/FIX42.xml",
        "FIX43.xml" => "ResetOnLogon=Y\nDataDictionary={spec}/FIX43.xml",
        "FIX44.xml" => "ResetOnLogon=Y\nDataDictionary={spec}/FIX44.xml",
        _ => unreachable!(),
    };
    Suite { dir, begin_string, app_ver: begin_string, extra_settings: extra }
}

#[tokio::test]
async fn acceptance_fix40() {
    run_suite(classic("fix40", "FIX.4.0", "FIX40.xml")).await;
}

#[tokio::test]
async fn acceptance_fix41() {
    run_suite(classic("fix41", "FIX.4.1", "FIX41.xml")).await;
}

#[tokio::test]
async fn acceptance_fix42() {
    run_suite(classic("fix42", "FIX.4.2", "FIX42.xml")).await;
}

#[tokio::test]
async fn acceptance_fix43() {
    run_suite(classic("fix43", "FIX.4.3", "FIX43.xml")).await;
}

#[tokio::test]
async fn acceptance_fix44() {
    run_suite(classic("fix44", "FIX.4.4", "FIX44.xml")).await;
}

#[tokio::test]
async fn acceptance_fix44_noreset() {
    run_suite(Suite {
        dir: "fix44noreset",
        begin_string: "FIX.4.4",
        app_ver: "FIX.4.4",
        extra_settings: "ResetOnLogon=N\nDataDictionary={spec}/FIX44.xml",
    })
    .await;
}

#[tokio::test]
async fn acceptance_misc() {
    run_suite(Suite {
        dir: "misc",
        begin_string: "FIX.4.2",
        app_ver: "FIX.4.2",
        extra_settings: "ResetOnLogon=Y\n\
             SenderSubID=SENDERSUB\nSenderLocationID=SENDERLOC\n\
             TargetSubID=TARGETSUB\nTargetLocationID=TARGETLOC\n\
             EnableLastMsgSeqNumProcessed=Y\n\
             MaxMessagesInResendRequest=2500\n\
             SendLogoutBeforeDisconnectFromTimeout=Y\n\
             DataDictionary={spec}/FIX42.xml",
    })
    .await;
}

#[tokio::test]
async fn acceptance_enhanced_resend() {
    run_suite(Suite {
        dir: "enhancedresend",
        begin_string: "FIX.4.4",
        app_ver: "FIX.4.4",
        extra_settings: "ResetOnLogon=Y\n\
             RequiresOrigSendingTime=N\n\
             EnableLastMsgSeqNumProcessed=Y\n\
             MaxMessagesInResendRequest=10\n\
             DataDictionary={spec}/FIX44.xml",
    })
    .await;
}

/// The validate suite (ported from quickfix C++): the first connection uses
/// the normal session (validation on); the second connects as
/// NO_CHECK_FIELDS_HAVE_VALUES, a session configured with
/// ValidateFieldsHaveValues=N, so an empty field value is accepted rather
/// than rejected. Two `[SESSION]` blocks share the acceptor port.
#[tokio::test]
async fn acceptance_validate() {
    run_suite(Suite {
        dir: "validate",
        begin_string: "FIX.4.4",
        app_ver: "FIX.4.4",
        extra_settings: "ResetOnLogon=Y\nDataDictionary={spec}/FIX44.xml\n\
             \n\
             [SESSION]\n\
             ConnectionType=acceptor\n\
             BeginString=FIX.4.4\n\
             SenderCompID=ISLD\n\
             TargetCompID=NO_CHECK_FIELDS_HAVE_VALUES\n\
             SocketAcceptPort={port}\n\
             HeartBtInt=30\n\
             ResetOnLogon=Y\n\
             ValidateFieldsHaveValues=N\n\
             DataDictionary={spec}/FIX44.xml",
    })
    .await;
}

fn fixt(dir: &'static str, app_ver: &'static str, extra: &'static str) -> Suite {
    Suite { dir, begin_string: "FIXT.1.1", app_ver, extra_settings: extra }
}

// ----- client (initiator-driven) suite -----
//
// Ported from quickfix C++'s `definitions/client`. Here the topology is
// reversed: the harness listens and the engine-under-test is an *initiator*
// that dials in. Directives invert accordingly:
//   `eCONNECT`     accept the engine's outbound connection
//   `E<msg>`       expect the next message *from* the engine
//   `R<msg>`       respond by sending a message *to* the engine
//   `eDISCONNECT`  expect the engine to close the socket

async fn run_client_def(path: &Path, listener: &TcpListener) -> Result<(), String> {
    let script = std::fs::read(path).map_err(|e| e.to_string())?;
    let mut conn: Option<Connection> = None;

    for (line_no, line) in script.split(|&b| b == b'\n').enumerate() {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let with_line = |e: String| format!("line {}: {e}", line_no + 1);
        if line.is_empty() || line[0] == b'#' {
            continue;
        }
        match line[0] {
            b'e' if line.ends_with(b"CONNECT") && !line.ends_with(b"DISCONNECT") => {
                let (stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
                    .await
                    .map_err(|_| with_line("timed out waiting for engine to connect".into()))?
                    .map_err(|e| with_line(format!("accept failed: {e}")))?;
                stream.set_nodelay(true).ok();
                conn = Some(Connection { stream, buf: BytesMut::new() });
            }
            b'e' if line.ends_with(b"DISCONNECT") => {
                let c = conn.as_mut().ok_or_else(|| with_line("not connected".into()))?;
                c.expect_disconnect().await.map_err(with_line)?;
                conn = None;
            }
            b'E' => {
                let expected = decorate(line[1..].to_vec());
                let c = conn.as_mut().ok_or_else(|| with_line("not connected".into()))?;
                let actual = c.read_message().await.map_err(&with_line)?;
                match_message(&expected, &actual).map_err(with_line)?;
            }
            b'R' => {
                let msg = decorate(line[1..].to_vec());
                let c = conn.as_mut().ok_or_else(|| with_line("not connected".into()))?;
                c.stream.write_all(&msg).await.map_err(|e| with_line(format!("send failed: {e}")))?;
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

#[tokio::test]
async fn acceptance_client() {
    struct NoApp;
    #[async_trait::async_trait]
    impl Application for NoApp {}

    // Harness listens; the engine dials in.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    // TimestampPrecision=0 (seconds) matches the C++ def's BodyLengths, and a
    // long ReconnectInterval keeps the engine from re-dialing mid-test.
    let engine = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=initiator\nBeginString=FIX.4.2\n\
             SenderCompID=TW\nTargetCompID=ISLD\nSocketConnectHost=127.0.0.1\n\
             SocketConnectPort={port}\nHeartBtInt=30\nReconnectInterval=999\n\
             TimestampPrecision=0\nUseDataDictionary=N\n"
        ))
        .unwrap(),
        Arc::new(NoApp),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("acceptance/definitions/client");
    let mut defs: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {dir:?}: {e}"))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "def"))
        .collect();
    defs.sort();
    assert!(!defs.is_empty(), "no .def files in {dir:?}");

    for def in &defs {
        let name = def.file_name().unwrap().to_string_lossy().into_owned();
        if let Err(e) = run_client_def(def, &listener).await {
            engine.stop().await;
            panic!("client/{name}: {e}");
        }
    }
    engine.stop().await;
}

// ----- NextExpectedMsgSeqNum(789) suite (ported from quickfix-go) -----
//
// Exercises the tag-789 logon handshake: in-sync (2a), peer expects unsent
// messages -> disconnect (2b), peer behind -> implied gap-fill after logon
// (2c), and the 141/789 interaction (reset def). Uses SET_SESSION to preset
// sequence numbers. ResetOnLogon=N so those presets survive to logon.

#[tokio::test]
async fn acceptance_next_expected_fix44() {
    run_suite(Suite {
        dir: "nextexpectedseqnum/fix44",
        begin_string: "FIX.4.4",
        app_ver: "FIX.4.4",
        extra_settings: "ResetOnLogon=N\nSendNextExpectedMsgSeqNum=Y\n\
             DataDictionary={spec}/FIX44.xml",
    })
    .await;
}

fn next_expected_fixt(dir: &'static str, app_ver: &'static str, app_spec: &'static str) -> Suite {
    Suite {
        dir,
        begin_string: "FIXT.1.1",
        app_ver,
        extra_settings: match app_spec {
            "FIX50" => "ResetOnLogon=N\nSendNextExpectedMsgSeqNum=Y\nDefaultApplVerID=FIX.5.0\n\
                 TransportDataDictionary={spec}/FIXT11.xml\nAppDataDictionary={spec}/FIX50.xml",
            "FIX50SP1" => "ResetOnLogon=N\nSendNextExpectedMsgSeqNum=Y\nDefaultApplVerID=FIX.5.0SP1\n\
                 TransportDataDictionary={spec}/FIXT11.xml\nAppDataDictionary={spec}/FIX50SP1.xml",
            _ => "ResetOnLogon=N\nSendNextExpectedMsgSeqNum=Y\nDefaultApplVerID=FIX.5.0SP2\n\
                 TransportDataDictionary={spec}/FIXT11.xml\nAppDataDictionary={spec}/FIX50SP2.xml",
        },
    }
}

#[tokio::test]
async fn acceptance_next_expected_fix50() {
    run_suite(next_expected_fixt("nextexpectedseqnum/fix50", "FIX.5.0", "FIX50")).await;
}

#[tokio::test]
async fn acceptance_next_expected_fix50sp1() {
    run_suite(next_expected_fixt("nextexpectedseqnum/fix50sp1", "FIX.5.0SP1", "FIX50SP1")).await;
}

#[tokio::test]
async fn acceptance_next_expected_fix50sp2() {
    run_suite(next_expected_fixt("nextexpectedseqnum/fix50sp2", "FIX.5.0SP2", "FIX50SP2")).await;
}

#[tokio::test]
async fn acceptance_fix50() {
    run_suite(fixt(
        "fix50",
        "FIX.5.0",
        "ResetOnLogon=Y\nDefaultApplVerID=FIX.5.0\n\
         TransportDataDictionary={spec}/FIXT11.xml\nAppDataDictionary={spec}/FIX50.xml",
    ))
    .await;
}

#[tokio::test]
async fn acceptance_fix50sp1() {
    run_suite(fixt(
        "fix50sp1",
        "FIX.5.0SP1",
        "ResetOnLogon=Y\nDefaultApplVerID=FIX.5.0SP1\n\
         TransportDataDictionary={spec}/FIXT11.xml\nAppDataDictionary={spec}/FIX50SP1.xml",
    ))
    .await;
}

#[tokio::test]
async fn acceptance_fix50sp2() {
    run_suite(fixt(
        "fix50sp2",
        "FIX.5.0SP2",
        "ResetOnLogon=Y\nDefaultApplVerID=FIX.5.0SP2\n\
         TransportDataDictionary={spec}/FIXT11.xml\nAppDataDictionary={spec}/FIX50SP2.xml",
    ))
    .await;
}
