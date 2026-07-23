//! End-to-end tests: real engines talking FIX over loopback TCP.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::BytesMut;
use quickfix_tokio::parser::{Frame, extract_frame};
use quickfix_tokio::{
    Application, Engine, MemoryStoreFactory, Message, Settings, TracingLogFactory, UtcTimestamp,
    tags,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Records everything interesting that happens to it.
#[derive(Default)]
struct RecordingApp {
    logons: Mutex<Vec<String>>,
    logouts: Mutex<Vec<String>>,
    app_messages: Mutex<Vec<Message>>,
}

#[async_trait::async_trait]
impl Application for RecordingApp {
    async fn on_logon(&self, session_id: &quickfix_tokio::SessionId) {
        self.logons.lock().unwrap().push(session_id.to_string());
    }
    async fn on_logout(&self, session_id: &quickfix_tokio::SessionId) {
        self.logouts.lock().unwrap().push(session_id.to_string());
    }
    async fn from_app(
        &self,
        msg: &Message,
        _session_id: &quickfix_tokio::SessionId,
    ) -> Result<(), quickfix_tokio::ApplicationError> {
        self.app_messages.lock().unwrap().push(msg.clone());
        Ok(())
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn wait_for(what: &str, mut cond: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !cond().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn acceptor_settings(port: u16, hbi: u32) -> Settings {
    Settings::parse(&format!(
        "[SESSION]\nConnectionType=acceptor\nBeginString=FIX.4.4\nSenderCompID=SERVER\n\
         TargetCompID=CLIENT\nSocketAcceptPort={port}\nHeartBtInt={hbi}\n"
    ))
    .unwrap()
}

fn initiator_settings(port: u16, hbi: u32) -> Settings {
    Settings::parse(&format!(
        "[SESSION]\nConnectionType=initiator\nBeginString=FIX.4.4\nSenderCompID=CLIENT\n\
         TargetCompID=SERVER\nSocketConnectHost=127.0.0.1\nSocketConnectPort={port}\n\
         HeartBtInt={hbi}\nReconnectInterval=1\n"
    ))
    .unwrap()
}

#[tokio::test]
async fn logon_exchange_and_logout() {
    let port = free_port();
    let server_app = Arc::new(RecordingApp::default());
    let client_app = Arc::new(RecordingApp::default());

    let server = Engine::start(
        &acceptor_settings(port, 30),
        server_app.clone(),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await
    .unwrap();
    let client = Engine::start(
        &initiator_settings(port, 30),
        client_app.clone(),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await
    .unwrap();

    let client_session = client.session("FIX.4.4", "CLIENT", "SERVER").unwrap();
    let server_session = server.session("FIX.4.4", "SERVER", "CLIENT").unwrap();

    wait_for("both sides logged on", async || {
        client_session.is_logged_on().await && server_session.is_logged_on().await
    })
    .await;
    assert_eq!(server_app.logons.lock().unwrap().len(), 1);
    assert_eq!(client_app.logons.lock().unwrap().len(), 1);

    // Client sends a NewOrderSingle; server's from_app should see it.
    let mut order = Message::with_type("D");
    order.set(11, "ORDER-1"); // ClOrdID
    order.set(55, "TSLA"); // Symbol
    order.set(54, '1'); // Side
    order.set(60, UtcTimestamp::now()); // TransactTime
    order.set(40, '1'); // OrdType
    client_session.send(order).await.unwrap();

    wait_for("server receives the order", async || {
        !server_app.app_messages.lock().unwrap().is_empty()
    })
    .await;
    {
        let msgs = server_app.app_messages.lock().unwrap();
        assert_eq!(msgs[0].msg_type().unwrap(), "D");
        assert_eq!(msgs[0].body.get_string(11).unwrap(), "ORDER-1");
        assert_eq!(msgs[0].seq_num().unwrap(), 2);
    }

    // Seqnums advanced on both sides.
    let status = server_session.status().await.unwrap();
    assert_eq!(status.next_target_seq_num, 3);

    // Graceful logout initiated by the client.
    client_session.logout().await.unwrap();
    wait_for("both sides logged out", async || {
        !client_session.is_logged_on().await && !server_session.is_logged_on().await
    })
    .await;
    assert_eq!(server_app.logouts.lock().unwrap().len(), 1);
    assert_eq!(client_app.logouts.lock().unwrap().len(), 1);

    client.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn heartbeats_keep_session_alive() {
    let port = free_port();
    let server = Engine::start(
        &acceptor_settings(port, 1),
        Arc::new(RecordingApp::default()),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await
    .unwrap();
    let client = Engine::start(
        &initiator_settings(port, 1),
        Arc::new(RecordingApp::default()),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await
    .unwrap();

    let client_session = client.session("FIX.4.4", "CLIENT", "SERVER").unwrap();
    wait_for("logged on", async || client_session.is_logged_on().await).await;

    // With HeartBtInt=1s, surviving 4 seconds requires heartbeats to flow
    // (2.4x the interval with silence would disconnect).
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(client_session.is_logged_on().await, "session dropped despite heartbeats");

    // Seqnums should have advanced past the logon due to heartbeats.
    let status = client_session.status().await.unwrap();
    assert!(status.next_sender_seq_num > 2, "no heartbeats were sent");

    client.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn next_expected_seq_num_handshake_between_engines() {
    // Two real engines with SendNextExpectedMsgSeqNum=Y must still log on and
    // exchange messages: this exercises the initiator's fresh-logon 789 and
    // the acceptor's reply 789, which the acceptor-driven acceptance defs
    // don't cover.
    let port = free_port();
    let with_789 = |base: Settings| {
        let mut s = base;
        for sess in &mut s.sessions {
            sess.insert("SendNextExpectedMsgSeqNum".into(), "Y".into());
        }
        s
    };
    let server_app = Arc::new(RecordingApp::default());
    let server = Engine::start(
        &with_789(acceptor_settings(port, 30)),
        server_app.clone(),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await
    .unwrap();
    let client = Engine::start(
        &with_789(initiator_settings(port, 30)),
        Arc::new(RecordingApp::default()),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await
    .unwrap();

    let client_session = client.session("FIX.4.4", "CLIENT", "SERVER").unwrap();
    let server_session = server.session("FIX.4.4", "SERVER", "CLIENT").unwrap();
    wait_for("both logged on with 789 enabled", async || {
        client_session.is_logged_on().await && server_session.is_logged_on().await
    })
    .await;

    // Normal message flow still works.
    let mut order = Message::with_type("D");
    order.set(11, "789-ORDER");
    order.set(55, "TSLA");
    order.set(54, '1');
    order.set(60, UtcTimestamp::now());
    order.set(40, '1');
    client_session.send(order).await.unwrap();

    wait_for("server received the order", async || {
        !server_app.app_messages.lock().unwrap().is_empty()
    })
    .await;
    assert_eq!(
        server_app.app_messages.lock().unwrap()[0].body.get_string(11).unwrap(),
        "789-ORDER"
    );

    client.stop().await;
    server.stop().await;
}

// ----- raw-socket counterparty for protocol-level tests -----

struct RawClient {
    stream: TcpStream,
    buf: BytesMut,
    seq: u64,
}

impl RawClient {
    async fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        Self { stream, buf: BytesMut::new(), seq: 0 }
    }

    /// Build and send a message with the standard header, at an explicit
    /// sequence number.
    async fn send_at(&mut self, seq: u64, msg_type: &str, fields: &[(i32, &str)]) {
        let mut m = Message::with_type(msg_type);
        m.header.set(tags::BEGIN_STRING, "FIX.4.4");
        m.header.set(tags::SENDER_COMP_ID, "CLIENT");
        m.header.set(tags::TARGET_COMP_ID, "SERVER");
        m.header.set(tags::MSG_SEQ_NUM, seq);
        m.stamp_sending_time(UtcTimestamp::now());
        for &(tag, val) in fields {
            if tag == tags::POSS_DUP_FLAG || tag == tags::ORIG_SENDING_TIME {
                m.header.set(tag, val);
            } else {
                m.set(tag, val);
            }
        }
        self.stream.write_all(&m.to_bytes()).await.unwrap();
    }

    async fn send(&mut self, msg_type: &str, fields: &[(i32, &str)]) {
        self.seq += 1;
        self.send_at(self.seq, msg_type, fields).await;
    }

    /// Read the next inbound message, skipping heartbeats.
    async fn expect(&mut self, msg_type: &str) -> Message {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Frame::Message(raw) = extract_frame(&mut self.buf) {
                let m = Message::parse(&raw, true).unwrap();
                let mt = m.msg_type().unwrap();
                if mt == "0" && msg_type != "0" {
                    continue; // ignore heartbeats
                }
                assert_eq!(
                    mt,
                    msg_type,
                    "expected 35={msg_type}, got: {}",
                    String::from_utf8_lossy(&raw).replace('\x01', "|")
                );
                return m;
            }
            let n = tokio::time::timeout_at(deadline, self.stream.read_buf(&mut self.buf))
                .await
                .expect("timed out waiting for message")
                .unwrap();
            assert!(n > 0, "server closed connection while expecting 35={msg_type}");
        }
    }
}

#[tokio::test]
async fn seqnum_gap_triggers_resend_request_and_gap_fill_recovers() {
    let port = free_port();
    let server_app = Arc::new(RecordingApp::default());
    let server = Engine::start(
        &acceptor_settings(port, 30),
        server_app.clone(),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await
    .unwrap();

    let mut client = RawClient::connect(port).await;
    client.send("A", &[(98, "0"), (108, "30")]).await; // Logon, seq 1
    let logon = client.expect("A").await;
    assert_eq!(logon.body.get::<u64>(108).unwrap(), 30);

    // Jump to seq 5: the server should ask for 2..∞.
    client.send_at(5, "D", &[(11, "LATE-ORDER"), (55, "TSLA"), (54, "1"), (40, "1")]).await;
    let rr = client.expect("2").await;
    assert_eq!(rr.body.get::<u64>(tags::BEGIN_SEQ_NO).unwrap(), 2);
    assert_eq!(rr.body.get::<u64>(tags::END_SEQ_NO).unwrap(), 0);

    // Nothing in 2..4 worth resending: gap-fill through 5.
    client
        .send_at(2, "4", &[(43, "Y"), (123, "Y"), (36, "5")])
        .await;

    // Once the gap fills, the stashed order must reach the application.
    wait_for("stashed order delivered", async || {
        !server_app.app_messages.lock().unwrap().is_empty()
    })
    .await;
    {
        let msgs = server_app.app_messages.lock().unwrap();
        assert_eq!(msgs[0].body.get_string(11).unwrap(), "LATE-ORDER");
    }

    // Server now expects 6: a normal logout at seq 6 completes cleanly.
    client.seq = 5;
    client.send("5", &[]).await;
    client.expect("5").await;

    server.stop().await;
}

#[tokio::test]
async fn dictionary_validation_rejects_bad_message() {
    let port = free_port();
    let spec = concat!(env!("CARGO_MANIFEST_DIR"), "/spec/FIX44.xml");
    let settings = Settings::parse(&format!(
        "[SESSION]\nConnectionType=acceptor\nBeginString=FIX.4.4\nSenderCompID=SERVER\n\
         TargetCompID=CLIENT\nSocketAcceptPort={port}\nHeartBtInt=30\nDataDictionary={spec}\n"
    ))
    .unwrap();
    let server_app = Arc::new(RecordingApp::default());
    let server = Engine::start(
        &settings,
        server_app.clone(),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await
    .unwrap();

    let mut client = RawClient::connect(port).await;
    client.send("A", &[(98, "0"), (108, "30")]).await;
    client.expect("A").await;

    // Side=Z is not a legal enum value: expect Reject 373=5 pointing at 54.
    let ts = chrono::Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();
    client
        .send("D", &[(11, "BAD-1"), (55, "TSLA"), (54, "Z"), (40, "1"), (60, &ts)])
        .await;
    let reject = client.expect("3").await;
    assert_eq!(reject.body.get::<u32>(tags::SESSION_REJECT_REASON).unwrap(), 5);
    assert_eq!(reject.body.get::<u32>(tags::REF_TAG_ID).unwrap(), 54);
    assert_eq!(reject.body.get::<u64>(tags::REF_SEQ_NUM).unwrap(), 2);
    assert!(server_app.app_messages.lock().unwrap().is_empty());

    // The rejected message consumed seq 2; a valid order at seq 3 flows.
    client
        .send("D", &[(11, "GOOD-1"), (55, "TSLA"), (54, "1"), (40, "1"), (38, "100"), (60, &ts)])
        .await;
    wait_for("valid order delivered", async || {
        !server_app.app_messages.lock().unwrap().is_empty()
    })
    .await;

    server.stop().await;
}

#[tokio::test]
async fn seqnum_too_low_causes_logout() {
    let port = free_port();
    let server = Engine::start(
        &acceptor_settings(port, 30),
        Arc::new(RecordingApp::default()),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await
    .unwrap();

    let mut client = RawClient::connect(port).await;
    client.send("A", &[(98, "0"), (108, "30")]).await;
    client.expect("A").await;

    // Replay seq 1 without PossDupFlag: protocol violation, expect Logout.
    client.send_at(1, "0", &[]).await;
    let logout = client.expect("5").await;
    let text = logout.body.get_string(tags::TEXT).unwrap();
    assert!(text.contains("too low"), "unexpected logout text: {text}");

    server.stop().await;
}

#[tokio::test]
async fn test_request_answered_with_matching_heartbeat() {
    let port = free_port();
    let server = Engine::start(
        &acceptor_settings(port, 30),
        Arc::new(RecordingApp::default()),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await
    .unwrap();

    let mut client = RawClient::connect(port).await;
    client.send("A", &[(98, "0"), (108, "30")]).await;
    client.expect("A").await;

    client.send("1", &[(112, "PING-42")]).await; // TestRequest
    let hb = client.expect("0").await;
    assert_eq!(hb.body.get_string(112).unwrap(), "PING-42");

    server.stop().await;
}
