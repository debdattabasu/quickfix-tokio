//! End-to-end TLS: real engines exchanging FIX over a rustls-encrypted
//! loopback connection, using a self-signed certificate.

#![cfg(feature = "tls")]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quickfix_tokio::{
    Application, Engine, MemoryStoreFactory, Message, NullLogFactory, SessionId, Settings,
    UtcTimestamp,
};

#[derive(Default)]
struct RecordingApp {
    logons: Mutex<u32>,
    app_messages: Mutex<Vec<Message>>,
}

#[async_trait::async_trait]
impl Application for RecordingApp {
    async fn on_logon(&self, _id: &SessionId) {
        *self.logons.lock().unwrap() += 1;
    }
    async fn from_app(
        &self,
        msg: &Message,
        _id: &SessionId,
    ) -> Result<(), quickfix_tokio::ApplicationError> {
        self.app_messages.lock().unwrap().push(msg.clone());
        Ok(())
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Generate a self-signed cert for "localhost"/127.0.0.1, write the cert and
/// key to unique temp files, and return their paths plus the cert PEM (usable
/// as a CA to pin against).
fn gen_cert() -> (String, String, String, tempfiles::Guard) {
    static N: AtomicU32 = AtomicU32::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir().join(format!("qft-tls-{}-{n}", std::process::id()));
    let cert_path = format!("{}.crt", base.display());
    let key_path = format!("{}.key", base.display());

    let certified = rcgen::generate_simple_self_signed(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .unwrap();
    let cert_pem = certified.cert.pem();
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, certified.key_pair.serialize_pem()).unwrap();

    let guard = tempfiles::Guard(vec![cert_path.clone(), key_path.clone()]);
    (cert_path, key_path, cert_pem, guard)
}

mod tempfiles {
    /// Removes the listed files on drop.
    pub struct Guard(pub Vec<String>);
    impl Drop for Guard {
        fn drop(&mut self) {
            for f in &self.0 {
                let _ = std::fs::remove_file(f);
            }
        }
    }
}

fn acceptor(port: u16, cert: &str, key: &str) -> Settings {
    Settings::parse(&format!(
        "[SESSION]\nConnectionType=acceptor\nBeginString=FIX.4.4\nSenderCompID=SERVER\n\
         TargetCompID=CLIENT\nSocketAcceptPort={port}\nHeartBtInt=30\n\
         SocketUseSSL=Y\nSocketCertificateFile={cert}\nSocketPrivateKeyFile={key}\n"
    ))
    .unwrap()
}

fn initiator(port: u16, extra: &str) -> Settings {
    Settings::parse(&format!(
        "[SESSION]\nConnectionType=initiator\nBeginString=FIX.4.4\nSenderCompID=CLIENT\n\
         TargetCompID=SERVER\nSocketConnectHost=127.0.0.1\nSocketConnectPort={port}\n\
         HeartBtInt=30\nReconnectInterval=1\nSocketUseSSL=Y\n{extra}\n"
    ))
    .unwrap()
}

async fn logged_on_within(handle: &quickfix_tokio::SessionHandle, secs: u64) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while tokio::time::Instant::now() < deadline {
        if handle.is_logged_on().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test]
async fn tls_logon_and_exchange_skip_verify() {
    let port = free_port();
    let (cert, key, _ca, _guard) = gen_cert();

    let server_app = Arc::new(RecordingApp::default());
    let server = Engine::start(
        &acceptor(port, &cert, &key),
        server_app.clone(),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();

    // Client accepts the self-signed cert without verification.
    let client = Engine::start(
        &initiator(port, "SocketInsecureSkipVerify=Y"),
        Arc::new(RecordingApp::default()),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();

    let client_session = client.session("FIX.4.4", "CLIENT", "SERVER").unwrap();
    assert!(logged_on_within(&client_session, 10).await, "TLS session did not log on");

    // Exchange a real application message over the encrypted channel.
    let mut order = Message::with_type("D");
    order.set(11, "TLS-ORDER");
    order.set(55, "TSLA");
    order.set(54, '1');
    order.set(60, UtcTimestamp::now());
    order.set(40, '1');
    client_session.send(order).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while server_app.app_messages.lock().unwrap().is_empty() {
        assert!(tokio::time::Instant::now() < deadline, "server never received the order");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        server_app.app_messages.lock().unwrap()[0].body.get_string(11).unwrap(),
        "TLS-ORDER"
    );

    client.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn tls_logon_ca_pinned() {
    let port = free_port();
    let (cert, key, ca_path_pem, _guard) = gen_cert();

    // Pin the self-signed cert as the client's CA, and verify the server name.
    static N: AtomicU32 = AtomicU32::new(0);
    let ca_file = std::env::temp_dir()
        .join(format!("qft-ca-{}-{}.pem", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
    std::fs::write(&ca_file, ca_path_pem).unwrap();

    let server = Engine::start(
        &acceptor(port, &cert, &key),
        Arc::new(RecordingApp::default()),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();
    let client = Engine::start(
        &initiator(
            port,
            &format!("SocketCAFile={}\nSocketServerName=localhost", ca_file.display()),
        ),
        Arc::new(RecordingApp::default()),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();

    let client_session = client.session("FIX.4.4", "CLIENT", "SERVER").unwrap();
    assert!(
        logged_on_within(&client_session, 10).await,
        "CA-pinned TLS session did not log on"
    );

    client.stop().await;
    server.stop().await;
    let _ = std::fs::remove_file(&ca_file);
}

#[tokio::test]
async fn tls_untrusted_cert_never_logs_on() {
    let port = free_port();
    let (cert, key, _ca, _guard) = gen_cert();

    let server = Engine::start(
        &acceptor(port, &cert, &key),
        Arc::new(RecordingApp::default()),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();
    // No skip-verify and no CA: the self-signed cert is untrusted, so the
    // handshake fails and the session never logs on.
    let client = Engine::start(
        &initiator(port, "SocketServerName=localhost"),
        Arc::new(RecordingApp::default()),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();

    let client_session = client.session("FIX.4.4", "CLIENT", "SERVER").unwrap();
    assert!(
        !logged_on_within(&client_session, 3).await,
        "session logged on despite an untrusted certificate"
    );

    client.stop().await;
    server.stop().await;
}
