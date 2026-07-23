//! Socket plumbing: per-connection read/write tasks, the acceptor's
//! listener (which identifies sessions from the first inbound message), and
//! the initiator's dial/reconnect loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};

use crate::message::Message;
use crate::parser::{Frame, extract_frame};
use crate::session::{Command, Connection, SessionHandle};
use crate::tags;

/// Registry key for routing an inbound connection to a session:
/// (BeginString, our CompID, their CompID).
pub(crate) type SessionKey = (String, String, String);

pub(crate) fn spawn_io_tasks(
    stream: TcpStream,
    leftover: BytesMut,
) -> (mpsc::Receiver<Bytes>, mpsc::Sender<Bytes>) {
    let (read_half, write_half) = stream.into_split();
    let (in_tx, in_rx) = mpsc::channel(256);
    let (out_tx, out_rx) = mpsc::channel::<Bytes>(256);
    tokio::spawn(read_task(read_half, leftover, in_tx));
    tokio::spawn(write_task(write_half, out_rx));
    (in_rx, out_tx)
}

async fn read_task(mut read_half: OwnedReadHalf, mut buf: BytesMut, in_tx: mpsc::Sender<Bytes>) {
    loop {
        while let Frame::Message(raw) = extract_frame(&mut buf) {
            if in_tx.send(raw).await.is_err() {
                return; // session detached
            }
        }
        match read_half.read_buf(&mut buf).await {
            Ok(0) | Err(_) => return, // dropping in_tx signals disconnect
            Ok(_) => {}
        }
    }
}

async fn write_task(mut write_half: OwnedWriteHalf, mut out_rx: mpsc::Receiver<Bytes>) {
    while let Some(raw) = out_rx.recv().await {
        if write_half.write_all(&raw).await.is_err() {
            return;
        }
    }
    // Channel closed: session detached; close the socket politely.
    let _ = write_half.shutdown().await;
}

// ----- acceptor -----

pub(crate) async fn run_acceptor(
    listener: tokio::net::TcpListener,
    registry: Arc<HashMap<SessionKey, SessionHandle>>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let registry = registry.clone();
                tokio::spawn(async move {
                    if let Err(e) = identify_and_attach(stream, &registry).await {
                        tracing::warn!("connection from {peer} rejected: {e}");
                    }
                });
            }
            Err(e) => {
                tracing::warn!("accept error: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Read the first frame off a fresh inbound connection, derive the session
/// identity (peer CompIDs reversed), and hand the connection to that session.
async fn identify_and_attach(
    mut stream: TcpStream,
    registry: &HashMap<SessionKey, SessionHandle>,
) -> std::result::Result<(), String> {
    let _ = stream.set_nodelay(true);
    let mut buf = BytesMut::with_capacity(8192);
    let first = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Frame::Message(raw) = extract_frame(&mut buf) {
                return Ok::<_, String>(raw);
            }
            match stream.read_buf(&mut buf).await {
                Ok(0) => return Err("peer closed before sending a message".into()),
                Ok(_) => {}
                Err(e) => return Err(format!("read error: {e}")),
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for first message".to_string())??;

    let msg = Message::parse(&first, false).map_err(|e| format!("unparseable first message: {e}"))?;
    let get = |tag| {
        msg.header
            .get_raw(tag)
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .ok_or_else(|| format!("first message missing tag {tag}"))
    };
    // Their sender is our target and vice versa.
    let key: SessionKey = (
        get(tags::BEGIN_STRING)?,
        get(tags::TARGET_COMP_ID)?,
        get(tags::SENDER_COMP_ID)?,
    );
    let handle = registry.get(&key).ok_or_else(|| {
        format!("no session configured for {}:{}->{}", key.0, key.1, key.2)
    })?;

    let (in_rx, out_tx) = spawn_io_tasks(stream, buf);
    // Deliver the already-read first message through a small relay so the
    // session sees it before the socket's subsequent frames.
    let (relay_tx, relay_rx) = mpsc::channel(256);
    relay_tx.send(first).await.map_err(|e| e.to_string())?;
    let mut in_rx = in_rx;
    tokio::spawn(async move {
        while let Some(raw) = in_rx.recv().await {
            if relay_tx.send(raw).await.is_err() {
                return;
            }
        }
    });

    handle
        .cmd_tx
        .send(Command::Attach(Connection {
            inbound: relay_rx,
            outbound: out_tx,
            disconnected: None,
        }))
        .await
        .map_err(|_| "session task not running".to_string())
}

// ----- initiator -----

pub(crate) async fn run_initiator(
    host: String,
    port: u16,
    reconnect_interval: Duration,
    handle: SessionHandle,
) {
    loop {
        match TcpStream::connect((host.as_str(), port)).await {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                let (in_rx, out_tx) = spawn_io_tasks(stream, BytesMut::with_capacity(8192));
                let (disc_tx, disc_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(Command::Attach(Connection {
                        inbound: in_rx,
                        outbound: out_tx,
                        disconnected: Some(disc_tx),
                    }))
                    .await
                    .is_err()
                {
                    return; // session stopped
                }
                // Wait until the session detaches (drops the sender).
                let _ = disc_rx.await;
            }
            Err(e) => {
                tracing::info!(session = %handle.id, "connect to {host}:{port} failed: {e}");
            }
        }
        tokio::time::sleep(reconnect_interval).await;
    }
}
