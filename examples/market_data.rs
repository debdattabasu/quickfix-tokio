//! Typed repeating groups end to end: a subscriber sends a MarketDataRequest
//! naming a symbol and the entry types it wants; a publisher answers with a
//! MarketDataSnapshotFullRefresh carrying a NoMDEntries group.
//!
//! Both sides run in one process over loopback, so a single
//! `cargo run --example market_data` shows the whole exchange.

use std::sync::Arc;
use std::time::Duration;

use quickfix_tokio::fix44::messages::market_data_request::{
    MarketDataRequest, NoMDEntryTypes, NoRelatedSym,
};
use quickfix_tokio::fix44::messages::market_data_snapshot_full_refresh::{
    MarketDataSnapshotFullRefresh, NoMDEntries,
};
use quickfix_tokio::fix44::{classify, fields, AnyMessage};
use quickfix_tokio::{
    Application, ApplicationError, Engine, MemoryStoreFactory, Message, NullLogFactory, SessionId,
    Settings,
};
use tokio::sync::mpsc;

// ----- publisher (acceptor) -----

struct Publisher {
    tx: mpsc::UnboundedSender<(SessionId, MarketDataSnapshotFullRefresh)>,
}

#[async_trait::async_trait]
impl Application for Publisher {
    async fn from_app(
        &self,
        msg: &Message,
        session_id: &SessionId,
    ) -> Result<(), ApplicationError> {
        if let AnyMessage::MarketDataRequest(req) = classify(msg.clone()) {
            // Read the request's repeating groups.
            let symbol = req
                .no_related_sym()
                .ok()
                .and_then(|syms| syms.into_iter().next())
                .and_then(|s| s.symbol().ok())
                .unwrap_or_default();
            let wants: Vec<char> = req
                .no_md_entry_types()
                .unwrap_or_default()
                .iter()
                .filter_map(|e| e.md_entry_type().ok())
                .collect();
            println!(
                "publisher: request {} for {symbol}, entry types {:?}",
                req.md_req_id().unwrap_or_default(),
                wants
            );

            // Build a snapshot with a NoMDEntries group: a bid and an offer.
            let mut snap = MarketDataSnapshotFullRefresh::new();
            if let Ok(id) = req.md_req_id() {
                snap.set_md_req_id(id);
            }
            snap.set_symbol(&symbol);

            let book = [
                (fields::MDEntryType::BID, 100.25, 500.0),
                (fields::MDEntryType::OFFER, 100.75, 300.0),
            ];
            let entries = book.iter().filter(|(t, _, _)| wants.contains(t)).map(|&(t, px, sz)| {
                let mut e = NoMDEntries::new();
                e.set_md_entry_type(t); // delimiter (269) first
                e.set_md_entry_px(px);
                e.set_md_entry_size(sz);
                e
            });
            snap.set_no_md_entries(entries);

            let _ = self.tx.send((session_id.clone(), snap));
            Ok(())
        } else {
            Err(ApplicationError::UnsupportedMessageType)
        }
    }
}

// ----- subscriber (initiator) -----

struct Subscriber {
    done: mpsc::UnboundedSender<MarketDataSnapshotFullRefresh>,
}

#[async_trait::async_trait]
impl Application for Subscriber {
    async fn from_app(
        &self,
        msg: &Message,
        _session_id: &SessionId,
    ) -> Result<(), ApplicationError> {
        if let AnyMessage::MarketDataSnapshotFullRefresh(snap) = classify(msg.clone()) {
            let _ = self.done.send(snap);
        }
        Ok(())
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[tokio::main]
async fn main() -> quickfix_tokio::Result<()> {
    let port = free_port();
    let spec = concat!(env!("CARGO_MANIFEST_DIR"), "/spec/FIX44.xml");

    // Publisher engine.
    let (snap_tx, mut snap_rx) = mpsc::unbounded_channel();
    let pub_engine = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=acceptor\nBeginString=FIX.4.4\n\
             SenderCompID=PUB\nTargetCompID=SUB\nSocketAcceptPort={port}\n\
             HeartBtInt=30\nDataDictionary={spec}\n"
        ))?,
        Arc::new(Publisher { tx: snap_tx }),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await?;
    // Worker: send each built snapshot back on the publisher's session.
    let pub_sessions = pub_engine.session("FIX.4.4", "PUB", "SUB").unwrap();
    tokio::spawn(async move {
        while let Some((_, snap)) = snap_rx.recv().await {
            let _ = pub_sessions.send(snap.into()).await;
        }
    });

    // Subscriber engine.
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    let sub_engine = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=initiator\nBeginString=FIX.4.4\n\
             SenderCompID=SUB\nTargetCompID=PUB\nSocketConnectHost=127.0.0.1\n\
             SocketConnectPort={port}\nHeartBtInt=30\nReconnectInterval=1\n\
             DataDictionary={spec}\n"
        ))?,
        Arc::new(Subscriber { done: done_tx }),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await?;
    let sub = sub_engine.session("FIX.4.4", "SUB", "PUB").unwrap();

    while !sub.is_logged_on().await {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Build a typed MarketDataRequest with two repeating groups.
    let mut req = MarketDataRequest::new(
        "MDREQ-1",
        fields::SubscriptionRequestType::SNAPSHOT,
        0, // MarketDepth: full book
    );
    let mut sym = NoRelatedSym::new();
    sym.set_symbol("TSLA");
    req.set_no_related_sym([sym]);

    let entry_types = [fields::MDEntryType::BID, fields::MDEntryType::OFFER].map(|t| {
        let mut e = NoMDEntryTypes::new();
        e.set_md_entry_type(t);
        e
    });
    req.set_no_md_entry_types(entry_types);

    println!("subscriber: sending MarketDataRequest for TSLA (bid + offer)");
    sub.send(req.into()).await?;

    // Await and print the snapshot's entries.
    let snap = done_rx.recv().await.expect("snapshot");
    println!("subscriber: snapshot for {}", snap.symbol().unwrap_or_default());
    for entry in snap.no_md_entries().unwrap() {
        let side = match entry.md_entry_type().unwrap() {
            fields::MDEntryType::BID => "bid",
            fields::MDEntryType::OFFER => "offer",
            _ => "?",
        };
        println!(
            "  {side:>5}  {} x {}",
            entry.md_entry_px().unwrap(),
            entry.md_entry_size().unwrap()
        );
    }

    sub_engine.stop().await;
    pub_engine.stop().await;
    Ok(())
}
