//! A tour of the `Application` callback surface (the same seven callbacks as
//! QuickFIX's IApplication):
//!
//!   to_admin    client stamps Username/Password onto its outgoing Logon
//!   from_admin  server checks those credentials, vetoes with RejectLogon
//!   from_app    server enforces a risk limit, vetoing orders with Reject
//!   on_logon / on_logout / on_create  lifecycle notifications
//!
//! The client sends four orders: two within the risk limit (filled) and two
//! over it (business-rejected). Everything runs in one process.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use quickfix_tokio::field_map::Field; // for fields::OrderQty::TAG
use quickfix_tokio::fix44::messages::execution_report::ExecutionReport;
use quickfix_tokio::fix44::messages::new_order_single::NewOrderSingle;
use quickfix_tokio::fix44::{classify, fields, AnyMessage};
use quickfix_tokio::{
    Amount, Application, ApplicationError, Engine, MemoryStoreFactory, Message, NullLogFactory,
    RejectError, SessionId, Settings, UtcTimestamp, dec,
};
use tokio::sync::mpsc;

const PASSWORD: &str = "hunter2";
fn max_qty() -> Amount {
    dec!(1000)
}

// ----- server: authenticates logons, risk-checks orders -----

struct RiskExecutor {
    fills_out: mpsc::UnboundedSender<(SessionId, ExecutionReport)>,
}

#[async_trait::async_trait]
impl Application for RiskExecutor {
    async fn on_logon(&self, id: &SessionId) {
        println!("[server] logon accepted: {id}");
    }

    /// Inbound admin messages. Reject the Logon unless the password matches.
    async fn from_admin(&self, msg: &Message, _id: &SessionId) -> Result<(), ApplicationError> {
        if let AnyMessage::Logon(logon) = classify(msg.clone()) {
            let user = logon.username().unwrap_or_default();
            match logon.password() {
                Ok(p) if p == PASSWORD => println!("[server] credentials OK for {user:?}"),
                _ => {
                    println!("[server] bad credentials for {user:?} -> RejectLogon");
                    return Err(ApplicationError::RejectLogon("invalid password".into()));
                }
            }
        }
        Ok(())
    }

    /// Inbound app messages. Fill orders within the risk limit; reject the
    /// rest with a session-level Reject.
    async fn from_app(&self, msg: &Message, id: &SessionId) -> Result<(), ApplicationError> {
        let AnyMessage::NewOrderSingle(order) = classify(msg.clone()) else {
            return Err(ApplicationError::UnsupportedMessageType);
        };
        let qty = order.order_qty().unwrap_or(dec!(0));
        let cl_ord_id = order.cl_ord_id().unwrap_or_default();
        let limit = max_qty();
        if qty > limit {
            println!("[server] REJECT {cl_ord_id}: qty {qty} over limit {limit}");
            return Err(ApplicationError::Reject(RejectError::other(
                format!("order qty {qty} exceeds limit {limit}"),
                fields::OrderQty::TAG,
            )));
        }
        println!("[server] FILL {cl_ord_id}: {qty} @ market");
        let mut er = ExecutionReport::new(
            format!("EXEC-{cl_ord_id}"),
            format!("X-{cl_ord_id}"),
            fields::ExecType::TRADE,
            fields::OrdStatus::FILLED,
            order.side().unwrap_or(fields::Side::BUY),
            dec!(0),
            qty,
            dec!(100),
        );
        er.set_cl_ord_id(cl_ord_id);
        er.set_transact_time(UtcTimestamp::now());
        let _ = self.fills_out.send((id.clone(), er));
        Ok(())
    }
}

// ----- client: authenticates, then streams orders -----

struct AuthClient {
    events: mpsc::UnboundedSender<ClientEvent>,
}

enum ClientEvent {
    Filled(String),
    Rejected(String),
}

#[async_trait::async_trait]
impl Application for AuthClient {
    /// Outbound admin messages. Add credentials to our Logon before it goes.
    async fn to_admin(&self, msg: &mut Message, _id: &SessionId) {
        if msg.msg_type().ok().as_deref() == Some("A") {
            msg.body.set_field::<fields::Username>("trader-1".to_string());
            msg.body.set_field::<fields::Password>(PASSWORD.to_string());
        }
    }

    /// A session-level Reject (35=3) is an *admin* message, so it arrives
    /// here — not in from_app.
    async fn from_admin(&self, msg: &Message, _id: &SessionId) -> Result<(), ApplicationError> {
        if let AnyMessage::Reject(rej) = classify(msg.clone()) {
            let _ = self.events.send(ClientEvent::Rejected(rej.text().unwrap_or_default()));
        }
        Ok(())
    }

    /// ExecutionReports are business messages and land here.
    async fn from_app(&self, msg: &Message, _id: &SessionId) -> Result<(), ApplicationError> {
        if let AnyMessage::ExecutionReport(er) = classify(msg.clone()) {
            let _ = self.events.send(ClientEvent::Filled(er.cl_ord_id().unwrap_or_default()));
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

    // Server.
    let (fills_tx, mut fills_rx) = mpsc::unbounded_channel();
    let server = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=acceptor\nBeginString=FIX.4.4\n\
             SenderCompID=EXEC\nTargetCompID=BUYSIDE\nSocketAcceptPort={port}\n\
             HeartBtInt=30\nDataDictionary={spec}\n"
        ))?,
        Arc::new(RiskExecutor { fills_out: fills_tx }),
        Arc::new(MemoryStoreFactory::new()),
        Arc::new(NullLogFactory),
    )
    .await?;
    let server_session = server.session("FIX.4.4", "EXEC", "BUYSIDE").unwrap();
    tokio::spawn(async move {
        while let Some((_, er)) = fills_rx.recv().await {
            let _ = server_session.send(er.into()).await;
        }
    });

    // Client.
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel();
    let client = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=initiator\nBeginString=FIX.4.4\n\
             SenderCompID=BUYSIDE\nTargetCompID=EXEC\nSocketConnectHost=127.0.0.1\n\
             SocketConnectPort={port}\nHeartBtInt=30\nReconnectInterval=1\n\
             DataDictionary={spec}\n"
        ))?,
        Arc::new(AuthClient { events: ev_tx }),
        Arc::new(MemoryStoreFactory::new()),
        Arc::new(NullLogFactory),
    )
    .await?;
    let session = client.session("FIX.4.4", "BUYSIDE", "EXEC").unwrap();

    while !session.is_logged_on().await {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Four orders: two within the limit, two over it.
    let orders =
        [("A-small", dec!(100)), ("B-big", dec!(5000)), ("C-small", dec!(250)), ("D-big", dec!(2000))];
    for (id, qty) in orders {
        let mut order = NewOrderSingle::new(
            id,
            fields::Side::BUY,
            UtcTimestamp::now(),
            fields::OrdType::MARKET,
        );
        order.set_symbol("TSLA");
        order.set_order_qty(qty);
        session.send(order.into()).await?;
    }

    // Collect the four responses.
    let filled = AtomicU64::new(0);
    let rejected = AtomicU64::new(0);
    for _ in 0..orders.len() {
        match ev_rx.recv().await.unwrap() {
            ClientEvent::Filled(id) => {
                println!("[client] filled: {id}");
                filled.fetch_add(1, Ordering::Relaxed);
            }
            ClientEvent::Rejected(text) => {
                println!("[client] rejected: {text}");
                rejected.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    println!(
        "[client] done: {} filled, {} rejected",
        filled.load(Ordering::Relaxed),
        rejected.load(Ordering::Relaxed)
    );

    client.stop().await;
    server.stop().await;
    Ok(())
}
