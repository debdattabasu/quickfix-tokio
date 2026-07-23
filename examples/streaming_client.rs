//! Streaming: fire many orders without waiting on each, then reconcile the
//! ExecutionReports as they stream back and matching each fill to its order
//! by ClOrdID. Shows the async, many-in-flight shape of a real client.
//!
//! Self-contained: an inline fill-everything executor runs alongside the
//! client in one process.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quickfix_tokio::fix44::messages::execution_report::ExecutionReport;
use quickfix_tokio::fix44::messages::new_order_single::NewOrderSingle;
use quickfix_tokio::fix44::{classify, fields, AnyMessage};
use quickfix_tokio::{
    Application, ApplicationError, Decimal, Engine, MemoryStoreFactory, Message, NullLogFactory,
    SessionId, Settings, UtcTimestamp, dec,
};
use tokio::sync::mpsc;

const N_ORDERS: usize = 10;

// ----- executor: fills everything -----

struct Executor {
    fills: mpsc::UnboundedSender<(SessionId, ExecutionReport)>,
}

#[async_trait::async_trait]
impl Application for Executor {
    async fn from_app(&self, msg: &Message, id: &SessionId) -> Result<(), ApplicationError> {
        let AnyMessage::NewOrderSingle(order) = classify(msg.clone()) else {
            return Err(ApplicationError::UnsupportedMessageType);
        };
        let cl_ord_id = order.cl_ord_id().unwrap_or_default();
        let qty = order.order_qty().unwrap_or(dec!(0));
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
        er.set_last_qty(qty);
        er.set_transact_time(UtcTimestamp::now());
        let _ = self.fills.send((id.clone(), er));
        Ok(())
    }
}

// ----- client: tracks outstanding orders -----

struct Client {
    outstanding: Arc<Mutex<HashSet<String>>>,
    fill_tx: mpsc::UnboundedSender<String>,
}

#[async_trait::async_trait]
impl Application for Client {
    async fn from_app(&self, msg: &Message, _id: &SessionId) -> Result<(), ApplicationError> {
        if let AnyMessage::ExecutionReport(er) = classify(msg.clone()) {
            if let Ok(cl_ord_id) = er.cl_ord_id() {
                self.outstanding.lock().unwrap().remove(&cl_ord_id);
                let _ = self.fill_tx.send(cl_ord_id);
            }
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

    // Executor.
    let (fills_tx, mut fills_rx) = mpsc::unbounded_channel();
    let server = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=acceptor\nBeginString=FIX.4.4\n\
             SenderCompID=EXEC\nTargetCompID=CLIENT\nSocketAcceptPort={port}\n\
             HeartBtInt=30\nDataDictionary={spec}\n"
        ))?,
        Arc::new(Executor { fills: fills_tx }),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await?;
    let exec_session = server.session("FIX.4.4", "EXEC", "CLIENT").unwrap();
    tokio::spawn(async move {
        while let Some((_, er)) = fills_rx.recv().await {
            let _ = exec_session.send(er.into()).await;
        }
    });

    // Client.
    let outstanding = Arc::new(Mutex::new(HashSet::new()));
    let (fill_tx, mut fill_rx) = mpsc::unbounded_channel();
    let client = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=initiator\nBeginString=FIX.4.4\n\
             SenderCompID=CLIENT\nTargetCompID=EXEC\nSocketConnectHost=127.0.0.1\n\
             SocketConnectPort={port}\nHeartBtInt=30\nReconnectInterval=1\n\
             DataDictionary={spec}\n"
        ))?,
        Arc::new(Client { outstanding: outstanding.clone(), fill_tx }),
        Arc::new(MemoryStoreFactory),
        Arc::new(NullLogFactory),
    )
    .await?;
    let session = client.session("FIX.4.4", "CLIENT", "EXEC").unwrap();

    while !session.is_logged_on().await {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Fire N orders back to back, recording each as outstanding first.
    println!("firing {N_ORDERS} orders...");
    for i in 0..N_ORDERS {
        let cl_ord_id = format!("ORD-{i:02}");
        outstanding.lock().unwrap().insert(cl_ord_id.clone());
        let mut order = NewOrderSingle::new(
            cl_ord_id,
            if i % 2 == 0 { fields::Side::BUY } else { fields::Side::SELL },
            UtcTimestamp::now(),
            fields::OrdType::MARKET,
        );
        order.set_symbol("TSLA");
        order.set_order_qty(Decimal::from(i as i64 + 1) * dec!(10));
        session.send(order.into()).await?;
    }

    // Reconcile fills as they arrive; stop when nothing is outstanding.
    let mut filled = 0;
    while filled < N_ORDERS {
        let cl_ord_id = fill_rx.recv().await.unwrap();
        filled += 1;
        let left = outstanding.lock().unwrap().len();
        println!("  filled {cl_ord_id}  ({filled}/{N_ORDERS}, {left} outstanding)");
    }
    println!("all {N_ORDERS} orders filled");

    client.stop().await;
    server.stop().await;
    Ok(())
}
