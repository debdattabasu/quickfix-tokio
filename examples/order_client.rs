//! A minimal buy-side client using the typed FIX 4.4 API: connects to the
//! executor example, sends one order, prints the fill.
//!
//! Run `cargo run --example executor` first, then
//! `cargo run --example order_client`.

use std::sync::Arc;

use quickfix_tokio::fix44::messages::execution_report::ExecutionReport;
use quickfix_tokio::fix44::messages::new_order_single::NewOrderSingle;
use quickfix_tokio::fix44::{AnyMessage, classify, fields};
use quickfix_tokio::{
    Application, ApplicationError, Engine, MemoryStoreFactory, Message, SessionId, Settings,
    TracingLogFactory, UtcTimestamp, dec,
};
use tokio::sync::mpsc;

struct Client {
    fills_tx: mpsc::UnboundedSender<ExecutionReport>,
}

#[async_trait::async_trait]
impl Application for Client {
    async fn on_logon(&self, session_id: &SessionId) {
        println!("logged on: {session_id}");
    }
    async fn from_app(
        &self,
        msg: &Message,
        _session_id: &SessionId,
    ) -> Result<(), ApplicationError> {
        if let AnyMessage::ExecutionReport(er) = classify(msg.clone()) {
            let _ = self.fills_tx.send(er);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> quickfix_tokio::Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    let settings = Settings::parse(
        "[SESSION]\n\
         ConnectionType=initiator\n\
         BeginString=FIX.4.4\n\
         SenderCompID=CLIENT\n\
         TargetCompID=EXECUTOR\n\
         SocketConnectHost=127.0.0.1\n\
         SocketConnectPort=9876\n\
         HeartBtInt=30\n\
         ReconnectInterval=2\n\
         DataDictionary=spec/FIX44.xml\n",
    )?;

    let (fills_tx, mut fills_rx) = mpsc::unbounded_channel();
    let engine = Engine::start(
        &settings,
        Arc::new(Client { fills_tx }),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await?;
    let session = engine.session("FIX.4.4", "CLIENT", "EXECUTOR").unwrap();

    // Wait for logon, then place a typed order.
    while !session.is_logged_on().await {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let mut order = NewOrderSingle::new(
        "ORDER-1",
        fields::Side::BUY,
        UtcTimestamp::now(),
        fields::OrdType::LIMIT,
    );
    order.set_symbol("TSLA");
    order.set_order_qty(dec!(100));
    order.set_price(dec!(101.25));
    session.send(order.into()).await?;
    println!("order sent, waiting for fill...");

    if let Some(fill) = fills_rx.recv().await {
        println!(
            "filled: {} {} x {} @ {} (status {})",
            fill.cl_ord_id().unwrap_or_default(),
            fill.cum_qty().unwrap_or(dec!(0)),
            fill.symbol().unwrap_or_default(),
            fill.avg_px().unwrap_or(dec!(0)),
            fill.ord_status().map(|c| c.to_string()).unwrap_or_default(),
        );
    }

    session.logout().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    engine.stop().await;
    Ok(())
}
