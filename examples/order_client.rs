//! A minimal buy-side client: connects to the executor example, sends one
//! order, prints the fill.
//!
//! Run `cargo run --example executor` first, then
//! `cargo run --example order_client`.

use std::sync::Arc;

use quickfix_tokio::{
    Application, ApplicationError, Engine, MemoryStoreFactory, Message, SessionId, Settings,
    TracingLogFactory, UtcTimestamp,
};
use tokio::sync::mpsc;

struct Client {
    fills_tx: mpsc::UnboundedSender<Message>,
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
        let _ = self.fills_tx.send(msg.clone());
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
         ReconnectInterval=2\n",
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

    // Wait for logon, then place an order.
    while !session.is_logged_on().await {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let mut order = Message::with_type("D"); // NewOrderSingle
    order.set(11, "ORDER-1"); // ClOrdID
    order.set(55, "TSLA"); // Symbol
    order.set(54, '1'); // Side = Buy
    order.set(38, 100); // OrderQty
    order.set(40, '1'); // OrdType = Market
    order.set(60, UtcTimestamp::now()); // TransactTime
    session.send(order).await?;
    println!("order sent, waiting for fill...");

    if let Some(fill) = fills_rx.recv().await {
        println!(
            "filled: ClOrdID={} status={} avgpx={}",
            fill.body.get_string(11).unwrap_or_default(),
            fill.body.get_string(39).unwrap_or_default(),
            fill.body.get_string(6).unwrap_or_default(),
        );
    }

    session.logout().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    engine.stop().await;
    Ok(())
}
