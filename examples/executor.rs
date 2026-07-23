//! A minimal sell-side executor using the typed FIX 4.4 API: accepts
//! sessions on port 9876 and fills every NewOrderSingle it receives.
//!
//! Note the reply pattern: `from_app` runs on the session's own task, so it
//! must not *wait* on `SessionHandle::send` for that same session. Instead
//! it forwards orders to a worker task that sends the ExecutionReports.
//!
//! Run with: `cargo run --example executor`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use quickfix_tokio::fix44::messages::execution_report::ExecutionReport;
use quickfix_tokio::fix44::messages::new_order_single::NewOrderSingle;
use quickfix_tokio::fix44::{AnyMessage, classify, fields};
use quickfix_tokio::{
    Application, ApplicationError, Engine, MemoryStoreFactory, Message, SessionId, Settings,
    TracingLogFactory, UtcTimestamp, dec,
};
use tokio::sync::mpsc;

struct Executor {
    orders_tx: mpsc::UnboundedSender<(SessionId, NewOrderSingle)>,
}

#[async_trait::async_trait]
impl Application for Executor {
    async fn on_logon(&self, session_id: &SessionId) {
        println!("logged on: {session_id}");
    }
    async fn on_logout(&self, session_id: &SessionId) {
        println!("logged out: {session_id}");
    }
    async fn from_app(
        &self,
        msg: &Message,
        session_id: &SessionId,
    ) -> Result<(), ApplicationError> {
        match classify(msg.clone()) {
            AnyMessage::NewOrderSingle(order) => {
                let _ = self.orders_tx.send((session_id.clone(), order));
                Ok(())
            }
            _ => Err(ApplicationError::UnsupportedMessageType),
        }
    }
}

fn fill_for(order: &NewOrderSingle, exec_seq: u64) -> ExecutionReport {
    let qty = order.order_qty().unwrap_or(dec!(0));
    let price = order.price().unwrap_or(dec!(100)); // market orders "fill" at 100
    let mut er = ExecutionReport::new(
        format!("ORD-{exec_seq}"),
        format!("EXEC-{exec_seq}"),
        fields::ExecType::TRADE,
        fields::OrdStatus::FILLED,
        order.side().unwrap_or(fields::Side::BUY),
        dec!(0), // LeavesQty
        qty,     // CumQty
        price,   // AvgPx
    );
    if let Ok(cl_ord_id) = order.cl_ord_id() {
        er.set_cl_ord_id(cl_ord_id);
    }
    if let Ok(symbol) = order.symbol() {
        er.set_symbol(symbol);
    }
    er.set_last_px(price);
    er.set_last_qty(qty);
    er.set_transact_time(UtcTimestamp::now());
    er
}

#[tokio::main]
async fn main() -> quickfix_tokio::Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    let settings = Settings::parse(
        "[SESSION]\n\
         ConnectionType=acceptor\n\
         BeginString=FIX.4.4\n\
         SenderCompID=EXECUTOR\n\
         TargetCompID=CLIENT\n\
         SocketAcceptPort=9876\n\
         HeartBtInt=30\n\
         DataDictionary=spec/FIX44.xml\n",
    )?;

    let (orders_tx, mut orders_rx) = mpsc::unbounded_channel();
    let engine = Engine::start(
        &settings,
        Arc::new(Executor { orders_tx }),
        Arc::new(MemoryStoreFactory::new()),
        Arc::new(TracingLogFactory),
    )
    .await?;

    println!("executor listening on :9876");
    let exec_count = AtomicU64::new(0);
    while let Some((session_id, order)) = orders_rx.recv().await {
        let n = exec_count.fetch_add(1, Ordering::Relaxed) + 1;
        println!(
            "filling {} x {} for {}",
            order.order_qty().unwrap_or(dec!(0)),
            order.symbol().unwrap_or_default(),
            order.cl_ord_id().unwrap_or_default(),
        );
        if let Some(handle) = engine.session(
            &session_id.begin_string,
            &session_id.sender_comp_id,
            &session_id.target_comp_id,
        ) {
            if let Err(e) = handle.send(fill_for(&order, n).into()).await {
                eprintln!("failed to send fill: {e}");
            }
        }
    }
    Ok(())
}
