//! A minimal sell-side executor: accepts FIX 4.4 sessions on port 9876 and
//! fills every NewOrderSingle it receives.
//!
//! Note the reply pattern: `from_app` runs on the session's own task, so it
//! must not *wait* on `SessionHandle::send` for that same session. Instead
//! it forwards orders to a worker task that sends the ExecutionReports.
//!
//! Run with: `cargo run --example executor`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use quickfix_tokio::{
    Application, ApplicationError, Engine, MemoryStoreFactory, Message, SessionId, Settings,
    TracingLogFactory, UtcTimestamp,
};
use tokio::sync::mpsc;

struct Executor {
    orders_tx: mpsc::UnboundedSender<(SessionId, Message)>,
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
        match msg.msg_type().unwrap_or_default().as_str() {
            "D" => {
                let _ = self.orders_tx.send((session_id.clone(), msg.clone()));
                Ok(())
            }
            _ => Err(ApplicationError::UnsupportedMessageType),
        }
    }
}

fn fill_for(order: &Message, exec_seq: u64) -> Message {
    let get = |tag| order.body.get_string(tag).unwrap_or_default();
    let qty = get(38);
    let mut er = Message::with_type("8"); // ExecutionReport
    er.set(37, format!("ORD-{exec_seq}").as_str()); // OrderID
    er.set(17, format!("EXEC-{exec_seq}").as_str()); // ExecID
    er.set(150, 'F'); // ExecType = Trade
    er.set(39, '2'); // OrdStatus = Filled
    er.set(11, get(11).as_str()); // ClOrdID
    er.set(55, get(55).as_str()); // Symbol
    er.set(54, get(54).as_str()); // Side
    er.set(151, 0); // LeavesQty
    er.set(14, qty.as_str()); // CumQty
    er.set(31, "100.00"); // LastPx
    er.set(32, qty.as_str()); // LastQty
    er.set(6, "100.00"); // AvgPx
    er.set(60, UtcTimestamp::now()); // TransactTime
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
         HeartBtInt=30\n",
    )?;

    let (orders_tx, mut orders_rx) = mpsc::unbounded_channel();
    let engine = Engine::start(
        &settings,
        Arc::new(Executor { orders_tx }),
        Arc::new(MemoryStoreFactory),
        Arc::new(TracingLogFactory),
    )
    .await?;

    println!("executor listening on :9876");
    let exec_count = AtomicU64::new(0);
    while let Some((session_id, order)) = orders_rx.recv().await {
        let n = exec_count.fetch_add(1, Ordering::Relaxed) + 1;
        println!(
            "filling order {} for {}",
            order.body.get_string(11).unwrap_or_default(),
            session_id
        );
        if let Some(handle) = engine.session(
            &session_id.begin_string,
            &session_id.sender_comp_id,
            &session_id.target_comp_id,
        ) {
            if let Err(e) = handle.send(fill_for(&order, n)).await {
                eprintln!("failed to send fill: {e}");
            }
        }
    }
    Ok(())
}
