//! The executor example, rewritten with `event_channel()` — note there is
//! **no `Application` impl at all**. Every session event streams to one loop
//! that replies via `SessionHandle`. Because the adapter's channel is
//! unbounded, forwarding a `from_app` never blocks the session task, so
//! replying to the *same* session from this loop can't deadlock.
//!
//! Compare with `examples/executor.rs`, which implements the trait and hops
//! orders to a worker task to dodge callback reentrancy. Here that hop is the
//! architecture: the loop *is* the worker.
//!
//! Run with: `cargo run --example events_executor`

use std::sync::Arc;

use quickfix_tokio::fix44::messages::execution_report::ExecutionReport;
use quickfix_tokio::fix44::messages::new_order_single::NewOrderSingle;
use quickfix_tokio::fix44::{AnyMessage, classify, fields};
use quickfix_tokio::{
    Engine, MemoryStoreFactory, SessionEvent, Settings, TracingLogFactory, UtcTimestamp,
    dec, event_channel,
};

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

    let (app, mut events) = event_channel();
    let engine = Engine::start(
        &settings,
        Arc::new(app),
        Arc::new(MemoryStoreFactory::new()),
        Arc::new(TracingLogFactory),
    )
    .await?;

    println!("executor listening on :9876");
    let mut exec_count = 0u64;
    while let Some(event) = events.recv().await {
        match event {
            SessionEvent::LoggedOn(id) => println!("logged on: {id}"),
            SessionEvent::LoggedOut(id) => println!("logged out: {id}"),
            SessionEvent::App(msg, id) => {
                let AnyMessage::NewOrderSingle(order) = classify(msg) else { continue };
                exec_count += 1;
                println!(
                    "filling {} x {} for {}",
                    order.order_qty().unwrap_or(dec!(0)),
                    order.symbol().unwrap_or_default(),
                    order.cl_ord_id().unwrap_or_default(),
                );
                if let Some(handle) =
                    engine.session(&id.begin_string, &id.sender_comp_id, &id.target_comp_id)
                {
                    if let Err(e) = handle.send(fill_for(&order, exec_count).into()).await {
                        eprintln!("failed to send fill: {e}");
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}
