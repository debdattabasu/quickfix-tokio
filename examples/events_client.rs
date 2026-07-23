//! A buy-side client driven by `event_channel()` in a single `select!` loop —
//! the shape the channel adapter unlocks. One arm reacts to inbound events
//! (execution reports), the other drives outbound orders from a timer (stand
//! in for your strategy: a command channel, market data, stdin...). Both
//! halves share local state with no cross-task plumbing.
//!
//! With the `Application` trait this logic has to split in two: you receive
//! fills in `from_app`, but you can't send from there (can't await your own
//! `SessionHandle` inside a callback), so order submission has to live in a
//! separate spawned task. Here it's one coherent loop.
//!
//! Run `cargo run --example events_executor` first, then
//! `cargo run --example events_client`.

use std::sync::Arc;
use std::time::Duration;

use quickfix_tokio::fix44::messages::new_order_single::NewOrderSingle;
use quickfix_tokio::fix44::{AnyMessage, classify, fields};
use quickfix_tokio::{
    Engine, MemoryStoreFactory, SessionEvent, Settings, TracingLogFactory, UtcTimestamp,
    dec, event_channel,
};

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

    let (app, mut events) = event_channel();
    let engine = Engine::start(
        &settings,
        Arc::new(app),
        Arc::new(MemoryStoreFactory::new()),
        Arc::new(TracingLogFactory),
    )
    .await?;
    let session = engine.session("FIX.4.4", "CLIENT", "EXECUTOR").unwrap();

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    const CAP: u64 = 3;
    let mut sent = 0u64;
    let mut filled = 0u64;

    loop {
        tokio::select! {
            // Inbound: things that happened on the wire.
            maybe = events.recv() => {
                let Some(event) = maybe else { break }; // engine stopped
                match event {
                    SessionEvent::LoggedOn(id) => println!("logged on: {id}"),
                    SessionEvent::LoggedOut(_) => break,
                    SessionEvent::App(msg, _) => {
                        if let AnyMessage::ExecutionReport(er) = classify(msg) {
                            filled += 1;
                            println!(
                                "fill #{filled}: {} {} x {} @ {} ({})",
                                er.cl_ord_id().unwrap_or_default(),
                                er.cum_qty().unwrap_or(dec!(0)),
                                er.symbol().unwrap_or_default(),
                                er.avg_px().unwrap_or(dec!(0)),
                                er.ord_status().map(|c| c.to_string()).unwrap_or_default(),
                            );
                            if filled >= CAP {
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }
            // Outbound: your own strategy source (here, a timer).
            _ = ticker.tick() => {
                if sent < CAP && session.is_logged_on().await {
                    sent += 1;
                    let mut order = NewOrderSingle::new(
                        format!("ORDER-{sent}"),
                        fields::Side::BUY,
                        UtcTimestamp::now(),
                        fields::OrdType::LIMIT,
                    );
                    order.set_symbol("TSLA");
                    order.set_order_qty(dec!(100));
                    order.set_price(dec!(101.25));
                    session.send(order.into()).await?;
                    println!("sent ORDER-{sent}");
                }
            }
        }
    }

    session.logout().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    engine.stop().await;
    Ok(())
}
