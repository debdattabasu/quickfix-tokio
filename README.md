# quickfix-tokio

A pure-Rust FIX protocol engine built natively on tokio. No C++ bindings, no
background threads — every session is a single tokio task wired to its socket
and to your code purely by channels.

Protocol behavior is ported from the reference QuickFIX engines vendored in
`reference/`: [QuickFIX C++](reference/quickfix-cpp) (canonical session
rules), [quickfix-go](reference/quickfix-go) (the concurrency blueprint:
one owner per session), and [QuickFIX/n](reference/quickfixn) (acceptance
test format).

## Quick start

```rust
use std::sync::Arc;
use quickfix_tokio::*;

struct MyApp;

#[async_trait::async_trait]
impl Application for MyApp {
    async fn from_app(&self, msg: &Message, _id: &SessionId) -> Result<(), ApplicationError> {
        println!("got {}", msg.msg_type().unwrap());
        Ok(())
    }
}

#[tokio::main]
async fn main() -> quickfix_tokio::Result<()> {
    let settings = Settings::from_file("fix.cfg").await?;
    let engine = Engine::start(
        &settings,
        Arc::new(MyApp),
        Arc::new(MemoryStoreFactory),      // or FileStoreFactory::new("store")
        Arc::new(TracingLogFactory),       // or FileLogFactory / NullLogFactory
    ).await?;

    let session = engine.session("FIX.4.4", "CLIENT", "EXECUTOR").unwrap();
    let mut order = Message::with_type("D");
    order.set(11, "ORDER-1");
    order.set(55, "TSLA");
    order.set(54, '1');
    order.set(40, '1');
    order.set(60, UtcTimestamp::now());
    session.send(order).await?;
    Ok(())
}
```

Config files use the classic QuickFIX INI format (`[DEFAULT]`/`[SESSION]`,
same key names), so existing configs carry over:

```ini
[DEFAULT]
ConnectionType=initiator
ReconnectInterval=5

[SESSION]
BeginString=FIX.4.4
SenderCompID=CLIENT
TargetCompID=EXECUTOR
SocketConnectHost=127.0.0.1
SocketConnectPort=9876
HeartBtInt=30
DataDictionary=spec/FIX44.xml
```

Try it: `cargo run --example executor` then `cargo run --example order_client`.

## Architecture

```
                 ┌──────────────────────────────────────────────┐
                 │              session task (one per session)  │
 SessionHandle ──┼─ cmd channel ─▶ run loop ── owns ──▶ state   │
                 │                 (select!)      seqnums, store,│
 read task ──────┼─ inbound ─────▶    │           log, timers,  │
 (socket ▶ frame)│                    ▼           resend stash  │
 write task ◀────┼─ outbound ──── handlers ──▶ Application      │
 (bytes ▶ socket)│                              callbacks       │
                 └──────────────────────────────────────────────┘
```

- **One task owns everything.** All session state — sequence numbers, logon
  flags, timers, the resend stash, the message store — lives inside one tokio
  task. There are no locks and no `Mutex<Session>`; the socket tasks and the
  `SessionHandle` talk to it over channels. This is quickfix-go's
  one-goroutine-per-session model, minus the goroutine-side mutexes.
- **Timers are part of the loop.** Heartbeat generation, TestRequest
  escalation (1.2×, 2.4×… of HeartBtInt), logon/logout timeouts, and
  peer-death detection (2.4× HeartBtInt) are evaluated on a 100 ms tick of
  the same `select!` — no timer threads.
- **Sockets are dumb.** The read task frames bytes (`8=` resync, BodyLength
  jump, `10=` check — the classic parser) and forwards complete messages;
  the write task drains an outbound channel. Disconnects propagate as channel
  closures in both directions.
- **Callbacks are async and run on the session task**, so a slow `from_app`
  applies backpressure to exactly that session. Don't `await` the same
  session's handle inside its own callback (deadlock) — forward to another
  task, as the executor example shows.

Modules: `message`/`field_map`/`value` (wire model — order-preserving, so
repeating groups round-trip byte-exactly without Go's raw-body splicing
workaround), `parser` (stream framing), `session` (state machine + run
loop), `transport` (acceptor/initiator/socket tasks), `engine` (wiring),
`store` (memory + file persistence), `log`, `settings`,
`datadictionary` (XML specs + validation).

## What works today

- Logon negotiation incl. ResetSeqNumFlag(141), acceptor HeartBtInt adoption,
  logon veto via `ApplicationError::RejectLogon`
- Heartbeats, TestRequest escalation and timeout disconnects
- Sequence tracking with the full recovery protocol: too-high stash +
  ResendRequest, GapFill/Reset handling, PossDup rules (OrigSendingTime
  checks), too-low → logout, resend answering with PossDup regeneration and
  admin-message gap-fill
- Session-level Reject and BusinessMessageReject generation
- Data dictionary validation (required fields, field formats, enums, unknown
  tags, group counts, out-of-order detection) from stock QuickFIX XML specs
- Memory and file-backed stores (QuickFIX C++-style file layout), file and
  tracing logs, classic INI settings
- FIX 4.0–4.4 and FIXT.1.1 headers/trailers, ephemeral + persistent sessions

## Not yet implemented

- Session schedules (`StartTime`/`EndTime`, weekly windows) — sessions are
  non-stop; connect/disconnect is driven by the engine lifecycle
- `NextExpectedMsgSeqNum(789)` logon sync, `ResendRequestChunkSize`
- TLS, per-message fsync durability, SQL/Mongo stores
- Typed message/field codegen from the XML specs (the `spec/` files and the
  dictionary parser are the input for it)
- The QuickFIX acceptance-test runner (`.def` scripts) — the format is
  documented in `reference/quickfixn/AcceptanceTest`

## Tests

`cargo test` runs unit tests plus integration tests that drive real engines
over loopback TCP: logon/exchange/logout, heartbeat keepalive, TestRequest
answering, dictionary rejects, seqnum-too-low logout, and a full
gap → ResendRequest → GapFill → stash-replay recovery.
