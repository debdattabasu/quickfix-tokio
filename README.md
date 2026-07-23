# quickfix-tokio

[![Crates.io](https://img.shields.io/crates/v/quickfix-tokio.svg)](https://crates.io/crates/quickfix-tokio)
[![Documentation](https://docs.rs/quickfix-tokio/badge.svg)](https://docs.rs/quickfix-tokio)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A pure-Rust FIX protocol engine built natively on tokio. No C++ bindings, no
background threads, no `unsafe` — every session is a single tokio task wired
to its socket and to your code purely by channels.

Protocol behavior is ported from the three reference QuickFIX engines:
[QuickFIX C++](https://github.com/quickfix/quickfix) (canonical session
rules), [quickfix-go](https://github.com/quickfixgo/quickfix) (the
concurrency blueprint — one owner per session), and
[QuickFIX/n](https://github.com/connamara/quickfixn) (acceptance test
format).

> **Status:** `0.1.0`. The protocol core is exercised by the full QuickFIX
> acceptance suite (see below) and the API is usable today, but this is a
> young library — the surface may still shift before `1.0`. Requires Rust
> 1.85+ (edition 2024).

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
        Arc::new(MemoryStoreFactory::new()), // or FileStoreFactory::new("store")
        Arc::new(TracingLogFactory),         // or FileLogFactory / NullLogFactory
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
# Schedule (optional): active 08:00–17:00 UTC, resets daily at the boundary
# StartTime=08:00:00
# EndTime=17:00:00
# TLS (optional): verify the server against a pinned CA
# SocketUseSSL=Y
# SocketCAFile=ca.pem
# SocketServerName=exec.example.com
```

For a TLS acceptor set `SocketUseSSL=Y` with `SocketCertificateFile` and
`SocketPrivateKeyFile`; add `SocketCAFile` to require and verify client
certificates (mutual TLS).

### Examples

Two-process (run the executor first, then the client in another terminal):

- `executor` / `order_client` — a sell-side acceptor that fills a
  NewOrderSingle, and a buy-side initiator that sends one and prints the fill.
  These use the `Application` **callback trait**.
- `events_executor` / `events_client` — the same pair rewritten with
  [`event_channel()`](#two-ways-to-drive-the-engine), the **channel surface**.
  The executor drains events with *no `Application` impl at all*; the client
  drives orders and fills from one `select!` loop.

Self-contained (both sides in one process, just `cargo run --example <name>`):

- `market_data` — typed **repeating groups**: a subscriber sends a
  MarketDataRequest (symbol + wanted entry types as groups); a publisher
  answers with a snapshot carrying a NoMDEntries group.
- `auth_and_risk` — a tour of the `Application` **callback surface**:
  `to_admin` stamps Username/Password onto the outgoing Logon, `from_admin`
  checks them (and can veto with `RejectLogon`), and `from_app` enforces a
  risk limit, rejecting oversized orders with a session-level `Reject`.
- `streaming_client` — fires ten orders without waiting on each, then
  reconciles the ExecutionReports as they stream back, matching fills to
  orders by ClOrdID.

## Typed messages

The `fix44` module (on by default via the `fix44` cargo feature) provides
typed messages generated from `spec/FIX44.xml` — 92 message types, 953 field
markers with enum constants, and repeating-group structs:

```rust
use quickfix_tokio::fix44::{classify, fields, AnyMessage};
use quickfix_tokio::fix44::messages::new_order_single::NewOrderSingle;

let mut order = NewOrderSingle::new(
    "ORDER-1", fields::Side::BUY, UtcTimestamp::now(), fields::OrdType::LIMIT,
);
order.set_symbol("TSLA");
order.set_order_qty(dec!(100));      // Amount is exact decimal by default
order.set_price(dec!(101.25));
session.send(order.into()).await?;

// Inbound dispatch:
match classify(msg.clone()) {
    AnyMessage::NewOrderSingle(order) => println!("{}", order.cl_ord_id()?),
    AnyMessage::ExecutionReport(er) => println!("{}", er.avg_px()?),
    _ => {}
}
```

Constructors take the message's required fields; every field gets
`x()`/`set_x()`/`has_x()` accessors typed per the dictionary (ints as `i64`,
prices/quantities/amounts as `Amount`, timestamps as `UtcTimestamp`, enums
as constants like `fields::Side::BUY`). `Amount` is exact fixed-point
[`rust_decimal::Decimal`](https://docs.rs/rust_decimal) by default (the
`decimal` feature) — so `dec!(0.1) + dec!(0.2)` is exactly `0.3` and a wire
value like `1000.50` round-trips with its scale intact — or `f64` with the
feature off. Repeating groups are structs with the same accessor pattern
(`order.set_no_party_ids([...])`). Message structs `Deref` to
[`Message`](src/message.rs) for anything not covered.

The generator is part of the crate: `cargo run --bin generate-fix --
spec/FIX42.xml src/fix42` regenerates or targets another FIX version.
Generated code is committed; re-run only when specs change.

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
  peer-death detection (2.4× HeartBtInt) fire at exact deadlines
  (`sleep_until`, recomputed each iteration) in the same `select!`; a
  separate 1 s tick drives schedule (session-window) checks. No timer
  threads. This is quickfix-go's event-driven timer model.
- **Sockets are dumb.** The read task frames bytes (`8=` resync, BodyLength
  jump, `10=` check — the classic parser) and forwards complete messages;
  the write task drains an outbound channel. Disconnects propagate as channel
  closures in both directions.
- **Callbacks are async and run on the session task**, so a slow `from_app`
  applies backpressure to exactly that session. Don't `await` the same
  session's handle inside its own callback (deadlock) — forward to another
  task, as the executor example shows.

### Two ways to drive the engine

The `Application` trait is the canonical surface — the seven async callbacks
every QuickFIX user already knows (`from_app`, `to_app`, `on_logon`…). It's
the right tool for the **decision hooks**: `to_app`→`DoNotSend`,
`from_admin`→`RejectLogon`, `to_admin` credential-stamping all need a
synchronous verdict the engine waits on, which is exactly an awaited method.

For everything else, `event_channel()` is the tokio-native alternative. It
returns an `Application` to hand to `Engine::start` plus an
`mpsc::UnboundedReceiver<SessionEvent>` you drain from your own task or
`select!` loop:

```rust
let (app, mut events) = quickfix_tokio::event_channel();
let engine = Engine::start(&settings, Arc::new(app), store, log).await?;
let session = engine.session("FIX.4.4", "CLIENT", "SERVER").unwrap();

loop {
    tokio::select! {
        Some(ev) = events.recv() => match ev {
            SessionEvent::LoggedOn(_)   => session.send(order()).await?,
            SessionEvent::App(msg, _)   => handle(msg), // your logic, off the protocol task
            _ => {}
        },
        cmd = strategy.recv() => session.send(cmd?.into()).await?,
    }
}
```

This retires both callback footguns: the channel is unbounded so forwarding
never stalls the protocol task, and inbound events plus outbound
`SessionHandle` sends share one loop with no reentrancy hazard. It's
**notify-only** — it can't carry the decision hooks above, so for vetoes you
implement the trait. The client case is where it shines: a client is a
send-and-react loop, which one `select!` expresses directly instead of
fragmenting across `from_app` and a spawned sender task.

Modules: `message`/`field_map`/`value` (wire model — order-preserving, so
repeating groups round-trip byte-exactly without Go's raw-body splicing
workaround), `parser` (stream framing), `session` (state machine + run
loop), `transport` (acceptor/initiator/socket tasks), `engine` (wiring),
`store` (memory + file persistence), `log`, `settings`,
`datadictionary` (XML specs + validation).

## Acceptance suite

The engine passes the **classic QuickFIX acceptance test suite** — ~500
protocol-conformance scripts across seventeen fixtures: FIX 4.0 through 4.4,
FIXT.1.1 with FIX 5.0/5.0SP1/5.0SP2, the no-reset FIX 4.4 variant, the misc
suite (LastMsgSeqNumProcessed(369), chunked ResendRequests, sub/location ID
routing, logout-before-timeout-disconnect), the CME enhanced-resend suite,
the NextExpectedMsgSeqNum(789) suite (in-sync / peer-ahead-disconnect /
peer-behind-implied-resend / 141+789 reset), plus two ported from quickfix
C++: `validate` (per-session ValidateFieldsHaveValues toggle) and `client`
(initiator-driven — the harness listens and the engine dials in). These are
the same `.def` scripts
the reference engines certify with (vendored from QuickFIX/n and quickfix
C++ into `acceptance/definitions/`). The runner
([tests/acceptance.rs](tests/acceptance.rs)) is a Rust port of QuickFIX/n's
Runner/ReflectorClient: it drives a raw TCP client (or several) against a
live engine, with `<TIME±n>` decoration, automatic BodyLength/CheckSum
insertion, and byte-for-byte positional matching of every engine response.
Run with `cargo test --test acceptance`. The only defs not run are
`future/` and `misc/broken/`, which QuickFIX/n also parks as known-failing.

Conformance details this suite locked in: canonical field ordering (header
8,9,35 then ascending; bodies ascending with repeating-group blocks intact),
version-specific reject shapes (pre-4.2 puts the offending tag in Text(58),
4.2 caps SessionRejectReason at 11, only 4.2 cites RefTagID on invalid
MsgType), reverse routing (115/116/144 ↔ 128/129/145) on rejects, CHAR→STRING
degradation for pre-4.2 dictionaries, C++-style tolerant framing (a lying
BodyLength still frames, then fails validation and is ignored as garbled),
silent disconnect on a bad-SendingTime logon, XMLnonFIX (35=n) as an admin
type, and the QuickFIX/n issue-309 rule (obey a too-low SequenceReset-GapFill
right after a queue replay).

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
- Pluggable message stores (the `MessageStoreFactory` trait — DB backends
  drop in the same way): a bounded-capacity memory store
  (`MemoryStoreFactory::with_capacity`) and a file store (QuickFIX C++-style
  layout) with opt-in `fsync` durability (`FileStoreFactory::with_sync`,
  offloaded to a blocking thread); tracing/null logs and a file log with
  size-based rotation + backup retention (`FileLogFactory::with_rotation`);
  classic INI settings
- FIXT.1.1 sessions: Transport/AppDataDictionary split (admin messages
  validate against the transport dictionary alone), DefaultApplVerID(1137)
  enum mapping
- LastMsgSeqNumProcessed(369), chunked ResendRequests
  (`MaxMessagesInResendRequest`), `RequiresOrigSendingTime=N`,
  `SendLogoutBeforeDisconnectFromTimeout`, sub/location ID identities
- `NextExpectedMsgSeqNum(789)` logon-handshake recovery
  (`SendNextExpectedMsgSeqNum=Y`, default off, C++ semantics) — folds gap
  recovery into logon; required by some venues (e.g. CME)
- TLS via rustls (`SocketUseSSL=Y`; `tls` cargo feature, on by default):
  acceptor certs, initiator server verification against `SocketCAFile` or
  `SocketInsecureSkipVerify=Y`, mutual TLS with a client cert
- Session schedules (`StartTime`/`EndTime`, weekly `StartDay`/`EndDay`,
  `NonStopSession`, `UseLocalTime`, separate `LogonTime`/`LogoutTime`):
  sequence numbers reset on the daily/weekly boundary, logons are gated to
  the window, and the session logs out when it closes — C++ `TimeRange`
  semantics including overnight and weekly windows
- Exact fixed-point decimal price/qty/amount fields via `rust_decimal`
  (`decimal` feature, on by default; `f64` without it)
- FIX 4.0–4.4 and FIXT.1.1, ephemeral + persistent sessions

## Not yet implemented

- SQL / Mongo message stores (the `MessageStoreFactory` trait is ready for
  them; they'd be added as new factory impls for HA/failover, not durability)

## Tests

`cargo test` runs unit tests plus integration tests that drive real engines
over loopback TCP: logon/exchange/logout, heartbeat keepalive, TestRequest
answering, dictionary rejects, seqnum-too-low logout, and a full
gap → ResendRequest → GapFill → stash-replay recovery.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
