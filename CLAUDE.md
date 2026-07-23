# CLAUDE.md

Guidance for Claude Code (and other agents) working in this repository.

## What this is

`quickfix-tokio` is a pure-Rust FIX (Financial Information eXchange) protocol
engine built natively on tokio — no C++ bindings, no blocking threads, no
`unsafe`. Protocol behavior is ported from three reference engines; the
concurrency model is quickfix-go's (one task owns a session), the session
rules are quickfix-cpp's, and the test harness is QuickFIX/n's.

## Commands

```bash
cargo build                       # library
cargo build --examples            # library + all examples
cargo test                        # everything (unit + integration + acceptance + doc)
cargo test --lib                  # unit tests only (fast)
cargo test --test acceptance      # the QuickFIX conformance suite (slow, ~100s)
cargo test --test integration     # loopback end-to-end tests
cargo clippy --all-targets        # must stay clean — no warnings
cargo run --example events_client # run an example (see README for the set)
```

Regenerate typed messages after editing a spec or the generator:

```bash
cargo run --bin generate-fix -- spec/FIX44.xml src/fix44
```

Generated code under `src/fix44/` is committed; only regenerate when
`spec/*.xml` or `src/bin/generate-fix.rs` changes.

## The reference engines

`reference/` holds the three upstream implementations (gitignored, not part of
the published crate, present locally for study):

- `reference/quickfix-cpp` — **canonical** session rules. When behavior is
  ambiguous, this is the tie-breaker. It's the most mature.
- `reference/quickfix-go` — the concurrency blueprint (one goroutine per
  session, channels not locks) and the event-driven timer model.
- `reference/quickfixn` — the C#/.NET engine; source of the `.def` acceptance
  scripts and their runner semantics.

**When implementing or changing protocol behavior, read what the references do
first** and match it — cpp wins ties. Note the divergences already discovered
(e.g. go echoes `141=Y` on SequenceReset, quickfixn does not; we gate on
`SendNextExpectedMsgSeqNum`).

## Architecture — invariants to preserve

The core model is **one tokio task per session that owns all its state**
(sequence numbers, logon flags, timers, resend stash, message store). Keep it
that way:

- **No locks, no `Mutex<Session>`.** The socket read/write tasks and the
  public `SessionHandle` communicate with the session task purely over
  channels. If you reach for a `Mutex` around session state, reconsider.
- **Persist-then-send.** A message is written to the store (and fsync'd if the
  store opts into level-3 durability) *before* the sequence number advances,
  so any message can always be resent. Don't reorder this.
- **Timers live in the `select!` loop** (`src/session.rs`): protocol deadlines
  (heartbeat, TestRequest escalation at 1.2×/2.4× HeartBtInt, logon/logout
  timeouts, peer death at 2.4×) fire at exact `sleep_until` instants recomputed
  each iteration; a separate 1 s tick drives schedule-window checks. No timer
  threads, no polling for protocol events.
- **Canonical field ordering** is load-bearing for conformance: header
  `8,9,35` then ascending; body ascending with repeating-group blocks kept
  intact. The wire model (`field_map`) is order-preserving on purpose.
- **`#![forbid(unsafe_code)]`** — the crate is 100% safe Rust. Keep it so.

### Module map

- `message` / `field_map` / `value` — wire model (order-preserving) + typed
  field encode/decode
- `parser` — stream framing (`8=` resync, BodyLength jump, `10=` checksum)
- `session` — the state machine and run loop (the heart; largest file)
- `schedule` — `TimeRange` session windows (daily/overnight/weekly)
- `transport` — acceptor/initiator + socket read/write tasks
- `tls` — rustls transport (feature `tls`)
- `engine` — wiring: parses settings, builds sessions, owns handles
- `store` — `MessageStore` + factories (memory, file); DB backends plug in via
  the `MessageStoreFactory` trait (not yet implemented)
- `log` — `Log` + factories (tracing, null, rotating file)
- `datadictionary` — XML spec parsing + message validation
- `settings` — classic QuickFIX INI config
- `application` — the `Application` callback trait **and** the `event_channel`
  adapter
- `bin/generate-fix.rs` — the typed-message code generator

## Two API surfaces (don't collapse them)

1. **`Application` trait** — the canonical callback surface every QuickFIX user
   knows. It is the right tool for the **decision hooks** that need a
   synchronous verdict the engine waits on: `to_app`→`DoNotSend`,
   `from_admin`→`RejectLogon`/`Reject`, `to_admin` mutation. Callbacks are
   `async` and run on the session task — a slow one backpressures *that*
   session. **Never `await` the same session's `SessionHandle` inside its own
   callback** (deadlock); forward to another task.

2. **`event_channel()`** — the tokio-native alternative for the
   **notification** path (`on_logon`/`on_logout`/`from_app`-consume). Returns
   an opaque `Application` + an `mpsc::UnboundedReceiver<SessionEvent>` to
   drive from a `select!` loop. Unbounded on purpose (forwarding never stalls
   the protocol task). It is **notify-only** — it cannot carry the decision
   hooks. If you extend it, preserve both properties.

## Conventions

- **The acceptance suite is the safety net.** `acceptance/definitions/*.def`
  are the same conformance scripts the reference engines certify with. Any
  protocol change must keep `cargo test --test acceptance` green. `future/`
  and `misc/broken/` are intentionally parked (QuickFIX/n parks them too) —
  don't try to make them pass.
- **Feature flags:** `fix44` (typed messages), `tls` (rustls), `decimal`
  (exact `rust_decimal::Decimal` for price/qty/amount fields) — all on by
  default. `crate::Amount` aliases `Decimal` with `decimal`, else `f64`.
  Typed accessors take/return `Amount`, so example and test code uses the
  `dec!()` macro, not `f64` literals, under the default feature set.
- **Comments** explain *why* / cite the reference, not *what*. Match the
  density and idiom of the surrounding file. Reference-derived behavior often
  carries a short note pointing at the source engine.
- **Store/log configuration** lives on the factories (builder-style:
  `FileStoreFactory::with_sync`, `MemoryStoreFactory::with_capacity`,
  `FileLogFactory::with_rotation`), not in the INI file — it's the user's code
  choosing the implementation.
- Run `cargo clippy --all-targets` and `cargo test` before considering a
  change done. Commit only when the user asks.

## Not yet implemented

- SQL / Mongo message stores. The `MessageStoreFactory` trait is shaped to
  accept them as new factory impls; they're an HA/failover feature, not a
  durability gap (file-store level-2/3 already covers durability).
