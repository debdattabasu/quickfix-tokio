//! The FIX session layer: logon negotiation, heartbeats, sequence number
//! tracking, resend/gap-fill recovery, and the per-session run loop.
//!
//! Protocol behavior follows QuickFIX C++ (`Session.cpp`); the concurrency
//! model follows quickfix-go: one task owns all session state, connected to
//! the socket tasks and API handles purely by channels.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::application::{Application, ApplicationError};
use crate::datadictionary::DataDictionary;
use crate::error::{Error, RejectError, Result, SessionRejectReason};
use crate::log::Log;
use crate::message::Message;
use crate::session_id::SessionId;
use crate::settings::{ConnectionType, SessionConfig};
use crate::store::MessageStore;
use crate::tags::{self, msg_type};
use crate::value::UtcTimestamp;

/// Commands sent to a session task through its handle.
pub(crate) enum Command {
    /// Queue an application (or custom admin) message for sending.
    Send(Message, oneshot::Sender<Result<()>>),
    /// A transport connection is ready for this session.
    Attach(Connection),
    /// Initiate a graceful logout (stays attached until peer replies/timeout).
    Logout,
    /// Detach and stop the session task.
    Stop(oneshot::Sender<()>),
    Status(oneshot::Sender<SessionStatus>),
    /// Operationally set the next sender/target sequence numbers (each
    /// `Some` is applied). Reply resolves once applied.
    SetSeqNums {
        sender: Option<u64>,
        target: Option<u64>,
        reply: oneshot::Sender<Result<()>>,
    },
}

pub(crate) struct Connection {
    pub inbound: mpsc::Receiver<Bytes>,
    pub outbound: mpsc::Sender<Bytes>,
    /// Dropped (closing the channel) when the session detaches; the
    /// initiator's connect loop uses this to schedule a reconnect.
    pub disconnected: Option<oneshot::Sender<()>>,
}

#[derive(Debug, Clone)]
pub struct SessionStatus {
    pub connected: bool,
    pub logged_on: bool,
    pub next_sender_seq_num: u64,
    pub next_target_seq_num: u64,
}

/// Cheap, cloneable handle to a running session task.
#[derive(Clone)]
pub struct SessionHandle {
    pub id: SessionId,
    pub(crate) cmd_tx: mpsc::Sender<Command>,
}

impl SessionHandle {
    /// Send an application message on this session. Resolves once the
    /// session has accepted (sequenced and persisted) it.
    pub async fn send(&self, msg: Message) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Send(msg, tx))
            .await
            .map_err(|_| Error::UnknownSession(self.id.to_string()))?;
        rx.await.map_err(|_| Error::UnknownSession(self.id.to_string()))?
    }

    pub async fn logout(&self) -> Result<()> {
        self.cmd_tx
            .send(Command::Logout)
            .await
            .map_err(|_| Error::UnknownSession(self.id.to_string()))
    }

    pub async fn status(&self) -> Result<SessionStatus> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Status(tx))
            .await
            .map_err(|_| Error::UnknownSession(self.id.to_string()))?;
        rx.await.map_err(|_| Error::UnknownSession(self.id.to_string()))
    }

    pub async fn is_logged_on(&self) -> bool {
        self.status().await.map(|s| s.logged_on).unwrap_or(false)
    }

    /// Set the next outbound sequence number (operational reset).
    pub async fn set_next_sender_seq_num(&self, n: u64) -> Result<()> {
        self.set_seq_nums(Some(n), None).await
    }

    /// Set the next expected inbound sequence number (operational reset).
    pub async fn set_next_target_seq_num(&self, n: u64) -> Result<()> {
        self.set_seq_nums(None, Some(n)).await
    }

    async fn set_seq_nums(&self, sender: Option<u64>, target: Option<u64>) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SetSeqNums { sender, target, reply })
            .await
            .map_err(|_| Error::UnknownSession(self.id.to_string()))?;
        rx.await.map_err(|_| Error::UnknownSession(self.id.to_string()))?
    }
}

/// Outcome of the verify pipeline for the calling handler.
enum Flow {
    /// Checks passed; the handler should do its work and increment the
    /// target seqnum.
    Continue,
    /// The message was consumed abnormally (rejected, queued, or ignored);
    /// the handler must not process it further.
    Stop,
}

/// A condition that ends the connection immediately.
struct Disconnect(String);

type Handling = std::result::Result<Flow, Disconnect>;

pub(crate) struct Session {
    cfg: SessionConfig,
    store: Box<dyn MessageStore>,
    log: Box<dyn Log>,
    app: Arc<dyn Application>,
    /// Dictionary for app-message validation (transport+app merged on FIXT).
    dictionary: Option<Arc<DataDictionary>>,
    /// Dictionary for admin-message validation (transport-only on FIXT —
    /// app-level fields are unknown tags in admin messages).
    admin_dictionary: Option<Arc<DataDictionary>>,
    cmd_rx: mpsc::Receiver<Command>,

    inbound: Option<mpsc::Receiver<Bytes>>,
    outbound: Option<mpsc::Sender<Bytes>>,
    disconnected_tx: Option<oneshot::Sender<()>>,

    // Session state flags (QuickFIX C++ SessionState).
    received_logon: bool,
    sent_logon: bool,
    sent_logout: bool,
    sent_reset: bool,
    received_reset: bool,
    /// Negotiated heartbeat interval (acceptors adopt the initiator's).
    heart_bt_int: Duration,
    last_sent: Instant,
    last_received: Instant,
    test_request_counter: u32,
    /// Outstanding resend range we asked the peer for:
    /// (begin, full_end, current_chunk_end). With MaxMessagesInResendRequest
    /// the range is requested in chunks; the next chunk goes out when the
    /// current one completes.
    resend_range: Option<(u64, u64, u64)>,
    /// MsgSeqNum of the most recently received message (even if stashed or
    /// rejected) — the value stamped into LastMsgSeqNumProcessed(369).
    last_received_seq: u64,
    /// Raw messages received with too-high seqnums, replayed once the gap
    /// fills.
    stash: BTreeMap<u64, Bytes>,
    /// Whether the most recently processed inbound message was replayed
    /// from the stash — a too-low SequenceReset-GapFill right after a
    /// stash replay is obeyed anyway (QuickFIX/n issue #309).
    last_processed_was_queued: bool,
    /// Negotiate recovery via NextExpectedMsgSeqNum(789) on logon.
    send_next_expected: bool,
    /// When the session is active/resets, and when logons are allowed.
    schedule: crate::schedule::Schedule,
    logon_schedule: crate::schedule::Schedule,
    /// Initiator connected but the logon window was closed; send the logon
    /// once it opens.
    pending_logon: bool,
}

impl Session {
    pub(crate) fn spawn(
        cfg: SessionConfig,
        store: Box<dyn MessageStore>,
        log: Box<dyn Log>,
        app: Arc<dyn Application>,
        dictionary: Option<Arc<DataDictionary>>,
        admin_dictionary: Option<Arc<DataDictionary>>,
    ) -> SessionHandle {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let handle = SessionHandle { id: cfg.session_id.clone(), cmd_tx };
        let heart_bt_int = cfg.heart_bt_int;
        let send_next_expected = cfg.send_next_expected_msg_seq_num;
        let schedule = cfg.schedule.clone();
        let logon_schedule = cfg.logon_schedule.clone();
        let session = Session {
            cfg,
            store,
            log,
            app,
            dictionary,
            admin_dictionary,
            cmd_rx,
            inbound: None,
            outbound: None,
            disconnected_tx: None,
            received_logon: false,
            sent_logon: false,
            sent_logout: false,
            sent_reset: false,
            received_reset: false,
            heart_bt_int,
            last_sent: Instant::now(),
            last_received: Instant::now(),
            test_request_counter: 0,
            resend_range: None,
            last_received_seq: 0,
            stash: BTreeMap::new(),
            last_processed_was_queued: false,
            send_next_expected,
            schedule,
            logon_schedule,
            pending_logon: false,
        };
        tokio::spawn(session.run());
        handle
    }

    fn is_initiator(&self) -> bool {
        self.cfg.connection_type == ConnectionType::Initiator
    }

    fn is_connected(&self) -> bool {
        self.outbound.is_some()
    }

    fn is_logged_on(&self) -> bool {
        self.received_logon && self.sent_logon
    }

    fn event(&mut self, text: &str) {
        self.log.on_event(text);
    }

    // ----- run loop -----

    async fn run(mut self) {
        self.app.on_create(&self.cfg.session_id).await;
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(Command::Send(msg, reply)) => {
                            let res = self.send_message(msg).await;
                            let _ = reply.send(res);
                        }
                        Some(Command::Attach(conn)) => self.on_attach(conn).await,
                        Some(Command::Logout) => {
                            if self.is_logged_on() {
                                let _ = self.initiate_logout("").await;
                            }
                        }
                        Some(Command::Status(reply)) => {
                            let _ = reply.send(SessionStatus {
                                connected: self.is_connected(),
                                logged_on: self.is_logged_on(),
                                next_sender_seq_num: self.store.next_sender_seq_num(),
                                next_target_seq_num: self.store.next_target_seq_num(),
                            });
                        }
                        Some(Command::SetSeqNums { sender, target, reply }) => {
                            let mut res = Ok(());
                            if let Some(n) = sender {
                                res = res.and(self.store.set_next_sender_seq_num(n).await);
                            }
                            if let Some(n) = target {
                                res = res.and(self.store.set_next_target_seq_num(n).await);
                            }
                            let _ = reply.send(res);
                        }
                        Some(Command::Stop(reply)) => {
                            if self.is_logged_on() {
                                let _ = self.initiate_logout("").await;
                                // Give the peer a moment to reply.
                                let deadline = Instant::now() + self.cfg.logout_timeout;
                                while self.is_connected() && Instant::now() < deadline {
                                    match tokio::time::timeout_at(
                                        deadline,
                                        recv_opt(&mut self.inbound),
                                    )
                                    .await
                                    {
                                        Ok(Some(raw)) => self.on_inbound(raw).await,
                                        _ => break,
                                    }
                                }
                            }
                            self.disconnect("session stopped").await;
                            let _ = reply.send(());
                            return;
                        }
                        None => {
                            self.disconnect("engine dropped").await;
                            return;
                        }
                    }
                }
                maybe_raw = recv_opt(&mut self.inbound) => {
                    match maybe_raw {
                        Some(raw) => self.on_inbound(raw).await,
                        None => self.disconnect("connection closed by peer").await,
                    }
                }
                _ = tick.tick() => self.on_timer().await,
            }
        }
    }

    async fn on_attach(&mut self, conn: Connection) {
        if self.is_connected() {
            self.event("Rejecting connection attempt: session already connected");
            // Dropping conn closes its channels and thus the new socket.
            return;
        }
        self.inbound = Some(conn.inbound);
        self.outbound = Some(conn.outbound);
        self.disconnected_tx = conn.disconnected;
        self.last_sent = Instant::now();
        self.last_received = Instant::now();
        self.test_request_counter = 0;
        self.event("Connection established");

        if self.is_initiator() {
            if self.cfg.reset_on_logon {
                let _ = self.store.reset().await;
            }
            if self.cfg.refresh_on_logon {
                let _ = self.store.refresh().await;
            }
            // Only log on inside the logon window; otherwise defer to the
            // timer, which sends it once the window opens.
            if self.logon_schedule.is_in_range(chrono::Utc::now()) {
                if let Err(Disconnect(reason)) = self.send_logon().await {
                    self.disconnect(&reason).await;
                }
            } else {
                self.pending_logon = true;
                self.event("Connected outside logon time; deferring logon");
            }
        }
    }

    async fn disconnect(&mut self, reason: &str) {
        if !self.is_connected() && self.inbound.is_none() {
            return;
        }
        self.event(&format!("Disconnecting: {reason}"));
        let was_logged_on = self.received_logon || self.sent_logon;
        self.inbound = None;
        self.outbound = None;
        self.disconnected_tx = None; // dropping signals the connect loop
        self.received_logon = false;
        self.sent_logon = false;
        self.sent_logout = false;
        self.sent_reset = false;
        self.received_reset = false;
        self.test_request_counter = 0;
        self.resend_range = None;
        self.stash.clear();
        if self.cfg.reset_on_disconnect {
            let _ = self.store.reset().await;
        }
        if was_logged_on {
            self.app.on_logout(&self.cfg.session_id).await;
        }
    }

    // ----- timers (predicates from QuickFIX C++ SessionState) -----

    async fn on_timer(&mut self) {
        // Schedule handling runs even while disconnected (so re-entering the
        // window can reset sequence numbers before the next connection).
        if !self.schedule.is_non_stop() {
            let utc_now = chrono::Utc::now();
            if !self.schedule.is_in_range(utc_now) {
                // Outside session time: don't operate. Log out and drop any
                // connection; the reset happens when we re-enter the window.
                if self.is_connected() {
                    if self.is_logged_on() && !self.sent_logout {
                        let _ = self.initiate_logout("").await;
                    }
                    self.disconnect("Outside session time").await;
                }
                return;
            }
            // In session time. If this instant belongs to a *different*
            // occurrence than the store's creation time, a new session has
            // begun — reset sequence numbers (and restamp creation time).
            if !self.schedule.is_in_same_range(utc_now, self.store.creation_time()) {
                self.event("New session instance; resetting sequence numbers");
                if self.is_connected() {
                    if self.is_logged_on() && !self.sent_logout {
                        let _ = self.initiate_logout("").await;
                    }
                    self.disconnect("Session time boundary crossed").await;
                }
                let _ = self.store.reset().await;
                return;
            }
            // Still in session time but the logon window closed: log out.
            if self.is_connected()
                && self.is_logged_on()
                && !self.sent_logout
                && !self.logon_schedule.is_in_range(utc_now)
            {
                self.event("Logon time expired, initiating logout");
                let _ = self.initiate_logout("").await;
            }
            // Initiator: send a deferred logon once the window opens.
            if self.pending_logon
                && self.is_connected()
                && !self.sent_logon
                && self.logon_schedule.is_in_range(utc_now)
            {
                self.pending_logon = false;
                if let Err(Disconnect(reason)) = self.send_logon().await {
                    self.disconnect(&reason).await;
                    return;
                }
            }
        }

        if !self.is_connected() {
            return;
        }
        let now = Instant::now();
        let since_sent = now.duration_since(self.last_sent);
        let since_recv = now.duration_since(self.last_received);

        // A pending logout times out regardless of logon state (we may have
        // sent Logout in response to a rejected logon attempt).
        if self.sent_logout && since_sent >= self.cfg.logout_timeout {
            self.disconnect("Timed out waiting for logout response").await;
            return;
        }
        if !self.received_logon {
            // Awaiting logon: initiators time out; acceptors give the peer
            // LogonTimeout to identify themselves.
            if since_recv >= self.cfg.logon_timeout {
                self.disconnect("Timed out waiting for logon").await;
            }
            return;
        }
        let hbi = self.heart_bt_int;
        if hbi.is_zero() {
            return;
        }
        if since_sent < hbi && since_recv < hbi {
            return;
        }
        if since_recv >= mul(hbi, 2.4) {
            if self.cfg.send_logout_before_disconnect_from_timeout {
                let _ = self.initiate_logout("").await;
            }
            self.disconnect("Timed out waiting for heartbeat").await;
        } else if since_recv >= mul(hbi, 1.2 * (self.test_request_counter + 1) as f64) {
            self.test_request_counter += 1;
            let mut tr = Message::with_type(msg_type::TEST_REQUEST);
            tr.set(tags::TEST_REQ_ID, "TEST");
            let _ = self.send_message(tr).await;
        } else if since_sent >= hbi && self.test_request_counter == 0 {
            let _ = self.send_message(Message::with_type(msg_type::HEARTBEAT)).await;
        }
    }

    // ----- outbound -----

    /// Fill the standard header: BeginString, CompIDs, MsgSeqNum, SendingTime.
    fn fill_header(&mut self, msg: &mut Message) {
        let id = &self.cfg.session_id;
        msg.header.set(tags::BEGIN_STRING, id.begin_string.as_str());
        msg.header.set(tags::SENDER_COMP_ID, id.sender_comp_id.as_str());
        msg.header.set(tags::TARGET_COMP_ID, id.target_comp_id.as_str());
        if !id.sender_sub_id.is_empty() {
            msg.header.set(tags::SENDER_SUB_ID, id.sender_sub_id.as_str());
        }
        if !id.sender_location_id.is_empty() {
            msg.header.set(tags::SENDER_LOCATION_ID, id.sender_location_id.as_str());
        }
        if !id.target_sub_id.is_empty() {
            msg.header.set(tags::TARGET_SUB_ID, id.target_sub_id.as_str());
        }
        if !id.target_location_id.is_empty() {
            msg.header.set(tags::TARGET_LOCATION_ID, id.target_location_id.as_str());
        }
        msg.header.set(tags::MSG_SEQ_NUM, self.store.next_sender_seq_num());
        if self.cfg.enable_last_msg_seq_num_processed {
            msg.header.set(tags::LAST_MSG_SEQ_NUM_PROCESSED, self.last_received_seq);
        }
        msg.stamp_sending_time(UtcTimestamp::new(
            chrono::Utc::now(),
            self.cfg.timestamp_precision,
        ));
    }

    /// The normal send path: assign seqnum, run callbacks, persist, transmit.
    async fn send_message(&mut self, mut msg: Message) -> Result<()> {
        // A fresh send is never a possible duplicate (C++ Session::send).
        msg.header.remove(tags::POSS_DUP_FLAG);
        msg.header.remove(tags::ORIG_SENDING_TIME);
        self.fill_header(&mut msg);
        let is_admin = msg.is_admin();
        let mt = msg.msg_type().unwrap_or_default();
        if is_admin {
            self.app.to_admin(&mut msg, &self.cfg.session_id).await;
        } else if self.app.to_app(&mut msg, &self.cfg.session_id).await.is_err() {
            return Err(Error::DoNotSend);
        }
        if let Some(dd) = &self.dictionary {
            dd.canonicalize_body(&mut msg);
        }
        let seq = msg.seq_num()?;
        let raw = msg.to_bytes();
        if self.cfg.persist_messages {
            self.store.save_message_and_incr(seq, &raw).await?;
        } else {
            self.store.incr_next_sender_seq_num().await?;
        }
        // Session-control admin messages always go out; everything else only
        // once logged on (unsent app messages are recovered via resend).
        let always = matches!(
            mt.as_str(),
            msg_type::LOGON | msg_type::LOGOUT | msg_type::RESEND_REQUEST | msg_type::SEQUENCE_RESET
        );
        if self.is_logged_on() || always || self.sent_logon {
            self.transmit(raw.into()).await;
        }
        Ok(())
    }

    /// Push raw bytes to the write task, bypassing sequencing (used for
    /// resent messages and gap fills as well as normal sends).
    async fn transmit(&mut self, raw: Bytes) {
        self.log.on_outgoing(&raw);
        if let Some(out) = &self.outbound {
            if out.send(raw).await.is_err() {
                self.disconnect("write side closed").await;
                return;
            }
            self.last_sent = Instant::now();
        }
    }

    async fn send_logon(&mut self) -> std::result::Result<(), Disconnect> {
        let mut logon = Message::with_type(msg_type::LOGON);
        logon.set(tags::ENCRYPT_METHOD, 0);
        logon.set(tags::HEART_BT_INT, self.heart_bt_int.as_secs());
        let should_reset = self.cfg.send_reset_seq_num_flag
            || (self.cfg.session_id.begin_string.as_str() >= "FIX.4.1"
                && self.cfg.reset_on_logon
                && self.store.next_sender_seq_num() == 1
                && self.store.next_target_seq_num() == 1);
        if should_reset {
            logon.set(tags::RESET_SEQ_NUM_FLAG, true);
            self.sent_reset = true;
        }
        if let Some(v) = &self.cfg.default_appl_ver_id {
            logon.set(tags::DEFAULT_APPL_VER_ID, appl_ver_id_enum(v));
        }
        // A fresh initiating logon reports the next seqnum we expect to
        // receive (C++ generateLogon uses getExpectedTargetNum(), no +1 —
        // we haven't received the peer's logon yet).
        if self.send_next_expected {
            logon.set(tags::NEXT_EXPECTED_MSG_SEQ_NUM, self.store.next_target_seq_num());
        }
        self.sent_logon = true;
        self.event("Initiated logon request");
        self.send_message(logon)
            .await
            .map_err(|e| Disconnect(format!("failed to send logon: {e}")))
    }

    async fn send_logon_reply(&mut self, peer_hbi: Option<u64>) -> Result<()> {
        if let Some(secs) = peer_hbi {
            self.heart_bt_int = self
                .cfg
                .heart_bt_int_override
                .unwrap_or(Duration::from_secs(secs));
        }
        let mut logon = Message::with_type(msg_type::LOGON);
        logon.set(tags::ENCRYPT_METHOD, 0);
        logon.set(tags::HEART_BT_INT, self.heart_bt_int.as_secs());
        // The reply echoes ResetSeqNumFlag(141) only in 789 mode: C++/go
        // echo it, QuickFIX/n does not (whose SessionReset acceptance test
        // expects no 141). Gating on send_next_expected keeps the default
        // QuickFIX/n-compatible while matching C++ when 789 is enabled.
        if self.received_reset {
            if self.send_next_expected {
                logon.set(tags::RESET_SEQ_NUM_FLAG, true);
            }
            self.sent_reset = true;
        }
        if let Some(v) = &self.cfg.default_appl_ver_id {
            logon.set(tags::DEFAULT_APPL_VER_ID, appl_ver_id_enum(v));
        }
        // The reply reports next-target + 1: the incoming logon we're
        // replying to has not incremented the target seqnum yet (C++
        // generateLogon(reply)).
        if self.send_next_expected {
            logon.set(tags::NEXT_EXPECTED_MSG_SEQ_NUM, self.store.next_target_seq_num() + 1);
        }
        self.sent_logon = true;
        self.event("Responding to logon request");
        self.send_message(logon).await
    }

    async fn initiate_logout(&mut self, reason: &str) -> Result<()> {
        let mut logout = Message::with_type(msg_type::LOGOUT);
        if !reason.is_empty() {
            logout.set(tags::TEXT, reason);
            self.event(&format!("Initiated logout: {reason}"));
        } else {
            self.event("Initiated logout request");
        }
        self.sent_logout = true;
        self.send_message(logout).await
    }

    /// Session-level Reject (35=3). Consumes the offending seqnum only when
    /// the offender was in sequence and is not a Logon or SequenceReset
    /// (C++ `generateReject`).
    async fn send_reject(&mut self, offender: &Message, rej: &RejectError) -> Result<()> {
        let mt = offender.msg_type().unwrap_or_default();
        if mt != msg_type::LOGON
            && mt != msg_type::SEQUENCE_RESET
            && offender.seq_num().ok() == Some(self.store.next_target_seq_num())
        {
            self.store.incr_next_target_seq_num().await?;
        }
        let fix42_plus = self.cfg.session_id.begin_string.as_str() >= "FIX.4.2"
            || self.cfg.session_id.is_fixt();
        // Body fields in ascending tag order (canonical form).
        let mut reject = Message::with_type(msg_type::REJECT);
        reverse_route(offender, &mut reject, &self.cfg.session_id.begin_string);
        if let Ok(seq) = offender.seq_num() {
            reject.set(tags::REF_SEQ_NUM, seq);
        }
        // Pre-4.2 has no RefTagID(371): the offending tag rides in Text(58)
        // as "reason (tag)" instead.
        let text = match rej.ref_tag {
            Some(tag) if !fix42_plus && rej.text.is_none() => format!("{rej} ({tag})"),
            _ => rej.to_string(),
        };
        reject.set(tags::TEXT, text.as_str());
        if fix42_plus {
            if let Some(tag) = rej.ref_tag {
                reject.set(tags::REF_TAG_ID, tag);
            }
            if let Ok(mt) = offender.msg_type() {
                reject.set(tags::REF_MSG_TYPE, mt.as_str());
            }
            // FIX.4.2's SessionRejectReason enum stops at 11; higher codes
            // exist only from FIX.4.3 on and are omitted before that.
            let code = rej.reason.code();
            if code <= 11 || self.cfg.session_id.begin_string.as_str() > "FIX.4.2" {
                reject.set(tags::SESSION_REJECT_REASON, code);
            }
        }
        self.event(&format!("Message rejected: {rej}"));
        self.send_message(reject).await
    }

    async fn send_business_reject(&mut self, offender: &Message) -> Result<()> {
        self.store.incr_next_target_seq_num().await?;
        let fix42_plus = self.cfg.session_id.begin_string.as_str() >= "FIX.4.2"
            || self.cfg.session_id.is_fixt();
        // Body fields in ascending tag order (canonical form). The casing
        // difference is QuickFIX/n-faithful: BusinessMessageReject says
        // "Unsupported Message Type", the pre-4.2 Reject fallback says
        // "Unsupported message type".
        let (mut reject, text) = if fix42_plus {
            (Message::with_type(msg_type::BUSINESS_MESSAGE_REJECT), "Unsupported Message Type")
        } else {
            (Message::with_type(msg_type::REJECT), "Unsupported message type")
        };
        reverse_route(offender, &mut reject, &self.cfg.session_id.begin_string);
        if let Ok(seq) = offender.seq_num() {
            reject.set(tags::REF_SEQ_NUM, seq);
        }
        reject.set(tags::TEXT, text);
        if fix42_plus {
            if let Ok(mt) = offender.msg_type() {
                reject.set(tags::REF_MSG_TYPE, mt.as_str());
            }
            reject.set(tags::BUSINESS_REJECT_REASON, 3u32); // unsupported message type
        }
        self.send_message(reject).await
    }

    async fn send_resend_request(&mut self, begin: u64, received: u64) -> Result<()> {
        let full_end = received - 1;
        let mut rr = Message::with_type(msg_type::RESEND_REQUEST);
        rr.set(tags::BEGIN_SEQ_NO, begin);
        let chunk_end = match self.cfg.max_messages_in_resend_request {
            // "To infinity": 0 for FIX >= 4.2, 999999 before.
            0 => {
                let open_ended = self.cfg.session_id.begin_string.as_str() >= "FIX.4.2"
                    || self.cfg.session_id.is_fixt();
                rr.set(tags::END_SEQ_NO, if open_ended { 0 } else { 999999u64 });
                full_end
            }
            max => {
                let chunk_end = full_end.min(begin + max - 1);
                rr.set(tags::END_SEQ_NO, chunk_end);
                chunk_end
            }
        };
        self.resend_range = Some((begin, full_end, chunk_end));
        self.event(&format!("Sent ResendRequest FROM: {begin} TO: {chunk_end}"));
        self.send_message(rr).await
    }

    /// After inbound processing: request the next chunk of an outstanding
    /// resend range, or note that it has been satisfied.
    async fn check_resend_chunks(&mut self) {
        let Some((_, full_end, chunk_end)) = self.resend_range else { return };
        let expected = self.store.next_target_seq_num();
        if expected <= chunk_end {
            return;
        }
        if expected <= full_end {
            let _ = self.send_resend_request(expected, full_end + 1).await;
        } else {
            self.event(&format!("ResendRequest for messages FROM ... TO {full_end} has been satisfied"));
            self.resend_range = None;
        }
    }

    /// SequenceReset-GapFill covering `[begin, new_seq)`. Bypasses normal
    /// sequencing: MsgSeqNum is the start of the gap being filled.
    async fn send_gap_fill(&mut self, begin: u64, new_seq: u64) {
        let mut m = Message::with_type(msg_type::SEQUENCE_RESET);
        self.fill_header(&mut m);
        m.header.set(tags::MSG_SEQ_NUM, begin);
        m.header.set(tags::POSS_DUP_FLAG, true);
        let now = UtcTimestamp::new(chrono::Utc::now(), self.cfg.timestamp_precision);
        m.header.set(tags::ORIG_SENDING_TIME, now);
        m.set(tags::NEW_SEQ_NO, new_seq);
        m.set(tags::GAP_FILL_FLAG, true);
        self.app.to_admin(&mut m, &self.cfg.session_id).await;
        self.event(&format!("Sent SequenceReset (GapFill) {begin} -> {new_seq}"));
        let raw = m.to_bytes();
        self.transmit(raw.into()).await;
    }

    // ----- inbound -----

    async fn on_inbound(&mut self, raw: Bytes) {
        self.log.on_incoming(&raw);
        let msg = match Message::parse(&raw, self.cfg.validate_length_checksum) {
            Ok(m) => m,
            Err(e) => {
                // Garbled: ignore, do not increment, do not reject.
                self.event(&format!("Invalid message: {e}"));
                // A garbled logon is fatal (mirrors the C++ engine).
                if !self.received_logon && contains_field(&raw, b"35=A") {
                    self.disconnect("garbled logon").await;
                }
                return;
            }
        };
        if let Ok(seq) = msg.seq_num() {
            self.last_received_seq = seq;
        }
        if let Err(Disconnect(reason)) = self.process(msg, raw).await {
            self.disconnect(&reason).await;
            return;
        }
        self.last_processed_was_queued = false;
        if let Err(Disconnect(reason)) = self.drain_stash().await {
            self.disconnect(&reason).await;
            return;
        }
        self.check_resend_chunks().await;
    }

    /// Handle one parsed message (without stash draining).
    async fn process(&mut self, msg: Message, raw: Bytes) -> std::result::Result<(), Disconnect> {
        let mt = match msg.msg_type() {
            Ok(mt) => mt,
            Err(_) => {
                self.event("Message without MsgType ignored");
                return Ok(());
            }
        };
        // BeginString mismatch: incorrect version is fatal.
        if msg.header.get_raw(tags::BEGIN_STRING)
            != Some(self.cfg.session_id.begin_string.as_bytes())
        {
            let got = msg
                .header
                .get_raw(tags::BEGIN_STRING)
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_default();
            let _ = self.store.incr_next_target_seq_num().await;
            let _ = self.initiate_logout(&format!("Incorrect BeginString ({got})")).await;
            return Err(Disconnect("incorrect BeginString".into()));
        }

        // Dictionary validation of all messages, admin included, before
        // dispatch (mirrors the C++ engine's ordering). Failures consume
        // the seqnum. FIXT admin messages validate against the transport
        // dictionary alone.
        let dd = if msg.is_admin() { &self.admin_dictionary } else { &self.dictionary };
        if let Some(dd) = dd.clone() {
            if let Err(rej) = dd.validate(&msg, &self.cfg.validation) {
                let _ = self.send_reject(&msg, &rej).await;
                return Ok(());
            }
        }

        match mt.as_str() {
            msg_type::LOGON => self.handle_logon(&msg).await,
            msg_type::HEARTBEAT | msg_type::REJECT => self.handle_plain_admin(&msg).await,
            msg_type::TEST_REQUEST => self.handle_test_request(&msg).await,
            msg_type::RESEND_REQUEST => self.handle_resend_request(&msg).await,
            msg_type::SEQUENCE_RESET => self.handle_sequence_reset(&msg).await,
            msg_type::LOGOUT => self.handle_logout(&msg).await,
            _ => self.handle_app_message(&msg, raw).await,
        }
    }

    // ----- verify pipeline (C++ Session::verify) -----

    async fn verify(
        &mut self,
        msg: &Message,
        check_too_high: bool,
        check_too_low: bool,
    ) -> Handling {
        let mt = msg.msg_type().unwrap_or_default();

        if !self.valid_logon_state(&mt) {
            return Err(Disconnect(format!("logon state invalid for message type {mt}")));
        }

        // SendingTime present and within the latency window.
        match msg.header.get_opt::<UtcTimestamp>(tags::SENDING_TIME) {
            Ok(Some(st)) => {
                if self.cfg.check_latency {
                    let delta = (chrono::Utc::now() - st.time).abs();
                    if delta.num_seconds().unsigned_abs()
                        > self.cfg.max_latency.as_secs()
                    {
                        let _ = self
                            .send_reject(
                                msg,
                                &RejectError::new(
                                    SessionRejectReason::SendingTimeAccuracyProblem,
                                ),
                            )
                            .await;
                        let _ = self.initiate_logout("").await;
                        return Ok(Flow::Stop);
                    }
                }
            }
            _ => {
                let _ = self
                    .send_reject(
                        msg,
                        &RejectError::with_tag(
                            SessionRejectReason::RequiredTagMissing,
                            tags::SENDING_TIME,
                        ),
                    )
                    .await;
                return Ok(Flow::Stop);
            }
        }

        // CompIDs must be the mirror image of ours.
        if self.cfg.check_comp_id {
            let sender = msg.header.get_raw(tags::SENDER_COMP_ID).unwrap_or_default();
            let target = msg.header.get_raw(tags::TARGET_COMP_ID).unwrap_or_default();
            if sender != self.cfg.session_id.target_comp_id.as_bytes()
                || target != self.cfg.session_id.sender_comp_id.as_bytes()
            {
                let _ = self
                    .send_reject(msg, &RejectError::new(SessionRejectReason::CompIDProblem))
                    .await;
                let _ = self.initiate_logout("").await;
                return Ok(Flow::Stop);
            }
        }

        // Sequence number checks.
        if check_too_high || check_too_low {
            let Ok(seq) = msg.seq_num() else {
                let _ = self
                    .send_reject(
                        msg,
                        &RejectError::with_tag(
                            SessionRejectReason::RequiredTagMissing,
                            tags::MSG_SEQ_NUM,
                        ),
                    )
                    .await;
                return Ok(Flow::Stop);
            };
            let expected = self.store.next_target_seq_num();
            if check_too_high && seq > expected {
                self.on_target_too_high(msg, seq, expected).await?;
                return Ok(Flow::Stop);
            }
            if check_too_low && seq < expected {
                // A too-low SequenceReset-GapFill straight after a stash
                // replay is obeyed anyway (QuickFIX/n issue #309).
                let obey_anyway = self.last_processed_was_queued
                    && mt == msg_type::SEQUENCE_RESET
                    && msg.body.get_raw(tags::GAP_FILL_FLAG) == Some(b"Y");
                if obey_anyway {
                    self.event(&format!(
                        "SequenceReset-GapFill {seq} is too low (expected {expected}), obeying it anyway"
                    ));
                } else {
                    return self.on_target_too_low(msg, seq, expected).await;
                }
            }
        }

        self.last_received = Instant::now();
        self.test_request_counter = 0;

        // Application callbacks.
        let cb = if msg.is_admin() {
            self.app.from_admin(msg, &self.cfg.session_id).await
        } else {
            self.app.from_app(msg, &self.cfg.session_id).await
        };
        match cb {
            Ok(()) => Ok(Flow::Continue),
            Err(ApplicationError::RejectLogon(reason)) => {
                let _ = self.store.incr_next_target_seq_num().await;
                let _ = self.initiate_logout(&reason).await;
                Err(Disconnect(format!("logon rejected by application: {reason}")))
            }
            Err(ApplicationError::Reject(rej)) => {
                let _ = self.send_reject(msg, &rej).await;
                Ok(Flow::Stop)
            }
            Err(ApplicationError::UnsupportedMessageType) => {
                let _ = self.send_business_reject(msg).await;
                Ok(Flow::Stop)
            }
        }
    }

    /// Which message types are acceptable in the current logon state
    /// (C++ `validLogonState`).
    fn valid_logon_state(&self, mt: &str) -> bool {
        (mt == msg_type::LOGON && (self.sent_reset || self.received_reset))
            || (mt == msg_type::LOGON && !self.received_logon)
            || (mt != msg_type::LOGON && self.received_logon)
            || (mt == msg_type::LOGOUT && self.sent_logon)
            || (mt != msg_type::LOGOUT && self.sent_logout)
            || mt == msg_type::SEQUENCE_RESET
            || mt == msg_type::REJECT
    }

    async fn on_target_too_high(
        &mut self,
        msg: &Message,
        seq: u64,
        expected: u64,
    ) -> std::result::Result<(), Disconnect> {
        self.event(&format!("MsgSeqNum too high, expecting {expected} but received {seq}"));
        // Stash for replay once the gap fills.
        self.stash.insert(seq, Bytes::from(msg.to_bytes()));
        let suppress = self.resend_range.is_some() && !self.cfg.send_redundant_resend_requests;
        if !suppress {
            self.send_resend_request(expected, seq)
                .await
                .map_err(|e| Disconnect(format!("failed to send ResendRequest: {e}")))?;
        }
        Ok(())
    }

    async fn on_target_too_low(&mut self, msg: &Message, seq: u64, expected: u64) -> Handling {
        if !msg.poss_dup() {
            let reason =
                format!("MsgSeqNum too low, expecting {expected} but received {seq}");
            let _ = self.initiate_logout(&reason).await;
            return Err(Disconnect(reason));
        }
        // PossDup replay of something we already have: validate and drop.
        let mt = msg.msg_type().unwrap_or_default();
        if mt != msg_type::SEQUENCE_RESET && self.cfg.requires_orig_sending_time {
            match msg.header.get_opt::<UtcTimestamp>(tags::ORIG_SENDING_TIME) {
                Ok(Some(orig)) => {
                    let sending = msg.header.get_opt::<UtcTimestamp>(tags::SENDING_TIME).ok().flatten();
                    if let Some(st) = sending {
                        if orig.time > st.time {
                            let _ = self
                                .send_reject(
                                    msg,
                                    &RejectError::new(
                                        SessionRejectReason::SendingTimeAccuracyProblem,
                                    ),
                                )
                                .await;
                            let _ = self.initiate_logout("").await;
                            return Ok(Flow::Stop);
                        }
                    }
                }
                _ => {
                    let _ = self
                        .send_reject(
                            msg,
                            &RejectError::with_tag(
                                SessionRejectReason::RequiredTagMissing,
                                tags::ORIG_SENDING_TIME,
                            ),
                        )
                        .await;
                    return Ok(Flow::Stop);
                }
            }
        }
        self.event(&format!("Already received message {seq}, ignoring PossDup"));
        Ok(Flow::Stop)
    }

    // ----- handlers -----

    async fn handle_logon(&mut self, msg: &Message) -> std::result::Result<(), Disconnect> {
        // A logon with a bad SendingTime is dropped with a silent disconnect
        // (no Reject/Logout), per the reference engines.
        if self.cfg.check_latency {
            let good = msg
                .header
                .get_opt::<UtcTimestamp>(tags::SENDING_TIME)
                .ok()
                .flatten()
                .is_some_and(|st| {
                    (chrono::Utc::now() - st.time).abs().num_seconds().unsigned_abs()
                        <= self.cfg.max_latency.as_secs()
                });
            if !good {
                return Err(Disconnect("logon has bad sending time".into()));
            }
        }
        // ResetSeqNumFlag(141)=Y from the peer.
        if msg.body.get_raw(tags::RESET_SEQ_NUM_FLAG) == Some(b"Y") {
            self.received_reset = true;
            if !self.sent_reset {
                self.event("Logon contains ResetSeqNumFlag=Y, resetting sequence numbers to 1");
                self.store
                    .reset()
                    .await
                    .map_err(|e| Disconnect(format!("store reset failed: {e}")))?;
            }
        }
        // A logon response we never asked for.
        if self.is_initiator() && !self.sent_logon {
            return Err(Disconnect("received logon response before sending request".into()));
        }
        if !self.is_initiator() {
            if self.cfg.refresh_on_logon {
                let _ = self.store.refresh().await;
            }
            if self.cfg.reset_on_logon && !self.received_reset {
                let _ = self.store.reset().await;
            }
        }

        match self.verify(msg, false, true).await? {
            Flow::Stop => return Ok(()),
            Flow::Continue => {}
        }
        self.received_logon = true;

        // NextExpectedMsgSeqNum(789): the peer tells us the next seqnum it
        // expects from us. Read it before replying (C++ nextLogon).
        let mut retransmit_from: Option<u64> = None;
        if self.send_next_expected {
            if let Ok(Some(peer_789)) = msg.body.get_opt::<u64>(tags::NEXT_EXPECTED_MSG_SEQ_NUM) {
                let next_sender = self.store.next_sender_seq_num();
                if peer_789 > next_sender {
                    // The peer expects messages we never sent — unrecoverable.
                    let reason = format!(
                        "Tag 789 (NextExpectedMsgSeqNum) is higher than expected. Expected {next_sender}, Received {peer_789}"
                    );
                    let _ = self.initiate_logout(&reason).await;
                    return Err(Disconnect(reason));
                } else if peer_789 < next_sender {
                    // The peer is behind; retransmit the gap after logon.
                    retransmit_from = Some(peer_789);
                }
            }
        }

        if !self.is_initiator() {
            let peer_hbi = msg.body.get_opt::<u64>(tags::HEART_BT_INT).ok().flatten();
            self.send_logon_reply(peer_hbi)
                .await
                .map_err(|e| Disconnect(format!("failed to send logon reply: {e}")))?;
        } else {
            self.event("Received logon response");
        }
        self.sent_reset = false;
        self.received_reset = false;

        // Seqnum-too-high on the logon itself is handled after replying.
        let seq = msg.seq_num().map_err(|_| Disconnect("logon missing MsgSeqNum".into()))?;
        let expected = self.store.next_target_seq_num();
        let reset = msg.body.get_raw(tags::RESET_SEQ_NUM_FLAG) == Some(b"Y");
        if seq > expected {
            if self.send_next_expected && !reset {
                // In 789 mode we do not send a ResendRequest for the peer's
                // gap: we already told the peer (via our reply's 789) what we
                // expect, and rely on it to retransmit. Just stash and record
                // the range (C++ nextLogon).
                self.event(&format!(
                    "Expecting retransmits FROM: {expected} TO: {}",
                    seq - 1
                ));
                self.stash.insert(seq, Bytes::from(msg.to_bytes()));
                self.resend_range = Some((expected, seq - 1, seq - 1));
            } else {
                self.on_target_too_high(msg, seq, expected).await?;
            }
        } else {
            self.store
                .incr_next_target_seq_num()
                .await
                .map_err(|e| Disconnect(format!("store error: {e}")))?;
        }

        if self.is_logged_on() {
            self.event("Logon successful");
            self.app.on_logon(&self.cfg.session_id).await;
        }

        // Finally, replay the gap the peer is missing (its 789 was too low).
        if let Some(begin) = retransmit_from {
            let end = self.store.next_sender_seq_num() - 1;
            self.event(&format!(
                "Sending retransmits due to received NextExpectedMsgSeqNum too low. FROM: {begin} TO: {end}"
            ));
            self.answer_resend(begin, end).await;
        }
        Ok(())
    }

    /// Heartbeat and Reject share the trivial handling: verify + increment.
    async fn handle_plain_admin(&mut self, msg: &Message) -> std::result::Result<(), Disconnect> {
        if let Flow::Continue = self.verify(msg, true, true).await? {
            self.incr_target().await?;
        }
        Ok(())
    }

    async fn handle_test_request(&mut self, msg: &Message) -> std::result::Result<(), Disconnect> {
        if let Flow::Continue = self.verify(msg, true, true).await? {
            let mut hb = Message::with_type(msg_type::HEARTBEAT);
            if let Some(id) = msg.body.get_raw(tags::TEST_REQ_ID) {
                hb.body.set_raw(tags::TEST_REQ_ID, id.to_vec());
            }
            let _ = self.send_message(hb).await;
            self.incr_target().await?;
        }
        Ok(())
    }

    async fn handle_resend_request(&mut self, msg: &Message) -> std::result::Result<(), Disconnect> {
        if let Flow::Stop = self.verify(msg, false, false).await? {
            return Ok(());
        }
        let begin: u64 = msg.body.get_opt(tags::BEGIN_SEQ_NO).ok().flatten().unwrap_or(1);
        let mut end: u64 = msg.body.get_opt(tags::END_SEQ_NO).ok().flatten().unwrap_or(0);
        let next_sender = self.store.next_sender_seq_num();
        if end == 0 || end == 999999 || end >= next_sender {
            end = next_sender - 1;
        }
        self.event(&format!("Received ResendRequest FROM: {begin} TO: {end}"));
        self.answer_resend(begin, end).await;

        // The ResendRequest consumes its own seqnum only if it was in
        // sequence (it may itself arrive during a two-way gap).
        if let Ok(seq) = msg.seq_num() {
            if seq == self.store.next_target_seq_num() {
                self.incr_target().await?;
            }
        }
        Ok(())
    }

    /// Answer a resend range `[begin, end]`: replay persisted messages, or
    /// gap-fill the whole range when persistence is off. Shared by the
    /// ResendRequest handler and the 789 retransmit-after-logon path.
    async fn answer_resend(&mut self, begin: u64, end: u64) {
        if begin > end {
            return;
        }
        if self.cfg.persist_messages {
            self.retransmit(begin, end).await;
        } else {
            self.send_gap_fill(begin, end + 1).await;
        }
    }

    /// Replay stored messages `[begin, end]`: app messages re-sent with
    /// PossDupFlag=Y, admin messages and gaps folded into GapFills.
    async fn retransmit(&mut self, begin: u64, end: u64) {
        let stored: BTreeMap<u64, Vec<u8>> = match self.store.get_messages(begin, end).await {
            Ok(v) => v.into_iter().collect(),
            Err(e) => {
                self.event(&format!("Store error during resend: {e}"));
                return;
            }
        };
        let now = UtcTimestamp::new(chrono::Utc::now(), self.cfg.timestamp_precision);
        let mut gap_begin: Option<u64> = None;
        for seq in begin..=end {
            let resend = stored.get(&seq).and_then(|raw| Message::parse(raw, false).ok());
            match resend {
                Some(mut m) if !m.is_admin() => {
                    m.header.set(tags::POSS_DUP_FLAG, true);
                    if let Some(orig) = m.header.get_raw(tags::SENDING_TIME).map(|v| v.to_vec()) {
                        m.header.set_raw(tags::ORIG_SENDING_TIME, orig);
                    }
                    m.stamp_sending_time(now);
                    if self.app.to_app(&mut m, &self.cfg.session_id).await.is_err() {
                        gap_begin.get_or_insert(seq);
                        continue;
                    }
                    if let Some(gb) = gap_begin.take() {
                        self.send_gap_fill(gb, seq).await;
                    }
                    self.event(&format!("Resending message {seq}"));
                    let raw = m.to_bytes();
                    self.transmit(raw.into()).await;
                }
                // Admin messages are never resent; missing seqnums gap-fill.
                _ => {
                    gap_begin.get_or_insert(seq);
                }
            }
        }
        if let Some(gb) = gap_begin.take() {
            self.send_gap_fill(gb, end + 1).await;
        }
    }

    async fn handle_sequence_reset(&mut self, msg: &Message) -> std::result::Result<(), Disconnect> {
        let gap_fill = msg.body.get_raw(tags::GAP_FILL_FLAG) == Some(b"Y");
        // Reset-Reset (GapFill=N) bypasses all seqnum checks.
        if let Flow::Stop = self.verify(msg, gap_fill, gap_fill).await? {
            return Ok(());
        }
        if let Ok(Some(new_seq)) = msg.body.get_opt::<u64>(tags::NEW_SEQ_NO) {
            let expected = self.store.next_target_seq_num();
            self.event(&format!("Received SequenceReset (GapFill={}) NewSeqNo={new_seq}",
                if gap_fill { "Y" } else { "N" }));
            if new_seq > expected {
                self.store
                    .set_next_target_seq_num(new_seq)
                    .await
                    .map_err(|e| Disconnect(format!("store error: {e}")))?;
                // Anything stashed below the new seqnum is superseded.
                self.stash.retain(|&k, _| k >= new_seq);
            } else if new_seq < expected {
                let _ = self
                    .send_reject(msg, &RejectError::new(SessionRejectReason::ValueIsIncorrect))
                    .await;
            }
        }
        Ok(())
    }

    async fn handle_logout(&mut self, msg: &Message) -> std::result::Result<(), Disconnect> {
        if let Flow::Stop = self.verify(msg, false, false).await? {
            return Ok(());
        }
        if self.sent_logout {
            self.event("Received logout response");
        } else {
            self.event("Received logout request");
            let _ = self.initiate_logout("").await;
        }
        self.incr_target().await?;
        if self.cfg.reset_on_logout {
            let _ = self.store.reset().await;
        }
        Err(Disconnect("logout complete".into()))
    }

    async fn handle_app_message(
        &mut self,
        msg: &Message,
        _raw: Bytes,
    ) -> std::result::Result<(), Disconnect> {
        if let Flow::Continue = self.verify(msg, true, true).await? {
            self.incr_target().await?;
        }
        Ok(())
    }

    async fn incr_target(&mut self) -> std::result::Result<(), Disconnect> {
        self.store
            .incr_next_target_seq_num()
            .await
            .map_err(|e| Disconnect(format!("store error: {e}")))
    }

    /// Replay stashed too-high messages that are now in sequence.
    async fn drain_stash(&mut self) -> std::result::Result<(), Disconnect> {
        loop {
            let expected = self.store.next_target_seq_num();
            let Some(raw) = self.stash.remove(&expected) else { return Ok(()) };
            self.event(&format!("Processing queued message: {expected}"));
            self.last_processed_was_queued = true;
            // Queued Logon/ResendRequest only advance the seqnum
            // (mirrors C++ nextQueued).
            if contains_field(&raw, b"35=A") || contains_field(&raw, b"35=2") {
                self.incr_target().await?;
                continue;
            }
            match Message::parse(&raw, false) {
                Ok(msg) => self.process(msg, raw).await?,
                Err(_) => self.incr_target().await?,
            }
        }
    }
}

async fn recv_opt(rx: &mut Option<mpsc::Receiver<Bytes>>) -> Option<Bytes> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

fn mul(d: Duration, f: f64) -> Duration {
    Duration::from_secs_f64(d.as_secs_f64() * f)
}

/// DefaultApplVerID(1137) rides on the wire as the ApplVerID enum, not the
/// BeginString-style name configured in settings.
fn appl_ver_id_enum(configured: &str) -> &str {
    match configured {
        "FIX.2.7" => "0",
        "FIX.3.0" => "1",
        "FIX.4.0" => "2",
        "FIX.4.1" => "3",
        "FIX.4.2" => "4",
        "FIX.4.3" => "5",
        "FIX.4.4" => "6",
        "FIX.5.0" => "7",
        "FIX.5.0SP1" => "8",
        "FIX.5.0SP2" => "9",
        other => other, // already an enum value
    }
}

/// Copy routing fields from a received message into a reply, reversed:
/// OnBehalfOf(115/116/144) becomes DeliverTo(128/129/145) and vice versa.
/// Empty values are not propagated.
fn reverse_route(offender: &Message, reply: &mut Message, begin_string: &str) {
    const PAIRS: [(crate::message::Tag, crate::message::Tag); 6] = [
        (tags::ON_BEHALF_OF_COMP_ID, tags::DELIVER_TO_COMP_ID),
        (tags::ON_BEHALF_OF_SUB_ID, tags::DELIVER_TO_SUB_ID),
        (tags::ON_BEHALF_OF_LOCATION_ID, tags::DELIVER_TO_LOCATION_ID),
        (tags::DELIVER_TO_COMP_ID, tags::ON_BEHALF_OF_COMP_ID),
        (tags::DELIVER_TO_SUB_ID, tags::ON_BEHALF_OF_SUB_ID),
        (tags::DELIVER_TO_LOCATION_ID, tags::ON_BEHALF_OF_LOCATION_ID),
    ];
    // Location IDs (144/145) only exist from FIX 4.1 on.
    let has_location_ids = begin_string >= "FIX.4.1";
    for (src, dst) in PAIRS {
        if !has_location_ids
            && matches!(src, tags::ON_BEHALF_OF_LOCATION_ID | tags::DELIVER_TO_LOCATION_ID)
        {
            continue;
        }
        if let Some(v) = offender.header.get_raw(src) {
            if !v.is_empty() {
                reply.header.set_raw(dst, v.to_vec());
            }
        }
    }
}

/// Whether `raw` contains `<SOH>field` (or starts with `field`).
fn contains_field(raw: &[u8], field: &[u8]) -> bool {
    if raw.starts_with(field) {
        return true;
    }
    let mut needle = Vec::with_capacity(field.len() + 1);
    needle.push(crate::message::SOH);
    needle.extend_from_slice(field);
    raw.windows(needle.len()).any(|w| w == needle)
}
