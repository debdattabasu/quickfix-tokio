//! The application callback interface — the async equivalent of the seven
//! classic QuickFIX callbacks.

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::RejectError;
use crate::message::Message;
use crate::session_id::SessionId;

/// Returned by `from_admin` / `from_app` to influence session behavior.
#[derive(Debug)]
pub enum ApplicationError {
    /// Veto a counterparty Logon: the session logs out and disconnects.
    RejectLogon(String),
    /// Emit a session-level Reject (35=3) for this message.
    Reject(RejectError),
    /// Emit a BusinessMessageReject (35=j) with BusinessRejectReason(380)=3.
    UnsupportedMessageType,
}

/// Returned by `to_app` to veto an outgoing (or resent) message.
#[derive(Debug)]
pub struct DoNotSend;

/// Implemented by users of the engine. All callbacks run on the session's
/// task: a slow callback delays that session (only), exactly like the
/// single-threaded session model of the reference engines.
///
/// Because callbacks run on the session task, do not `await` a
/// [`crate::SessionHandle`] operation for the *same* session inside a
/// callback — forward work to another task instead (see the executor
/// example).
#[async_trait]
#[allow(clippy::wrong_self_convention)] // from_admin/from_app are the canonical QuickFIX names
pub trait Application: Send + Sync + 'static {
    /// A session was created at engine start.
    async fn on_create(&self, _session_id: &SessionId) {}

    /// The session completed a logon exchange.
    async fn on_logon(&self, _session_id: &SessionId) {}

    /// The session went offline (logout or disconnect).
    async fn on_logout(&self, _session_id: &SessionId) {}

    /// An admin message is about to be sent; last chance to mutate it
    /// (e.g. add credentials to a Logon).
    async fn to_admin(&self, _msg: &mut Message, _session_id: &SessionId) {}

    /// An application message is about to be sent (also called for resends,
    /// with PossDupFlag=Y). Return `Err(DoNotSend)` to suppress it — a resend
    /// is then gap-filled.
    async fn to_app(
        &self,
        _msg: &mut Message,
        _session_id: &SessionId,
    ) -> Result<(), DoNotSend> {
        Ok(())
    }

    /// An admin message was received and verified.
    async fn from_admin(
        &self,
        _msg: &Message,
        _session_id: &SessionId,
    ) -> Result<(), ApplicationError> {
        Ok(())
    }

    /// An application message was received and verified. This is the main
    /// inbound entry point for business messages.
    async fn from_app(
        &self,
        _msg: &Message,
        _session_id: &SessionId,
    ) -> Result<(), ApplicationError> {
        Ok(())
    }
}

// ----- channel adapter -----

/// A protocol event surfaced by [`event_channel`]. Every variant carries the
/// [`SessionId`] it happened on, so one receiver can serve many sessions —
/// match on the id to demultiplex.
#[derive(Debug)]
pub enum SessionEvent {
    /// A session was created at engine start ([`Application::on_create`]).
    Created(SessionId),
    /// A logon exchange completed ([`Application::on_logon`]).
    LoggedOn(SessionId),
    /// The session went offline ([`Application::on_logout`]).
    LoggedOut(SessionId),
    /// An application (business) message arrived ([`Application::from_app`]).
    App(Message, SessionId),
    /// An admin message arrived ([`Application::from_admin`]) — Heartbeat,
    /// TestRequest, Reject, etc. Usually ignored; here for observability.
    Admin(Message, SessionId),
}

/// The [`Application`] half of [`event_channel`]: it forwards the
/// *notification* callbacks onto the channel and accepts everything else with
/// the trait defaults. Construct it only via [`event_channel`].
pub struct ChannelApplication {
    tx: mpsc::UnboundedSender<SessionEvent>,
}

/// Bridge the callback interface to a tokio channel: returns an
/// [`Application`] to hand to [`crate::Engine::start`], plus a receiver you
/// drain from your own task or `select!` loop. This is the tokio-native
/// alternative to implementing [`Application`] by hand — inbound events and
/// your outbound [`crate::SessionHandle`] sends live in one place, with no
/// callback reentrancy hazard.
///
/// The channel is **unbounded on purpose**: pushing an event never blocks the
/// session task, so a slow consumer never stalls the protocol (heartbeats,
/// gap-fills). The cost is that a consumer which stops draining entirely will
/// grow memory — that's a consumer bug, since inbound rate is paced by one
/// socket.
///
/// This adapter is **notify-only**. It cannot carry the *decision* hooks —
/// [`Application::to_app`]/[`DoNotSend`], [`Application::from_admin`] ->
/// [`ApplicationError::RejectLogon`], or [`Application::to_admin`] mutation —
/// because those need a synchronous verdict the engine waits on, which a
/// fire-and-forward channel has nowhere to return. Implement [`Application`]
/// directly if you need to veto messages, reject logons, or stamp
/// credentials.
pub fn event_channel() -> (ChannelApplication, mpsc::UnboundedReceiver<SessionEvent>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (ChannelApplication { tx }, rx)
}

#[async_trait]
impl Application for ChannelApplication {
    async fn on_create(&self, session_id: &SessionId) {
        let _ = self.tx.send(SessionEvent::Created(session_id.clone()));
    }
    async fn on_logon(&self, session_id: &SessionId) {
        let _ = self.tx.send(SessionEvent::LoggedOn(session_id.clone()));
    }
    async fn on_logout(&self, session_id: &SessionId) {
        let _ = self.tx.send(SessionEvent::LoggedOut(session_id.clone()));
    }
    async fn from_admin(
        &self,
        msg: &Message,
        session_id: &SessionId,
    ) -> Result<(), ApplicationError> {
        let _ = self.tx.send(SessionEvent::Admin(msg.clone(), session_id.clone()));
        Ok(())
    }
    async fn from_app(
        &self,
        msg: &Message,
        session_id: &SessionId,
    ) -> Result<(), ApplicationError> {
        let _ = self.tx.send(SessionEvent::App(msg.clone(), session_id.clone()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn event_channel_forwards_notifications() {
        let (app, mut rx) = event_channel();
        let sid = SessionId::new("FIX.4.4", "ME", "YOU");

        app.on_logon(&sid).await;
        let mut msg = Message::default();
        msg.set(35, "D"); // NewOrderSingle
        app.from_app(&msg, &sid).await.unwrap();
        app.on_logout(&sid).await;

        assert!(matches!(rx.recv().await, Some(SessionEvent::LoggedOn(id)) if id == sid));
        assert!(matches!(rx.recv().await, Some(SessionEvent::App(_, id)) if id == sid));
        assert!(matches!(rx.recv().await, Some(SessionEvent::LoggedOut(id)) if id == sid));
    }

    #[tokio::test]
    async fn send_never_blocks_and_survives_dropped_receiver() {
        let (app, rx) = event_channel();
        let sid = SessionId::new("FIX.4.4", "ME", "YOU");
        drop(rx); // consumer went away
        // Forwarding must not panic or block even with no receiver.
        app.on_logon(&sid).await;
        app.from_app(&Message::default(), &sid).await.unwrap();
    }
}
