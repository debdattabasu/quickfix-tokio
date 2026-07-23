//! The application callback interface — the async equivalent of the seven
//! classic QuickFIX callbacks.

use async_trait::async_trait;

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
