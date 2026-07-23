//! Session logging: raw wire traffic and human-readable events.

use std::io::Write;
use std::path::PathBuf;

use crate::error::Result;
use crate::session_id::SessionId;

pub trait Log: Send {
    fn on_incoming(&mut self, raw: &[u8]);
    fn on_outgoing(&mut self, raw: &[u8]);
    fn on_event(&mut self, event: &str);
}

pub trait LogFactory: Send + Sync {
    fn create(&self, session_id: &SessionId) -> Result<Box<dyn Log>>;
}

// ----- null -----

#[derive(Debug, Default)]
pub struct NullLog;

impl Log for NullLog {
    fn on_incoming(&mut self, _raw: &[u8]) {}
    fn on_outgoing(&mut self, _raw: &[u8]) {}
    fn on_event(&mut self, _event: &str) {}
}

#[derive(Debug, Default)]
pub struct NullLogFactory;

impl LogFactory for NullLogFactory {
    fn create(&self, _session_id: &SessionId) -> Result<Box<dyn Log>> {
        Ok(Box::new(NullLog))
    }
}

// ----- tracing (screen) -----

/// Emits traffic and events through the `tracing` crate, with SOH rendered
/// as `|` for readability.
pub struct TracingLog {
    session_id: String,
}

fn printable(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).replace('\x01', "|")
}

impl Log for TracingLog {
    fn on_incoming(&mut self, raw: &[u8]) {
        tracing::debug!(session = %self.session_id, "<- {}", printable(raw));
    }
    fn on_outgoing(&mut self, raw: &[u8]) {
        tracing::debug!(session = %self.session_id, "-> {}", printable(raw));
    }
    fn on_event(&mut self, event: &str) {
        tracing::info!(session = %self.session_id, "{event}");
    }
}

#[derive(Debug, Default)]
pub struct TracingLogFactory;

impl LogFactory for TracingLogFactory {
    fn create(&self, session_id: &SessionId) -> Result<Box<dyn Log>> {
        Ok(Box::new(TracingLog { session_id: session_id.to_string() }))
    }
}

// ----- file -----

/// Two append-only files per session, like the reference engines:
/// `<prefix>.messages.log` (raw traffic) and `<prefix>.event.log`.
pub struct FileLog {
    messages: std::fs::File,
    events: std::fs::File,
}

impl Log for FileLog {
    fn on_incoming(&mut self, raw: &[u8]) {
        let _ = writeln!(self.messages, "{} <- {}", now(), printable(raw));
    }
    fn on_outgoing(&mut self, raw: &[u8]) {
        let _ = writeln!(self.messages, "{} -> {}", now(), printable(raw));
    }
    fn on_event(&mut self, event: &str) {
        let _ = writeln!(self.events, "{} {}", now(), event);
    }
}

fn now() -> String {
    chrono::Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string()
}

pub struct FileLogFactory {
    pub path: PathBuf,
}

impl FileLogFactory {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl LogFactory for FileLogFactory {
    fn create(&self, session_id: &SessionId) -> Result<Box<dyn Log>> {
        std::fs::create_dir_all(&self.path)?;
        let prefix = self.path.join(session_id.file_prefix());
        let open = |ext: &str| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(prefix.with_extension(ext))
        };
        Ok(Box::new(FileLog { messages: open("messages.log")?, events: open("event.log")? }))
    }
}
