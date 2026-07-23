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

/// Size-based rotation with a retention limit. `max_size == 0` disables
/// rotation (unbounded append). When a write would exceed `max_size`, the
/// current file is rolled to `<name>.1`, existing backups shift up, and
/// anything beyond `max_backups` is deleted.
#[derive(Debug, Clone, Copy)]
pub struct Rotation {
    pub max_size: u64,
    pub max_backups: usize,
}

impl Rotation {
    fn none() -> Self {
        Self { max_size: 0, max_backups: 0 }
    }
}

/// An append log file that rotates by size and prunes old backups.
struct RotatingFile {
    path: PathBuf,
    file: std::fs::File,
    size: u64,
    rotation: Rotation,
}

impl RotatingFile {
    fn open(path: PathBuf, rotation: Rotation) -> Result<Self> {
        let file =
            std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
        let size = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Self { path, file, size, rotation })
    }

    fn write_line(&mut self, line: &str) {
        let bytes = line.len() as u64 + 1; // + newline
        if self.rotation.max_size > 0
            && self.size > 0
            && self.size + bytes > self.rotation.max_size
        {
            if let Err(e) = self.rotate() {
                tracing::warn!("log rotation failed for {:?}: {e}", self.path);
            }
        }
        if writeln!(self.file, "{line}").is_ok() {
            self.size += bytes;
        }
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        let backup = |n: usize| -> PathBuf {
            let mut p = self.path.clone().into_os_string();
            p.push(format!(".{n}"));
            PathBuf::from(p)
        };
        // Drop the oldest backup, then shift the rest up by one.
        let _ = std::fs::remove_file(backup(self.rotation.max_backups));
        for n in (1..self.rotation.max_backups).rev() {
            if backup(n).exists() {
                std::fs::rename(backup(n), backup(n + 1))?;
            }
        }
        if self.rotation.max_backups > 0 {
            std::fs::rename(&self.path, backup(1))?;
        } else {
            // No backups retained: just truncate.
            std::fs::remove_file(&self.path)?;
        }
        self.file =
            std::fs::OpenOptions::new().create(true).append(true).open(&self.path)?;
        self.size = 0;
        Ok(())
    }
}

/// Two rotating append files per session, like the reference engines:
/// `<prefix>.messages.log` (raw traffic) and `<prefix>.event.log`.
pub struct FileLog {
    messages: RotatingFile,
    events: RotatingFile,
}

impl Log for FileLog {
    fn on_incoming(&mut self, raw: &[u8]) {
        self.messages.write_line(&format!("{} <- {}", now(), printable(raw)));
    }
    fn on_outgoing(&mut self, raw: &[u8]) {
        self.messages.write_line(&format!("{} -> {}", now(), printable(raw)));
    }
    fn on_event(&mut self, event: &str) {
        self.events.write_line(&format!("{} {}", now(), event));
    }
}

fn now() -> String {
    chrono::Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string()
}

pub struct FileLogFactory {
    pub path: PathBuf,
    rotation: Rotation,
}

impl FileLogFactory {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), rotation: Rotation::none() }
    }

    /// Rotate each log at `max_size` bytes, keeping at most `max_backups`
    /// rolled files (`<name>.1` .. `<name>.max_backups`).
    pub fn with_rotation(mut self, max_size: u64, max_backups: usize) -> Self {
        self.rotation = Rotation { max_size, max_backups };
        self
    }
}

impl LogFactory for FileLogFactory {
    fn create(&self, session_id: &SessionId) -> Result<Box<dyn Log>> {
        std::fs::create_dir_all(&self.path)?;
        let prefix = self.path.join(session_id.file_prefix());
        Ok(Box::new(FileLog {
            messages: RotatingFile::open(prefix.with_extension("messages.log"), self.rotation)?,
            events: RotatingFile::open(prefix.with_extension("event.log"), self.rotation)?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_log_rotates_and_prunes_backups() {
        let dir = std::env::temp_dir().join(format!("qft-log-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let sid = SessionId::new("FIX.4.4", "S", "T");
        // Rotate every ~80 bytes, keep 2 backups.
        let factory = FileLogFactory::new(&dir).with_rotation(80, 2);
        let mut log = factory.create(&sid).unwrap();

        // Each line is ~40+ bytes; write enough to force several rotations.
        for i in 0..20 {
            log.on_outgoing(format!("8=FIX.4.4|35=0|34={i}|line-padding-here|").as_bytes());
        }
        drop(log);

        let base = dir.join(sid.file_prefix()).with_extension("messages.log");
        assert!(base.exists(), "current log missing");
        let b1 = with_suffix(&base, ".1");
        let b2 = with_suffix(&base, ".2");
        let b3 = with_suffix(&base, ".3");
        assert!(b1.exists() && b2.exists(), "expected 2 rotated backups");
        // Retention: no third backup is ever kept.
        assert!(!b3.exists(), "backups exceeded max_backups=2");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn with_suffix(p: &std::path::Path, s: &str) -> PathBuf {
        let mut o = p.to_path_buf().into_os_string();
        o.push(s);
        PathBuf::from(o)
    }
}
