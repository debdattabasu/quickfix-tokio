//! Message stores: persist sequence numbers and sent messages for resend.
//!
//! Counters hold the *next* sequence number to use (starting at 1), matching
//! the public semantics of the reference engines.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::{Error, Result};
use crate::session_id::SessionId;

#[async_trait]
pub trait MessageStore: Send {
    fn next_sender_seq_num(&self) -> u64;
    fn next_target_seq_num(&self) -> u64;
    async fn incr_next_sender_seq_num(&mut self) -> Result<()>;
    async fn incr_next_target_seq_num(&mut self) -> Result<()>;
    async fn set_next_sender_seq_num(&mut self, n: u64) -> Result<()>;
    async fn set_next_target_seq_num(&mut self, n: u64) -> Result<()>;
    fn creation_time(&self) -> DateTime<Utc>;
    /// Persist an outgoing message under its seqnum.
    async fn save_message(&mut self, seq_num: u64, raw: &[u8]) -> Result<()>;
    /// Persist and advance the sender seqnum as one operation.
    async fn save_message_and_incr(&mut self, seq_num: u64, raw: &[u8]) -> Result<()> {
        self.save_message(seq_num, raw).await?;
        self.incr_next_sender_seq_num().await
    }
    /// Stored messages in `[begin, end]` as `(seq_num, raw)` pairs, ascending.
    /// Missing seqnums are simply absent (the session layer gap-fills them).
    async fn get_messages(&mut self, begin: u64, end: u64) -> Result<Vec<(u64, Vec<u8>)>>;
    /// Reload state from the backing medium (RefreshOnLogon).
    async fn refresh(&mut self) -> Result<()>;
    /// Wipe messages, reset both seqnums to 1, restamp creation time.
    async fn reset(&mut self) -> Result<()>;
}

pub trait MessageStoreFactory: Send + Sync {
    fn create(&self, session_id: &SessionId) -> Result<Box<dyn MessageStore>>;
}

// ----- memory store -----

#[derive(Debug)]
pub struct MemoryStore {
    next_sender: u64,
    next_target: u64,
    creation_time: DateTime<Utc>,
    messages: BTreeMap<u64, Vec<u8>>,
    /// Cap on retained sent messages (the resend buffer). `None` = unbounded.
    /// When exceeded, the oldest messages are dropped; a later resend request
    /// for a dropped seqnum gap-fills it (like a non-persistent store).
    capacity: Option<usize>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            next_sender: 1,
            next_target: 1,
            creation_time: Utc::now(),
            messages: BTreeMap::new(),
            capacity: None,
        }
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Retain at most `capacity` recent sent messages for resend.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { capacity: Some(capacity), ..Self::default() }
    }

    fn trim(&mut self) {
        if let Some(cap) = self.capacity {
            while self.messages.len() > cap {
                // Drop the oldest retained message.
                let Some((&oldest, _)) = self.messages.iter().next() else { break };
                self.messages.remove(&oldest);
            }
        }
    }
}

#[async_trait]
impl MessageStore for MemoryStore {
    fn next_sender_seq_num(&self) -> u64 {
        self.next_sender
    }
    fn next_target_seq_num(&self) -> u64 {
        self.next_target
    }
    async fn incr_next_sender_seq_num(&mut self) -> Result<()> {
        self.next_sender += 1;
        Ok(())
    }
    async fn incr_next_target_seq_num(&mut self) -> Result<()> {
        self.next_target += 1;
        Ok(())
    }
    async fn set_next_sender_seq_num(&mut self, n: u64) -> Result<()> {
        self.next_sender = n;
        Ok(())
    }
    async fn set_next_target_seq_num(&mut self, n: u64) -> Result<()> {
        self.next_target = n;
        Ok(())
    }
    fn creation_time(&self) -> DateTime<Utc> {
        self.creation_time
    }
    async fn save_message(&mut self, seq_num: u64, raw: &[u8]) -> Result<()> {
        self.messages.insert(seq_num, raw.to_vec());
        self.trim();
        Ok(())
    }
    async fn get_messages(&mut self, begin: u64, end: u64) -> Result<Vec<(u64, Vec<u8>)>> {
        Ok(self.messages.range(begin..=end).map(|(k, v)| (*k, v.clone())).collect())
    }
    async fn refresh(&mut self) -> Result<()> {
        Ok(())
    }
    async fn reset(&mut self) -> Result<()> {
        self.next_sender = 1;
        self.next_target = 1;
        self.creation_time = Utc::now();
        self.messages.clear();
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MemoryStoreFactory {
    capacity: Option<usize>,
}

impl MemoryStoreFactory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bound each session's in-memory resend buffer to `capacity` messages.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { capacity: Some(capacity) }
    }
}

impl MessageStoreFactory for MemoryStoreFactory {
    fn create(&self, _session_id: &SessionId) -> Result<Box<dyn MessageStore>> {
        Ok(Box::new(match self.capacity {
            Some(cap) => MemoryStore::with_capacity(cap),
            None => MemoryStore::new(),
        }))
    }
}

// ----- file store -----
//
// Layout mirrors QuickFIX C++: per-session prefix with
//   <prefix>.body     concatenated raw messages
//   <prefix>.header   "seqnum,offset,length\n" index records
//   <prefix>.seqnums  "%020u : %020u" (sender, target), rewritten in place
//   <prefix>.session  creation time, RFC3339
//
// Writes go through std::fs: each is a small append or in-place rewrite that
// lands in the page cache, so the store survives a process crash. With
// `sync` enabled it additionally fsyncs each durability-critical write, so it
// survives power loss too — at the cost of a disk flush per write, offloaded
// to a blocking thread so it never stalls the async runtime.

pub struct FileStore {
    prefix: PathBuf,
    cache: MemoryStore,
    body: std::fs::File,
    header: std::fs::File,
    seqnums: std::fs::File,
    body_len: u64,
    offsets: BTreeMap<u64, (u64, u64)>,
    sync: bool,
}

impl FileStore {
    pub fn open(dir: &std::path::Path, session_id: &SessionId) -> Result<Self> {
        Self::open_with_sync(dir, session_id, false)
    }

    pub fn open_with_sync(
        dir: &std::path::Path,
        session_id: &SessionId,
        sync: bool,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let prefix = dir.join(session_id.file_prefix());
        let open = |ext: &str| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(prefix.with_extension(ext))
        };
        let mut store = Self {
            cache: MemoryStore::new(),
            body: open("body")?,
            header: open("header")?,
            seqnums: open("seqnums")?,
            body_len: 0,
            offsets: BTreeMap::new(),
            sync,
            prefix,
        };
        store.load()?;
        Ok(store)
    }

    /// Flush the given files to stable storage on a blocking thread. `fsync`
    /// operates on the underlying file, so a cheap `try_clone`d handle
    /// suffices and the store keeps its own handles for further writes.
    async fn fsync(&self, files: &[&std::fs::File]) -> Result<()> {
        if !self.sync {
            return Ok(());
        }
        let clones: Vec<std::fs::File> =
            files.iter().map(|f| f.try_clone()).collect::<std::io::Result<_>>()?;
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            for f in &clones {
                f.sync_data()?;
            }
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("fsync task failed: {e}")))??;
        Ok(())
    }

    fn load(&mut self) -> Result<()> {
        // seqnums
        let mut s = String::new();
        self.seqnums.seek(SeekFrom::Start(0))?;
        self.seqnums.read_to_string(&mut s)?;
        if let Some((a, b)) = s.trim().split_once(" : ") {
            let sender: u64 = a.trim().parse().map_err(|_| corrupt("seqnums"))?;
            let target: u64 = b.trim().parse().map_err(|_| corrupt("seqnums"))?;
            self.cache.next_sender = sender;
            self.cache.next_target = target;
        }
        // header index
        let mut h = String::new();
        self.header.seek(SeekFrom::Start(0))?;
        self.header.read_to_string(&mut h)?;
        for line in h.lines() {
            let mut parts = line.splitn(3, ',');
            let (Some(seq), Some(off), Some(len)) = (parts.next(), parts.next(), parts.next())
            else {
                return Err(corrupt("header"));
            };
            let seq: u64 = seq.parse().map_err(|_| corrupt("header"))?;
            let off: u64 = off.parse().map_err(|_| corrupt("header"))?;
            let len: u64 = len.parse().map_err(|_| corrupt("header"))?;
            self.offsets.insert(seq, (off, len));
        }
        self.body_len = self.body.seek(SeekFrom::End(0))?;
        // creation time
        let session_file = self.prefix.with_extension("session");
        match std::fs::read_to_string(&session_file) {
            Ok(ts) => {
                self.cache.creation_time = ts
                    .trim()
                    .parse::<DateTime<Utc>>()
                    .map_err(|_| corrupt("session"))?;
            }
            Err(_) => {
                self.cache.creation_time = Utc::now();
                std::fs::write(&session_file, self.cache.creation_time.to_rfc3339())?;
            }
        }
        Ok(())
    }

    fn write_seqnums(&mut self) -> Result<()> {
        self.seqnums.seek(SeekFrom::Start(0))?;
        let line = format!("{:020} : {:020}", self.cache.next_sender, self.cache.next_target);
        self.seqnums.write_all(line.as_bytes())?;
        self.seqnums.flush()?;
        Ok(())
    }
}

fn corrupt(which: &str) -> Error {
    Error::Store(format!("corrupt {which} file"))
}

#[async_trait]
impl MessageStore for FileStore {
    fn next_sender_seq_num(&self) -> u64 {
        self.cache.next_sender
    }
    fn next_target_seq_num(&self) -> u64 {
        self.cache.next_target
    }
    async fn incr_next_sender_seq_num(&mut self) -> Result<()> {
        self.cache.next_sender += 1;
        self.write_seqnums()?;
        self.fsync(&[&self.seqnums]).await
    }
    async fn incr_next_target_seq_num(&mut self) -> Result<()> {
        self.cache.next_target += 1;
        self.write_seqnums()?;
        self.fsync(&[&self.seqnums]).await
    }
    async fn set_next_sender_seq_num(&mut self, n: u64) -> Result<()> {
        self.cache.next_sender = n;
        self.write_seqnums()?;
        self.fsync(&[&self.seqnums]).await
    }
    async fn set_next_target_seq_num(&mut self, n: u64) -> Result<()> {
        self.cache.next_target = n;
        self.write_seqnums()?;
        self.fsync(&[&self.seqnums]).await
    }
    fn creation_time(&self) -> DateTime<Utc> {
        self.cache.creation_time
    }
    async fn save_message(&mut self, seq_num: u64, raw: &[u8]) -> Result<()> {
        let offset = self.body_len;
        self.body.seek(SeekFrom::End(0))?;
        self.body.write_all(raw)?;
        self.body_len += raw.len() as u64;

        self.header.seek(SeekFrom::End(0))?;
        self.header
            .write_all(format!("{seq_num},{offset},{}\n", raw.len()).as_bytes())?;
        self.offsets.insert(seq_num, (offset, raw.len() as u64));
        // The message must be durable before its seqnum advances, so the
        // caller can honor a resend for it (save_message_and_incr).
        self.fsync(&[&self.body, &self.header]).await
    }
    async fn get_messages(&mut self, begin: u64, end: u64) -> Result<Vec<(u64, Vec<u8>)>> {
        let ranges: Vec<(u64, u64, u64)> = self
            .offsets
            .range(begin..=end)
            .map(|(seq, (off, len))| (*seq, *off, *len))
            .collect();
        let mut out = Vec::with_capacity(ranges.len());
        for (seq, off, len) in ranges {
            let mut buf = vec![0u8; len as usize];
            self.body.seek(SeekFrom::Start(off))?;
            self.body.read_exact(&mut buf)?;
            out.push((seq, buf));
        }
        Ok(out)
    }
    async fn refresh(&mut self) -> Result<()> {
        self.offsets.clear();
        self.cache = MemoryStore::new();
        self.load()
    }
    async fn reset(&mut self) -> Result<()> {
        self.cache.reset().await?;
        self.offsets.clear();
        self.body_len = 0;
        self.body.set_len(0)?;
        self.header.set_len(0)?;
        self.seqnums.set_len(0)?;
        std::fs::write(self.prefix.with_extension("session"), self.cache.creation_time.to_rfc3339())?;
        self.write_seqnums()
    }
}

pub struct FileStoreFactory {
    pub path: PathBuf,
    sync: bool,
}

impl FileStoreFactory {
    /// Level 2 durability: survives a process crash (writes reach the OS),
    /// but not power loss. Matches quickfix C++/n's default.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), sync: false }
    }

    /// Level 3 durability: `fsync` each seqnum/message write to stable
    /// storage, so the session survives power loss too (`FileStoreSync=Y`).
    /// Costs a disk flush per write; offloaded to a blocking thread.
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }
}

impl MessageStoreFactory for FileStoreFactory {
    fn create(&self, session_id: &SessionId) -> Result<Box<dyn MessageStore>> {
        Ok(Box::new(FileStore::open_with_sync(&self.path, session_id, self.sync)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_store_roundtrip() {
        let mut s = MemoryStore::new();
        assert_eq!(s.next_sender_seq_num(), 1);
        s.save_message_and_incr(1, b"one").await.unwrap();
        s.save_message_and_incr(2, b"two").await.unwrap();
        assert_eq!(s.next_sender_seq_num(), 3);
        let msgs = s.get_messages(1, 10).await.unwrap();
        assert_eq!(msgs, vec![(1, b"one".to_vec()), (2, b"two".to_vec())]);
        s.reset().await.unwrap();
        assert_eq!(s.next_sender_seq_num(), 1);
        assert!(s.get_messages(1, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn memory_store_capacity_bounds_resend_buffer() {
        // Retain only the last 2 messages.
        let mut s = MemoryStore::with_capacity(2);
        for n in 1..=5 {
            s.save_message_and_incr(n, format!("m{n}").as_bytes()).await.unwrap();
        }
        // Seqnums still advance normally.
        assert_eq!(s.next_sender_seq_num(), 6);
        // Only the two most recent are retained; older ones are dropped.
        let msgs = s.get_messages(1, 10).await.unwrap();
        assert_eq!(msgs, vec![(4, b"m4".to_vec()), (5, b"m5".to_vec())]);
        // A resend request for a dropped seqnum simply finds nothing.
        assert!(s.get_messages(1, 3).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn file_store_fsync_roundtrips() {
        // With sync on, writes are fsync'd (offloaded to a blocking thread);
        // the data must still be readable and survive a reopen.
        let dir = std::env::temp_dir().join(format!("qft-store-sync-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let sid = SessionId::new("FIX.4.4", "S", "T");
        {
            let mut s = FileStore::open_with_sync(&dir, &sid, true).unwrap();
            s.save_message_and_incr(1, b"synced-one").await.unwrap();
            s.save_message_and_incr(2, b"synced-two").await.unwrap();
        }
        {
            let mut s = FileStore::open_with_sync(&dir, &sid, true).unwrap();
            assert_eq!(s.next_sender_seq_num(), 3);
            let msgs = s.get_messages(1, 5).await.unwrap();
            assert_eq!(msgs, vec![(1, b"synced-one".to_vec()), (2, b"synced-two".to_vec())]);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn file_store_persists_across_reopen() {
        let dir = std::env::temp_dir().join(format!("qft-store-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let sid = SessionId::new("FIX.4.2", "SENDER", "TARGET");

        {
            let mut s = FileStore::open(&dir, &sid).unwrap();
            s.save_message_and_incr(1, b"8=FIX.4.2\x01...one").await.unwrap();
            s.save_message_and_incr(2, b"8=FIX.4.2\x01...two").await.unwrap();
            s.incr_next_target_seq_num().await.unwrap();
        }
        {
            let mut s = FileStore::open(&dir, &sid).unwrap();
            assert_eq!(s.next_sender_seq_num(), 3);
            assert_eq!(s.next_target_seq_num(), 2);
            let msgs = s.get_messages(2, 5).await.unwrap();
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].0, 2);
            assert_eq!(msgs[0].1, b"8=FIX.4.2\x01...two".to_vec());

            s.reset().await.unwrap();
            assert_eq!(s.next_sender_seq_num(), 1);
            assert!(s.get_messages(1, 10).await.unwrap().is_empty());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
