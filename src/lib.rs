//! # quickfix-tokio
//!
//! A pure-Rust FIX protocol engine built natively on tokio — no C++
//! bindings, no blocking threads. Protocol behavior follows the reference
//! QuickFIX engines (C++, Go, .NET); the concurrency model is one tokio
//! task per session that owns all session state, with sockets and API
//! handles connected purely by channels.
//!
//! ```no_run
//! use std::sync::Arc;
//! use quickfix_tokio::{Application, Engine, Settings, MemoryStoreFactory, TracingLogFactory};
//!
//! struct MyApp;
//! impl Application for MyApp {}
//!
//! # async fn run() -> quickfix_tokio::Result<()> {
//! let settings = Settings::from_file("fix.cfg").await?;
//! let engine = Engine::start(
//!     &settings,
//!     Arc::new(MyApp),
//!     Arc::new(MemoryStoreFactory),
//!     Arc::new(TracingLogFactory),
//! ).await?;
//! # Ok(())
//! # }
//! ```

pub mod application;
pub mod datadictionary;
pub mod engine;
#[cfg(feature = "fix44")]
pub mod fix44;
pub mod error;
pub mod field_map;
pub mod log;
pub mod message;
pub mod parser;
pub mod session;
pub mod session_id;
pub mod settings;
pub mod store;
pub mod tags;
mod transport;
pub mod value;

pub use application::{Application, ApplicationError, DoNotSend};
pub use datadictionary::{DataDictionary, ValidationSettings};
pub use engine::Engine;
pub use error::{Error, RejectError, Result, SessionRejectReason};
pub use field_map::{FieldMap, GroupTemplate};
pub use log::{FileLogFactory, Log, LogFactory, NullLogFactory, TracingLogFactory};
pub use message::{Message, Tag};
pub use session::{SessionHandle, SessionStatus};
pub use session_id::SessionId;
pub use settings::{ConnectionType, SessionConfig, Settings};
pub use store::{
    FileStoreFactory, MemoryStoreFactory, MessageStore, MessageStoreFactory,
};
pub use value::{FixDate, TimestampPrecision, UtcTimestamp};
