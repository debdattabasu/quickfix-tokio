//! The engine: builds sessions from settings, spawns their tasks, binds
//! acceptor listeners, and runs initiator connect loops.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::application::Application;
use crate::datadictionary::DataDictionary;
use crate::error::{Error, Result};
use crate::log::LogFactory;
use crate::session::{Command, Session, SessionHandle};
use crate::session_id::SessionId;
use crate::settings::{ConnectionType, Settings};
use crate::store::MessageStoreFactory;
use crate::transport::{self, SessionKey};

pub struct Engine {
    handles: HashMap<SessionKey, SessionHandle>,
    io_tasks: Vec<JoinHandle<()>>,
}

fn key_of(id: &SessionId) -> SessionKey {
    (id.begin_string.clone(), id.sender_comp_id.clone(), id.target_comp_id.clone())
}

impl Engine {
    /// Create all configured sessions and start their network activity:
    /// acceptors listen, initiators dial (and keep re-dialing).
    pub async fn start(
        settings: &Settings,
        app: Arc<dyn Application>,
        store_factory: Arc<dyn MessageStoreFactory>,
        log_factory: Arc<dyn LogFactory>,
    ) -> Result<Engine> {
        let configs = settings.session_configs()?;
        if configs.is_empty() {
            return Err(Error::Config("no [SESSION] sections configured".into()));
        }

        let mut handles = HashMap::new();
        let mut io_tasks = Vec::new();
        let mut acceptors_by_port: HashMap<u16, HashMap<SessionKey, SessionHandle>> =
            HashMap::new();
        let mut dictionaries: HashMap<String, Arc<DataDictionary>> = HashMap::new();

        for cfg in configs {
            // App-message dictionary and admin-message dictionary. For FIXT
            // these differ: app messages use transport+app merged, admin
            // messages the transport dictionary alone.
            let (dictionary, admin_dictionary) = if !cfg.use_data_dictionary {
                (None, None)
            } else {
                match (&cfg.transport_data_dictionary, &cfg.app_data_dictionary) {
                    (Some(transport), Some(app)) => {
                        let merged_key = format!("{transport}+{app}");
                        let (merged, transport_dd) = match (
                            dictionaries.get(&merged_key),
                            dictionaries.get(transport),
                        ) {
                            (Some(m), Some(t)) => (m.clone(), t.clone()),
                            _ => {
                                let t = Arc::new(DataDictionary::load(transport).await?);
                                let a = DataDictionary::load(app).await?;
                                let m = Arc::new((*t).clone().merged_with_app(&a));
                                dictionaries.insert(merged_key, m.clone());
                                dictionaries.insert(transport.clone(), t.clone());
                                (m, t)
                            }
                        };
                        (Some(merged), Some(transport_dd))
                    }
                    _ => match &cfg.data_dictionary {
                        Some(path) => {
                            let dd = match dictionaries.get(path) {
                                Some(dd) => dd.clone(),
                                None => {
                                    let dd = Arc::new(DataDictionary::load(path).await?);
                                    dictionaries.insert(path.clone(), dd.clone());
                                    dd
                                }
                            };
                            (Some(dd.clone()), Some(dd))
                        }
                        None => (None, None),
                    },
                }
            };
            let key = key_of(&cfg.session_id);
            if handles.contains_key(&key) {
                return Err(Error::Config(format!(
                    "duplicate session {}",
                    cfg.session_id
                )));
            }
            let store = store_factory.create(&cfg.session_id)?;
            let log = log_factory.create(&cfg.session_id)?;
            let connection_type = cfg.connection_type;
            let (host, port, reconnect) =
                (cfg.socket_connect_host.clone(), cfg.socket_connect_port, cfg.reconnect_interval);
            let accept_port = cfg.socket_accept_port;

            let handle =
                Session::spawn(cfg, store, log, app.clone(), dictionary, admin_dictionary);
            handles.insert(key.clone(), handle.clone());

            match connection_type {
                ConnectionType::Initiator => {
                    io_tasks.push(tokio::spawn(transport::run_initiator(
                        host, port, reconnect, handle,
                    )));
                }
                ConnectionType::Acceptor => {
                    acceptors_by_port.entry(accept_port).or_default().insert(key, handle);
                }
            }
        }

        for (port, registry) in acceptors_by_port {
            let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
            io_tasks.push(tokio::spawn(transport::run_acceptor(listener, Arc::new(registry))));
        }

        Ok(Engine { handles, io_tasks })
    }

    /// Handle for the session matching this BeginString + CompID pair.
    pub fn session(
        &self,
        begin_string: &str,
        sender_comp_id: &str,
        target_comp_id: &str,
    ) -> Option<SessionHandle> {
        self.handles
            .get(&(begin_string.to_owned(), sender_comp_id.to_owned(), target_comp_id.to_owned()))
            .cloned()
    }

    pub fn sessions(&self) -> impl Iterator<Item = &SessionHandle> {
        self.handles.values()
    }

    /// Graceful shutdown: log out every session, stop their tasks, and stop
    /// listening/dialing.
    pub async fn stop(self) {
        for task in &self.io_tasks {
            task.abort();
        }
        for handle in self.handles.values() {
            let (tx, rx) = oneshot::channel();
            if handle.cmd_tx.send(Command::Stop(tx)).await.is_ok() {
                let _ = rx.await;
            }
        }
    }
}
