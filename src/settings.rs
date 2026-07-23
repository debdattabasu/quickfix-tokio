//! QuickFIX-compatible configuration: `[DEFAULT]` + `[SESSION]` INI sections.
//!
//! Key names follow the classic engines (`SenderCompID`, `HeartBtInt`,
//! `SocketConnectPort`, ...) so existing config files carry over.

use std::collections::HashMap;
use std::time::Duration;

use crate::datadictionary::ValidationSettings;
use crate::error::{Error, Result};
use crate::session_id::SessionId;
use crate::value::TimestampPrecision;

#[derive(Debug, Clone, Default)]
pub struct Settings {
    pub defaults: HashMap<String, String>,
    pub sessions: Vec<HashMap<String, String>>,
}

impl Settings {
    pub fn parse(text: &str) -> Result<Self> {
        let mut settings = Settings::default();
        let mut current: Option<HashMap<String, String>> = None;
        let mut in_default = false;

        for (line_no, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if line.starts_with('[') && line.ends_with(']') {
                if let Some(section) = current.take() {
                    settings.sessions.push(section);
                }
                let name = line[1..line.len() - 1].trim().to_ascii_uppercase();
                match name.as_str() {
                    "DEFAULT" => in_default = true,
                    "SESSION" => {
                        in_default = false;
                        current = Some(HashMap::new());
                    }
                    other => {
                        return Err(Error::Config(format!(
                            "line {}: unknown section [{other}]",
                            line_no + 1
                        )));
                    }
                }
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                return Err(Error::Config(format!(
                    "line {}: expected key=value, got {line:?}",
                    line_no + 1
                )));
            };
            let (key, value) = (key.trim().to_owned(), value.trim().to_owned());
            if let Some(section) = current.as_mut() {
                section.insert(key, value);
            } else if in_default {
                settings.defaults.insert(key, value);
            } else {
                return Err(Error::Config(format!(
                    "line {}: key outside of [DEFAULT]/[SESSION]",
                    line_no + 1
                )));
            }
        }
        if let Some(section) = current.take() {
            settings.sessions.push(section);
        }
        Ok(settings)
    }

    pub async fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let text = tokio::fs::read_to_string(path).await?;
        Self::parse(&text)
    }

    /// Resolve every `[SESSION]` into a typed config (session keys override
    /// `[DEFAULT]`).
    pub fn session_configs(&self) -> Result<Vec<SessionConfig>> {
        self.sessions
            .iter()
            .map(|s| {
                let mut merged = self.defaults.clone();
                merged.extend(s.iter().map(|(k, v)| (k.clone(), v.clone())));
                SessionConfig::from_map(&merged)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionType {
    Initiator,
    Acceptor,
}

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub session_id: SessionId,
    pub connection_type: ConnectionType,
    /// Negotiated heartbeat interval. Required for initiators; acceptors
    /// adopt the initiator's value unless `HeartBtIntOverride` is set.
    pub heart_bt_int: Duration,
    pub heart_bt_int_override: Option<Duration>,
    pub socket_connect_host: String,
    pub socket_connect_port: u16,
    pub socket_accept_port: u16,
    pub reconnect_interval: Duration,
    pub logon_timeout: Duration,
    pub logout_timeout: Duration,
    pub reset_on_logon: bool,
    pub reset_on_logout: bool,
    pub reset_on_disconnect: bool,
    pub refresh_on_logon: bool,
    /// Send ResetSeqNumFlag(141)=Y on our Logon.
    pub send_reset_seq_num_flag: bool,
    pub persist_messages: bool,
    pub check_comp_id: bool,
    pub check_latency: bool,
    pub max_latency: Duration,
    pub send_redundant_resend_requests: bool,
    pub timestamp_precision: TimestampPrecision,
    pub validate_length_checksum: bool,
    pub use_data_dictionary: bool,
    pub data_dictionary: Option<String>,
    /// FIXT sessions: session-level dictionary (FIXT11.xml).
    pub transport_data_dictionary: Option<String>,
    /// FIXT sessions: application-level dictionary (FIX50.xml etc).
    pub app_data_dictionary: Option<String>,
    pub validation: ValidationSettings,
    /// Stamp LastMsgSeqNumProcessed(369) on every outgoing header.
    pub enable_last_msg_seq_num_processed: bool,
    /// Cap ResendRequests to this many messages per chunk (0 = unlimited,
    /// EndSeqNo sent as 0/999999 "infinity").
    pub max_messages_in_resend_request: u64,
    /// Send a Logout before disconnecting on heartbeat timeout.
    pub send_logout_before_disconnect_from_timeout: bool,
    /// Require OrigSendingTime(122) on PossDup messages (default Y).
    pub requires_orig_sending_time: bool,
    pub file_store_path: Option<String>,
    pub file_log_path: Option<String>,
    /// FIXT.1.1 sessions: DefaultApplVerID(1137) for our Logon.
    pub default_appl_ver_id: Option<String>,
}

fn get_bool(m: &HashMap<String, String>, key: &str, default: bool) -> Result<bool> {
    match m.get(key).map(|s| s.as_str()) {
        None => Ok(default),
        Some("Y") => Ok(true),
        Some("N") => Ok(false),
        Some(other) => Err(Error::Config(format!("{key} must be Y or N, got {other:?}"))),
    }
}

fn get_u64(m: &HashMap<String, String>, key: &str, default: u64) -> Result<u64> {
    match m.get(key) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| Error::Config(format!("{key} must be a number"))),
    }
}

impl SessionConfig {
    pub fn from_map(m: &HashMap<String, String>) -> Result<Self> {
        let require = |key: &str| {
            m.get(key)
                .cloned()
                .ok_or_else(|| Error::Config(format!("missing required setting {key}")))
        };

        let begin_string = require("BeginString")?;
        const VALID: &[&str] =
            &["FIX.4.0", "FIX.4.1", "FIX.4.2", "FIX.4.3", "FIX.4.4", "FIXT.1.1"];
        if !VALID.contains(&begin_string.as_str()) {
            return Err(Error::Config(format!("unsupported BeginString {begin_string:?}")));
        }

        let session_id = SessionId {
            begin_string: begin_string.clone(),
            sender_comp_id: require("SenderCompID")?,
            sender_sub_id: m.get("SenderSubID").cloned().unwrap_or_default(),
            sender_location_id: m.get("SenderLocationID").cloned().unwrap_or_default(),
            target_comp_id: require("TargetCompID")?,
            target_sub_id: m.get("TargetSubID").cloned().unwrap_or_default(),
            target_location_id: m.get("TargetLocationID").cloned().unwrap_or_default(),
            qualifier: m.get("SessionQualifier").cloned().unwrap_or_default(),
        };

        let connection_type = match require("ConnectionType")?.as_str() {
            "initiator" => ConnectionType::Initiator,
            "acceptor" => ConnectionType::Acceptor,
            other => {
                return Err(Error::Config(format!(
                    "ConnectionType must be initiator or acceptor, got {other:?}"
                )));
            }
        };

        let heart_bt_int = match connection_type {
            ConnectionType::Initiator => {
                let secs: u64 = require("HeartBtInt")?
                    .parse()
                    .map_err(|_| Error::Config("HeartBtInt must be a number".into()))?;
                if secs == 0 {
                    return Err(Error::Config("HeartBtInt must be > 0".into()));
                }
                Duration::from_secs(secs)
            }
            ConnectionType::Acceptor => Duration::from_secs(get_u64(m, "HeartBtInt", 30)?),
        };

        let (socket_connect_host, socket_connect_port, socket_accept_port) = match connection_type {
            ConnectionType::Initiator => (
                require("SocketConnectHost")?,
                require("SocketConnectPort")?
                    .parse()
                    .map_err(|_| Error::Config("SocketConnectPort must be a port".into()))?,
                0,
            ),
            ConnectionType::Acceptor => (
                String::new(),
                0,
                require("SocketAcceptPort")?
                    .parse()
                    .map_err(|_| Error::Config("SocketAcceptPort must be a port".into()))?,
            ),
        };

        // FIX < 4.2 has no sub-second timestamps.
        let default_precision = if begin_string.as_str() < "FIX.4.2" {
            TimestampPrecision::Seconds
        } else {
            TimestampPrecision::Millis
        };
        let timestamp_precision = match m.get("TimestampPrecision").map(|s| s.as_str()) {
            None => match get_bool(m, "MillisecondsInTimeStamp", true)? {
                true => default_precision,
                false => TimestampPrecision::Seconds,
            },
            Some("0") => TimestampPrecision::Seconds,
            Some("3") => TimestampPrecision::Millis,
            Some("6") => TimestampPrecision::Micros,
            Some("9") => TimestampPrecision::Nanos,
            Some(other) => {
                return Err(Error::Config(format!(
                    "TimestampPrecision must be 0, 3, 6 or 9, got {other:?}"
                )));
            }
        };

        if session_id.is_fixt() && !m.contains_key("DefaultApplVerID") {
            return Err(Error::Config("FIXT.1.1 sessions require DefaultApplVerID".into()));
        }

        Ok(Self {
            connection_type,
            heart_bt_int,
            heart_bt_int_override: m
                .get("HeartBtIntOverride")
                .map(|v| {
                    v.parse::<u64>()
                        .map(Duration::from_secs)
                        .map_err(|_| Error::Config("HeartBtIntOverride must be a number".into()))
                })
                .transpose()?,
            socket_connect_host,
            socket_connect_port,
            socket_accept_port,
            reconnect_interval: Duration::from_secs(get_u64(m, "ReconnectInterval", 30)?),
            logon_timeout: Duration::from_secs(get_u64(m, "LogonTimeout", 10)?),
            logout_timeout: Duration::from_secs(get_u64(m, "LogoutTimeout", 2)?),
            reset_on_logon: get_bool(m, "ResetOnLogon", false)?,
            reset_on_logout: get_bool(m, "ResetOnLogout", false)?,
            reset_on_disconnect: get_bool(m, "ResetOnDisconnect", false)?,
            refresh_on_logon: get_bool(m, "RefreshOnLogon", false)?,
            send_reset_seq_num_flag: get_bool(m, "SendResetSeqNumFlag", false)?,
            persist_messages: get_bool(m, "PersistMessages", true)?,
            check_comp_id: get_bool(m, "CheckCompID", true)?,
            check_latency: get_bool(m, "CheckLatency", true)?,
            max_latency: Duration::from_secs(get_u64(m, "MaxLatency", 120)?),
            send_redundant_resend_requests: get_bool(m, "SendRedundantResendRequests", false)?,
            timestamp_precision,
            validate_length_checksum: get_bool(m, "ValidateLengthAndChecksum", true)?,
            use_data_dictionary: get_bool(m, "UseDataDictionary", true)?,
            data_dictionary: m.get("DataDictionary").cloned(),
            transport_data_dictionary: m.get("TransportDataDictionary").cloned(),
            app_data_dictionary: m.get("AppDataDictionary").cloned(),
            enable_last_msg_seq_num_processed: get_bool(m, "EnableLastMsgSeqNumProcessed", false)?,
            max_messages_in_resend_request: get_u64(m, "MaxMessagesInResendRequest", 0)?,
            send_logout_before_disconnect_from_timeout: get_bool(
                m,
                "SendLogoutBeforeDisconnectFromTimeout",
                false,
            )?,
            requires_orig_sending_time: get_bool(m, "RequiresOrigSendingTime", true)?,
            validation: ValidationSettings {
                check_fields_out_of_order: get_bool(m, "ValidateFieldsOutOfOrder", true)?,
                check_fields_have_values: get_bool(m, "ValidateFieldsHaveValues", true)?,
                check_user_defined_fields: get_bool(m, "ValidateUserDefinedFields", true)?,
                allow_unknown_message_fields: get_bool(m, "AllowUnknownMsgFields", false)?,
            },
            file_store_path: m.get("FileStorePath").cloned(),
            file_log_path: m.get("FileLogPath").cloned(),
            default_appl_ver_id: m.get("DefaultApplVerID").cloned(),
            session_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
# comment
[DEFAULT]
ConnectionType=initiator
ReconnectInterval=15
SocketConnectHost=127.0.0.1

[SESSION]
BeginString=FIX.4.2
SenderCompID=CLIENT1
TargetCompID=EXEC
HeartBtInt=30
SocketConnectPort=9876

[SESSION]
BeginString=FIX.4.4
SenderCompID=CLIENT1
TargetCompID=EXEC2
HeartBtInt=20
SocketConnectPort=9877
ResetOnLogon=Y
"#;

    #[test]
    fn parses_sessions_with_default_overlay() {
        let settings = Settings::parse(SAMPLE).unwrap();
        let configs = settings.session_configs().unwrap();
        assert_eq!(configs.len(), 2);

        let c = &configs[0];
        assert_eq!(c.session_id.to_string(), "FIX.4.2:CLIENT1->EXEC");
        assert_eq!(c.connection_type, ConnectionType::Initiator);
        assert_eq!(c.heart_bt_int, Duration::from_secs(30));
        assert_eq!(c.reconnect_interval, Duration::from_secs(15));
        assert_eq!(c.socket_connect_host, "127.0.0.1");
        assert_eq!(c.socket_connect_port, 9876);
        assert!(!c.reset_on_logon);

        let c = &configs[1];
        assert!(c.reset_on_logon);
        assert_eq!(c.timestamp_precision, TimestampPrecision::Millis);
    }

    #[test]
    fn missing_required_key_errors() {
        let settings = Settings::parse(
            "[SESSION]\nBeginString=FIX.4.2\nSenderCompID=A\nConnectionType=initiator\n",
        )
        .unwrap();
        assert!(settings.session_configs().is_err());
    }

    #[test]
    fn old_fix_defaults_to_second_precision() {
        let text = "[SESSION]\nConnectionType=acceptor\nBeginString=FIX.4.0\nSenderCompID=A\nTargetCompID=B\nSocketAcceptPort=5001\n";
        let c = &Settings::parse(text).unwrap().session_configs().unwrap()[0];
        assert_eq!(c.timestamp_precision, TimestampPrecision::Seconds);
    }
}
