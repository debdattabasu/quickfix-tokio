//! Session identity: BeginString + CompIDs (+ optional sub/location IDs and
//! qualifier), matching the reference engines' `SessionID`.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct SessionId {
    pub begin_string: String,
    pub sender_comp_id: String,
    pub sender_sub_id: String,
    pub sender_location_id: String,
    pub target_comp_id: String,
    pub target_sub_id: String,
    pub target_location_id: String,
    pub qualifier: String,
}

impl SessionId {
    pub fn new(begin_string: &str, sender_comp_id: &str, target_comp_id: &str) -> Self {
        Self {
            begin_string: begin_string.to_owned(),
            sender_comp_id: sender_comp_id.to_owned(),
            target_comp_id: target_comp_id.to_owned(),
            ..Default::default()
        }
    }

    pub fn is_fixt(&self) -> bool {
        self.begin_string == "FIXT.1.1"
    }

    /// The identity an acceptor derives from an inbound Logon: the peer's
    /// SenderCompID is our TargetCompID and vice versa.
    pub fn reversed(&self) -> Self {
        Self {
            begin_string: self.begin_string.clone(),
            sender_comp_id: self.target_comp_id.clone(),
            sender_sub_id: self.target_sub_id.clone(),
            sender_location_id: self.target_location_id.clone(),
            target_comp_id: self.sender_comp_id.clone(),
            target_sub_id: self.sender_sub_id.clone(),
            target_location_id: self.sender_location_id.clone(),
            qualifier: self.qualifier.clone(),
        }
    }

    /// Filename prefix for file-based stores/logs:
    /// `BeginString-Sender-Target[-Qualifier]`.
    pub fn file_prefix(&self) -> String {
        let mut s = format!(
            "{}-{}-{}",
            self.begin_string, self.sender_comp_id, self.target_comp_id
        );
        if !self.qualifier.is_empty() {
            s.push('-');
            s.push_str(&self.qualifier);
        }
        s
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.begin_string, self.sender_comp_id)?;
        if !self.sender_sub_id.is_empty() {
            write!(f, "/{}", self.sender_sub_id)?;
        }
        if !self.sender_location_id.is_empty() {
            write!(f, "/{}", self.sender_location_id)?;
        }
        write!(f, "->{}", self.target_comp_id)?;
        if !self.target_sub_id.is_empty() {
            write!(f, "/{}", self.target_sub_id)?;
        }
        if !self.target_location_id.is_empty() {
            write!(f, "/{}", self.target_location_id)?;
        }
        if !self.qualifier.is_empty() {
            write!(f, ":{}", self.qualifier)?;
        }
        Ok(())
    }
}
