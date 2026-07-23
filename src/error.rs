use crate::message::Tag;

/// Session-level reject reasons (tag 373) as defined by the FIX spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRejectReason {
    InvalidTagNumber,
    RequiredTagMissing,
    TagNotDefinedForThisMessageType,
    UndefinedTag,
    TagSpecifiedWithoutAValue,
    ValueIsIncorrect,
    IncorrectDataFormatForValue,
    DecryptionProblem,
    SignatureProblem,
    CompIDProblem,
    SendingTimeAccuracyProblem,
    InvalidMsgType,
    XMLValidationError,
    TagAppearsMoreThanOnce,
    TagSpecifiedOutOfRequiredOrder,
    RepeatingGroupFieldsOutOfOrder,
    IncorrectNumInGroupCountForRepeatingGroup,
    NonDataValueIncludesFieldDelimiter,
    Other,
}

impl SessionRejectReason {
    pub fn code(&self) -> u32 {
        match self {
            Self::InvalidTagNumber => 0,
            Self::RequiredTagMissing => 1,
            Self::TagNotDefinedForThisMessageType => 2,
            Self::UndefinedTag => 3,
            Self::TagSpecifiedWithoutAValue => 4,
            Self::ValueIsIncorrect => 5,
            Self::IncorrectDataFormatForValue => 6,
            Self::DecryptionProblem => 7,
            Self::SignatureProblem => 8,
            Self::CompIDProblem => 9,
            Self::SendingTimeAccuracyProblem => 10,
            Self::InvalidMsgType => 11,
            Self::XMLValidationError => 12,
            Self::TagAppearsMoreThanOnce => 13,
            Self::TagSpecifiedOutOfRequiredOrder => 14,
            Self::RepeatingGroupFieldsOutOfOrder => 15,
            Self::IncorrectNumInGroupCountForRepeatingGroup => 16,
            Self::NonDataValueIncludesFieldDelimiter => 17,
            Self::Other => 99,
        }
    }

    pub fn text(&self) -> &'static str {
        match self {
            Self::InvalidTagNumber => "Invalid tag number",
            Self::RequiredTagMissing => "Required tag missing",
            Self::TagNotDefinedForThisMessageType => "Tag not defined for this message type",
            Self::UndefinedTag => "Undefined tag",
            Self::TagSpecifiedWithoutAValue => "Tag specified without a value",
            Self::ValueIsIncorrect => "Value is incorrect (out of range) for this tag",
            Self::IncorrectDataFormatForValue => "Incorrect data format for value",
            Self::DecryptionProblem => "Decryption problem",
            Self::SignatureProblem => "Signature problem",
            Self::CompIDProblem => "CompID problem",
            Self::SendingTimeAccuracyProblem => "SendingTime accuracy problem",
            Self::InvalidMsgType => "Invalid MsgType",
            Self::XMLValidationError => "XML validation error",
            Self::TagAppearsMoreThanOnce => "Tag appears more than once",
            Self::TagSpecifiedOutOfRequiredOrder => "Tag specified out of required order",
            Self::RepeatingGroupFieldsOutOfOrder => "Repeating group fields out of order",
            Self::IncorrectNumInGroupCountForRepeatingGroup => {
                "Incorrect NumInGroup count for repeating group"
            }
            Self::NonDataValueIncludesFieldDelimiter => {
                "Non-data value includes field delimiter (SOH character)"
            }
            Self::Other => "Other",
        }
    }
}

/// A message failed validation and should be answered with a session-level Reject (35=3).
#[derive(Debug, Clone, thiserror::Error)]
#[error("{}{}", .reason.text(), .ref_tag.map(|t| format!(" (tag {t})")).unwrap_or_default())]
pub struct RejectError {
    pub reason: SessionRejectReason,
    pub ref_tag: Option<Tag>,
    /// True when the offending message should NOT increment NextTargetMsgSeqNum
    /// (e.g. garbled messages per the spec are ignored, not rejected).
    pub is_garbled: bool,
}

impl RejectError {
    pub fn new(reason: SessionRejectReason) -> Self {
        Self { reason, ref_tag: None, is_garbled: false }
    }
    pub fn with_tag(reason: SessionRejectReason, tag: Tag) -> Self {
        Self { reason, ref_tag: Some(tag), is_garbled: false }
    }
}

/// Errors converting a field value to/from its wire representation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConversionError {
    #[error("field {tag} not found")]
    FieldNotFound { tag: Tag },
    #[error("cannot convert value {value:?} for tag {tag}")]
    InvalidValue { tag: Tag, value: String },
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("message parse error: {0}")]
    Parse(String),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Reject(#[from] RejectError),
    #[error("session {0} not found")]
    UnknownSession(String),
    #[error("session {0} is not logged on")]
    NotLoggedOn(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("data dictionary error: {0}")]
    Dictionary(String),
    #[error("store error: {0}")]
    Store(String),
    #[error("do not send")]
    DoNotSend,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
