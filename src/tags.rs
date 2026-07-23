//! Well-known FIX tag numbers used by the session layer, plus classification
//! of standard header/trailer tags needed to parse a message without a
//! data dictionary.

use crate::message::Tag;

pub const BEGIN_SEQ_NO: Tag = 7;
pub const BEGIN_STRING: Tag = 8;
pub const BODY_LENGTH: Tag = 9;
pub const CHECK_SUM: Tag = 10;
pub const END_SEQ_NO: Tag = 16;
pub const MSG_SEQ_NUM: Tag = 34;
pub const MSG_TYPE: Tag = 35;
pub const NEW_SEQ_NO: Tag = 36;
pub const POSS_DUP_FLAG: Tag = 43;
pub const REF_SEQ_NUM: Tag = 45;
pub const SENDER_COMP_ID: Tag = 49;
pub const SENDER_SUB_ID: Tag = 50;
pub const SENDING_TIME: Tag = 52;
pub const TARGET_COMP_ID: Tag = 56;
pub const TARGET_SUB_ID: Tag = 57;
pub const TEXT: Tag = 58;
pub const SIGNATURE: Tag = 89;
pub const SECURE_DATA_LEN: Tag = 90;
pub const SECURE_DATA: Tag = 91;
pub const SIGNATURE_LENGTH: Tag = 93;
pub const RAW_DATA_LENGTH: Tag = 95;
pub const RAW_DATA: Tag = 96;
pub const POSS_RESEND: Tag = 97;
pub const ENCRYPT_METHOD: Tag = 98;
pub const HEART_BT_INT: Tag = 108;
pub const TEST_REQ_ID: Tag = 112;
pub const ON_BEHALF_OF_COMP_ID: Tag = 115;
pub const ON_BEHALF_OF_SUB_ID: Tag = 116;
pub const ORIG_SENDING_TIME: Tag = 122;
pub const GAP_FILL_FLAG: Tag = 123;
pub const DELIVER_TO_COMP_ID: Tag = 128;
pub const DELIVER_TO_SUB_ID: Tag = 129;
pub const RESET_SEQ_NUM_FLAG: Tag = 141;
pub const SENDER_LOCATION_ID: Tag = 142;
pub const TARGET_LOCATION_ID: Tag = 143;
pub const ON_BEHALF_OF_LOCATION_ID: Tag = 144;
pub const DELIVER_TO_LOCATION_ID: Tag = 145;
pub const XML_DATA_LEN: Tag = 212;
pub const XML_DATA: Tag = 213;
pub const MESSAGE_ENCODING: Tag = 347;
pub const ENCODED_TEXT_LEN: Tag = 354;
pub const ENCODED_TEXT: Tag = 355;
pub const LAST_MSG_SEQ_NUM_PROCESSED: Tag = 369;
pub const REF_TAG_ID: Tag = 371;
pub const REF_MSG_TYPE: Tag = 372;
pub const SESSION_REJECT_REASON: Tag = 373;
pub const BUSINESS_REJECT_REASON: Tag = 380;
pub const NO_HOPS: Tag = 627;
pub const HOP_COMP_ID: Tag = 628;
pub const HOP_SENDING_TIME: Tag = 629;
pub const HOP_REF_ID: Tag = 630;
pub const NEXT_EXPECTED_MSG_SEQ_NUM: Tag = 789;
pub const APPL_VER_ID: Tag = 1128;
pub const CSTM_APPL_VER_ID: Tag = 1129;
pub const DEFAULT_APPL_VER_ID: Tag = 1137;

/// Administrative message types.
pub mod msg_type {
    pub const HEARTBEAT: &str = "0";
    pub const TEST_REQUEST: &str = "1";
    pub const RESEND_REQUEST: &str = "2";
    pub const REJECT: &str = "3";
    pub const SEQUENCE_RESET: &str = "4";
    pub const LOGOUT: &str = "5";
    pub const LOGON: &str = "A";
    pub const BUSINESS_MESSAGE_REJECT: &str = "j";

    /// "n" (XMLnonFIX) counts as admin like in QuickFIX/n, so it reaches
    /// `from_admin` and skips message-definition validation.
    pub fn is_admin(msg_type: &str) -> bool {
        matches!(msg_type, "0" | "1" | "2" | "3" | "4" | "5" | "A" | "n")
    }
}

/// Tags belonging to the standard message header (FIX 4.x / FIXT.1.1 superset).
pub fn is_header_tag(tag: Tag) -> bool {
    matches!(
        tag,
        BEGIN_STRING
            | BODY_LENGTH
            | MSG_TYPE
            | SENDER_COMP_ID
            | TARGET_COMP_ID
            | ON_BEHALF_OF_COMP_ID
            | DELIVER_TO_COMP_ID
            | SECURE_DATA_LEN
            | SECURE_DATA
            | MSG_SEQ_NUM
            | SENDER_SUB_ID
            | SENDER_LOCATION_ID
            | TARGET_SUB_ID
            | TARGET_LOCATION_ID
            | ON_BEHALF_OF_SUB_ID
            | ON_BEHALF_OF_LOCATION_ID
            | DELIVER_TO_SUB_ID
            | DELIVER_TO_LOCATION_ID
            | POSS_DUP_FLAG
            | POSS_RESEND
            | SENDING_TIME
            | ORIG_SENDING_TIME
            | XML_DATA_LEN
            | XML_DATA
            | MESSAGE_ENCODING
            | LAST_MSG_SEQ_NUM_PROCESSED
            | NO_HOPS
            | HOP_COMP_ID
            | HOP_SENDING_TIME
            | HOP_REF_ID
            | APPL_VER_ID
            | CSTM_APPL_VER_ID
    )
}

/// Tags belonging to the standard message trailer.
pub fn is_trailer_tag(tag: Tag) -> bool {
    matches!(tag, SIGNATURE_LENGTH | SIGNATURE | CHECK_SUM)
}

/// Standard length-prefixed data fields: maps a Length tag to the data tag
/// that immediately follows it, whose value may legally contain SOH bytes.
/// A data dictionary can extend this set; these are the ones from the
/// standard dictionaries needed for dictionary-less parsing.
pub fn data_tag_for_length_tag(len_tag: Tag) -> Option<Tag> {
    Some(match len_tag {
        90 => 91,    // SecureDataLen -> SecureData
        93 => 89,    // SignatureLength -> Signature
        95 => 96,    // RawDataLength -> RawData
        212 => 213,  // XmlDataLen -> XmlData
        348 => 349,  // EncodedIssuerLen -> EncodedIssuer
        350 => 351,  // EncodedSecurityDescLen -> EncodedSecurityDesc
        352 => 353,  // EncodedListExecInstLen -> EncodedListExecInst
        354 => 355,  // EncodedTextLen -> EncodedText
        356 => 357,  // EncodedSubjectLen -> EncodedSubject
        358 => 359,  // EncodedHeadlineLen -> EncodedHeadline
        360 => 361,  // EncodedAllocTextLen -> EncodedAllocText
        362 => 363,  // EncodedUnderlyingIssuerLen -> EncodedUnderlyingIssuer
        364 => 365,  // EncodedUnderlyingSecurityDescLen -> EncodedUnderlyingSecurityDesc
        445 => 446,  // EncodedListStatusTextLen -> EncodedListStatusText
        618 => 619,  // EncodedLegIssuerLen -> EncodedLegIssuer
        621 => 622,  // EncodedLegSecurityDescLen -> EncodedLegSecurityDesc
        _ => return None,
    })
}
