//! FIX data dictionary: loads the standard QuickFIX XML specs
//! (`spec/FIX44.xml` etc.) and validates messages against them.

use std::collections::{HashMap, HashSet};

use quick_xml::events::Event;

use crate::error::{Error, RejectError, Result, SessionRejectReason};
use crate::field_map::GroupTemplate;
use crate::message::{Message, Tag};
use crate::tags;

/// Tags >= this are user-defined per the FIX spec.
pub const USER_DEFINED_TAG_MIN: Tag = 5000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Int,
    Length,
    SeqNum,
    NumInGroup,
    DayOfMonth,
    Float,
    Qty,
    Price,
    PriceOffset,
    Amt,
    Percentage,
    Char,
    Boolean,
    String,
    Data,
    UtcTimestamp,
    UtcDateOnly,
    UtcTimeOnly,
    LocalMktDate,
    MonthYear,
    Other,
}

impl FieldType {
    fn from_name(name: &str) -> Self {
        match name {
            "INT" => Self::Int,
            "LENGTH" => Self::Length,
            "SEQNUM" => Self::SeqNum,
            "NUMINGROUP" => Self::NumInGroup,
            "DAYOFMONTH" => Self::DayOfMonth,
            "FLOAT" => Self::Float,
            "QTY" | "QUANTITY" => Self::Qty,
            "PRICE" => Self::Price,
            "PRICEOFFSET" => Self::PriceOffset,
            "AMT" => Self::Amt,
            "PERCENTAGE" => Self::Percentage,
            "CHAR" => Self::Char,
            "BOOLEAN" => Self::Boolean,
            "STRING" | "MULTIPLEVALUESTRING" | "MULTIPLESTRINGVALUE" | "MULTIPLECHARVALUE"
            | "COUNTRY" | "CURRENCY" | "EXCHANGE" | "LANGUAGE" => Self::String,
            "DATA" | "XMLDATA" => Self::Data,
            "UTCTIMESTAMP" | "TIME" => Self::UtcTimestamp,
            "UTCDATEONLY" | "UTCDATE" | "DATE" => Self::UtcDateOnly,
            "UTCTIMEONLY" => Self::UtcTimeOnly,
            "LOCALMKTDATE" => Self::LocalMktDate,
            "MONTHYEAR" => Self::MonthYear,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FieldDef {
    pub tag: Tag,
    pub name: String,
    pub field_type: FieldType,
    /// Allowed values (enum fields); empty = unrestricted.
    pub values: HashSet<String>,
}

#[derive(Debug, Clone, Default)]
pub struct GroupDef {
    pub counter: Tag,
    pub delimiter: Tag,
    /// All member tags, including nested groups' tags.
    pub tags: HashSet<Tag>,
    pub required: Vec<Tag>,
    pub groups: HashMap<Tag, GroupDef>,
}

#[derive(Debug, Clone, Default)]
pub struct MessageDef {
    pub name: String,
    pub msg_type: String,
    pub tags: HashSet<Tag>,
    pub required: Vec<Tag>,
    pub groups: HashMap<Tag, GroupDef>,
}

#[derive(Debug, Clone, Default)]
pub struct DataDictionary {
    pub begin_string: String,
    pub fields_by_tag: HashMap<Tag, FieldDef>,
    pub tags_by_name: HashMap<String, Tag>,
    pub header_tags: HashSet<Tag>,
    pub header_required: Vec<Tag>,
    pub trailer_tags: HashSet<Tag>,
    pub trailer_required: Vec<Tag>,
    pub messages: HashMap<String, MessageDef>,
}

/// Validation toggles (classic QuickFIX setting names).
#[derive(Debug, Clone)]
pub struct ValidationSettings {
    pub check_fields_out_of_order: bool,
    pub check_fields_have_values: bool,
    pub check_user_defined_fields: bool,
    pub allow_unknown_message_fields: bool,
}

impl Default for ValidationSettings {
    fn default() -> Self {
        Self {
            check_fields_out_of_order: true,
            check_fields_have_values: true,
            check_user_defined_fields: true,
            allow_unknown_message_fields: false,
        }
    }
}

// ----- XML loading -----

/// Minimal DOM for the dictionary document.
struct Node {
    name: String,
    attrs: HashMap<String, String>,
    children: Vec<Node>,
}

fn parse_xml(text: &str) -> Result<Node> {
    let mut reader = quick_xml::Reader::from_str(text);
    reader.config_mut().trim_text(true);
    let mut stack: Vec<Node> = vec![Node {
        name: "(root)".into(),
        attrs: HashMap::new(),
        children: Vec::new(),
    }];

    let read_node = |e: &quick_xml::events::BytesStart<'_>| -> Result<Node> {
        let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
        let mut attrs = HashMap::new();
        for attr in e.attributes() {
            let attr = attr.map_err(|e| Error::Dictionary(format!("bad attribute: {e}")))?;
            attrs.insert(
                String::from_utf8_lossy(attr.key.as_ref()).into_owned(),
                String::from_utf8_lossy(&attr.value).into_owned(),
            );
        }
        Ok(Node { name, attrs, children: Vec::new() })
    };

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => stack.push(read_node(&e)?),
            Ok(Event::Empty(e)) => {
                let node = read_node(&e)?;
                stack.last_mut().unwrap().children.push(node);
            }
            Ok(Event::End(_)) => {
                let node = stack.pop().unwrap();
                stack
                    .last_mut()
                    .ok_or_else(|| Error::Dictionary("unbalanced XML".into()))?
                    .children
                    .push(node);
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(Error::Dictionary(format!("XML parse error: {e}"))),
        }
    }
    let mut root = stack.pop().ok_or_else(|| Error::Dictionary("empty document".into()))?;
    root.children
        .drain(..)
        .find(|n| n.name == "fix")
        .ok_or_else(|| Error::Dictionary("no <fix> root element".into()))
}

impl DataDictionary {
    pub fn parse(text: &str) -> Result<Self> {
        let fix = parse_xml(text)?;
        let major = fix.attrs.get("major").cloned().unwrap_or_default();
        let minor = fix.attrs.get("minor").cloned().unwrap_or_default();
        let fixt = fix.attrs.get("type").map(|t| t == "FIXT").unwrap_or(false);
        let mut dd = DataDictionary {
            begin_string: if fixt {
                format!("FIXT.{major}.{minor}")
            } else {
                format!("FIX.{major}.{minor}")
            },
            ..Default::default()
        };
        // Pre-FIX.4.2 dictionaries type most fields as CHAR meaning "string";
        // single-character CHAR semantics only exist from 4.2 on.
        let char_is_string = !fixt && dd.begin_string.as_str() < "FIX.4.2";

        // Pass 1: field definitions (name -> tag/type/enums).
        let fields = child(&fix, "fields");
        if let Some(fields) = fields {
            for f in fields.children.iter().filter(|c| c.name == "field") {
                let tag: Tag = f
                    .attrs
                    .get("number")
                    .and_then(|n| n.parse().ok())
                    .ok_or_else(|| Error::Dictionary("field without number".into()))?;
                let name = f.attrs.get("name").cloned().unwrap_or_default();
                let mut field_type =
                    FieldType::from_name(f.attrs.get("type").map(|s| s.as_str()).unwrap_or(""));
                if char_is_string && field_type == FieldType::Char {
                    field_type = FieldType::String;
                }
                let values = f
                    .children
                    .iter()
                    .filter(|c| c.name == "value")
                    .filter_map(|c| c.attrs.get("enum").cloned())
                    .collect();
                dd.tags_by_name.insert(name.clone(), tag);
                dd.fields_by_tag.insert(tag, FieldDef { tag, name, field_type, values });
            }
        }

        // Component definitions, unexpanded.
        let mut components: HashMap<&str, &Node> = HashMap::new();
        if let Some(comps) = child(&fix, "components") {
            for c in comps.children.iter().filter(|c| c.name == "component") {
                if let Some(name) = c.attrs.get("name") {
                    components.insert(name, c);
                }
            }
        }

        // Header / trailer.
        if let Some(header) = child(&fix, "header") {
            let mut def = MessageDef::default();
            dd.collect(header, &components, &mut def.tags, &mut def.required, &mut def.groups)?;
            dd.header_tags = def.tags;
            dd.header_required = def.required;
        }
        if let Some(trailer) = child(&fix, "trailer") {
            let mut def = MessageDef::default();
            dd.collect(trailer, &components, &mut def.tags, &mut def.required, &mut def.groups)?;
            dd.trailer_tags = def.tags;
            dd.trailer_required = def.required;
        }

        // Messages.
        if let Some(messages) = child(&fix, "messages") {
            for m in messages.children.iter().filter(|c| c.name == "message") {
                let mut def = MessageDef {
                    name: m.attrs.get("name").cloned().unwrap_or_default(),
                    msg_type: m
                        .attrs
                        .get("msgtype")
                        .cloned()
                        .ok_or_else(|| Error::Dictionary("message without msgtype".into()))?,
                    ..Default::default()
                };
                dd.collect(m, &components, &mut def.tags, &mut def.required, &mut def.groups)?;
                dd.messages.insert(def.msg_type.clone(), def);
            }
        }
        Ok(dd)
    }

    pub async fn load(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let text = tokio::fs::read_to_string(path).await?;
        Self::parse(&text)
    }

    /// Recursively collect fields/components/groups of a message-like node.
    fn collect(
        &self,
        node: &Node,
        components: &HashMap<&str, &Node>,
        tags_out: &mut HashSet<Tag>,
        required_out: &mut Vec<Tag>,
        groups_out: &mut HashMap<Tag, GroupDef>,
    ) -> Result<()> {
        for c in &node.children {
            let required = c.attrs.get("required").map(|r| r == "Y").unwrap_or(false);
            match c.name.as_str() {
                "field" => {
                    let name = c.attrs.get("name").cloned().unwrap_or_default();
                    let tag = *self
                        .tags_by_name
                        .get(&name)
                        .ok_or_else(|| Error::Dictionary(format!("unknown field {name}")))?;
                    tags_out.insert(tag);
                    if required {
                        required_out.push(tag);
                    }
                }
                "component" => {
                    let name = c.attrs.get("name").cloned().unwrap_or_default();
                    let def = components
                        .get(name.as_str())
                        .ok_or_else(|| Error::Dictionary(format!("unknown component {name}")))?;
                    // Component fields are required only if the component is.
                    let mut comp_required = Vec::new();
                    self.collect(def, components, tags_out, &mut comp_required, groups_out)?;
                    if required {
                        required_out.extend(comp_required);
                    }
                }
                "group" => {
                    let name = c.attrs.get("name").cloned().unwrap_or_default();
                    let counter = *self
                        .tags_by_name
                        .get(&name)
                        .ok_or_else(|| Error::Dictionary(format!("unknown group {name}")))?;
                    tags_out.insert(counter);
                    if required {
                        required_out.push(counter);
                    }
                    let mut g = GroupDef { counter, ..Default::default() };
                    let mut member_order: Vec<Tag> = Vec::new();
                    self.collect_group(c, components, &mut g, &mut member_order)?;
                    g.delimiter = *member_order
                        .first()
                        .ok_or_else(|| Error::Dictionary(format!("empty group {name}")))?;
                    // Group member tags also count as "in message" for
                    // tag-allowed checks.
                    tags_out.extend(g.tags.iter().copied());
                    groups_out.insert(counter, g);
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn collect_group(
        &self,
        node: &Node,
        components: &HashMap<&str, &Node>,
        group: &mut GroupDef,
        member_order: &mut Vec<Tag>,
    ) -> Result<()> {
        for c in &node.children {
            let required = c.attrs.get("required").map(|r| r == "Y").unwrap_or(false);
            match c.name.as_str() {
                "field" => {
                    let name = c.attrs.get("name").cloned().unwrap_or_default();
                    let tag = *self
                        .tags_by_name
                        .get(&name)
                        .ok_or_else(|| Error::Dictionary(format!("unknown field {name}")))?;
                    group.tags.insert(tag);
                    member_order.push(tag);
                    if required {
                        group.required.push(tag);
                    }
                }
                "component" => {
                    let name = c.attrs.get("name").cloned().unwrap_or_default();
                    let def = components
                        .get(name.as_str())
                        .ok_or_else(|| Error::Dictionary(format!("unknown component {name}")))?;
                    self.collect_group(def, components, group, member_order)?;
                }
                "group" => {
                    let name = c.attrs.get("name").cloned().unwrap_or_default();
                    let counter = *self
                        .tags_by_name
                        .get(&name)
                        .ok_or_else(|| Error::Dictionary(format!("unknown group {name}")))?;
                    group.tags.insert(counter);
                    member_order.push(counter);
                    if required {
                        group.required.push(counter);
                    }
                    let mut nested = GroupDef { counter, ..Default::default() };
                    let mut nested_order = Vec::new();
                    self.collect_group(c, components, &mut nested, &mut nested_order)?;
                    nested.delimiter = *nested_order
                        .first()
                        .ok_or_else(|| Error::Dictionary(format!("empty group {name}")))?;
                    group.tags.extend(nested.tags.iter().copied());
                    group.groups.insert(counter, nested);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Reorder a message body into the canonical form the reference engines
    /// produce: top-level fields ascending by tag, with each repeating-group
    /// block kept intact (in received/insertion order) under its counter tag.
    pub fn canonicalize_body(&self, msg: &mut Message) {
        let Ok(mt) = msg.msg_type() else { return };
        let Some(def) = self.messages.get(&mt) else { return };
        let fields = msg.body.take_fields();

        let mut segments: Vec<(Tag, Vec<crate::field_map::TagValue>)> = Vec::new();
        let mut i = 0;
        while i < fields.len() {
            let tag = fields[i].tag;
            let mut seg = vec![fields[i].clone()];
            i += 1;
            if let Some(group) = def.groups.get(&tag) {
                while i < fields.len() && group.tags.contains(&fields[i].tag) {
                    seg.push(fields[i].clone());
                    i += 1;
                }
            }
            segments.push((tag, seg));
        }
        segments.sort_by_key(|s| s.0);
        msg.body.set_fields(segments.into_iter().flat_map(|(_, seg)| seg).collect());
    }

    /// A ready-made [`GroupTemplate`] for reading a repeating group of this
    /// message type (top-level groups only).
    pub fn group_template(&self, msg_type: &str, counter: Tag) -> Option<GroupTemplate> {
        let g = self.messages.get(msg_type)?.groups.get(&counter)?;
        let mut members = vec![g.delimiter];
        members.extend(g.tags.iter().copied().filter(|&t| t != g.delimiter));
        Some(GroupTemplate::new(counter, members))
    }

    // ----- validation -----

    /// Validate a parsed message. Returns the reject that should be sent
    /// when it fails.
    pub fn validate(
        &self,
        msg: &Message,
        settings: &ValidationSettings,
    ) -> std::result::Result<(), RejectError> {
        let msg_type = msg
            .header
            .get_string(tags::MSG_TYPE)
            .map_err(|_| RejectError::with_tag(SessionRejectReason::RequiredTagMissing, tags::MSG_TYPE))?;
        // XMLnonFIX (35=n) is accepted without a message definition,
        // matching QuickFIX/n.
        if msg_type == "n" {
            return Ok(());
        }
        let def = self.messages.get(&msg_type).ok_or_else(|| {
            // Only FIX.4.2 cites RefTagID=35 on an invalid MsgType.
            if self.begin_string == "FIX.4.2" {
                RejectError::with_tag(SessionRejectReason::InvalidMsgType, tags::MSG_TYPE)
            } else {
                RejectError::new(SessionRejectReason::InvalidMsgType)
            }
        })?;

        if settings.check_fields_out_of_order {
            if let Some(tag) = msg.structure_error() {
                return Err(RejectError::with_tag(
                    SessionRejectReason::TagSpecifiedOutOfRequiredOrder,
                    tag,
                ));
            }
        }

        // A repeating group's first entry must begin with its delimiter.
        // Groups are checked in wire order so the first violation wins.
        let body_fields: Vec<_> = msg.body.iter().collect();
        for (i, f) in body_fields.iter().enumerate() {
            let Some(group) = def.groups.get(&f.tag) else { continue };
            let declared: u64 = msg.body.get_opt(group.counter).ok().flatten().unwrap_or(0);
            if declared == 0 {
                continue;
            }
            if let Some(first) = body_fields.get(i + 1) {
                if group.tags.contains(&first.tag) && first.tag != group.delimiter {
                    return Err(RejectError::other(
                        format!(
                            "Group {}'s first entry does not start with delimiter {}",
                            group.counter, group.delimiter
                        ),
                        group.counter,
                    ));
                }
            }
        }

        // Required fields.
        for &tag in &self.header_required {
            if !msg.header.contains(tag) {
                return Err(RejectError::with_tag(SessionRejectReason::RequiredTagMissing, tag));
            }
        }
        for &tag in &self.trailer_required {
            if !msg.trailer.contains(tag) {
                return Err(RejectError::with_tag(SessionRejectReason::RequiredTagMissing, tag));
            }
        }
        for &tag in &def.required {
            if !msg.body.contains(tag) {
                return Err(RejectError::with_tag(SessionRejectReason::RequiredTagMissing, tag));
            }
        }

        // Duplicate tags: only repeating-group members may repeat.
        let mut group_member_tags: HashSet<Tag> = HashSet::new();
        for g in def.groups.values() {
            group_member_tags.extend(g.tags.iter().copied());
        }
        let mut seen: HashSet<Tag> = HashSet::new();
        for f in msg.body.iter() {
            if !group_member_tags.contains(&f.tag) && !seen.insert(f.tag) {
                return Err(RejectError::with_tag(
                    SessionRejectReason::TagAppearsMoreThanOnce,
                    f.tag,
                ));
            }
        }

        // Per-field checks.
        for section in [&msg.header, &msg.body, &msg.trailer] {
            let is_body = std::ptr::eq(section, &msg.body);
            for f in section.iter() {
                self.check_field(f.tag, &f.value, is_body, def, settings)?;
            }
        }

        // Group counts: declared NumInGroup vs actual delimiter count.
        for group in def.groups.values() {
            if let Some(raw) = msg.body.get_raw(group.counter) {
                let declared: usize = std::str::from_utf8(raw)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| {
                        RejectError::with_tag(
                            SessionRejectReason::IncorrectDataFormatForValue,
                            group.counter,
                        )
                    })?;
                let actual =
                    msg.body.iter().filter(|f| f.tag == group.delimiter).count();
                if declared != actual {
                    return Err(RejectError::with_tag(
                        SessionRejectReason::IncorrectNumInGroupCountForRepeatingGroup,
                        group.counter,
                    ));
                }
            }
        }
        Ok(())
    }

    fn check_field(
        &self,
        tag: Tag,
        value: &[u8],
        is_body: bool,
        def: &MessageDef,
        settings: &ValidationSettings,
    ) -> std::result::Result<(), RejectError> {
        if tag >= USER_DEFINED_TAG_MIN && !settings.check_user_defined_fields {
            return Ok(());
        }
        if settings.check_fields_have_values && value.is_empty() {
            return Err(RejectError::with_tag(
                SessionRejectReason::TagSpecifiedWithoutAValue,
                tag,
            ));
        }
        let Some(field) = self.fields_by_tag.get(&tag) else {
            // User-defined tags were already skipped above when
            // ValidateUserDefinedFields=N; here an unknown tag is an error
            // unless unknown fields are allowed outright.
            if settings.allow_unknown_message_fields {
                return Ok(());
            }
            return Err(RejectError::with_tag(SessionRejectReason::InvalidTagNumber, tag));
        };
        if is_body
            && !settings.allow_unknown_message_fields
            && !def.tags.contains(&tag)
            && tag < USER_DEFINED_TAG_MIN
        {
            return Err(RejectError::with_tag(
                SessionRejectReason::TagNotDefinedForThisMessageType,
                tag,
            ));
        }
        // Format before enum membership: a malformed value is "incorrect
        // data format" (373=6), not "value out of range" (373=5).
        self.check_format(field, value).map_err(|_| {
            RejectError::with_tag(SessionRejectReason::IncorrectDataFormatForValue, tag)
        })?;
        if !field.values.is_empty() {
            let v = String::from_utf8_lossy(value);
            // MultipleValue fields carry space-separated entries.
            let ok = v.split(' ').all(|part| field.values.contains(part));
            if !ok {
                return Err(RejectError::with_tag(SessionRejectReason::ValueIsIncorrect, tag));
            }
        }
        Ok(())
    }

    fn check_format(&self, field: &FieldDef, value: &[u8]) -> std::result::Result<(), ()> {
        use crate::value::{FixDate, FixDecode, UtcTimestamp};
        let ok = match field.field_type {
            FieldType::Int => i64::decode(field.tag, value).is_ok(),
            FieldType::Length | FieldType::SeqNum | FieldType::NumInGroup => {
                u64::decode(field.tag, value).is_ok()
            }
            FieldType::DayOfMonth => {
                u64::decode(field.tag, value).map(|d| (1..=31).contains(&d)).unwrap_or(false)
            }
            FieldType::Float
            | FieldType::Qty
            | FieldType::Price
            | FieldType::PriceOffset
            | FieldType::Amt
            | FieldType::Percentage => f64::decode(field.tag, value).is_ok(),
            FieldType::Char => value.len() == 1,
            FieldType::Boolean => matches!(value, b"Y" | b"N"),
            FieldType::UtcTimestamp => UtcTimestamp::decode(field.tag, value).is_ok(),
            FieldType::UtcDateOnly | FieldType::LocalMktDate => {
                FixDate::decode(field.tag, value).is_ok()
            }
            FieldType::MonthYear => {
                value.len() >= 6 && value[..6].iter().all(|b| b.is_ascii_digit())
            }
            FieldType::UtcTimeOnly | FieldType::String | FieldType::Data | FieldType::Other => {
                true
            }
        };
        if ok { Ok(()) } else { Err(()) }
    }
}

fn child<'a>(node: &'a Node, name: &str) -> Option<&'a Node> {
    node.children.iter().find(|c| c.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fix44() -> DataDictionary {
        let text = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/spec/FIX44.xml"
        ))
        .unwrap();
        DataDictionary::parse(&text).unwrap()
    }

    fn valid_order() -> Message {
        let mut m = Message::with_type("D");
        m.header.set(tags::BEGIN_STRING, "FIX.4.4");
        m.header.set(tags::SENDER_COMP_ID, "A");
        m.header.set(tags::TARGET_COMP_ID, "B");
        m.header.set(tags::MSG_SEQ_NUM, 2u64);
        m.stamp_sending_time(crate::value::UtcTimestamp::now());
        m.set(11, "ORDER-1");
        m.set(55, "TSLA");
        m.set(54, '1');
        m.set(60, crate::value::UtcTimestamp::now());
        m.set(40, '1');
        m
    }

    #[test]
    fn loads_fix44_spec() {
        let dd = fix44();
        assert_eq!(dd.begin_string, "FIX.4.4");
        assert!(dd.messages.contains_key("D"), "NewOrderSingle should exist");
        assert!(dd.header_tags.contains(&tags::MSG_SEQ_NUM));
        assert_eq!(dd.fields_by_tag[&54].field_type, FieldType::Char);
        assert!(dd.fields_by_tag[&54].values.contains("1"));
        // NewOrderSingle has the NoPartyIDs group via the Parties component.
        let d = &dd.messages["D"];
        assert!(d.groups.contains_key(&453), "NoPartyIDs group expected");
        assert_eq!(d.groups[&453].delimiter, 448);
    }

    #[test]
    fn validates_good_message() {
        let dd = fix44();
        let msg =
            Message::parse(&valid_order().to_bytes(), true).expect("roundtrip");
        dd.validate(&msg, &ValidationSettings::default()).expect("should validate");
    }

    #[test]
    fn missing_required_field_rejected() {
        let dd = fix44();
        let mut order = valid_order();
        order.body.remove(11); // ClOrdID is required='Y' in NewOrderSingle
        let msg = Message::parse(&order.to_bytes(), true).unwrap();
        let err = dd.validate(&msg, &ValidationSettings::default()).unwrap_err();
        assert_eq!(err.reason, SessionRejectReason::RequiredTagMissing);
        assert_eq!(err.ref_tag, Some(11));
    }

    #[test]
    fn bad_enum_value_rejected() {
        let dd = fix44();
        let mut order = valid_order();
        order.set(54, 'Z'); // not a valid Side
        let msg = Message::parse(&order.to_bytes(), true).unwrap();
        let err = dd.validate(&msg, &ValidationSettings::default()).unwrap_err();
        assert_eq!(err.reason, SessionRejectReason::ValueIsIncorrect);
        assert_eq!(err.ref_tag, Some(54));
    }

    #[test]
    fn undefined_tag_rejected() {
        let dd = fix44();
        let mut order = valid_order();
        order.set(4999, "bogus"); // not defined in FIX44
        let msg = Message::parse(&order.to_bytes(), true).unwrap();
        let err = dd.validate(&msg, &ValidationSettings::default()).unwrap_err();
        assert_eq!(err.reason, SessionRejectReason::InvalidTagNumber);
    }

    #[test]
    fn tag_not_defined_for_message_type() {
        let dd = fix44();
        let mut order = valid_order();
        order.set(112, "TR1"); // TestReqID doesn't belong in NewOrderSingle
        let msg = Message::parse(&order.to_bytes(), true).unwrap();
        let err = dd.validate(&msg, &ValidationSettings::default()).unwrap_err();
        assert_eq!(err.reason, SessionRejectReason::TagNotDefinedForThisMessageType);
        assert_eq!(err.ref_tag, Some(112));
    }

    #[test]
    fn wrong_group_count_rejected() {
        let dd = fix44();
        let mut order = valid_order();
        order.set(453, 2u32); // declare two parties...
        order.body.push(448, "PARTY-A"); // ...but provide one
        order.body.push(447, 'D');
        order.body.push(452, 1u32);
        let msg = Message::parse(&order.to_bytes(), true).unwrap();
        let err = dd.validate(&msg, &ValidationSettings::default()).unwrap_err();
        assert_eq!(
            err.reason,
            SessionRejectReason::IncorrectNumInGroupCountForRepeatingGroup
        );
    }

    #[test]
    fn unknown_msg_type_rejected() {
        let dd = fix44();
        let mut m = valid_order();
        m.header.set(tags::MSG_TYPE, "ZZ");
        let msg = Message::parse(&m.to_bytes(), true).unwrap();
        let err = dd.validate(&msg, &ValidationSettings::default()).unwrap_err();
        assert_eq!(err.reason, SessionRejectReason::InvalidMsgType);
    }

    #[test]
    fn group_template_reads_parties() {
        let dd = fix44();
        let tpl = dd.group_template("D", 453).unwrap();
        assert_eq!(tpl.num_tag, 453);
        assert_eq!(tpl.delimiter(), 448);

        let mut order = valid_order();
        let mut g1 = crate::field_map::FieldMap::new();
        g1.push(448, "PARTY-A");
        g1.push(447, 'D');
        let mut g2 = crate::field_map::FieldMap::new();
        g2.push(448, "PARTY-B");
        g2.push(447, 'D');
        order.body.write_groups(&tpl, &[g1, g2]);

        let msg = Message::parse(&order.to_bytes(), true).unwrap();
        dd.validate(&msg, &ValidationSettings::default()).expect("groups valid");
        let groups = msg.body.read_groups(&tpl).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[1].get_string(448).unwrap(), "PARTY-B");
    }
}
