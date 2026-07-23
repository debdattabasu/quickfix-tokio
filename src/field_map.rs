//! An ordered collection of FIX fields, preserving wire order and allowing
//! duplicate tags (as required for repeating groups).

use crate::error::ConversionError;
use crate::message::Tag;
use crate::value::{FixDecode, FixEncode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagValue {
    pub tag: Tag,
    pub value: Vec<u8>,
}

/// Ordered multi-map of FIX fields.
///
/// Fields are kept in insertion (wire) order. `set` replaces the first
/// occurrence of a tag in place; `push` always appends, which is what
/// repeating-group construction requires.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldMap {
    fields: Vec<TagValue>,
}

impl FieldMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn clear(&mut self) {
        self.fields.clear();
    }

    pub fn contains(&self, tag: Tag) -> bool {
        self.fields.iter().any(|f| f.tag == tag)
    }

    /// Raw bytes of the first occurrence of `tag`.
    pub fn get_raw(&self, tag: Tag) -> Option<&[u8]> {
        self.fields.iter().find(|f| f.tag == tag).map(|f| f.value.as_slice())
    }

    /// Decode the first occurrence of `tag` as `T`.
    pub fn get<T: FixDecode>(&self, tag: Tag) -> Result<T, ConversionError> {
        let raw = self.get_raw(tag).ok_or(ConversionError::FieldNotFound { tag })?;
        T::decode(tag, raw)
    }

    /// Decode the first occurrence of `tag` as `T`, or `None` when absent.
    pub fn get_opt<T: FixDecode>(&self, tag: Tag) -> Result<Option<T>, ConversionError> {
        match self.get_raw(tag) {
            Some(raw) => T::decode(tag, raw).map(Some),
            None => Ok(None),
        }
    }

    pub fn get_string(&self, tag: Tag) -> Result<String, ConversionError> {
        self.get::<String>(tag)
    }

    /// Replace the first occurrence of `tag` (keeping its position), or append.
    pub fn set(&mut self, tag: Tag, value: impl FixEncode) {
        let mut buf = Vec::new();
        value.encode(&mut buf);
        self.set_raw(tag, buf);
    }

    pub fn set_raw(&mut self, tag: Tag, value: Vec<u8>) {
        match self.fields.iter_mut().find(|f| f.tag == tag) {
            Some(f) => f.value = value,
            None => self.fields.push(TagValue { tag, value }),
        }
    }

    /// Append a field regardless of whether the tag already exists.
    pub fn push(&mut self, tag: Tag, value: impl FixEncode) {
        let mut buf = Vec::new();
        value.encode(&mut buf);
        self.fields.push(TagValue { tag, value: buf });
    }

    /// Remove all occurrences of `tag`. Returns true if anything was removed.
    pub fn remove(&mut self, tag: Tag) -> bool {
        let before = self.fields.len();
        self.fields.retain(|f| f.tag != tag);
        self.fields.len() != before
    }

    pub fn iter(&self) -> impl Iterator<Item = &TagValue> {
        self.fields.iter()
    }

    pub(crate) fn fields(&self) -> &[TagValue] {
        &self.fields
    }

    pub(crate) fn push_tag_value(&mut self, tv: TagValue) {
        self.fields.push(tv);
    }

    pub(crate) fn take_fields(&mut self) -> Vec<TagValue> {
        std::mem::take(&mut self.fields)
    }

    pub(crate) fn set_fields(&mut self, fields: Vec<TagValue>) {
        self.fields = fields;
    }

    /// Total serialized size of these fields: `tag=value<SOH>` for each.
    pub fn wire_len(&self) -> usize {
        self.fields
            .iter()
            .map(|f| dec_len(f.tag) + 1 + f.value.len() + 1)
            .sum()
    }

    /// Serialize fields in stored order as `tag=value<SOH>`.
    pub fn write_to(&self, buf: &mut Vec<u8>) {
        for f in &self.fields {
            write_tag_value(buf, f.tag, &f.value);
        }
    }

    // ----- repeating groups -----

    /// Read the repeating group counted by `template.num_tag`.
    ///
    /// Group instances are split on `template.delimiter()`. Any tag that is
    /// not in the template's member set terminates the group section.
    pub fn read_groups(&self, template: &GroupTemplate) -> Result<Vec<FieldMap>, ConversionError> {
        let Some(pos) = self.fields.iter().position(|f| f.tag == template.num_tag) else {
            return Ok(Vec::new());
        };
        let declared: usize = self.get(template.num_tag)?;

        let mut groups: Vec<FieldMap> = Vec::new();
        for f in &self.fields[pos + 1..] {
            if f.tag == template.delimiter() {
                groups.push(FieldMap::new());
            } else if groups.is_empty() || !template.is_member(f.tag) {
                break;
            }
            match groups.last_mut() {
                Some(g) => g.push_tag_value(f.clone()),
                // Member tag before the first delimiter: malformed group.
                None => {
                    return Err(ConversionError::InvalidValue {
                        tag: f.tag,
                        value: String::from_utf8_lossy(&f.value).into_owned(),
                    });
                }
            }
        }

        if groups.len() != declared {
            return Err(ConversionError::InvalidValue {
                tag: template.num_tag,
                value: declared.to_string(),
            });
        }
        Ok(groups)
    }

    /// Append repeating-group instances, setting/updating the count field.
    ///
    /// Instances must have the delimiter tag as their first field; members are
    /// appended verbatim in the order given.
    pub fn write_groups(&mut self, template: &GroupTemplate, groups: &[FieldMap]) {
        self.set(template.num_tag, groups.len());
        for g in groups {
            debug_assert_eq!(
                g.fields.first().map(|f| f.tag),
                Some(template.delimiter()),
                "group instance must start with its delimiter tag"
            );
            for f in &g.fields {
                self.fields.push(f.clone());
            }
        }
    }
}

/// Describes a repeating group: its NumInGroup counter tag and its member
/// tags in required order. The first member tag is the delimiter.
///
/// Members must include the tags of any nested groups (counter and members);
/// nested instances stay flat inside each returned `FieldMap` and can be
/// split further with the nested group's own template.
#[derive(Debug, Clone)]
pub struct GroupTemplate {
    pub num_tag: Tag,
    pub member_tags: Vec<Tag>,
}

impl GroupTemplate {
    pub fn new(num_tag: Tag, member_tags: Vec<Tag>) -> Self {
        assert!(!member_tags.is_empty(), "group template needs at least a delimiter tag");
        Self { num_tag, member_tags }
    }

    pub fn delimiter(&self) -> Tag {
        self.member_tags[0]
    }

    pub fn is_member(&self, tag: Tag) -> bool {
        self.member_tags.contains(&tag)
    }
}

pub(crate) fn write_tag_value(buf: &mut Vec<u8>, tag: Tag, value: &[u8]) {
    buf.extend_from_slice(tag.to_string().as_bytes());
    buf.push(b'=');
    buf.extend_from_slice(value);
    buf.push(crate::message::SOH);
}

fn dec_len(v: crate::message::Tag) -> usize {
    let (mut v, mut n) = if v < 0 { (-(v as i64), 2usize) } else { (v as i64, 1) };
    while v >= 10 {
        v /= 10;
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_replaces_push_appends() {
        let mut fm = FieldMap::new();
        fm.set(55, "TSLA");
        fm.set(54, '1');
        fm.set(55, "AAPL");
        assert_eq!(fm.len(), 2);
        assert_eq!(fm.get_string(55).unwrap(), "AAPL");

        fm.push(55, "MSFT");
        assert_eq!(fm.len(), 3);
        // get returns first occurrence
        assert_eq!(fm.get_string(55).unwrap(), "AAPL");
    }

    #[test]
    fn group_roundtrip() {
        // NoMDEntryTypes-style group: 267 counts, members [269]
        let tpl = GroupTemplate::new(267, vec![269]);
        let mut body = FieldMap::new();
        body.set(262, "REQ1");

        let mut g1 = FieldMap::new();
        g1.push(269, '0');
        let mut g2 = FieldMap::new();
        g2.push(269, '1');
        body.write_groups(&tpl, &[g1, g2]);

        assert_eq!(body.get::<usize>(267).unwrap(), 2);
        let groups = body.read_groups(&tpl).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].get::<char>(269).unwrap(), '0');
        assert_eq!(groups[1].get::<char>(269).unwrap(), '1');
    }

    #[test]
    fn group_count_mismatch_errors() {
        let tpl = GroupTemplate::new(267, vec![269]);
        let mut body = FieldMap::new();
        body.set(267, 3usize);
        body.push(269, '0');
        assert!(body.read_groups(&tpl).is_err());
    }

    #[test]
    fn group_terminates_on_foreign_tag() {
        let tpl = GroupTemplate::new(267, vec![269, 270]);
        let mut body = FieldMap::new();
        body.set(267, 1usize);
        body.push(269, '0');
        body.push(270, "101.5");
        body.push(58, "trailing field, not a member");
        let groups = body.read_groups(&tpl).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 2);
    }
}
