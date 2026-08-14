//! The mutable, in-memory half of a collection (PRD §10: writes land here
//! after the WAL append and are searched before the immutable segments).
//!
//! Internal doc ids increase monotonically across the whole collection and are
//! never reused, so a memtable owns one contiguous range `[base, base + len)`
//! and can be a `Vec` rather than a map. A replaced or deleted document leaves
//! a hole; holes are reclaimed when the memtable is flushed into a segment.

use std::collections::HashMap;

use serde_json::Value as Json;

use tachyon_core::{CollectionSchema, DocId, FieldId, ParsedDocument, Value};

use crate::columns::Columns;
use crate::inverted::InvertedIndex;
use crate::tokenizer::{tokenize, Token};

/// Position gap inserted between the values of a multi-valued text field, so a
/// phrase query cannot match across two separate values.
pub const MULTI_VALUE_POSITION_GAP: u32 = 100;

/// A document as held in memory: the source we hand back in hits, plus the
/// coerced field values the index and columns are built from.
#[derive(Debug, Clone)]
pub struct StoredDoc {
    pub id: String,
    pub source: Json,
    pub values: Vec<Value>,
    /// Token count per field, positionally aligned with the schema. BM25's
    /// `|d|`; zero for fields that are not full-text or are absent.
    pub field_lengths: Vec<u32>,
}

impl StoredDoc {
    /// Rough heap cost, used to decide when to flush. An estimate is fine —
    /// the threshold it feeds is itself a heuristic.
    fn heap_bytes(&self) -> usize {
        let values: usize = self.values.iter().map(value_bytes).sum();
        self.id.len()
            + json_bytes(&self.source)
            + values
            + self.field_lengths.len() * std::mem::size_of::<u32>()
            + std::mem::size_of::<StoredDoc>()
    }
}

fn value_bytes(v: &Value) -> usize {
    match v {
        Value::Str(s) => s.len() + std::mem::size_of::<String>(),
        Value::Array(items) => {
            items.iter().map(value_bytes).sum::<usize>() + std::mem::size_of::<Vec<Value>>()
        }
        _ => std::mem::size_of::<Value>(),
    }
}

/// Tokenize a field value, handling the multi-valued case by inserting a
/// position gap between values.
fn tokenize_value(value: &Value) -> Vec<Token> {
    match value {
        Value::Str(s) => tokenize(s),
        Value::Array(_) => {
            let mut out: Vec<Token> = Vec::new();
            let mut offset = 0u32;
            for scalar in value.iter_scalars() {
                let Some(text) = scalar.as_str() else { continue };
                let tokens = tokenize(text);
                let len = tokens.len() as u32;
                out.extend(tokens.into_iter().map(|mut t| {
                    t.position += offset;
                    t
                }));
                if len > 0 {
                    offset += len + MULTI_VALUE_POSITION_GAP;
                }
            }
            out
        }
        _ => Vec::new(),
    }
}

fn json_bytes(v: &Json) -> usize {
    match v {
        Json::Null | Json::Bool(_) | Json::Number(_) => std::mem::size_of::<Json>(),
        Json::String(s) => s.len() + std::mem::size_of::<String>(),
        Json::Array(items) => {
            items.iter().map(json_bytes).sum::<usize>() + std::mem::size_of::<Json>()
        }
        Json::Object(map) => {
            map.iter().map(|(k, v)| k.len() + json_bytes(v)).sum::<usize>()
                + std::mem::size_of::<Json>()
        }
    }
}

#[derive(Debug)]
pub struct MemTable {
    /// Doc id of `docs[0]`.
    base: DocId,
    docs: Vec<Option<StoredDoc>>,
    /// User-facing id to internal doc id, for the live documents only.
    ids: HashMap<String, DocId>,
    index: InvertedIndex,
    columns: Columns,
    /// Field ids that are tokenized, cached from the schema.
    searchable: Vec<FieldId>,
    num_fields: usize,
    live: usize,
    heap_bytes: usize,
}

impl MemTable {
    /// Create a memtable whose first document will get id `base`.
    pub fn new(base: DocId, schema: &CollectionSchema) -> MemTable {
        MemTable {
            base,
            docs: Vec::new(),
            ids: HashMap::new(),
            index: InvertedIndex::new(schema.fields.len()),
            columns: Columns::new(schema),
            searchable: schema.searchable_field_ids(),
            num_fields: schema.fields.len(),
            live: 0,
            heap_bytes: 0,
        }
    }

    /// The inverted index over everything written since the last flush.
    pub fn index(&self) -> &InvertedIndex {
        &self.index
    }

    /// The filter, sort, and facet columns.
    pub fn columns(&self) -> &Columns {
        &self.columns
    }

    /// Whether a doc id refers to a live document in this memtable.
    pub fn is_live(&self, doc_id: DocId) -> bool {
        self.get(doc_id).is_some()
    }

    pub fn base(&self) -> DocId {
        self.base
    }

    /// Doc id that will be assigned to the next inserted document.
    pub fn next_doc_id(&self) -> DocId {
        self.base + self.docs.len() as DocId
    }

    /// Live document count, excluding replaced and deleted documents.
    pub fn len(&self) -> usize {
        self.live
    }

    pub fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// Approximate heap footprint of the stored documents.
    pub fn heap_bytes(&self) -> usize {
        self.heap_bytes
    }

    /// Every live document, in doc id order.
    pub fn iter(&self) -> impl Iterator<Item = (DocId, &StoredDoc)> {
        self.docs
            .iter()
            .enumerate()
            .filter_map(move |(i, slot)| slot.as_ref().map(|d| (self.base + i as DocId, d)))
    }

    /// Append a document, tokenizing its text fields into the inverted index,
    /// and return the doc id it was given.
    ///
    /// The caller is responsible for having already removed any previous
    /// version of this `id` — [`MemTable`] does not deduplicate, because the
    /// previous version may live in a segment it cannot see.
    pub fn insert(&mut self, doc: ParsedDocument) -> DocId {
        let doc_id = self.next_doc_id();
        let mut field_lengths = vec![0u32; self.num_fields];

        for &field in &self.searchable {
            let tokens = tokenize_value(doc.value(field));
            if tokens.is_empty() {
                continue;
            }
            field_lengths[field as usize] = tokens.len() as u32;
            self.index.add_field(doc_id, field, &tokens);
        }

        for (field, value) in doc.values.iter().enumerate() {
            self.columns.push(field as FieldId, doc_id, value);
        }

        let stored =
            StoredDoc { id: doc.id, source: doc.source, values: doc.values, field_lengths };
        self.heap_bytes += stored.heap_bytes();
        self.ids.insert(stored.id.clone(), doc_id);
        self.docs.push(Some(stored));
        self.live += 1;
        doc_id
    }

    /// Internal doc id for a user-facing id, if it lives in this memtable.
    pub fn lookup(&self, id: &str) -> Option<DocId> {
        self.ids.get(id).copied()
    }

    pub fn get(&self, doc_id: DocId) -> Option<&StoredDoc> {
        let idx = doc_id.checked_sub(self.base)? as usize;
        self.docs.get(idx)?.as_ref()
    }

    /// Drop a document. Returns `true` if it was present and live.
    pub fn remove(&mut self, doc_id: DocId) -> bool {
        let Some(idx) = doc_id.checked_sub(self.base).map(|i| i as usize) else {
            return false;
        };
        let Some(slot) = self.docs.get_mut(idx) else {
            return false;
        };
        match slot.take() {
            Some(doc) => {
                self.heap_bytes = self.heap_bytes.saturating_sub(doc.heap_bytes());
                // Only clear the id mapping if it still points at this doc: an
                // upsert re-inserts the id at a new doc id before the old one
                // is removed.
                if self.ids.get(&doc.id) == Some(&doc_id) {
                    self.ids.remove(&doc.id);
                }
                self.live -= 1;
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tachyon_core::{CollectionSchema, FieldSchema, FieldType};

    fn schema() -> CollectionSchema {
        CollectionSchema::new(
            "c",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("tags", FieldType::Text),
            ],
        )
    }

    fn memtable(base: DocId) -> MemTable {
        MemTable::new(base, &schema())
    }

    fn doc(id: &str, title: &str) -> ParsedDocument {
        ParsedDocument::parse(json!({ "id": id, "title": title }), &schema()).unwrap()
    }

    #[test]
    fn assigns_contiguous_ids_from_the_base() {
        let mut m = memtable(100);
        assert_eq!(m.insert(doc("a", "one")), 100);
        assert_eq!(m.insert(doc("b", "two")), 101);
        assert_eq!(m.next_doc_id(), 102);
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn looks_up_and_fetches() {
        let mut m = memtable(0);
        let id = m.insert(doc("a", "one"));
        assert_eq!(m.lookup("a"), Some(id));
        assert_eq!(m.get(id).unwrap().source["title"], json!("one"));
        assert!(m.lookup("missing").is_none());
        assert!(m.get(999).is_none());
    }

    #[test]
    fn removal_frees_the_slot_and_the_id() {
        let mut m = memtable(0);
        let id = m.insert(doc("a", "one"));
        assert!(m.remove(id));
        assert!(!m.remove(id), "second removal is a no-op");
        assert!(m.lookup("a").is_none());
        assert!(m.get(id).is_none());
        assert_eq!(m.len(), 0);
        // Ids keep increasing; the hole is not reused.
        assert_eq!(m.insert(doc("b", "two")), 1);
    }

    #[test]
    fn removing_an_old_version_keeps_the_new_mapping() {
        let mut m = memtable(0);
        let old = m.insert(doc("a", "one"));
        let new = m.insert(doc("a", "two"));
        assert!(m.remove(old));
        assert_eq!(m.lookup("a"), Some(new), "the newer version stays reachable");
    }

    #[test]
    fn iteration_skips_holes_and_is_ordered() {
        let mut m = memtable(10);
        let a = m.insert(doc("a", "one"));
        m.insert(doc("b", "two"));
        m.insert(doc("c", "three"));
        m.remove(a);
        let seen: Vec<_> = m.iter().map(|(id, d)| (id, d.id.clone())).collect();
        assert_eq!(seen, vec![(11, "b".to_string()), (12, "c".to_string())]);
    }

    #[test]
    fn heap_accounting_rises_and_falls() {
        let mut m = memtable(0);
        assert_eq!(m.heap_bytes(), 0);
        let id = m.insert(doc("a", "a fairly long title to make the delta obvious"));
        let after_insert = m.heap_bytes();
        assert!(after_insert > 0);
        m.remove(id);
        assert_eq!(m.heap_bytes(), 0);
    }

    #[test]
    fn doc_ids_below_the_base_are_not_ours() {
        let mut m = memtable(50);
        m.insert(doc("a", "one"));
        assert!(m.get(49).is_none());
        assert!(!m.remove(49));
    }
}
