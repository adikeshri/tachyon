//! Reads an on-disk segment back into query-able structures, behind
//! [`IndexSource`] — the same interface [`MemTable`](crate::memtable::MemTable)
//! implements, so the executor never needs to know which one it is holding.
//!
//! Everything here is mmap'd, and everything decodes lazily: opening a
//! segment costs O(number of terms) + O(number of fields) + O(corpus size /
//! 8) for the presence bitmap — never O(corpus size × document size). A
//! query pays only for the postings, columns, and document values it
//! actually touches, decoded fresh from the mapped pages each time and never
//! retained. See `codec.rs`'s module doc for the on-disk layout this reads.

use std::borrow::Cow;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use memmap2::{Mmap, MmapOptions};
use roaring::RoaringBitmap;
use serde_json::Value as Json;
use tachyon_core::{CollectionSchema, DocId, Error, FieldId, Result, Value};

use crate::columns::{KeywordColumn, NumericColumn};
use crate::cursor::PostingCursor;
use crate::fuzzy::FuzzyMatcher;
use crate::source::IndexSource;

use super::codec::{self, ColHeader, DocHeader, PostHeader};
use super::cursor::SegmentPostingCursor;
use super::format::{HEADER_LEN, IDS_MAGIC, TERMS_MAGIC};

/// The five files one segment is stored as.
#[derive(Debug, Clone)]
pub struct SegmentFilePaths {
    pub terms: PathBuf,
    pub ids: PathBuf,
    pub post: PathBuf,
    pub col: PathBuf,
    pub doc: PathBuf,
}

pub struct SegmentReader {
    terms: fst::Map<Mmap>,
    ids: fst::Map<Mmap>,
    post: Mmap,
    post_header: PostHeader,
    col: Mmap,
    col_header: ColHeader,
    doc: Mmap,
    doc_header: DocHeader,
}

/// Map a whole file. Used for `.post`/`.col`/`.doc`, whose own headers (read
/// by `codec::decode_*_header`) start at byte 0.
///
/// # Safety
///
/// `Mmap::map` is unsafe because another process mutating or truncating the
/// file while it's mapped is undefined behavior. Segment files never are:
/// they're written once via `write_atomic` (temp file, then rename) and a
/// given segment id is only opened after `state.json` commits it — nothing
/// in Tachyon's commit protocol ever rewrites a file in place once a reader
/// might be mapping it, in this process or any other, since `Collection::open`
/// only ever names ids `state.json` already lists.
fn open_mmap(path: &Path) -> Result<Mmap> {
    let file = File::open(path)?;
    unsafe { Mmap::map(&file) }.map_err(Error::from)
}

/// Map an `fst::Map`'s file starting just past its header, so the FST sees
/// nothing but its own bytes — `fst::Map::new` requires that.
fn open_fst(path: &Path, magic: &[u8; 8]) -> Result<fst::Map<Mmap>> {
    let mut file = File::open(path)?;
    let mut header = [0u8; HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|e| Error::corruption(format!("{}: {e}", path.display())))?;
    codec::validate_header(&header, magic, "segment fst")?;

    // SAFETY: see `open_mmap` — the same immutability guarantee applies.
    let payload = unsafe { MmapOptions::new().offset(HEADER_LEN as u64).map(&file) }?;
    fst::Map::new(payload).map_err(|e| Error::corruption(format!("{}: {e}", path.display())))
}

impl SegmentReader {
    /// Open and validate all five files. Actual I/O and mmap'ing happen only
    /// here; every other method just reads from the pages already mapped.
    pub fn open(paths: &SegmentFilePaths, schema: &CollectionSchema) -> Result<SegmentReader> {
        let terms = open_fst(&paths.terms, TERMS_MAGIC)?;
        let ids = open_fst(&paths.ids, IDS_MAGIC)?;

        let post = open_mmap(&paths.post)?;
        let post_header = codec::decode_post_header(&post)?;

        let col = open_mmap(&paths.col)?;
        let col_header = codec::decode_col_header(&col)?;

        let doc = open_mmap(&paths.doc)?;
        let doc_header = codec::decode_doc_header(&doc)?;

        if post_header.field_doc_count.len() != schema.fields.len() {
            return Err(Error::corruption(format!(
                "segment at {}: field count does not match the collection schema",
                paths.doc.display()
            )));
        }

        Ok(SegmentReader { terms, ids, post, post_header, col, col_header, doc, doc_header })
    }

    /// Segment-local id resolution, mirroring `MemTable::lookup`. Not part of
    /// [`IndexSource`], which has no string-id concept — `Collection` calls
    /// this directly on the concrete type.
    pub fn lookup_id(&self, id: &str) -> Option<DocId> {
        self.terms_get(&self.ids, id)
    }

    fn terms_get(&self, map: &fst::Map<Mmap>, key: &str) -> Option<DocId> {
        map.get(key).map(|v| v as DocId)
    }

    /// Segment-local full document fetch, mirroring `MemTable::get`. Owned,
    /// not borrowed: the source JSON is parsed fresh from the mapped bytes on
    /// every call rather than held decoded between calls.
    pub fn get(&self, doc_id: DocId) -> Option<Json> {
        if !self.doc_header.presence.contains(doc_id) {
            return None;
        }
        match codec::source_at(&self.doc, &self.doc_header, doc_id) {
            Ok(source) => source,
            Err(e) => {
                tracing::warn!(doc_id, error = %e, "segment: failed to decode document source");
                None
            }
        }
    }
}

impl IndexSource for SegmentReader {
    fn min_doc_id(&self) -> DocId {
        self.doc_header.base
    }

    fn end_doc_id(&self) -> DocId {
        self.doc_header.end
    }

    fn value(&self, doc_id: DocId, field: FieldId) -> Option<Cow<'_, Value>> {
        if !self.doc_header.presence.contains(doc_id) {
            return None;
        }
        match codec::value_at(&self.doc, &self.doc_header, doc_id, field) {
            Ok(v) => v.map(Cow::Owned),
            Err(e) => {
                tracing::warn!(doc_id, field, error = %e, "segment: failed to decode value");
                None
            }
        }
    }

    fn numeric_column(&self, field: FieldId) -> Option<Cow<'_, NumericColumn>> {
        match codec::decode_numeric_column(&self.col, &self.col_header, field) {
            Ok(col) => col.map(Cow::Owned),
            Err(e) => {
                tracing::warn!(field, error = %e, "segment: failed to decode numeric column");
                None
            }
        }
    }

    fn keyword_column(&self, field: FieldId) -> Option<Cow<'_, KeywordColumn>> {
        match codec::decode_keyword_column(&self.col, &self.col_header, field) {
            Ok(col) => col.map(Cow::Owned),
            Err(e) => {
                tracing::warn!(field, error = %e, "segment: failed to decode keyword column");
                None
            }
        }
    }

    fn posting_cursor(&self, term: &str, field: FieldId) -> Option<Box<dyn PostingCursor + '_>> {
        let term_id = self.terms.get(term)?;
        match codec::decode_term_field_blocks(&self.post, &self.post_header, term_id, field) {
            Ok(Some(term_field)) => {
                Some(Box::new(SegmentPostingCursor::new(&self.post, term_field)) as Box<dyn PostingCursor + '_>)
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(term, field, error = %e, "segment: failed to decode postings");
                None
            }
        }
    }

    // Overridden rather than left to the trait's default (which goes through
    // `posting_cursor()`): both only ever need doc ids, never positions, and
    // autocomplete calls these across every candidate term and every source.
    // `doc_freq` in particular is a stored count (O(1) once the term's block
    // directory is located), not a walk.

    fn doc_freq(&self, term: &str, field: FieldId) -> u32 {
        let Some(term_id) = self.terms.get(term) else { return 0 };
        match codec::decode_term_field_blocks(&self.post, &self.post_header, term_id, field) {
            Ok(Some(term_field)) => term_field.doc_freq,
            Ok(None) => 0,
            Err(e) => {
                tracing::warn!(term, field, error = %e, "segment: failed to decode postings");
                0
            }
        }
    }

    fn live_doc_freq(&self, term: &str, field: FieldId, deleted: &RoaringBitmap) -> u64 {
        let Some(term_id) = self.terms.get(term) else { return 0 };
        match codec::decode_term_field_doc_ids(&self.post, &self.post_header, term_id, field) {
            Ok(doc_ids) => doc_ids
                .into_iter()
                .filter(|&doc_id| self.is_live(doc_id) && !deleted.contains(doc_id))
                .count() as u64,
            Err(e) => {
                tracing::warn!(term, field, error = %e, "segment: failed to decode doc ids");
                0
            }
        }
    }

    fn field_doc_count(&self, field: FieldId) -> u32 {
        self.post_header.field_doc_count.get(field as usize).copied().unwrap_or(0)
    }

    fn total_field_len(&self, field: FieldId) -> u64 {
        self.post_header.field_total_len.get(field as usize).copied().unwrap_or(0)
    }

    fn field_len(&self, doc_id: DocId, field: FieldId) -> u32 {
        if !self.doc_header.presence.contains(doc_id) {
            return 0;
        }
        match codec::field_len_at(&self.doc, &self.doc_header, doc_id, field) {
            Ok(len) => len,
            Err(e) => {
                tracing::warn!(doc_id, field, error = %e, "segment: failed to read field length");
                0
            }
        }
    }

    fn is_live(&self, doc_id: DocId) -> bool {
        self.doc_header.presence.contains(doc_id)
    }

    fn collect_terms_with_prefix(&self, prefix: &str, limit: usize, out: &mut Vec<String>) {
        use fst::automaton::Str;
        use fst::{Automaton, IntoStreamer, Streamer};

        let matcher = Str::new(prefix).starts_with();
        let mut stream = self.terms.search(matcher).into_stream();
        while out.len() < limit {
            let Some((term, _)) = stream.next() else { break };
            if let Ok(s) = std::str::from_utf8(term) {
                out.push(s.to_owned());
            }
        }
    }

    fn collect_fuzzy_terms(&self, matcher: &mut FuzzyMatcher, out: &mut Vec<(String, u32)>) {
        use fst::Streamer;

        // A linear scan over the sorted dictionary, same as `MemTable`'s own
        // `collect_fuzzy_terms` — a smarter Levenshtein-automaton
        // intersection with the FST is future work, not required for
        // correctness.
        let mut stream = self.terms.stream();
        while let Some((term, _)) = stream.next() {
            let Ok(s) = std::str::from_utf8(term) else { continue };
            if let Some(distance) = matcher.distance(s) {
                out.push((s.to_owned(), distance));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tachyon_core::{FieldSchema, FieldType, ParsedDocument};

    use crate::columns::NumKey;
    use crate::fuzzy::FuzzyMatcher;
    use crate::memtable::MemTable;

    fn schema() -> CollectionSchema {
        CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text).required(),
                FieldSchema::new("tags", FieldType::Text),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("price", FieldType::Int).with_filter(true).with_sort(true),
            ],
        )
    }

    fn doc(id: &str, title: &str, tags: &[&str], brand: &str, price: i64) -> ParsedDocument {
        ParsedDocument::parse(
            json!({ "id": id, "title": title, "tags": tags, "brand": brand, "price": price }),
            &schema(),
        )
        .unwrap()
    }

    /// The hole reuses "mouse" rather than a unique term, so the surface this
    /// test compares (live terms, prefix scans, fuzzy scans) doesn't hit the
    /// separately-tested "a hole-only term disappears entirely" case.
    fn fixture() -> (MemTable, CollectionSchema) {
        let schema = schema();
        let mut m = MemTable::new(0, &schema);
        m.insert(doc("1", "wireless mouse", &["red", "blue"], "Logitech", 2999));
        m.insert(doc("2", "mouse pad", &["blue"], "Razer", 1999));
        let hole = m.insert(doc("3", "mouse", &[], "Logitech", 4999));
        m.remove(hole);
        (m, schema)
    }

    fn write_segment(dir: &Path, encoded: &super::super::EncodedSegment) -> SegmentFilePaths {
        let paths = SegmentFilePaths {
            terms: dir.join("0000000001.terms"),
            ids: dir.join("0000000001.ids"),
            post: dir.join("0000000001.post"),
            col: dir.join("0000000001.col"),
            doc: dir.join("0000000001.doc"),
        };
        std::fs::write(&paths.terms, &encoded.terms).unwrap();
        std::fs::write(&paths.ids, &encoded.ids).unwrap();
        std::fs::write(&paths.post, &encoded.post).unwrap();
        std::fs::write(&paths.col, &encoded.col).unwrap();
        std::fs::write(&paths.doc, &encoded.doc).unwrap();
        paths
    }

    #[test]
    fn index_source_answers_agree_with_the_memtable_it_was_built_from() {
        let (m, schema) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let encoded = super::super::encode(&m, &schema).unwrap();
        let paths = write_segment(dir.path(), &encoded);
        let reader = SegmentReader::open(&paths, &schema).unwrap();

        let mem: &dyn IndexSource = &m;
        let seg: &dyn IndexSource = &reader;

        assert_eq!(mem.min_doc_id(), seg.min_doc_id());
        assert_eq!(mem.end_doc_id(), seg.end_doc_id());

        for doc_id in mem.min_doc_id()..mem.end_doc_id() {
            assert_eq!(mem.is_live(doc_id), seg.is_live(doc_id), "doc {doc_id}");
            for field in 0..schema.fields.len() as FieldId {
                assert_eq!(
                    mem.value(doc_id, field).as_deref(),
                    seg.value(doc_id, field).as_deref(),
                    "doc {doc_id} field {field}"
                );
                assert_eq!(
                    mem.field_len(doc_id, field),
                    seg.field_len(doc_id, field),
                    "doc {doc_id} field {field}"
                );
            }
        }

        // Drain a source's cursor into the same `(doc_id, positions)` shape
        // `posting_cursor()` promises, one `advance()` at a time — the
        // comparable shape both `mem`/`seg` are checked against below.
        fn drain(source: &dyn IndexSource, term: &str, field: FieldId) -> Option<Vec<(u32, Vec<u32>)>> {
            let mut cursor = source.posting_cursor(term, field)?;
            let mut out = Vec::new();
            while let Some(doc_id) = cursor.doc_id() {
                out.push((doc_id, cursor.positions()));
                cursor.advance();
            }
            Some(out)
        }

        for term in ["mouse", "wireless", "pad", "blue", "red", "keyboard", "nope"] {
            for field in [0u16, 1] {
                // The memtable's postings still carry the hole until a
                // flush; a segment never does. Filter to what the memtable
                // itself considers live before comparing.
                let mem_docs = drain(mem, term, field).map(|docs| {
                    docs.into_iter().filter(|(doc_id, _)| mem.is_live(*doc_id)).collect::<Vec<_>>()
                });
                let mem_docs = mem_docs.filter(|d| !d.is_empty());
                let seg_docs = drain(seg, term, field);
                assert_eq!(mem_docs, seg_docs, "term {term:?} field {field}");

                // doc_freq/live_doc_freq go through a separate, positions-free
                // decode path in the segment (see reader.rs's override) —
                // exercise it against the same live-only expectation.
                let expected_live_freq = mem_docs.as_ref().map_or(0, |d| d.len() as u64);
                assert_eq!(
                    seg.live_doc_freq(term, field, &RoaringBitmap::new()),
                    expected_live_freq,
                    "live_doc_freq term {term:?} field {field}"
                );
                assert_eq!(
                    seg.doc_freq(term, field) as u64,
                    expected_live_freq,
                    "doc_freq term {term:?} field {field} (segment has no holes to disagree over)"
                );
            }
        }

        for prefix in ["mo", "b", "z", ""] {
            let mut mem_terms = Vec::new();
            mem.collect_terms_with_prefix(prefix, 100, &mut mem_terms);
            let mut seg_terms = Vec::new();
            seg.collect_terms_with_prefix(prefix, 100, &mut seg_terms);
            mem_terms.sort();
            seg_terms.sort();
            assert_eq!(mem_terms, seg_terms, "prefix {prefix:?}");
        }

        for typo in ["mouss", "moose", "mouse"] {
            let mut mem_fuzzy = Vec::new();
            mem.collect_fuzzy_terms(&mut FuzzyMatcher::new(typo, 2), &mut mem_fuzzy);
            let mut seg_fuzzy = Vec::new();
            seg.collect_fuzzy_terms(&mut FuzzyMatcher::new(typo, 2), &mut seg_fuzzy);
            mem_fuzzy.sort();
            seg_fuzzy.sort();
            assert_eq!(mem_fuzzy, seg_fuzzy, "typo {typo:?}");
        }

        let brand = seg.keyword_column(2).unwrap();
        assert_eq!(brand.equals("Logitech").iter().collect::<Vec<_>>(), vec![0]);
        let price = seg.numeric_column(3).unwrap();
        assert_eq!(price.range(Some(NumKey::Int(1999)), Some(NumKey::Int(2999))).len(), 2);
    }

    #[test]
    fn lookup_and_get_mirror_the_memtable_for_live_docs_only() {
        let (m, schema) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let encoded = super::super::encode(&m, &schema).unwrap();
        let paths = write_segment(dir.path(), &encoded);
        let reader = SegmentReader::open(&paths, &schema).unwrap();

        assert_eq!(reader.lookup_id("1"), m.lookup("1"));
        assert_eq!(reader.lookup_id("2"), m.lookup("2"));
        assert_eq!(reader.lookup_id("3"), None, "the hole's id must not resolve");

        let doc_id = reader.lookup_id("1").unwrap();
        assert_eq!(reader.get(doc_id), Some(m.get(doc_id).unwrap().source.clone()));
    }

    #[test]
    fn open_rejects_a_schema_with_a_different_field_count() {
        let (m, schema) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let encoded = super::super::encode(&m, &schema).unwrap();
        let paths = write_segment(dir.path(), &encoded);

        let wrong_schema =
            CollectionSchema::new("products", vec![FieldSchema::new("title", FieldType::Text)]);
        assert!(SegmentReader::open(&paths, &wrong_schema).is_err());
    }

    #[test]
    fn open_surfaces_a_missing_file_as_an_error_not_a_panic() {
        let (m, schema) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let encoded = super::super::encode(&m, &schema).unwrap();
        let mut paths = write_segment(dir.path(), &encoded);
        paths.col = dir.path().join("does-not-exist.col");
        assert!(SegmentReader::open(&paths, &schema).is_err());
    }

    #[test]
    fn a_large_corpus_opens_without_decoding_everything_up_front() {
        // Not a memory-measurement test (fragile in a unit test), but a
        // sanity check that `open` really does stay O(terms + fields), not
        // O(documents): a few thousand documents must open just as fast as a
        // few, since nothing about `open` scans postings or doc records.
        let schema = schema();
        let mut m = MemTable::new(0, &schema);
        for i in 0..5000 {
            m.insert(doc(&i.to_string(), &format!("item number {i}"), &["tag"], "Brand", i as i64));
        }
        let dir = tempfile::tempdir().unwrap();
        let encoded = super::super::encode(&m, &schema).unwrap();
        let paths = write_segment(dir.path(), &encoded);

        let reader = SegmentReader::open(&paths, &schema).unwrap();
        assert_eq!(reader.min_doc_id(), 0);
        assert_eq!(reader.end_doc_id(), 5000);
        assert_eq!(reader.lookup_id("2500"), Some(2500));
        assert_eq!(reader.get(2500).unwrap()["title"], json!("item number 2500"));
        assert!(reader.is_live(4999));
        assert!(!reader.is_live(5000));
    }
}
