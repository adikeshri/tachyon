//! In-memory inverted index (PRD §11: term, doc_id, term frequency, field id,
//! positions).
//!
//! # Shape
//!
//! ```text
//! term -> [ field -> [ (doc_id, positions) ] ]
//! ```
//!
//! Postings are grouped by field before doc, which makes per-field document
//! frequency — the `df` BM25 needs — a `len()` call rather than a scan, and
//! keeps a multi-field query touching only the fields it asked for.
//!
//! # Deletions
//!
//! Postings are never removed. A deleted document stays in the lists and is
//! skipped at query time against the caller's live set, which is how deletes
//! stay O(1) instead of O(terms in document). The space is reclaimed when the
//! memtable is flushed into a segment.
//!
//! Terms live in a `BTreeMap` rather than a `HashMap`: lookup is barely slower
//! at these sizes, and the ordering gives prefix scans (autocomplete, PRD §7.5)
//! for free.

use std::collections::BTreeMap;
use std::ops::Bound;

use tachyon_core::{DocId, FieldId};

use crate::tokenizer::Token;

/// One document's occurrences of one term in one field.
#[derive(Debug, Clone)]
pub struct DocPosting {
    pub doc_id: DocId,
    /// Token positions, ascending. Its length is the term frequency.
    pub positions: Vec<u32>,
}

impl DocPosting {
    /// Term frequency: how often the term occurs in this field of this doc.
    pub fn tf(&self) -> u32 {
        self.positions.len() as u32
    }
}

/// A term's postings within one field, ordered by doc id.
#[derive(Debug, Clone, Default)]
pub struct FieldPostings {
    pub docs: Vec<DocPosting>,
}

impl FieldPostings {
    /// Number of documents containing the term in this field, including any
    /// since deleted.
    pub fn doc_freq(&self) -> u32 {
        self.docs.len() as u32
    }
}

#[derive(Debug, Clone, Default)]
struct TermEntry {
    /// Ordered by field id; a term usually appears in one or two fields, so a
    /// short vector beats a map.
    fields: Vec<(FieldId, FieldPostings)>,
}

impl TermEntry {
    fn get(&self, field: FieldId) -> Option<&FieldPostings> {
        self.fields.binary_search_by_key(&field, |(f, _)| *f).ok().map(|i| &self.fields[i].1)
    }

    fn get_or_insert(&mut self, field: FieldId) -> &mut FieldPostings {
        let idx = match self.fields.binary_search_by_key(&field, |(f, _)| *f) {
            Ok(i) => i,
            Err(i) => {
                self.fields.insert(i, (field, FieldPostings::default()));
                i
            }
        };
        &mut self.fields[idx].1
    }
}

/// Per-field corpus statistics BM25 needs.
#[derive(Debug, Clone, Default)]
struct FieldStats {
    /// Documents with at least one token in this field.
    doc_count: u32,
    /// Sum of field lengths in tokens, for the average.
    total_len: u64,
}

/// Per-term dictionary overhead beyond the term text itself: the `Box<str>`,
/// the `TermEntry`, and the `BTreeMap` node share.
const TERM_OVERHEAD: usize = 48;

/// Per-posting overhead beyond the [`DocPosting`] itself, covering the heap
/// its positions vector allocates.
const POSTING_OVERHEAD: usize = 16;

#[derive(Debug, Default)]
pub struct InvertedIndex {
    terms: BTreeMap<Box<str>, TermEntry>,
    stats: Vec<FieldStats>,
    total_postings: usize,
    /// Running total for [`InvertedIndex::heap_bytes`], so asking how large the
    /// index has grown does not walk the whole dictionary. That question is
    /// asked to decide when to flush, which must not itself cost O(terms).
    term_bytes: usize,
}

impl InvertedIndex {
    pub fn new(num_fields: usize) -> InvertedIndex {
        InvertedIndex {
            terms: BTreeMap::new(),
            stats: vec![FieldStats::default(); num_fields],
            total_postings: 0,
            term_bytes: 0,
        }
    }

    /// Index one field of one document.
    ///
    /// Must be called in ascending `doc_id` order for a given field, which the
    /// memtable guarantees — doc ids are handed out sequentially. This keeps
    /// every posting list sorted without any sorting.
    pub fn add_field(&mut self, doc_id: DocId, field: FieldId, tokens: &[Token]) {
        if tokens.is_empty() {
            return;
        }

        if let Some(stats) = self.stats.get_mut(field as usize) {
            stats.doc_count += 1;
            stats.total_len += tokens.len() as u64;
        }

        // Group this document's positions per term before touching the map, so
        // each term is looked up once per document rather than once per token.
        let mut by_term: BTreeMap<&str, Vec<u32>> = BTreeMap::new();
        for token in tokens {
            by_term.entry(token.text.as_str()).or_default().push(token.position);
        }

        for (term, positions) in by_term {
            let entry = match self.terms.get_mut(term) {
                Some(entry) => entry,
                None => {
                    self.term_bytes += term.len() + std::mem::size_of::<Box<str>>() + TERM_OVERHEAD;
                    self.terms.entry(Box::from(term)).or_default()
                }
            };
            let postings = entry.get_or_insert(field);
            debug_assert!(
                postings.docs.last().is_none_or(|last| last.doc_id < doc_id),
                "documents must be indexed in ascending doc id order"
            );
            postings.docs.push(DocPosting { doc_id, positions });
            self.total_postings += 1;
        }
    }

    /// Postings for a term in a field, or `None` if it never occurs there.
    pub fn postings(&self, term: &str, field: FieldId) -> Option<&FieldPostings> {
        self.terms.get(term)?.get(field)
    }

    /// Documents containing `term` in `field`, including deleted ones.
    pub fn doc_freq(&self, term: &str, field: FieldId) -> u32 {
        self.postings(term, field).map_or(0, FieldPostings::doc_freq)
    }

    /// Whether the term appears anywhere in the index.
    pub fn contains_term(&self, term: &str) -> bool {
        self.terms.contains_key(term)
    }

    /// Documents that have any content in `field`. BM25's `N`.
    pub fn field_doc_count(&self, field: FieldId) -> u32 {
        self.stats.get(field as usize).map_or(0, |s| s.doc_count)
    }

    /// Total tokens indexed into `field` across all documents.
    pub fn total_field_len(&self, field: FieldId) -> u64 {
        self.stats.get(field as usize).map_or(0, |s| s.total_len)
    }

    /// Mean field length in tokens. BM25's `avgdl`; 0 when the field is empty.
    pub fn avg_field_len(&self, field: FieldId) -> f32 {
        match self.stats.get(field as usize) {
            Some(s) if s.doc_count > 0 => s.total_len as f32 / s.doc_count as f32,
            _ => 0.0,
        }
    }

    pub fn num_terms(&self) -> usize {
        self.terms.len()
    }

    pub fn num_postings(&self) -> usize {
        self.total_postings
    }

    /// Every term, in sorted order.
    pub fn iter_terms(&self) -> impl Iterator<Item = &str> {
        self.terms.keys().map(|k| k.as_ref())
    }

    /// Every term, in sorted order, with its postings per field — including
    /// postings for since-deleted documents. Used by the segment writer,
    /// which filters those out itself against the memtable's live set.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[(FieldId, FieldPostings)])> {
        self.terms.iter().map(|(term, entry)| (term.as_ref(), entry.fields.as_slice()))
    }

    /// Terms starting with `prefix`, in sorted order (PRD §7.5, §7.3 prefix
    /// matching). Walks only the matching range, not the whole dictionary.
    pub fn terms_with_prefix<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.terms
            .range::<str, _>((Bound::Included(prefix), Bound::Unbounded))
            .map(|(k, _)| k.as_ref())
            .take_while(move |term| term.starts_with(prefix))
    }

    /// Rough heap footprint, for flush decisions and `/metrics`.
    ///
    /// Both halves are running totals, so this is O(1): postings dominate and
    /// are approximated as their positions plus overhead rather than by walking
    /// every vector, and the dictionary total is accumulated as terms are added.
    pub fn heap_bytes(&self) -> usize {
        self.term_bytes
            + self.total_postings * (std::mem::size_of::<DocPosting>() + POSTING_OVERHEAD)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::tokenize;

    fn index_docs(docs: &[(DocId, FieldId, &str)]) -> InvertedIndex {
        let mut index = InvertedIndex::new(3);
        for (doc_id, field, text) in docs {
            index.add_field(*doc_id, *field, &tokenize(text));
        }
        index
    }

    #[test]
    fn records_term_frequency_and_positions() {
        let index = index_docs(&[(0, 0, "mouse mouse pad mouse")]);
        let postings = index.postings("mouse", 0).unwrap();
        assert_eq!(postings.doc_freq(), 1);
        assert_eq!(postings.docs[0].doc_id, 0);
        assert_eq!(postings.docs[0].tf(), 3);
        assert_eq!(postings.docs[0].positions, vec![0, 1, 3]);

        assert_eq!(index.postings("pad", 0).unwrap().docs[0].positions, vec![2]);
    }

    #[test]
    fn separates_fields() {
        let index = index_docs(&[(0, 0, "wireless mouse"), (0, 1, "a wireless device")]);
        assert_eq!(index.doc_freq("wireless", 0), 1);
        assert_eq!(index.doc_freq("wireless", 1), 1);
        assert_eq!(index.doc_freq("mouse", 1), 0);
        assert!(index.postings("mouse", 1).is_none());
    }

    #[test]
    fn posting_lists_are_sorted_by_doc_id() {
        let index = index_docs(&[
            (0, 0, "mouse"),
            (1, 0, "keyboard"),
            (2, 0, "mouse pad"),
            (5, 0, "mouse"),
        ]);
        let docs = &index.postings("mouse", 0).unwrap().docs;
        assert_eq!(docs.iter().map(|d| d.doc_id).collect::<Vec<_>>(), vec![0, 2, 5]);
        assert_eq!(index.doc_freq("mouse", 0), 3);
    }

    #[test]
    fn tracks_field_statistics_for_bm25() {
        let index = index_docs(&[
            (0, 0, "one two three four"), // 4 tokens
            (1, 0, "one two"),            // 2 tokens
        ]);
        assert_eq!(index.field_doc_count(0), 2);
        assert_eq!(index.avg_field_len(0), 3.0);

        // A field nobody wrote to has no documents and no average.
        assert_eq!(index.field_doc_count(2), 0);
        assert_eq!(index.avg_field_len(2), 0.0);
    }

    #[test]
    fn empty_text_indexes_nothing() {
        let index = index_docs(&[(0, 0, "   !!! ")]);
        assert_eq!(index.num_terms(), 0);
        assert_eq!(index.field_doc_count(0), 0, "an empty field is not a document in that field");
    }

    #[test]
    fn prefix_scan_returns_only_the_matching_range() {
        let index = index_docs(&[(0, 0, "wire wireless wired wig zebra apple")]);
        let found: Vec<_> = index.terms_with_prefix("wir").collect();
        assert_eq!(found, vec!["wire", "wired", "wireless"]);

        assert_eq!(index.terms_with_prefix("z").collect::<Vec<_>>(), vec!["zebra"]);
        assert!(index.terms_with_prefix("qqq").next().is_none());
        // An empty prefix is every term.
        assert_eq!(index.terms_with_prefix("").count(), index.num_terms());
    }

    #[test]
    fn terms_are_iterated_in_sorted_order() {
        let index = index_docs(&[(0, 0, "zebra apple mango")]);
        assert_eq!(index.iter_terms().collect::<Vec<_>>(), vec!["apple", "mango", "zebra"]);
    }

    #[test]
    fn repeated_terms_across_docs_share_one_dictionary_entry() {
        let index = index_docs(&[(0, 0, "mouse"), (1, 0, "mouse"), (2, 0, "mouse")]);
        assert_eq!(index.num_terms(), 1);
        assert_eq!(index.num_postings(), 3);
        assert!(index.contains_term("mouse"));
        assert!(!index.contains_term("keyboard"));
    }
}
