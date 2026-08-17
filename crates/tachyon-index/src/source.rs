//! The interface the query engine reads through.
//!
//! A search runs over an ordered list of sources — the memtable plus every
//! committed segment (PRD §10: "search memtable, search immutable segments,
//! merge results"). Both sides implement this trait, so the executor never
//! branches on which it is holding.
//!
//! Deliberately object-safe: the executor holds `&[&dyn IndexSource]`.
//!
//! # Borrowed vs. owned
//!
//! Every method that hands back a whole value returns [`Cow`] rather than a
//! bare reference. The memtable's data already lives in memory in exactly the
//! shape callers want, so it returns `Cow::Borrowed` at zero cost — nothing
//! about the memtable's performance changes. A segment, however, keeps almost
//! nothing decoded at rest: its columns and document values are read from an
//! mmap'd file and decoded into an owned value only when asked for, which is
//! what keeps a segment's resident memory bounded by corpus size rather than
//! by how much of it has ever been queried. `Cow` lets one trait describe
//! both without forcing the memtable to pay a clone it never needed.
//!
//! Postings are the one exception: [`IndexSource::posting_cursor`] hands back
//! a `Box<dyn PostingCursor>` rather than a `Cow`, since even a segment's
//! *whole* posting list is never materialized — a cursor decodes one block at
//! a time, lazily, regardless of which source it reads from.

use std::borrow::Cow;

use roaring::RoaringBitmap;

use tachyon_core::{DocId, FieldId, Value};

use crate::columns::{KeywordColumn, NumericColumn};
use crate::cursor::{MemTablePostingCursor, PostingCursor};
use crate::fuzzy::FuzzyMatcher;
use crate::memtable::MemTable;

pub trait IndexSource: Send + Sync {
    /// The lowest doc id this source can hold, so the executor can route a doc
    /// id to its owning source without probing.
    fn min_doc_id(&self) -> DocId;

    /// One past the highest doc id this source can hold.
    fn end_doc_id(&self) -> DocId;

    /// A document's stored value for a field, used by sorting and the
    /// popularity signal.
    fn value(&self, doc_id: DocId, field: FieldId) -> Option<Cow<'_, Value>>;

    /// The numeric filter/sort column for a field, if it has one.
    fn numeric_column(&self, field: FieldId) -> Option<Cow<'_, NumericColumn>>;

    /// The keyword filter/facet column for a field, if it has one.
    fn keyword_column(&self, field: FieldId) -> Option<Cow<'_, KeywordColumn>>;

    /// A lazy, doc-at-a-time cursor over a term's postings in a field, or
    /// `None` if absent. The query engine's pruning walk reads postings
    /// exclusively through this — never materializing a term's full posting
    /// list — so a document a sound bound proves unnecessary is never
    /// decoded at all, not merely scored cheaply.
    fn posting_cursor(&self, term: &str, field: FieldId) -> Option<Box<dyn PostingCursor + '_>>;

    /// Documents in this source containing `term` in `field`.
    fn doc_freq(&self, term: &str, field: FieldId) -> u32 {
        let Some(mut cursor) = self.posting_cursor(term, field) else { return 0 };
        let mut count = 0u32;
        while cursor.doc_id().is_some() {
            count += 1;
            cursor.advance();
        }
        count
    }

    /// Documents containing `term` in `field` that are still live.
    ///
    /// Distinct from [`Self::doc_freq`], which counts postings and so includes
    /// deleted documents. BM25 tolerates that staleness — Lucene has the same
    /// property until a merge — but anything user-facing, such as an
    /// autocomplete count, must not promise results that no longer exist.
    fn live_doc_freq(&self, term: &str, field: FieldId, deleted: &RoaringBitmap) -> u64 {
        let Some(mut cursor) = self.posting_cursor(term, field) else { return 0 };
        let mut count = 0u64;
        while let Some(doc_id) = cursor.doc_id() {
            if self.is_live(doc_id) && !deleted.contains(doc_id) {
                count += 1;
            }
            cursor.advance();
        }
        count
    }

    /// Documents in this source with any content in `field`. Summed across
    /// sources to give BM25's `N`.
    fn field_doc_count(&self, field: FieldId) -> u32;

    /// Total tokens across all documents in `field`. Summed across sources to
    /// give BM25's `avgdl`.
    fn total_field_len(&self, field: FieldId) -> u64;

    /// Token count of one field of one document. BM25's `|d|`.
    fn field_len(&self, doc_id: DocId, field: FieldId) -> u32;

    /// Whether the document is still present in *this* source. Collection-wide
    /// tombstones are applied separately by the executor.
    fn is_live(&self, doc_id: DocId) -> bool;

    /// Append up to `limit` terms starting with `prefix`, in sorted order (PRD
    /// §7.3 prefix matching, §7.5 autocomplete).
    ///
    /// The cap is the callee's job rather than the caller's: truncating an
    /// already-materialized list still pays to walk and copy every term a
    /// one-character prefix matches, which on a large dictionary is most of it.
    /// Because terms come back sorted, taking the first `limit` from each
    /// source and merging yields the same set as capping the merged result.
    fn collect_terms_with_prefix(&self, prefix: &str, limit: usize, out: &mut Vec<String>);

    /// Append every term within the matcher's edit budget, with its distance.
    ///
    /// The scan lives behind the trait so each source can prune with whatever
    /// structure it has — the memtable walks its dictionary, and a segment can
    /// drive the matcher from its FST instead.
    fn collect_fuzzy_terms(&self, matcher: &mut FuzzyMatcher, out: &mut Vec<(String, u32)>);
}

impl IndexSource for MemTable {
    fn min_doc_id(&self) -> DocId {
        self.base()
    }

    fn end_doc_id(&self) -> DocId {
        self.next_doc_id()
    }

    fn value(&self, doc_id: DocId, field: FieldId) -> Option<Cow<'_, Value>> {
        self.get(doc_id).and_then(|d| d.values.get(field as usize)).map(Cow::Borrowed)
    }

    fn numeric_column(&self, field: FieldId) -> Option<Cow<'_, NumericColumn>> {
        MemTable::columns(self).numeric(field).map(Cow::Borrowed)
    }

    fn keyword_column(&self, field: FieldId) -> Option<Cow<'_, KeywordColumn>> {
        MemTable::columns(self).keyword(field).map(Cow::Borrowed)
    }

    fn posting_cursor(&self, term: &str, field: FieldId) -> Option<Box<dyn PostingCursor + '_>> {
        self.index()
            .postings(term, field)
            .map(|p| Box::new(MemTablePostingCursor::new(&p.docs)) as Box<dyn PostingCursor + '_>)
    }

    fn doc_freq(&self, term: &str, field: FieldId) -> u32 {
        self.index().doc_freq(term, field)
    }

    fn field_doc_count(&self, field: FieldId) -> u32 {
        self.index().field_doc_count(field)
    }

    fn total_field_len(&self, field: FieldId) -> u64 {
        self.index().total_field_len(field)
    }

    fn field_len(&self, doc_id: DocId, field: FieldId) -> u32 {
        self.get(doc_id).and_then(|d| d.field_lengths.get(field as usize).copied()).unwrap_or(0)
    }

    fn is_live(&self, doc_id: DocId) -> bool {
        MemTable::is_live(self, doc_id)
    }

    fn collect_terms_with_prefix(&self, prefix: &str, limit: usize, out: &mut Vec<String>) {
        out.extend(self.index().terms_with_prefix(prefix).take(limit).map(str::to_owned));
    }

    fn collect_fuzzy_terms(&self, matcher: &mut FuzzyMatcher, out: &mut Vec<(String, u32)>) {
        for term in self.index().iter_terms() {
            if let Some(distance) = matcher.distance(term) {
                out.push((term.to_owned(), distance));
            }
        }
    }
}
