//! Columnar stores backing filters, sorting, and facets (PRD §11: "Numeric
//! Index: sorted `(value, doc_id)` arrays").
//!
//! # Numeric columns
//!
//! A range filter wants a sorted array so it can binary-search both ends and
//! take the slice between. But the memtable is written to constantly, and
//! re-sorting on every insert would be quadratic. So a numeric column is kept
//! as a large sorted region plus a small unsorted tail:
//!
//! ```text
//! sorted:  [(9, d3), (12, d1), (40, d7), …]      binary-searched
//! pending: [(31, d9), (11, d4)]                  linearly scanned, bounded
//! ```
//!
//! Inserts push onto `pending`; once it reaches [`MERGE_THRESHOLD`] the tail is
//! sorted and merged into the sorted region in one linear pass. Queries read
//! both. That gives O(log n) range lookups with O(1) amortized inserts, and the
//! bounded tail means the linear part never dominates.
//!
//! # Keyword columns
//!
//! Equality and faceting want the opposite shape: value → the set of documents
//! holding it. A roaring bitmap per distinct value answers both a filter
//! (return the bitmap) and a facet count (its cardinality) directly.

use std::cmp::Ordering;
use std::collections::HashMap;

use roaring::RoaringBitmap;

use tachyon_core::{DocId, FieldId, Value};

/// Pending entries tolerated before a merge into the sorted region.
pub const MERGE_THRESHOLD: usize = 4096;

/// A numeric key that keeps integers exact.
///
/// Storing everything as `f64` would silently round `int` values beyond 2^53,
/// which for a database is the kind of bug that surfaces years later in
/// someone's id column.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumKey {
    Int(i64),
    Float(f64),
}

impl NumKey {
    pub fn as_f64(self) -> f64 {
        match self {
            NumKey::Int(i) => i as f64,
            NumKey::Float(f) => f,
        }
    }

    /// Total ordering. Same-variant comparisons are exact; mixed variants fall
    /// back to `f64`, and NaN sorts last so the order stays total.
    pub fn cmp_key(&self, other: &NumKey) -> Ordering {
        match (self, other) {
            (NumKey::Int(a), NumKey::Int(b)) => a.cmp(b),
            (a, b) => a.as_f64().total_cmp(&b.as_f64()),
        }
    }

    /// Extract a numeric key from a schema value, if it has one.
    pub fn from_value(value: &Value) -> Option<NumKey> {
        match value {
            Value::Int(i) => Some(NumKey::Int(*i)),
            Value::Float(f) => Some(NumKey::Float(*f)),
            Value::Bool(b) => Some(NumKey::Int(*b as i64)),
            _ => None,
        }
    }
}

/// One end of a numeric range, and whether the endpoint itself is included.
#[derive(Debug, Clone, Copy)]
struct Bound {
    key: NumKey,
    inclusive: bool,
}

impl Bound {
    fn inclusive(key: NumKey) -> Bound {
        Bound { key, inclusive: true }
    }

    fn exclusive(key: NumKey) -> Bound {
        Bound { key, inclusive: false }
    }

    /// Whether `value` clears this bound treated as a lower one.
    fn admits_low(self, value: &NumKey) -> bool {
        let order = value.cmp_key(&self.key);
        if self.inclusive {
            order.is_ge()
        } else {
            order.is_gt()
        }
    }

    /// Whether `value` clears this bound treated as an upper one.
    fn admits_high(self, value: &NumKey) -> bool {
        let order = value.cmp_key(&self.key);
        if self.inclusive {
            order.is_le()
        } else {
            order.is_lt()
        }
    }
}

/// Build a bitmap from doc ids arriving in `(value, doc_id)` order, which is
/// not doc id order.
///
/// Deliberately a plain `insert` per id rather than a sort followed by
/// `from_sorted_iter`. Sorting first is the obvious optimization and it is a
/// loss here, measurably: the selections these columns produce are dense
/// enough that roaring stores them as bitmap containers, where an out-of-order
/// insert is a single bit-set and the sort buys nothing but its own `n log n`.
/// Benchmarked at 200k documents, sorting made a range filter ~17% slower.
///
/// Somewhere sparse, that trade would flip — see `sorted_bitmap` in the query
/// executor, which does sort, for a set where it pays off.
fn collect_docs(docs: impl Iterator<Item = DocId>) -> RoaringBitmap {
    docs.collect()
}

/// Sorted `(value, doc_id)` pairs for one numeric field.
#[derive(Debug, Default, Clone)]
pub struct NumericColumn {
    sorted: Vec<(NumKey, DocId)>,
    pending: Vec<(NumKey, DocId)>,
}

impl NumericColumn {
    /// Record a value. Multi-valued fields insert one entry per value, so a
    /// range filter matches a document if *any* of its values falls inside.
    pub fn push(&mut self, key: NumKey, doc_id: DocId) {
        self.pending.push((key, doc_id));
        if self.pending.len() >= MERGE_THRESHOLD {
            self.merge();
        }
    }

    /// Fold the pending tail into the sorted region.
    fn merge(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        self.pending.sort_by(|a, b| a.0.cmp_key(&b.0).then(a.1.cmp(&b.1)));

        let mut merged = Vec::with_capacity(self.sorted.len() + self.pending.len());
        let (mut i, mut j) = (0usize, 0usize);
        while i < self.sorted.len() && j < self.pending.len() {
            let order = self.sorted[i]
                .0
                .cmp_key(&self.pending[j].0)
                .then(self.sorted[i].1.cmp(&self.pending[j].1));
            if order.is_le() {
                merged.push(self.sorted[i]);
                i += 1;
            } else {
                merged.push(self.pending[j]);
                j += 1;
            }
        }
        merged.extend_from_slice(&self.sorted[i..]);
        merged.extend_from_slice(&self.pending[j..]);

        self.sorted = merged;
        self.pending.clear();
    }

    pub fn len(&self) -> usize {
        self.sorted.len() + self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Documents whose value satisfies `predicate`.
    fn select(&self, mut predicate: impl FnMut(NumKey) -> bool) -> RoaringBitmap {
        let matches = self
            .sorted
            .iter()
            .chain(self.pending.iter())
            .filter(|(key, _)| predicate(*key))
            .map(|(_, doc_id)| *doc_id);
        collect_docs(matches)
    }

    /// Documents with a value between the given bounds.
    ///
    /// The sorted region is bounded by binary search so only the matching
    /// slice is touched; the pending tail is short enough to scan. Each bound
    /// carries whether it is inclusive, so a strict comparison is one pass and
    /// not a range scan minus an equality scan.
    fn bounded(&self, lo: Option<Bound>, hi: Option<Bound>) -> RoaringBitmap {
        // `partition_point` needs the predicate to be false-then-true across
        // the region, which holds because `sorted` is ordered by key.
        let start = match lo {
            Some(Bound { key, inclusive: true }) => {
                self.sorted.partition_point(|(k, _)| k.cmp_key(&key).is_lt())
            }
            Some(Bound { key, inclusive: false }) => {
                self.sorted.partition_point(|(k, _)| k.cmp_key(&key).is_le())
            }
            None => 0,
        };
        let end = match hi {
            Some(Bound { key, inclusive: true }) => {
                self.sorted.partition_point(|(k, _)| k.cmp_key(&key).is_le())
            }
            Some(Bound { key, inclusive: false }) => {
                self.sorted.partition_point(|(k, _)| k.cmp_key(&key).is_lt())
            }
            None => self.sorted.len(),
        };

        let within = |key: &NumKey| {
            lo.is_none_or(|lo| lo.admits_low(key)) && hi.is_none_or(|hi| hi.admits_high(key))
        };
        let selected = self.sorted[start.min(end)..end]
            .iter()
            .map(|(_, doc_id)| *doc_id)
            .chain(self.pending.iter().filter(|(key, _)| within(key)).map(|(_, doc_id)| *doc_id));

        collect_docs(selected)
    }

    /// Documents with a value in `[lo, hi]`, either bound optional.
    pub fn range(&self, lo: Option<NumKey>, hi: Option<NumKey>) -> RoaringBitmap {
        self.bounded(lo.map(Bound::inclusive), hi.map(Bound::inclusive))
    }

    /// Documents with a value strictly below `key`.
    pub fn less_than(&self, key: NumKey) -> RoaringBitmap {
        self.bounded(None, Some(Bound::exclusive(key)))
    }

    /// Documents with a value strictly above `key`.
    pub fn greater_than(&self, key: NumKey) -> RoaringBitmap {
        self.bounded(Some(Bound::exclusive(key)), None)
    }

    /// Documents whose value differs from `key`. Distinct from
    /// `!range(key, key)`: a document with no value at all is not "not equal",
    /// it is absent, and the caller decides what that means.
    pub fn not_equal(&self, key: NumKey) -> RoaringBitmap {
        self.select(|k| k.cmp_key(&key) != Ordering::Equal)
    }

    /// Distinct values with their document counts, restricted to a result set.
    ///
    /// The sorted region already groups equal values together, so counting is
    /// one pass with no intermediate map; the pending tail is folded in
    /// afterwards.
    pub fn value_counts_within(&self, docs: &RoaringBitmap) -> Vec<(NumKey, u64)> {
        let mut counts: Vec<(NumKey, u64)> = Vec::new();

        let push = |key: NumKey, counts: &mut Vec<(NumKey, u64)>| match counts.last_mut() {
            Some((last, count)) if last.cmp_key(&key).is_eq() => *count += 1,
            _ => counts.push((key, 1)),
        };

        for (key, doc_id) in &self.sorted {
            if docs.contains(*doc_id) {
                push(*key, &mut counts);
            }
        }

        for (key, doc_id) in &self.pending {
            if !docs.contains(*doc_id) {
                continue;
            }
            match counts.binary_search_by(|(k, _)| k.cmp_key(key)) {
                Ok(i) => counts[i].1 += 1,
                Err(i) => counts.insert(i, (*key, 1)),
            }
        }

        counts
    }

    /// Every document with any value in this column.
    pub fn present(&self) -> RoaringBitmap {
        self.select(|_| true)
    }

    pub fn heap_bytes(&self) -> usize {
        self.len() * std::mem::size_of::<(NumKey, DocId)>()
    }

    /// Build a column directly from an already-sorted vector, with no
    /// `pending` tail — used when decoding a segment, where nothing writes to
    /// the column again so there is nothing left to amortize.
    pub fn from_sorted(sorted: Vec<(NumKey, DocId)>) -> NumericColumn {
        NumericColumn { sorted, pending: Vec::new() }
    }

    /// Every `(value, doc)` pair this column holds, in no particular order.
    /// Used by the segment writer, which sorts and filters live docs itself.
    pub fn iter(&self) -> impl Iterator<Item = (NumKey, DocId)> + '_ {
        self.sorted.iter().copied().chain(self.pending.iter().copied())
    }
}

/// Value → documents, for keyword equality and facets.
#[derive(Debug, Default, Clone)]
pub struct KeywordColumn {
    by_value: HashMap<Box<str>, RoaringBitmap>,
    /// Documents with any value, so `!=` can exclude without resurrecting
    /// documents that simply lack the field.
    present: RoaringBitmap,
    /// Running totals for [`KeywordColumn::heap_bytes`]: the distinct value
    /// text, and how many `(value, document)` pairs the bitmaps hold.
    value_bytes: usize,
    entries: usize,
}

impl KeywordColumn {
    pub fn push(&mut self, value: &str, doc_id: DocId) {
        // Looked up before it is owned: `entry` would allocate a `Box<str>` on
        // every call, and a keyword column exists precisely because its values
        // repeat — a brand column over a million documents holds a few dozen
        // distinct strings, so all but a few dozen of those allocations are
        // built only to be thrown away.
        let added = match self.by_value.get_mut(value) {
            Some(docs) => docs.insert(doc_id),
            None => {
                self.value_bytes += value.len() + std::mem::size_of::<Box<str>>() + 32;
                self.by_value.entry(Box::from(value)).or_default().insert(doc_id)
            }
        };
        if added {
            self.entries += 1;
        }
        self.present.insert(doc_id);
    }

    pub fn equals(&self, value: &str) -> RoaringBitmap {
        self.by_value.get(value).cloned().unwrap_or_default()
    }

    pub fn not_equal(&self, value: &str) -> RoaringBitmap {
        &self.present - self.equals(value)
    }

    pub fn present(&self) -> &RoaringBitmap {
        &self.present
    }

    /// Distinct values with their document counts, for faceting.
    pub fn value_counts(&self) -> impl Iterator<Item = (&str, u64)> {
        self.by_value.iter().map(|(v, docs)| (v.as_ref(), docs.len()))
    }

    /// Counts restricted to a result set (PRD §7.7: accurate after filters).
    pub fn value_counts_within(&self, docs: &RoaringBitmap) -> Vec<(&str, u64)> {
        self.by_value
            .iter()
            .filter_map(|(value, bitmap)| {
                let count = bitmap.intersection_len(docs);
                (count > 0).then_some((value.as_ref(), count))
            })
            .collect()
    }

    pub fn num_values(&self) -> usize {
        self.by_value.len()
    }

    /// Rough heap footprint. O(1): `serialized_size` would have to be asked of
    /// every bitmap, and this is consulted to decide when to flush, so it
    /// tracks running totals and treats each `(value, document)` pair as a
    /// bare `DocId` — an upper bound, since roaring compresses dense runs.
    pub fn heap_bytes(&self) -> usize {
        self.value_bytes + self.entries * std::mem::size_of::<DocId>()
    }

    /// Build a column directly from decoded per-value bitmaps — used when
    /// decoding a segment. Recomputes the running heap-accounting totals
    /// [`KeywordColumn::push`] normally maintains incrementally.
    pub fn from_parts(
        by_value: HashMap<Box<str>, RoaringBitmap>,
        present: RoaringBitmap,
    ) -> KeywordColumn {
        let value_bytes =
            by_value.keys().map(|v| v.len() + std::mem::size_of::<Box<str>>() + 32).sum();
        let entries = by_value.values().map(RoaringBitmap::len).sum::<u64>() as usize;
        KeywordColumn { by_value, present, value_bytes, entries }
    }

    /// Every distinct value with its bitmap. Used by the segment writer,
    /// which filters and re-serializes them itself.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &RoaringBitmap)> {
        self.by_value.iter().map(|(v, b)| (v.as_ref(), b))
    }
}

/// All columns of one collection, addressed by field id.
#[derive(Debug)]
pub struct Columns {
    numeric: Vec<Option<NumericColumn>>,
    keyword: Vec<Option<KeywordColumn>>,
}

impl Columns {
    /// Allocate columns for every field the schema says needs one.
    pub fn new(schema: &tachyon_core::CollectionSchema) -> Columns {
        let mut numeric = Vec::with_capacity(schema.fields.len());
        let mut keyword = Vec::with_capacity(schema.fields.len());

        for field in &schema.fields {
            let (n, k) = if !field.needs_column() {
                (None, None)
            } else if field.field_type.is_numeric() {
                (Some(NumericColumn::default()), None)
            } else {
                (None, Some(KeywordColumn::default()))
            };
            numeric.push(n);
            keyword.push(k);
        }

        Columns { numeric, keyword }
    }

    /// Build directly from decoded per-field columns — used when decoding a
    /// segment, in place of [`Columns::new`]'s schema-driven allocation.
    pub fn from_parts(
        numeric: Vec<Option<NumericColumn>>,
        keyword: Vec<Option<KeywordColumn>>,
    ) -> Columns {
        Columns { numeric, keyword }
    }

    pub fn numeric(&self, field: FieldId) -> Option<&NumericColumn> {
        self.numeric.get(field as usize)?.as_ref()
    }

    pub fn keyword(&self, field: FieldId) -> Option<&KeywordColumn> {
        self.keyword.get(field as usize)?.as_ref()
    }

    /// Record one document's value for one field.
    pub fn push(&mut self, field: FieldId, doc_id: DocId, value: &Value) {
        if value.is_null() {
            return;
        }
        for scalar in value.iter_scalars() {
            if let Some(column) = self.numeric.get_mut(field as usize).and_then(Option::as_mut) {
                if let Some(key) = NumKey::from_value(scalar) {
                    column.push(key, doc_id);
                }
            } else if let Some(column) =
                self.keyword.get_mut(field as usize).and_then(Option::as_mut)
            {
                if let Some(text) = scalar.as_str() {
                    column.push(text, doc_id);
                }
            }
        }
    }

    pub fn heap_bytes(&self) -> usize {
        let n: usize = self.numeric.iter().flatten().map(NumericColumn::heap_bytes).sum();
        let k: usize = self.keyword.iter().flatten().map(KeywordColumn::heap_bytes).sum();
        n + k
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_core::{CollectionSchema, FieldSchema, FieldType};

    fn ids(bitmap: &RoaringBitmap) -> Vec<DocId> {
        bitmap.iter().collect()
    }

    fn column_of(values: &[(i64, DocId)]) -> NumericColumn {
        let mut column = NumericColumn::default();
        for (v, d) in values {
            column.push(NumKey::Int(*v), *d);
        }
        column
    }

    #[test]
    fn numeric_ranges_are_inclusive_on_both_ends() {
        let c = column_of(&[(10, 0), (20, 1), (30, 2), (40, 3)]);
        assert_eq!(ids(&c.range(Some(NumKey::Int(20)), Some(NumKey::Int(30)))), vec![1, 2]);
        assert_eq!(ids(&c.range(Some(NumKey::Int(20)), None)), vec![1, 2, 3]);
        assert_eq!(ids(&c.range(None, Some(NumKey::Int(20)))), vec![0, 1]);
        assert_eq!(ids(&c.range(None, None)), vec![0, 1, 2, 3]);
    }

    #[test]
    fn an_empty_range_selects_nothing() {
        let c = column_of(&[(10, 0), (20, 1)]);
        assert!(c.range(Some(NumKey::Int(30)), Some(NumKey::Int(40))).is_empty());
        assert!(c.range(Some(NumKey::Int(30)), Some(NumKey::Int(20))).is_empty());
    }

    #[test]
    fn results_are_identical_across_the_merge_boundary() {
        // The sorted/pending split must be invisible to callers, so build a
        // column large enough to have merged several times and check that a
        // range still returns exactly the right documents.
        let mut c = NumericColumn::default();
        let total = MERGE_THRESHOLD * 2 + 137;
        for i in 0..total {
            // Interleave values so insertion order never matches sort order.
            let value = ((i * 7919) % total) as i64;
            c.push(NumKey::Int(value), i as DocId);
        }
        assert_eq!(c.len(), total);

        let selected = c.range(Some(NumKey::Int(100)), Some(NumKey::Int(199)));
        assert_eq!(selected.len(), 100, "one document per value in the range");

        let expected: Vec<DocId> = (0..total)
            .filter(|i| {
                let v = ((i * 7919) % total) as i64;
                (100..=199).contains(&v)
            })
            .map(|i| i as DocId)
            .collect();
        assert_eq!(ids(&selected), expected);
    }

    #[test]
    fn strict_comparisons_exclude_the_endpoint() {
        let c = column_of(&[(10, 0), (20, 1), (20, 2), (30, 3)]);
        assert_eq!(ids(&c.less_than(NumKey::Int(20))), vec![0]);
        assert_eq!(ids(&c.greater_than(NumKey::Int(20))), vec![3]);
        // Nothing matches below the minimum or above the maximum.
        assert!(c.less_than(NumKey::Int(10)).is_empty());
        assert!(c.greater_than(NumKey::Int(30)).is_empty());
    }

    #[test]
    fn strict_comparisons_agree_with_the_inclusive_ones_across_the_merge_boundary() {
        // The bounds are resolved by binary search over the sorted region and
        // by predicate over the pending tail; both halves must agree.
        let mut c = NumericColumn::default();
        let total = MERGE_THRESHOLD + 313;
        for i in 0..total {
            c.push(NumKey::Int(((i * 31) % 97) as i64), i as DocId);
        }

        for pivot in [0i64, 1, 48, 96, 200] {
            let key = NumKey::Int(pivot);
            let equal = c.range(Some(key), Some(key));

            assert_eq!(c.less_than(key), &c.range(None, Some(key)) - &equal, "< {pivot}");
            assert_eq!(c.greater_than(key), &c.range(Some(key), None) - &equal, "> {pivot}");
        }
    }

    #[test]
    fn duplicate_values_all_come_back() {
        let c = column_of(&[(5, 0), (5, 1), (5, 2), (9, 3)]);
        assert_eq!(ids(&c.range(Some(NumKey::Int(5)), Some(NumKey::Int(5)))), vec![0, 1, 2]);
    }

    #[test]
    fn not_equal_excludes_only_that_value() {
        let c = column_of(&[(10, 0), (20, 1), (10, 2)]);
        assert_eq!(ids(&c.not_equal(NumKey::Int(10))), vec![1]);
        assert_eq!(ids(&c.present()), vec![0, 1, 2]);
    }

    #[test]
    fn integers_stay_exact_beyond_float_precision() {
        // 2^53 and 2^53 + 1 are the same f64; they must not be the same key.
        let big = 1i64 << 53;
        let c = column_of(&[(big, 0), (big + 1, 1)]);
        assert_eq!(ids(&c.range(Some(NumKey::Int(big)), Some(NumKey::Int(big)))), vec![0]);
        assert_eq!(ids(&c.not_equal(NumKey::Int(big))), vec![1]);
    }

    #[test]
    fn floats_order_correctly_including_negatives() {
        let mut c = NumericColumn::default();
        for (v, d) in [(-1.5, 0), (0.0, 1), (2.25, 2), (-10.0, 3)] {
            c.push(NumKey::Float(v), d);
        }
        assert_eq!(ids(&c.range(None, Some(NumKey::Float(0.0)))), vec![0, 1, 3]);
        assert_eq!(ids(&c.range(Some(NumKey::Float(0.0)), None)), vec![1, 2]);
    }

    #[test]
    fn numeric_facet_counts_group_equal_values() {
        let c = column_of(&[(10, 0), (20, 1), (10, 2), (30, 3), (10, 4)]);
        let all = RoaringBitmap::from_iter([0u32, 1, 2, 3, 4]);
        assert_eq!(
            c.value_counts_within(&all),
            vec![(NumKey::Int(10), 3), (NumKey::Int(20), 1), (NumKey::Int(30), 1)]
        );

        let subset = RoaringBitmap::from_iter([0u32, 1]);
        assert_eq!(
            c.value_counts_within(&subset),
            vec![(NumKey::Int(10), 1), (NumKey::Int(20), 1)]
        );

        assert!(c.value_counts_within(&RoaringBitmap::new()).is_empty());
    }

    #[test]
    fn numeric_facet_counts_are_correct_across_the_merge_boundary() {
        // Values must be grouped whether they sit in the sorted region, the
        // pending tail, or both.
        let mut c = NumericColumn::default();
        let total = MERGE_THRESHOLD + 500;
        for i in 0..total {
            c.push(NumKey::Int((i % 5) as i64), i as DocId);
        }
        let all: RoaringBitmap = (0..total as u32).collect();
        let counts = c.value_counts_within(&all);

        assert_eq!(counts.len(), 5, "five distinct values: {counts:?}");
        assert_eq!(counts.iter().map(|(_, n)| n).sum::<u64>(), total as u64);
        assert!(
            counts.windows(2).all(|w| w[0].0.cmp_key(&w[1].0).is_lt()),
            "counts must come back in value order"
        );
    }

    #[test]
    fn keyword_equality_and_negation() {
        let mut c = KeywordColumn::default();
        c.push("Logitech", 0);
        c.push("Razer", 1);
        c.push("Logitech", 2);

        assert_eq!(ids(&c.equals("Logitech")), vec![0, 2]);
        assert!(c.equals("Nobody").is_empty());
        assert_eq!(ids(&c.not_equal("Logitech")), vec![1]);
        assert_eq!(c.num_values(), 2);
    }

    #[test]
    fn keyword_negation_does_not_invent_documents_without_the_field() {
        let mut c = KeywordColumn::default();
        c.push("Logitech", 0);
        // Document 5 never wrote to this column at all.
        assert!(!c.not_equal("Logitech").contains(5));
    }

    #[test]
    fn facet_counts_respect_a_result_set() {
        let mut c = KeywordColumn::default();
        for (brand, doc) in [("Logitech", 0), ("Razer", 1), ("Logitech", 2), ("Anker", 3)] {
            c.push(brand, doc);
        }

        let mut all: Vec<_> = c.value_counts().collect();
        all.sort();
        assert_eq!(all, vec![("Anker", 1), ("Logitech", 2), ("Razer", 1)]);

        let subset = RoaringBitmap::from_iter([0u32, 1]);
        let mut within = c.value_counts_within(&subset);
        within.sort();
        assert_eq!(within, vec![("Logitech", 1), ("Razer", 1)]);
    }

    #[test]
    fn columns_are_allocated_only_where_the_schema_asks() {
        let schema = CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("price", FieldType::Int).with_filter(true),
                FieldSchema::new("notes", FieldType::Keyword),
            ],
        );
        let columns = Columns::new(&schema);

        assert!(columns.keyword(1).is_some(), "faceted keyword gets a column");
        assert!(columns.numeric(2).is_some(), "filterable int gets a column");
        assert!(columns.numeric(0).is_none() && columns.keyword(0).is_none());
        assert!(columns.keyword(3).is_none(), "a plain keyword field needs no column");
    }

    #[test]
    fn pushing_routes_values_to_the_right_column() {
        let schema = CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("price", FieldType::Int).with_filter(true),
            ],
        );
        let mut columns = Columns::new(&schema);
        columns.push(1, 0, &Value::Str("Logitech".into()));
        columns.push(2, 0, &Value::Int(2999));
        columns.push(2, 1, &Value::Null);

        assert_eq!(ids(&columns.keyword(1).unwrap().equals("Logitech")), vec![0]);
        assert_eq!(columns.numeric(2).unwrap().len(), 1, "null is not a value");
    }

    #[test]
    fn multi_valued_fields_record_every_value() {
        let schema = CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("tags", FieldType::Keyword).with_facet(true),
            ],
        );
        let mut columns = Columns::new(&schema);
        columns.push(1, 0, &Value::Array(vec![Value::Str("a".into()), Value::Str("b".into())]));

        let tags = columns.keyword(1).unwrap();
        assert_eq!(ids(&tags.equals("a")), vec![0]);
        assert_eq!(ids(&tags.equals("b")), vec![0]);
        assert_eq!(tags.num_values(), 2);
    }
}
