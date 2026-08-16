//! Search execution: expand the query, walk the postings, score, take top-K.
//!
//! # Shape of a search
//!
//! ```text
//! q -> tokens -> per-token candidate terms -> postings walk
//!   -> flat per-(document, field, token) accumulator -> composite score -> page
//! ```
//!
//! # Multi-field scoring
//!
//! A document is scored on its *best* field, not the sum of all of them.
//! Summing double-counts text that appears in both `title` and `description`
//! and lets a long, repetitive field outrank a precise title match. "Best
//! field" is chosen by boosted BM25, and that same field supplies the
//! proximity and typo signals, so all five PRD §12 components describe the
//! same match rather than a blend of unrelated ones.
//!
//! # Why the accumulator is flat
//!
//! A broad query on a large collection matches a large fraction of the corpus,
//! and every match has to be scored. The obvious structure — a map from doc id
//! to a struct holding a vector per field per token — allocates several times
//! per matched document, and at a hundred thousand matches that allocation
//! traffic dominates the query.
//!
//! So the evidence lives in flat vectors addressed arithmetically by
//! `(slot, field, token)`. A newly matched document appends one block; nothing
//! else allocates.

use std::borrow::Cow;
use std::cmp::Ordering;

use roaring::RoaringBitmap;

use tachyon_core::{CollectionSchema, DocId, FieldId, Value};
use tachyon_index::{FieldPostings, FuzzyMatcher, IndexSource};

use crate::bm25::{self, FieldStats};
use crate::filter;
use crate::query_text::{self, ParsedQuery};
use crate::request::{MatchMode, SearchRequest};
use crate::score::{self, ScoreComponents, ScoreWeights};
use crate::sort::{self, SortClause, SortKey, SortValue};

/// Cap on how many dictionary terms one query token may expand into. Without
/// it, a one-character prefix on a large collection would walk the entire term
/// dictionary.
pub const MAX_TERM_EXPANSIONS: usize = 128;

/// Sentinel in the edits array meaning "this token did not match here".
const UNMATCHED: u32 = u32::MAX;

/// Hasher for the document accumulator's index.
///
/// The standard library's default is SipHash, which is the right choice for a
/// map whose keys an attacker chooses — and the wrong one here. These keys are
/// internal doc ids we assigned ourselves, sequential and dense, and a broad
/// query hashes one per match.
#[derive(Default, Clone, Copy)]
struct DocIdHasher(u64);

impl std::hash::Hasher for DocIdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        // Only ever called with a doc id, but stay correct if that changes.
        for byte in bytes {
            self.write_u64(*byte as u64);
        }
    }

    fn write_u32(&mut self, value: u32) {
        self.write_u64(value as u64);
    }

    fn write_u64(&mut self, value: u64) {
        // Fibonacci hashing: multiply by 2^64 / φ and fold the high bits, which
        // mix in every input bit, down to where the map reads them.
        self.0 = (self.0 ^ value).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 ^= self.0 >> 32;
    }
}

type DocIdBuildHasher = std::hash::BuildHasherDefault<DocIdHasher>;

/// Everything a search reads.
pub struct SearchContext<'a> {
    pub schema: &'a CollectionSchema,
    /// Memtable first, then committed segments.
    pub sources: Vec<&'a dyn IndexSource>,
    /// Collection-wide tombstones, covering documents inside segments.
    pub deleted: &'a RoaringBitmap,
}

impl<'a> SearchContext<'a> {
    pub fn new(
        schema: &'a CollectionSchema,
        sources: Vec<&'a dyn IndexSource>,
        deleted: &'a RoaringBitmap,
    ) -> Self {
        SearchContext { schema, sources, deleted }
    }

    /// Corpus statistics for a field, summed across every source. BM25 needs
    /// global numbers; per-source stats would score the same document
    /// differently depending on which segment it landed in.
    fn field_stats(&self, field: FieldId) -> FieldStats {
        let mut doc_count = 0u32;
        let mut total_len = 0u64;
        for source in &self.sources {
            doc_count = doc_count.saturating_add(source.field_doc_count(field));
            total_len = total_len.saturating_add(source.total_field_len(field));
        }
        FieldStats::new(doc_count, total_len)
    }

    fn value(&self, doc_id: DocId, field: FieldId) -> Option<Cow<'a, Value>> {
        self.sources.iter().find_map(|s| {
            (doc_id >= s.min_doc_id() && doc_id < s.end_doc_id()).then(|| s.value(doc_id, field))?
        })
    }
}

/// A term a query token was expanded into, and what it cost to get there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermCandidate {
    pub term: String,
    /// Edit distance from the original token. 0 for exact and prefix matches.
    pub edits: u32,
}

/// A scored document, before its source JSON is attached.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoredDoc {
    pub doc_id: DocId,
    pub score: f32,
    pub components: ScoreComponents,
}

/// What the executor produces: the page, plus the total that matched.
#[derive(Debug, Clone)]
pub struct SearchOutcome {
    /// Total matching documents, not just the returned page (PRD §13 `found`).
    pub found: usize,
    pub hits: Vec<ScoredDoc>,
    /// Every document that matched, for faceting over the full result set
    /// rather than the page (PRD §7.7: "accurate counts after filters").
    pub matched: RoaringBitmap,
}

/// Per-(document, field, token) evidence, in flat arrays.
///
/// `slot` identifies a matched document; `field` is an index into the
/// request's `query_by` list, not a schema field id.
struct Accumulator {
    num_fields: usize,
    num_tokens: usize,
    /// Doc id to slot.
    index: std::collections::HashMap<DocId, u32, DocIdBuildHasher>,
    /// Slot to doc id.
    docs: Vec<DocId>,
    /// Best BM25 contribution, addressed by [`Accumulator::cell`].
    scores: Vec<f32>,
    /// Cheapest edit distance, same addressing. [`UNMATCHED`] if absent.
    edits: Vec<u32>,
    /// Token positions, same addressing. Empty unless positions are needed.
    positions: Vec<Vec<u32>>,
    needs_positions: bool,
}

impl Accumulator {
    fn new(num_fields: usize, num_tokens: usize, needs_positions: bool) -> Accumulator {
        Accumulator {
            num_fields: num_fields.max(1),
            num_tokens: num_tokens.max(1),
            index: std::collections::HashMap::default(),
            docs: Vec::new(),
            scores: Vec::new(),
            edits: Vec::new(),
            positions: Vec::new(),
            needs_positions,
        }
    }

    /// Slot for a document, appending a fresh block the first time it is seen.
    ///
    /// One hash lookup, not the two a `get` followed by an `insert` would cost:
    /// this runs once per posting on a broad query, which is the single most
    /// executed line in a search.
    fn slot(&mut self, doc_id: DocId) -> usize {
        let next = self.docs.len() as u32;
        match self.index.entry(doc_id) {
            std::collections::hash_map::Entry::Occupied(existing) => *existing.get() as usize,
            std::collections::hash_map::Entry::Vacant(empty) => {
                empty.insert(next);
                self.docs.push(doc_id);

                let block = self.num_fields * self.num_tokens;
                self.scores.resize(self.scores.len() + block, 0.0);
                self.edits.resize(self.edits.len() + block, UNMATCHED);
                if self.needs_positions {
                    self.positions.resize(self.positions.len() + block, Vec::new());
                }
                next as usize
            }
        }
    }

    /// Put every position list into the sorted, deduplicated form the phrase
    /// and proximity checks need.
    ///
    /// Done once, after the walk, rather than on each read: a position list is
    /// read once per field per phrase token and again for proximity, and
    /// sorting a fresh clone every time was the largest cost in scoring a
    /// multi-token query. Positions arrive out of order because one token can
    /// expand into several terms, each contributing its own occurrences.
    fn normalize_positions(&mut self) {
        for list in &mut self.positions {
            if list.len() > 1 {
                list.sort_unstable();
                list.dedup();
            }
        }
    }

    fn cell(&self, slot: usize, field: usize, token: usize) -> usize {
        (slot * self.num_fields + field) * self.num_tokens + token
    }

    /// Whether the token matched in this field of this document.
    fn matched(&self, slot: usize, field: usize, token: usize) -> bool {
        self.edits[self.cell(slot, field, token)] != UNMATCHED
    }

    /// BM25 for one field of one document: the sum over its matched tokens.
    fn field_bm25(&self, slot: usize, field: usize) -> f32 {
        let start = self.cell(slot, field, 0);
        self.scores[start..start + self.num_tokens].iter().sum()
    }

    fn field_edits(&self, slot: usize, field: usize) -> u32 {
        let start = self.cell(slot, field, 0);
        self.edits[start..start + self.num_tokens].iter().filter(|e| **e != UNMATCHED).sum()
    }

    fn field_matched_tokens(&self, slot: usize, field: usize) -> usize {
        let start = self.cell(slot, field, 0);
        self.edits[start..start + self.num_tokens].iter().filter(|e| **e != UNMATCHED).count()
    }

    /// Sorted, deduplicated positions of one token in one field.
    ///
    /// Valid only after [`Accumulator::normalize_positions`]; borrowed rather
    /// than cloned, so reading a position list costs nothing.
    fn token_positions(&self, slot: usize, field: usize, token: usize) -> &[u32] {
        if !self.needs_positions {
            return &[];
        }
        &self.positions[self.cell(slot, field, token)]
    }

    fn len(&self) -> usize {
        self.docs.len()
    }
}

/// Run a search.
///
/// Filters are evaluated first, into a bitmap, so the postings walk can reject
/// a document before scoring it rather than after.
pub fn execute(ctx: &SearchContext, req: &SearchRequest) -> SearchOutcome {
    let filter_set = req.filter_expr.as_ref().map(|expr| filter::evaluate(expr, &ctx.sources));
    let filter = filter_set.as_ref();

    let query = query_text::parse(&req.q);
    let tokens = &query.tokens;

    if query.is_empty() {
        return match_all(ctx, req, filter);
    }

    // Expanding a token depends only on the term dictionary, not on which
    // field is being searched, so do it once per token rather than once per
    // (token, field) pair.
    let expansions: Vec<Vec<TermCandidate>> = tokens
        .iter()
        .enumerate()
        .map(|(i, token)| {
            // A phrase asks for these exact terms adjacent to each other, so
            // prefix expansion is suppressed inside one.
            let expand_prefix = i + 1 == tokens.len() && !query.in_phrase(i);
            expand_token(ctx, ctx.schema, token, req, expand_prefix, query.in_phrase(i))
        })
        .collect();

    // Positions are read only by proximity scoring and phrase verification. A
    // single-token query needs neither — the proximity of one term is 1.0 by
    // definition — and collecting them anyway is the largest avoidable cost on
    // a query that matches a lot of documents.
    let needs_positions = tokens.len() > 1 || !query.phrases.is_empty();
    let mut acc = Accumulator::new(req.query_by.len(), tokens.len(), needs_positions);

    // Reused across candidates so the outer `Vec` does not reallocate per
    // candidate. The `Cow` each slot holds is a zero-cost borrow for a
    // memtable source but a fresh decode for a segment one — resolving a
    // term against every source once, rather than once for `doc_freq` and
    // again for the postings walk, still matters for a segment exactly
    // because that decode is real work now.
    let mut per_source: Vec<Option<Cow<'_, FieldPostings>>> = Vec::with_capacity(ctx.sources.len());

    for (field_pos, &(field, _boost)) in req.query_by.iter().enumerate() {
        let stats = ctx.field_stats(field);
        if stats.doc_count == 0 {
            continue;
        }

        for (token_idx, candidates) in expansions.iter().enumerate() {
            for candidate in candidates {
                // Resolve the term in each source once. Asking for the document
                // frequency and then for the postings would walk every source's
                // term dictionary twice for the same term.
                per_source.clear();
                per_source.extend(ctx.sources.iter().map(|s| s.postings(&candidate.term, field)));

                let doc_freq: u32 =
                    per_source.iter().map(|p| p.as_ref().map_or(0, |p| p.doc_freq())).sum();
                if doc_freq == 0 {
                    continue;
                }
                let idf = bm25::idf(doc_freq, stats.doc_count);

                for (source, postings) in ctx.sources.iter().zip(per_source.iter()) {
                    let Some(postings) = postings else {
                        continue;
                    };
                    for posting in &postings.docs {
                        let doc_id = posting.doc_id;
                        if !source.is_live(doc_id) || ctx.deleted.contains(doc_id) {
                            continue;
                        }
                        if filter.is_some_and(|f| !f.contains(doc_id)) {
                            continue;
                        }

                        let field_len = source.field_len(doc_id, field);
                        let contribution = bm25::term_score(posting.tf(), field_len, stats, idf);

                        let slot = acc.slot(doc_id);
                        let cell = acc.cell(slot, field_pos, token_idx);

                        // A token expanded into several terms keeps its best
                        // single contribution rather than the sum, so matching
                        // many prefix variants is not itself a relevance signal.
                        if contribution > acc.scores[cell] {
                            acc.scores[cell] = contribution;
                        }
                        if candidate.edits < acc.edits[cell] {
                            acc.edits[cell] = candidate.edits;
                        }
                        if needs_positions {
                            acc.positions[cell].extend_from_slice(&posting.positions);
                        }
                    }
                }
            }
        }
    }

    acc.normalize_positions();
    finish(ctx, req, &query, acc)
}

/// Turn the accumulator into a ranked page.
fn finish(
    ctx: &SearchContext,
    req: &SearchRequest,
    query: &ParsedQuery,
    acc: Accumulator,
) -> SearchOutcome {
    let tokens = &query.tokens;
    let weights = ScoreWeights::default();
    let max_boost = score::max_boost(ctx.schema);
    let allowed_edits = total_allowed_edits(ctx.schema, tokens, req);
    let popularity_field = ctx.schema.field(score::POPULARITY_FIELD).map(|(id, _)| id);

    let required = match req.match_mode {
        MatchMode::All => tokens.len(),
        MatchMode::Any => 1,
    };

    let num_fields = req.query_by.len();
    let mut candidates: Vec<Ranked> = Vec::new();
    // Gathered flat and turned into a bitmap at the end. Documents are visited
    // in postings-walk order, which is not doc id order, and inserting an
    // unordered run into a roaring bitmap shifts elements inside its container
    // on nearly every call — a real cost when a broad query matches thousands
    // of documents, and this set is built on every search that facets.
    let mut matched_ids: Vec<DocId> = Vec::new();
    // Reused across documents so scoring allocates once, not once per hit.
    let mut present: Vec<&[u32]> = Vec::with_capacity(tokens.len());

    for slot in 0..acc.len() {
        let doc_id = acc.docs[slot];

        // A document qualifies on the union of its fields: "wireless" in the
        // title and "mouse" in the description is still a match for
        // "wireless mouse".
        let union = (0..tokens.len())
            .filter(|token| (0..num_fields).any(|f| acc.matched(slot, f, *token)))
            .count();
        if union < required {
            continue;
        }

        // A phrase is all-or-nothing regardless of match mode: quoting terms
        // is an explicit request for them to be adjacent.
        if !query.phrases.is_empty() && !satisfies_phrases(&acc, slot, req, &query.phrases) {
            continue;
        }

        matched_ids.push(doc_id);

        // Pick the field that best explains this match.
        let mut best_field = 0usize;
        let mut best_bm25 = 0.0f32;
        let mut best_boosted = f32::NEG_INFINITY;
        for field_pos in 0..num_fields {
            let raw = acc.field_bm25(slot, field_pos);
            let boosted = raw * req.query_by[field_pos].1;
            if boosted > best_boosted {
                best_boosted = boosted;
                best_bm25 = raw;
                best_field = field_pos;
            }
        }

        // Proximity is only meaningful over the tokens this field actually has.
        present.clear();
        present.extend(
            (0..tokens.len())
                .filter(|token| acc.matched(slot, best_field, *token))
                .map(|token| acc.token_positions(slot, best_field, token)),
        );

        let matched_here = acc.field_matched_tokens(slot, best_field);
        let proximity = if matched_here == tokens.len() {
            score::proximity(&present)
        } else {
            // A partial match in this field cannot claim tight proximity.
            score::proximity(&present) * matched_here as f32 / tokens.len() as f32
        };

        let popularity = popularity_field
            .and_then(|f| ctx.value(doc_id, f))
            .and_then(|v| v.as_f64())
            .map(|v| score::normalize_popularity(v as f32))
            .unwrap_or(0.0);

        let components = ScoreComponents {
            bm25: score::normalize_bm25(best_bm25),
            field_boost: score::normalize_field_boost(req.query_by[best_field].1, max_boost),
            proximity,
            typo_penalty: score::typo_penalty(acc.field_edits(slot, best_field), allowed_edits),
            popularity,
        };

        let score = components.combine(&weights);
        candidates.push(Ranked {
            doc_id,
            score,
            components,
            sort_values: sort_values(ctx, req, doc_id, score),
        });
    }

    // A document reaches this point at most once, so the count is exact and no
    // deduplication is needed.
    let found = matched_ids.len();
    let matched = sorted_bitmap(matched_ids);

    let hits = paginate(req, candidates);
    SearchOutcome { found, hits, matched }
}

/// Build a bitmap from doc ids in arbitrary order, sorting first so the fill
/// is linear rather than an insert-and-shift per element.
///
/// Worth it because a result set is sparse: roaring holds it in array
/// containers, where an out-of-order insert shifts the tail of the container.
/// The same trick loses on a dense set — see `collect_docs` in the index
/// crate's columns, which deliberately does not sort.
fn sorted_bitmap(mut doc_ids: Vec<DocId>) -> RoaringBitmap {
    if doc_ids.is_empty() {
        return RoaringBitmap::new();
    }
    doc_ids.sort_unstable();
    doc_ids.dedup();
    RoaringBitmap::from_sorted_iter(doc_ids).expect("doc ids were sorted and deduplicated")
}

/// An empty query matches every live document, so filters and sorting can be
/// used on their own to browse a collection.
fn match_all(
    ctx: &SearchContext,
    req: &SearchRequest,
    filter: Option<&RoaringBitmap>,
) -> SearchOutcome {
    let mut matched = RoaringBitmap::new();
    let mut candidates = Vec::new();

    for source in &ctx.sources {
        for doc_id in source.min_doc_id()..source.end_doc_id() {
            if !source.is_live(doc_id) || ctx.deleted.contains(doc_id) {
                continue;
            }
            if filter.is_some_and(|f| !f.contains(doc_id)) {
                continue;
            }
            matched.insert(doc_id);
            candidates.push(Ranked {
                doc_id,
                score: 0.0,
                components: ScoreComponents::default(),
                sort_values: sort_values(ctx, req, doc_id, 0.0),
            });
        }
    }

    let found = matched.len() as usize;
    let hits = paginate(req, candidates);
    SearchOutcome { found, hits, matched }
}

/// Values for each sort clause, in clause order. Empty when the request does
/// not sort, in which case ranking falls back to score.
fn sort_values(
    ctx: &SearchContext,
    req: &SearchRequest,
    doc_id: DocId,
    score: f32,
) -> Vec<SortValue> {
    let Some(clauses) = &req.sort_clauses else {
        return Vec::new();
    };
    clauses
        .iter()
        .map(|clause| match clause.key {
            SortKey::TextMatch => SortValue::Score(score),
            SortKey::Field(field) => sort::sort_value(ctx.value(doc_id, field).as_deref()),
        })
        .collect()
}

/// Order the candidates and cut out the requested page.
///
/// Partitioning around the window with `select_nth_unstable_by` is O(n) and
/// only the window itself is fully sorted, so a query matching a million
/// documents does not pay to order all of them.
fn paginate(req: &SearchRequest, mut candidates: Vec<Ranked>) -> Vec<ScoredDoc> {
    let window = req.window();
    if window == 0 {
        // `limit=0` asks for no documents. Ordering a whole collection to
        // return none of it is pure waste, and it is exactly the request an
        // impatient caller sends to read `found` alone.
        return Vec::new();
    }

    let clauses: &[SortClause] = req.sort_clauses.as_deref().unwrap_or(&[]);
    let compare = |a: &Ranked, b: &Ranked| -> Ordering {
        if clauses.is_empty() {
            // Highest score first, doc id to break ties.
            b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal).then(a.doc_id.cmp(&b.doc_id))
        } else {
            sort::compare(clauses, &a.sort_values, &b.sort_values, a.doc_id, b.doc_id)
        }
    };

    if candidates.len() > window {
        candidates.select_nth_unstable_by(window - 1, compare);
        candidates.truncate(window);
    }
    candidates.sort_by(compare);

    candidates
        .into_iter()
        .skip(req.offset)
        .take(req.limit)
        .map(|r| ScoredDoc { doc_id: r.doc_id, score: r.score, components: r.components })
        .collect()
}

/// Whether some single field places every phrase's tokens consecutively.
///
/// A phrase must be satisfied within one field — `title` ending in "mouse" and
/// `description` starting with "pad" is not the phrase "mouse pad".
fn satisfies_phrases(
    acc: &Accumulator,
    slot: usize,
    req: &SearchRequest,
    phrases: &[(usize, usize)],
) -> bool {
    phrases.iter().all(|&(start, end)| {
        (0..req.query_by.len()).any(|field| phrase_in_field(acc, slot, field, start, end))
    })
}

fn phrase_in_field(acc: &Accumulator, slot: usize, field: usize, start: usize, end: usize) -> bool {
    // Every token of the phrase has to be present in this field at all.
    if (start..=end).any(|token| !acc.matched(slot, field, token)) {
        return false;
    }

    // Walk the first token's positions; each is a candidate phrase start.
    let first = acc.token_positions(slot, field, start);

    first.iter().any(|&anchor| {
        (start + 1..=end).enumerate().all(|(offset, token)| {
            let positions = acc.token_positions(slot, field, token);
            anchor
                .checked_add(offset as u32 + 1)
                .is_some_and(|expected| positions.binary_search(&expected).is_ok())
        })
    })
}

/// Total edit budget the typo table grants this query, used to normalize the
/// typo component.
fn total_allowed_edits(schema: &CollectionSchema, tokens: &[String], req: &SearchRequest) -> u32 {
    if !req.typo_tolerance {
        return 0;
    }
    tokens.iter().map(|t| schema.typo_tolerance.typos_for_length(t.chars().count()) as u32).sum()
}

/// Terms a query token should match.
///
/// Three sources, in decreasing confidence:
///
/// 1. The token itself, always, at zero edits.
/// 2. Terms it prefixes, when the request asks and the token is not inside a
///    phrase — this is what makes search-as-you-type work.
/// 3. Terms within the typo table's edit budget (PRD §7.4).
///
/// Expansions are capped and the cheapest edits are kept first, so a short
/// token on a large dictionary cannot turn one query into a dictionary scan's
/// worth of postings walks.
fn expand_token(
    ctx: &SearchContext,
    schema: &CollectionSchema,
    token: &str,
    req: &SearchRequest,
    expand_prefix: bool,
    in_phrase: bool,
) -> Vec<TermCandidate> {
    let mut candidates = vec![TermCandidate { term: token.to_string(), edits: 0 }];
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::from([token.to_string()]);

    if req.prefix && expand_prefix {
        let mut terms = Vec::new();
        for source in &ctx.sources {
            // Bounded per source, not just after merging: the merge keeps the
            // alphabetically first `MAX_TERM_EXPANSIONS` terms, and every one
            // of those is within the first `MAX_TERM_EXPANSIONS` of the source
            // it came from, so capping here cannot change the result.
            source.collect_terms_with_prefix(token, MAX_TERM_EXPANSIONS, &mut terms);
        }
        terms.sort_unstable();
        terms.dedup();

        for term in terms.into_iter().take(MAX_TERM_EXPANSIONS) {
            if seen.insert(term.clone()) {
                candidates.push(TermCandidate { term, edits: 0 });
            }
        }
    }

    // A phrase is a request for these exact words; correcting them would
    // quietly answer a different question.
    let max_edits = if req.typo_tolerance && !in_phrase {
        schema.typo_tolerance.typos_for_length(token.chars().count())
    } else {
        0
    };

    if max_edits > 0 {
        let mut matcher = FuzzyMatcher::new(token, max_edits as u32);
        let mut matches = Vec::new();
        for source in &ctx.sources {
            source.collect_fuzzy_terms(&mut matcher, &mut matches);
        }
        // Nearest first, so the cap keeps the most plausible corrections.
        matches.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        matches.dedup_by(|a, b| a.0 == b.0);

        for (term, edits) in matches.into_iter().take(MAX_TERM_EXPANSIONS) {
            if edits > 0 && seen.insert(term.clone()) {
                candidates.push(TermCandidate { term, edits });
            }
        }
    }

    candidates
}

/// A candidate document with everything needed to order it.
#[derive(Debug, Clone)]
struct Ranked {
    doc_id: DocId,
    score: f32,
    components: ScoreComponents,
    /// One entry per sort clause; empty when ranking by relevance.
    sort_values: Vec<SortValue>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tachyon_core::{FieldSchema, FieldType, ParsedDocument};
    use tachyon_index::MemTable;

    use crate::request::SearchParams;

    fn schema() -> CollectionSchema {
        CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("description", FieldType::Text),
                FieldSchema::new("popularity", FieldType::Int).with_sort(true),
            ],
        )
    }

    struct Fixture {
        schema: CollectionSchema,
        memtable: MemTable,
        deleted: RoaringBitmap,
    }

    impl Fixture {
        fn new(docs: &[serde_json::Value]) -> Fixture {
            let schema = schema();
            let mut memtable = MemTable::new(0, &schema);
            for doc in docs {
                memtable.insert(ParsedDocument::parse(doc.clone(), &schema).unwrap());
            }
            Fixture { schema, memtable, deleted: RoaringBitmap::new() }
        }

        fn search(&self, params: SearchParams) -> SearchOutcome {
            let req = SearchRequest::resolve(params, &self.schema).unwrap();
            let ctx = SearchContext::new(&self.schema, vec![&self.memtable], &self.deleted);
            execute(&ctx, &req)
        }

        fn ids(&self, outcome: &SearchOutcome) -> Vec<String> {
            outcome.hits.iter().map(|h| self.memtable.get(h.doc_id).unwrap().id.clone()).collect()
        }
    }

    fn query(q: &str) -> SearchParams {
        SearchParams { q: Some(q.into()), prefix: Some(false), ..Default::default() }
    }

    fn corpus() -> Fixture {
        Fixture::new(&[
            json!({"id": "1", "title": "Wireless Mouse", "description": "A comfortable wireless mouse for the office"}),
            json!({"id": "2", "title": "Mechanical Keyboard", "description": "Loud and tactile"}),
            json!({"id": "3", "title": "Mouse Pad", "description": "Large desk mat for a mouse"}),
            json!({"id": "4", "title": "Wireless Charger", "description": "Charges phones wirelessly"}),
        ])
    }

    #[test]
    fn finds_documents_containing_the_term() {
        let f = corpus();
        let out = f.search(query("mouse"));
        assert_eq!(out.found, 2);
        let ids = f.ids(&out);
        assert!(ids.contains(&"1".to_string()));
        assert!(ids.contains(&"3".to_string()));
    }

    #[test]
    fn all_tokens_must_match_by_default() {
        let f = corpus();
        let out = f.search(query("wireless mouse"));
        assert_eq!(out.found, 1, "only document 1 has both terms");
        assert_eq!(f.ids(&out), vec!["1"]);
    }

    #[test]
    fn any_mode_widens_the_result_set() {
        let f = corpus();
        let params = SearchParams { match_mode: Some("any".into()), ..query("wireless mouse") };
        let out = f.search(params);
        // Documents 1, 3 and 4 have at least one term; the keyboard has neither.
        assert_eq!(out.found, 3);
        assert!(!f.ids(&out).contains(&"2".to_string()));
        // And the document with both still ranks first.
        assert_eq!(f.ids(&out)[0], "1");
    }

    #[test]
    fn a_title_hit_outranks_a_description_hit() {
        // PRD §12: title is boosted 10, description 2.
        let f = Fixture::new(&[
            json!({"id": "desc", "title": "Something Else", "description": "a wireless thing"}),
            json!({"id": "title", "title": "Wireless Adapter", "description": "plain"}),
        ]);
        let out = f.search(query("wireless"));
        assert_eq!(f.ids(&out), vec!["title", "desc"]);
    }

    #[test]
    fn adjacent_terms_outrank_scattered_ones() {
        let f = Fixture::new(&[
            json!({"id": "scattered", "title": "wireless charging pad for a mouse and more", "description": ""}),
            json!({"id": "adjacent", "title": "wireless mouse", "description": ""}),
        ]);
        let out = f.search(query("wireless mouse"));
        assert_eq!(f.ids(&out), vec!["adjacent", "scattered"]);
        assert!(out.hits[0].components.proximity > out.hits[1].components.proximity);
    }

    #[test]
    fn popularity_breaks_a_tie() {
        let f = Fixture::new(&[
            json!({"id": "unpopular", "title": "wireless mouse", "popularity": 0}),
            json!({"id": "popular", "title": "wireless mouse", "popularity": 5000}),
        ]);
        let out = f.search(query("wireless mouse"));
        assert_eq!(f.ids(&out), vec!["popular", "unpopular"]);
        assert!(out.hits[0].components.popularity > out.hits[1].components.popularity);
    }

    #[test]
    fn no_match_returns_nothing() {
        let f = corpus();
        let out = f.search(query("helicopter"));
        assert_eq!(out.found, 0);
        assert!(out.hits.is_empty());
    }

    #[test]
    fn deleted_documents_are_invisible() {
        let mut f = corpus();
        let doc_id = f.memtable.lookup("1").unwrap();
        f.memtable.remove(doc_id);
        let out = f.search(query("mouse"));
        assert_eq!(out.found, 1);
        assert_eq!(f.ids(&out), vec!["3"]);
    }

    #[test]
    fn tombstoned_documents_are_invisible() {
        let mut f = corpus();
        f.deleted.insert(f.memtable.lookup("3").unwrap());
        let out = f.search(query("mouse"));
        assert_eq!(out.found, 1);
        assert_eq!(f.ids(&out), vec!["1"]);
    }

    #[test]
    fn prefix_matching_completes_the_last_token() {
        let f = corpus();
        let exact = f.search(query("wirel"));
        assert_eq!(exact.found, 0, "without prefix, a partial word matches nothing");

        let prefixed = f.search(SearchParams {
            q: Some("wirel".into()),
            prefix: Some(true),
            ..Default::default()
        });
        assert_eq!(prefixed.found, 2, "wireless mouse and wireless charger");
    }

    #[test]
    fn prefix_only_applies_to_the_final_token() {
        let f = corpus();
        let out = f.search(SearchParams {
            q: Some("wirel mouse".into()),
            prefix: Some(true),
            ..Default::default()
        });
        assert_eq!(out.found, 0, "the leading token must match exactly");
    }

    #[test]
    fn an_empty_query_matches_everything() {
        let f = corpus();
        let out = f.search(SearchParams { q: Some("".into()), ..Default::default() });
        assert_eq!(out.found, 4);
        assert_eq!(out.hits.len(), 4);
    }

    #[test]
    fn pagination_walks_a_stable_ranking() {
        let f = corpus();
        let all = f.search(SearchParams { limit: Some(10), ..query("mouse") });
        let page1 = f.search(SearchParams { limit: Some(1), ..query("mouse") });
        let page2 = f.search(SearchParams { limit: Some(1), offset: Some(1), ..query("mouse") });

        assert_eq!(all.found, 2);
        assert_eq!(page1.found, 2, "found is the total, not the page size");
        assert_eq!(f.ids(&page1), vec![f.ids(&all)[0].clone()]);
        assert_eq!(f.ids(&page2), vec![f.ids(&all)[1].clone()]);
    }

    #[test]
    fn scoring_is_deterministic() {
        let f = corpus();
        let a = f.search(query("mouse"));
        let b = f.search(query("mouse"));
        assert_eq!(f.ids(&a), f.ids(&b));
        assert_eq!(a.hits[0].score, b.hits[0].score);
    }

    #[test]
    fn a_zero_limit_still_reports_the_total() {
        // `limit=0` short-circuits ranking entirely, so the counts it does
        // report have to be the ones a normal request would have produced.
        let f = corpus();
        let out = f.search(SearchParams { limit: Some(0), ..query("mouse") });
        assert!(out.hits.is_empty());
        assert_eq!(out.found, 2, "the total is independent of the page size");
        assert_eq!(out.matched.len(), 2, "and faceting still sees every match");
    }

    #[test]
    fn matched_set_covers_every_hit_not_just_the_page() {
        let f = corpus();
        let out = f.search(SearchParams { limit: Some(1), ..query("mouse") });
        assert_eq!(out.hits.len(), 1);
        assert_eq!(out.matched.len(), 2, "faceting needs the full match set");
    }

    /// A catalogue with brands and prices, for filter and sort coverage.
    fn catalogue() -> Fixture {
        let schema = CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("description", FieldType::Text),
                FieldSchema::new("popularity", FieldType::Int).with_sort(true),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("price", FieldType::Int).with_filter(true).with_sort(true),
            ],
        );
        let docs = [
            json!({"id": "1", "title": "Wireless Mouse", "brand": "Logitech", "price": 2999}),
            json!({"id": "2", "title": "Gaming Mouse", "brand": "Razer", "price": 5999}),
            json!({"id": "3", "title": "Mouse Pad", "brand": "Logitech", "price": 999}),
            json!({"id": "4", "title": "Silent Mouse", "brand": "Anker", "price": 1999}),
            json!({"id": "5", "title": "Keyboard", "brand": "Razer", "price": 8999}),
        ];
        let mut memtable = MemTable::new(0, &schema);
        for doc in &docs {
            memtable.insert(ParsedDocument::parse(doc.clone(), &schema).unwrap());
        }
        Fixture { schema, memtable, deleted: RoaringBitmap::new() }
    }

    fn filtered(filter: &str) -> SearchParams {
        SearchParams {
            q: Some("mouse".into()),
            prefix: Some(false),
            filter: Some(filter.into()),
            ..Default::default()
        }
    }

    #[test]
    fn a_filter_narrows_the_result_set() {
        let f = catalogue();
        assert_eq!(f.search(query("mouse")).found, 4);

        let out = f.search(filtered("brand:=Logitech"));
        assert_eq!(out.found, 2);
        let mut got = f.ids(&out);
        got.sort();
        assert_eq!(got, vec!["1", "3"]);
    }

    #[test]
    fn the_prd_filter_example_works() {
        // PRD §7.6: brand:=Logitech && price:<5000
        let f = catalogue();
        let out = f.search(filtered("brand:=Logitech && price:<5000"));
        let mut got = f.ids(&out);
        got.sort();
        assert_eq!(got, vec!["1", "3"]);
    }

    #[test]
    fn range_and_set_filters() {
        let f = catalogue();

        let out = f.search(filtered("price:[1000..3000]"));
        let mut got = f.ids(&out);
        got.sort();
        assert_eq!(got, vec!["1", "4"]);

        let out = f.search(filtered("brand:=[Razer,Anker]"));
        let mut got = f.ids(&out);
        got.sort();
        assert_eq!(got, vec!["2", "4"], "the keyboard is excluded by the query, not the filter");
    }

    #[test]
    fn or_and_parentheses_in_filters() {
        let f = catalogue();
        let out = f.search(filtered("(brand:=Anker || brand:=Razer) && price:>3000"));
        assert_eq!(f.ids(&out), vec!["2"]);
    }

    #[test]
    fn a_filter_matching_nothing_returns_nothing() {
        let f = catalogue();
        let out = f.search(filtered("brand:=Nobody"));
        assert_eq!(out.found, 0);
        assert!(out.hits.is_empty());
    }

    #[test]
    fn filters_compose_with_an_empty_query_to_browse() {
        let f = catalogue();
        let params = SearchParams {
            q: Some("".into()),
            filter: Some("brand:=Razer".into()),
            ..Default::default()
        };
        let out = f.search(params);
        assert_eq!(out.found, 2, "both Razer products, query or no query");
    }

    #[test]
    fn filters_do_not_resurrect_deleted_documents() {
        let mut f = catalogue();
        f.memtable.remove(f.memtable.lookup("1").unwrap());
        let out = f.search(filtered("brand:=Logitech"));
        assert_eq!(f.ids(&out), vec!["3"]);
    }

    fn sorted(sort: &str) -> SearchParams {
        SearchParams {
            q: Some("mouse".into()),
            prefix: Some(false),
            sort: Some(sort.into()),
            limit: Some(10),
            ..Default::default()
        }
    }

    #[test]
    fn sorting_by_a_numeric_field_overrides_relevance() {
        let f = catalogue();
        assert_eq!(f.ids(&f.search(sorted("price:asc"))), vec!["3", "4", "1", "2"]);
        assert_eq!(f.ids(&f.search(sorted("price:desc"))), vec!["2", "1", "4", "3"]);
    }

    #[test]
    fn the_prd_sort_example_ranks_by_relevance_then_price() {
        // PRD §7.8: sort=_text_match:desc,price:asc
        let f = catalogue();
        let out = f.search(sorted("_text_match:desc,price:asc"));
        assert_eq!(out.found, 4);
        let scores: Vec<f32> = out.hits.iter().map(|h| h.score).collect();
        assert!(scores.windows(2).all(|w| w[0] >= w[1]), "relevance leads: {scores:?}");
    }

    #[test]
    fn price_breaks_ties_when_relevance_is_equal() {
        let schema = CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("description", FieldType::Text),
                FieldSchema::new("popularity", FieldType::Int).with_sort(true),
                FieldSchema::new("price", FieldType::Int).with_sort(true),
            ],
        );
        // Identical text, so the only thing separating them is price.
        let mut memtable = MemTable::new(0, &schema);
        for (id, price) in [("expensive", 900), ("cheap", 100), ("mid", 500)] {
            memtable.insert(
                ParsedDocument::parse(
                    json!({"id": id, "title": "wireless mouse", "price": price}),
                    &schema,
                )
                .unwrap(),
            );
        }
        let f = Fixture { schema, memtable, deleted: RoaringBitmap::new() };
        let out = f.search(sorted("_text_match:desc,price:asc"));
        assert_eq!(f.ids(&out), vec!["cheap", "mid", "expensive"]);
    }

    #[test]
    fn documents_missing_the_sort_field_go_last() {
        let schema = CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("description", FieldType::Text),
                FieldSchema::new("popularity", FieldType::Int).with_sort(true),
                FieldSchema::new("price", FieldType::Int).with_sort(true),
            ],
        );
        let mut memtable = MemTable::new(0, &schema);
        for doc in [
            json!({"id": "priced", "title": "mouse", "price": 500}),
            json!({"id": "unpriced", "title": "mouse"}),
            json!({"id": "cheap", "title": "mouse", "price": 100}),
        ] {
            memtable.insert(ParsedDocument::parse(doc, &schema).unwrap());
        }
        let f = Fixture { schema, memtable, deleted: RoaringBitmap::new() };

        assert_eq!(f.ids(&f.search(sorted("price:asc"))), vec!["cheap", "priced", "unpriced"]);
        assert_eq!(
            f.ids(&f.search(sorted("price:desc"))),
            vec!["priced", "cheap", "unpriced"],
            "missing stays last even descending"
        );
    }

    #[test]
    fn sorting_and_filtering_combine() {
        let f = catalogue();
        let params =
            SearchParams { filter: Some("brand:=Logitech".into()), ..sorted("price:desc") };
        assert_eq!(f.ids(&f.search(params)), vec!["1", "3"]);
    }

    #[test]
    fn sorted_pagination_is_consistent() {
        let f = catalogue();
        let all = f.ids(&f.search(sorted("price:asc")));
        let page1 = f.ids(&f.search(SearchParams { limit: Some(2), ..sorted("price:asc") }));
        let page2 = f.ids(&f.search(SearchParams {
            limit: Some(2),
            offset: Some(2),
            ..sorted("price:asc")
        }));
        assert_eq!(page1, all[..2].to_vec());
        assert_eq!(page2, all[2..].to_vec());
    }

    /// Query with typo tolerance left at the collection default.
    fn typo_query(q: &str) -> SearchParams {
        SearchParams { q: Some(q.into()), prefix: Some(false), ..Default::default() }
    }

    #[test]
    fn a_typo_still_finds_the_document() {
        let f = Fixture::new(&[
            json!({"id": "1", "title": "Wireless Mouse"}),
            json!({"id": "2", "title": "Mechanical Keyboard"}),
        ]);
        // `wireless` is 8 characters, so the table allows two edits.
        for typo in ["wirelss", "wierless", "wireles"] {
            let out = f.search(typo_query(typo));
            assert_eq!(f.ids(&out), vec!["1"], "`{typo}` should still find the mouse");
        }
    }

    #[test]
    fn the_typo_table_governs_how_much_correction_is_allowed() {
        let f = Fixture::new(&[
            json!({"id": "short", "title": "cat"}),
            json!({"id": "medium", "title": "mouse"}),
        ]);

        // 1-3 characters: no typos permitted.
        assert_eq!(f.search(typo_query("bat")).found, 0);
        assert_eq!(f.search(typo_query("cat")).found, 1);

        // 4-7 characters: one typo.
        assert_eq!(f.search(typo_query("mouze")).found, 1);
        // Two edits is beyond the budget at this length.
        assert_eq!(f.search(typo_query("mozze")).found, 0);
    }

    #[test]
    fn an_exact_match_outranks_a_corrected_one() {
        let f = Fixture::new(&[
            json!({"id": "corrected", "title": "moose"}),
            json!({"id": "exact", "title": "mouse"}),
        ]);
        let out = f.search(typo_query("mouse"));
        assert_eq!(f.ids(&out), vec!["exact", "corrected"]);
        assert!(
            out.hits[0].components.typo_penalty > out.hits[1].components.typo_penalty,
            "the exact match should carry no typo penalty"
        );
    }

    #[test]
    fn typo_tolerance_can_be_turned_off_per_request() {
        let f = Fixture::new(&[json!({"id": "1", "title": "Wireless Mouse"})]);
        assert_eq!(f.search(typo_query("wirelss")).found, 1);

        let strict = SearchParams { typo_tolerance: Some(false), ..typo_query("wirelss") };
        assert_eq!(f.search(strict).found, 0);
    }

    #[test]
    fn a_phrase_is_never_typo_corrected() {
        let f = Fixture::new(&[json!({"id": "1", "title": "wireless mouse adapter"})]);
        assert_eq!(f.search(typo_query("wirelss mouse")).found, 1, "loose terms are corrected");
        assert_eq!(
            f.search(typo_query("\"wirelss mouse\"")).found,
            0,
            "quoting asks for these exact words"
        );
    }

    #[test]
    fn transpositions_count_as_a_single_edit() {
        // Damerau, not plain Levenshtein: `mesuo` is two swaps from `mouse`
        // but `moues` is one.
        let f = Fixture::new(&[json!({"id": "1", "title": "mouse"})]);
        assert_eq!(f.search(typo_query("moues")).found, 1);
    }

    #[test]
    fn a_phrase_requires_adjacent_terms_in_order() {
        let f = Fixture::new(&[
            json!({"id": "adjacent", "title": "a mouse pad for desks"}),
            json!({"id": "reversed", "title": "a pad mouse for desks"}),
            json!({"id": "apart", "title": "a mouse on a rubber pad"}),
        ]);

        let loose = f.search(query("mouse pad"));
        assert_eq!(loose.found, 3, "without quotes all three match");

        let phrase = f.search(query("\"mouse pad\""));
        assert_eq!(phrase.found, 1);
        assert_eq!(f.ids(&phrase), vec!["adjacent"]);
    }

    #[test]
    fn a_phrase_must_sit_within_one_field() {
        let f = Fixture::new(&[
            json!({"id": "split", "title": "ends with mouse", "description": "pad starts here"}),
            json!({"id": "whole", "title": "unrelated", "description": "a mouse pad here"}),
        ]);
        let out = f.search(query("\"mouse pad\""));
        assert_eq!(f.ids(&out), vec!["whole"], "a phrase cannot straddle two fields");
    }

    #[test]
    fn a_three_word_phrase_needs_all_three_in_sequence() {
        let f = Fixture::new(&[
            json!({"id": "yes", "title": "the large mouse pad edition"}),
            json!({"id": "no", "title": "the large pad mouse edition"}),
        ]);
        let out = f.search(query("\"large mouse pad\""));
        assert_eq!(f.ids(&out), vec!["yes"]);
    }

    #[test]
    fn a_phrase_can_be_mixed_with_loose_terms() {
        let f = Fixture::new(&[
            json!({"id": "both", "title": "wireless mouse pad combo"}),
            json!({"id": "phrase_only", "title": "mouse pad combo"}),
            json!({"id": "loose_only", "title": "wireless pad mouse"}),
        ]);
        let out = f.search(query("wireless \"mouse pad\""));
        assert_eq!(f.ids(&out), vec!["both"]);
    }

    #[test]
    fn a_phrase_is_enforced_even_in_any_mode() {
        let f = Fixture::new(&[json!({"id": "apart", "title": "a mouse on a pad"})]);
        let params = SearchParams { match_mode: Some("any".into()), ..query("\"mouse pad\"") };
        assert_eq!(f.search(params).found, 0, "quoting is an explicit adjacency request");
    }

    #[test]
    fn repeated_terms_do_not_confuse_phrase_detection() {
        let f = Fixture::new(&[
            json!({"id": "late", "title": "pad here and there a mouse pad"}),
            json!({"id": "never", "title": "pad pad pad mouse"}),
        ]);
        let out = f.search(query("\"mouse pad\""));
        assert_eq!(f.ids(&out), vec!["late"], "the phrase occurs at the end");
    }

    #[test]
    fn searching_a_single_named_field_ignores_the_others() {
        let f = corpus();
        let out = f.search(SearchParams { query_by: Some("title".into()), ..query("wireless") });
        assert_eq!(out.found, 2, "only titles contain wireless");
        let out =
            f.search(SearchParams { query_by: Some("description".into()), ..query("comfortable") });
        assert_eq!(f.ids(&out), vec!["1"]);
    }
}
