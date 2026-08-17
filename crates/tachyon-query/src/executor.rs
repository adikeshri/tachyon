//! Search execution: expand the query, then hand the walk itself to
//! `wand.rs`.
//!
//! # Shape of a search
//!
//! ```text
//! q -> tokens -> per-token candidate terms -> wand::build_frontiers
//!   -> wand::run_disjunctive / run_conjunctive -> page
//! ```
//!
//! # Two pruning mechanisms, composed
//!
//! `wand.rs`'s drivers ("true WAND") decide which documents are worth
//! resolving at all, skipping whole blocks — sometimes whole documents —
//! when a sound bound proves they cannot affect the top-K; that's the only
//! source of approximation (`SearchOutcome::found_is_exact`). A document
//! that *is* resolved still passes through the older "scoped" mechanism
//! inside `wand::DocScorer::score`: a real, exact per-field BM25 sum (not a
//! bound — the document was already resolved) gates the expensive tail —
//! proximity, the popularity read, `combine()` — the same way it always
//! has. That mechanism never causes approximation; it only ever decides
//! how much of an already-counted match's scoring to skip.
//!
//! # Multi-field scoring
//!
//! A document is scored on its *best* field, not the sum of all of them.
//! Summing double-counts text that appears in both `title` and `description`
//! and lets a long, repetitive field outrank a precise title match. "Best
//! field" is chosen by boosted BM25, and that same field supplies the
//! proximity and typo signals, so all five PRD §12 components describe the
//! same match rather than a blend of unrelated ones.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use roaring::RoaringBitmap;

use tachyon_core::{CollectionSchema, DocId, FieldId, Value};
use tachyon_index::{FuzzyMatcher, IndexSource};

use crate::bm25::FieldStats;
use crate::filter;
use crate::query_text;
use crate::request::{MatchMode, SearchRequest};
use crate::score::ScoreComponents;
use crate::sort::{self, SortClause, SortKey, SortValue};
use crate::wand;

/// Cap on how many dictionary terms one query token may expand into. Without
/// it, a one-character prefix on a large collection would walk the entire term
/// dictionary.
pub const MAX_TERM_EXPANSIONS: usize = 128;

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
    pub(crate) fn field_stats(&self, field: FieldId) -> FieldStats {
        let mut doc_count = 0u32;
        let mut total_len = 0u64;
        for source in &self.sources {
            doc_count = doc_count.saturating_add(source.field_doc_count(field));
            total_len = total_len.saturating_add(source.total_field_len(field));
        }
        FieldStats::new(doc_count, total_len)
    }

    /// A document's declared *range* (`min_doc_id()..end_doc_id()`) can be
    /// claimed by more than one source at once: a memtable that reserved a
    /// range a later merge went on to claim (see `MemTable::reserve`) still
    /// reports that span as its own, even though nothing is ever live there.
    /// So a range match alone never stops the search below — every method
    /// here tries each range-matching source in turn and falls through
    /// whenever one turns out not to actually hold `doc_id` live, rather
    /// than trusting the first (possibly hole-only) match.
    pub(crate) fn value(&self, doc_id: DocId, field: FieldId) -> Option<Cow<'a, Value>> {
        self.sources.iter().find_map(|s| {
            (doc_id >= s.min_doc_id() && doc_id < s.end_doc_id()).then(|| s.value(doc_id, field))?
        })
    }

    /// The owning source's own `field_len`, or `0` if no source actually
    /// holds this doc id live. See `Self::value`'s doc comment for why a
    /// range match alone isn't enough to stop the search.
    pub(crate) fn field_len(&self, doc_id: DocId, field: FieldId) -> u32 {
        self.sources
            .iter()
            .find(|s| doc_id >= s.min_doc_id() && doc_id < s.end_doc_id() && s.is_live(doc_id))
            .map_or(0, |s| s.field_len(doc_id, field))
    }

    /// Whether `doc_id` is live in whichever source actually holds it, and
    /// not collection-wide tombstoned. Centralizes the liveness/tombstone
    /// check the old per-posting walk repeated once per source per token;
    /// here it runs once per document actually reached by a driver. See
    /// `Self::value`'s doc comment for why a range match alone isn't enough.
    pub(crate) fn is_live(&self, doc_id: DocId) -> bool {
        self.sources
            .iter()
            .any(|s| doc_id >= s.min_doc_id() && doc_id < s.end_doc_id() && s.is_live(doc_id))
            && !self.deleted.contains(doc_id)
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
    /// A lower bound, not necessarily exact, whenever `found_is_exact` is
    /// `false`.
    pub found: usize,
    pub hits: Vec<ScoredDoc>,
    /// Every document that matched, for faceting over the full result set
    /// rather than the page (PRD §7.7: "accurate counts after filters").
    /// Same approximation caveat as `found` — facets read this bitmap
    /// directly, so they inherit it automatically.
    pub matched: RoaringBitmap,
    /// `false` iff pruning skipped at least one block of at least one
    /// term's postings while answering this query. See `wand.rs`'s module
    /// doc for exactly which mechanism this tracks (only the block-level
    /// one; the older scoped mechanism never causes approximation).
    pub found_is_exact: bool,
}

/// Run a search.
///
/// Filters are evaluated first, into a bitmap, so the pruning walk can
/// reject a document before ever resolving it.
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
    // definition — and decoding them anyway is the largest avoidable cost on
    // a query that matches a lot of documents.
    let needs_positions = tokens.len() > 1 || !query.phrases.is_empty();
    let allowed_edits = total_allowed_edits(ctx.schema, tokens, req);

    // Same eligibility rule the scoped mechanism has always used: only sound
    // when relevance score decides the *primary* order. A field-only sort
    // could still need a low-relevance document, so pruning is skipped
    // entirely and both drivers degrade to a plain, exact merge/leapfrog —
    // see `wand.rs` §7.3 in the design notes for why this falls out of the
    // algorithm itself rather than needing a separate code path.
    let prunable = req
        .sort_clauses
        .as_ref()
        .is_none_or(|clauses| clauses.first().is_some_and(|c| c.key == SortKey::TextMatch));

    let mut frontiers = wand::build_frontiers(ctx, req, &expansions);
    let scorer = wand::DocScorer::new(ctx, req, filter, allowed_edits, needs_positions, tokens.len());
    let mut top_k = prunable.then(|| TopKByScore::new(req.window()));
    let mut candidates: Vec<Ranked> = Vec::new(); // used only when `top_k` is None

    let (matched_ids, any_skip) = match req.match_mode {
        MatchMode::Any => {
            wand::run_disjunctive(&mut frontiers, &query, &scorer, &mut top_k, &mut candidates)
        }
        MatchMode::All => {
            wand::run_conjunctive(&mut frontiers, &query, &scorer, &mut top_k, &mut candidates)
        }
    };

    // Both drivers visit doc ids in strictly ascending order (`MergeCursor`
    // merges across sources in doc-id order, and a driver only ever visits
    // a doc id once), so this is a direct build, not a sort-then-dedup.
    let found = matched_ids.len();
    let matched = RoaringBitmap::from_sorted_iter(matched_ids)
        .expect("both drivers visit docs in ascending order");

    let candidates = top_k.map_or(candidates, TopKByScore::into_vec);
    let hits = paginate(req, candidates);
    SearchOutcome { found, hits, matched, found_is_exact: !any_skip }
}

/// Bounds how many fully-scored candidates `finish` carries forward, so a
/// broad query does not carry every match through proximity/popularity
/// scoring only to discard most of them in `paginate`. A max-heap ordered so
/// `peek`/`pop` return the *worst* of the currently-kept set — the one
/// candidate for eviction, and the threshold a new candidate's bound must
/// clear to be worth fully scoring.
pub(crate) struct TopKByScore {
    window: usize,
    heap: BinaryHeap<ScoredEntry>,
}

struct ScoredEntry(Ranked);

impl PartialEq for ScoredEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.score == other.0.score && self.0.doc_id == other.0.doc_id
    }
}
impl Eq for ScoredEntry {}
impl PartialOrd for ScoredEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ScoredEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed against the real ranking (lower score / higher doc id
        // first): a `BinaryHeap` is a max-heap, and `peek`/`pop` should
        // surface the *worst* kept candidate, not the best.
        other.0.score.total_cmp(&self.0.score).then(other.0.doc_id.cmp(&self.0.doc_id))
    }
}

impl TopKByScore {
    pub(crate) fn new(window: usize) -> TopKByScore {
        TopKByScore { window, heap: BinaryHeap::with_capacity(window.min(1024)) }
    }

    /// The score a new candidate's bound must meet or beat to be worth fully
    /// scoring. `None` while there's still room in the kept set — nothing to
    /// prune against yet.
    pub(crate) fn threshold(&self) -> Option<f32> {
        if self.heap.len() < self.window {
            None
        } else {
            self.heap.peek().map(|worst| worst.0.score)
        }
    }

    pub(crate) fn push(&mut self, ranked: Ranked) {
        if self.heap.len() < self.window {
            self.heap.push(ScoredEntry(ranked));
            return;
        }
        let Some(worst) = self.heap.peek() else { return };
        let replaces =
            ranked.score > worst.0.score || (ranked.score == worst.0.score && ranked.doc_id < worst.0.doc_id);
        if replaces {
            self.heap.pop();
            self.heap.push(ScoredEntry(ranked));
        }
    }

    pub(crate) fn into_vec(self) -> Vec<Ranked> {
        self.heap.into_iter().map(|entry| entry.0).collect()
    }
}

/// An empty query matches every live document, so filters and sorting can be
/// used on their own to browse a collection. No relevance signal to bound
/// here — score is hardcoded `0.0` — so this never approximates.
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
    SearchOutcome { found, hits, matched, found_is_exact: true }
}

/// Values for each sort clause, in clause order. Empty when the request does
/// not sort, in which case ranking falls back to score.
pub(crate) fn sort_values(
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
pub(crate) struct Ranked {
    pub(crate) doc_id: DocId,
    pub(crate) score: f32,
    pub(crate) components: ScoreComponents,
    /// One entry per sort clause; empty when ranking by relevance.
    pub(crate) sort_values: Vec<SortValue>,
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
                // A second sortable field, independent of relevance — unlike
                // `popularity`, which is also a scoring signal (`combine()`'s
                // 5% component), so it can't double as a tie-break that's
                // independent of the thing it's supposedly tying on.
                FieldSchema::new("price", FieldType::Int).with_sort(true),
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

    // --- score-bound pruning (scoped block-max WAND) -----------------------

    /// Every document matches "mouse", with relevance varying by how many
    /// times it repeats in the title — enough documents, with enough score
    /// variation, that a small `limit` genuinely exercises pruning rather
    /// than the top-k set just happening to equal every match.
    fn broad_corpus(n: usize) -> Fixture {
        let docs: Vec<serde_json::Value> = (0..n)
            .map(|i| {
                let repeats = (i % 5) + 1;
                let title = vec!["mouse"; repeats].join(" ");
                json!({
                    "id": i.to_string(),
                    "title": title,
                    "description": "a mouse for the desk",
                    "popularity": (i * 37 % 1000) as i64,
                })
            })
            .collect();
        Fixture::new(&docs)
    }

    #[test]
    fn pruning_returns_the_identical_top_k_as_exhaustive_scoring() {
        let f = broad_corpus(40);
        let small = f.search(SearchParams { limit: Some(5), ..query("mouse") });
        let large = f.search(SearchParams { limit: Some(40), ..query("mouse") });

        assert_eq!(small.found, 40);
        assert_eq!(small.found, large.found);

        let small_ids: Vec<DocId> = small.hits.iter().map(|h| h.doc_id).collect();
        let large_first_5: Vec<DocId> = large.hits.iter().take(5).map(|h| h.doc_id).collect();
        assert_eq!(small_ids, large_first_5, "pruning must not change which docs win the page");

        let small_scores: Vec<f32> = small.hits.iter().map(|h| h.score).collect();
        let large_scores: Vec<f32> = large.hits.iter().take(5).map(|h| h.score).collect();
        assert_eq!(small_scores, large_scores, "and not their exact scores either");
    }

    #[test]
    fn pruning_does_not_change_the_matched_set() {
        // facets::compute reads this bitmap directly — it must be identical
        // regardless of how aggressively scoring was pruned to build the page.
        let f = broad_corpus(40);
        let small = f.search(SearchParams { limit: Some(5), ..query("mouse") });
        let large = f.search(SearchParams { limit: Some(40), ..query("mouse") });
        assert_eq!(small.matched, large.matched);
    }

    #[test]
    fn pruning_is_bypassed_for_a_field_only_sort() {
        // "winner" barely matches (weak relevance) but has the lowest
        // popularity; every filler has much stronger relevance. Under
        // sort=popularity:asc, "winner" must still win — which is only true
        // if a field-only sort disables score-bound pruning entirely.
        let mut docs = vec![json!({
            "id": "winner", "title": "desk accessory", "description": "a mouse pad, barely",
            "popularity": 0,
        })];
        for i in 0..30 {
            docs.push(json!({
                "id": format!("filler{i}"),
                "title": "mouse mouse mouse mouse mouse",
                "description": "mouse mouse mouse",
                "popularity": 100 + i,
            }));
        }
        let f = Fixture::new(&docs);
        let out = f.search(SearchParams {
            sort: Some("popularity:asc".into()),
            limit: Some(3),
            match_mode: Some("any".into()),
            ..query("mouse")
        });
        assert_eq!(
            f.ids(&out)[0],
            "winner",
            "the lowest-popularity doc must win regardless of its weak relevance"
        );
    }

    #[test]
    fn pruning_stays_sound_with_a_secondary_sort_clause() {
        // Two documents tie exactly on relevance — identical title *and*
        // popularity, since popularity is itself part of combine()'s score,
        // not just a sort key — differing only in `price`.
        // sort=_text_match:desc,price:asc must still order them correctly,
        // and both must survive pruning to even be compared, proving a
        // TextMatch-primary sort keeps pruning valid.
        //
        // Popularity is held constant across the whole corpus so it can't
        // confound relevance, and a block of non-matching documents keeps
        // "mouse"'s document frequency well under the corpus size — with
        // *every* document matching, idf collapses toward zero and the tiny
        // 5%-weighted popularity term would dominate instead of BM25,
        // which is the opposite of what this test needs to isolate.
        let mut docs = vec![
            json!({"id": "tie_high_price", "title": "mouse mouse mouse", "description": "d", "popularity": 50, "price": 500}),
            json!({"id": "tie_low_price", "title": "mouse mouse mouse", "description": "d", "popularity": 50, "price": 10}),
        ];
        for i in 0..30 {
            docs.push(json!({
                "id": format!("filler{i}"),
                "title": "mouse", "description": "d",
                "popularity": 50,
                "price": (i * 7 % 1000) as i64,
            }));
        }
        for i in 0..50 {
            docs.push(json!({
                "id": format!("noise{i}"),
                "title": "keyboard", "description": "d",
                "popularity": 50,
                "price": (i * 11 % 1000) as i64,
            }));
        }
        let f = Fixture::new(&docs);
        let out = f.search(SearchParams {
            sort: Some("_text_match:desc,price:asc".into()),
            limit: Some(3),
            match_mode: Some("any".into()),
            ..query("mouse")
        });
        let ids = f.ids(&out);
        assert_eq!(
            ids,
            vec!["tie_low_price", "tie_high_price", "filler0"],
            "the tf=3 docs must clearly outrank every tf=1 filler, then order by price on their tie"
        );
    }

    #[test]
    fn pruning_is_correct_beyond_the_first_page() {
        let f = broad_corpus(40);
        let page1 = f.search(SearchParams { limit: Some(5), offset: Some(0), ..query("mouse") });
        let page2 = f.search(SearchParams { limit: Some(5), offset: Some(5), ..query("mouse") });
        let exhaustive = f.search(SearchParams { limit: Some(40), ..query("mouse") });

        let combined: Vec<DocId> =
            page1.hits.iter().chain(page2.hits.iter()).map(|h| h.doc_id).collect();
        let expected: Vec<DocId> = exhaustive.hits.iter().take(10).map(|h| h.doc_id).collect();
        assert_eq!(combined, expected, "window = offset + limit must size the kept set correctly");
    }

    #[test]
    fn limit_zero_still_reports_found_and_matched_without_scoring() {
        let f = broad_corpus(10);
        let out = f.search(SearchParams { limit: Some(0), ..query("mouse") });
        assert!(out.hits.is_empty());
        assert_eq!(out.found, 10);
        assert_eq!(out.matched.len(), 10);
    }

    // --- true block-max WAND (wand.rs) --------------------------------------

    /// A corpus flushed to a real on-disk segment, for tests that need
    /// genuine block structure (`POSTING_BLOCK_SIZE = 128` postings per
    /// block) — a memtable alone has none, so a test that must exercise
    /// `SegmentPostingCursor`'s block-jump path needs a real segment.
    struct SegmentFixture {
        _dir: tempfile::TempDir,
        schema: CollectionSchema,
        reader: tachyon_index::SegmentReader,
    }

    impl SegmentFixture {
        fn new(docs: &[serde_json::Value]) -> SegmentFixture {
            let schema = schema();
            let mut m = MemTable::new(0, &schema);
            for doc in docs {
                m.insert(ParsedDocument::parse(doc.clone(), &schema).unwrap());
            }
            let dir = tempfile::tempdir().unwrap();
            let encoded = tachyon_index::encode(&m, &schema).unwrap();
            let paths = tachyon_index::SegmentFilePaths {
                terms: dir.path().join("seg.terms"),
                ids: dir.path().join("seg.ids"),
                post: dir.path().join("seg.post"),
                col: dir.path().join("seg.col"),
                doc: dir.path().join("seg.doc"),
            };
            std::fs::write(&paths.terms, &encoded.terms).unwrap();
            std::fs::write(&paths.ids, &encoded.ids).unwrap();
            std::fs::write(&paths.post, &encoded.post).unwrap();
            std::fs::write(&paths.col, &encoded.col).unwrap();
            std::fs::write(&paths.doc, &encoded.doc).unwrap();
            let reader = tachyon_index::SegmentReader::open(&paths, &schema).unwrap();
            SegmentFixture { _dir: dir, schema, reader }
        }

        fn search(&self, params: SearchParams) -> SearchOutcome {
            let req = SearchRequest::resolve(params, &self.schema).unwrap();
            let deleted = RoaringBitmap::new();
            let ctx = SearchContext::new(&self.schema, vec![&self.reader], &deleted);
            execute(&ctx, &req)
        }
    }

    /// A corpus engineered so a *block*-level bound can be proven, not just
    /// hoped, to fall below an already-full top-K's threshold:
    ///
    /// - `block0_count` "kept" documents come first (the lowest doc ids, so
    ///   they occupy the term's first posting block): term frequency
    ///   `high_tf` and a high `popularity`. `popularity` matters because
    ///   `bound_to_combined_score` always assumes the *best possible*
    ///   popularity (1.0) — giving the real kept documents a genuinely high
    ///   one too keeps that assumption from being free, unearned slack for
    ///   every other block's bound to hide behind. BM25's own saturation
    ///   (`K1 = 1.2`) caps how much a *single* token's real score can ever
    ///   exceed a `tf = 1` bound (≈1.3x, however extreme `high_tf` gets, or
    ///   however favorable the length normalization) — nowhere near enough
    ///   on its own to clear a threshold that also credits a full
    ///   popularity component.
    /// - `filler_docs` documents that do NOT contain "mouse" at all, so
    ///   "mouse"'s idf stays well above zero — an every-document-matches
    ///   query collapses idf toward zero and makes any bound comparison
    ///   meaningless (the same trap `pruning_stays_sound_with_a_secondary_
    ///   sort_clause` above avoids). Long enough (20 tokens) to keep the
    ///   corpus's average field length from being dominated by the tiny
    ///   `tf = 1` tail, which is what keeps the *kept* documents' own
    ///   length normalization close to its floor.
    /// - `block1_count` "tail" documents, at the lowest possible term
    ///   frequency (1) and no popularity, forming the term's second (and
    ///   later) posting blocks.
    fn skewed_block_corpus(
        filler_docs: usize,
        block0_count: usize,
        high_tf: usize,
        block1_count: usize,
    ) -> Vec<serde_json::Value> {
        let mut docs = Vec::new();
        for i in 0..block0_count {
            let title = vec!["mouse"; high_tf].join(" ");
            docs.push(json!({"id": format!("k{i}"), "title": title, "description": "d", "popularity": 3000}));
        }
        for i in 0..filler_docs {
            let title = (0..20).map(|w| format!("filler{w}")).collect::<Vec<_>>().join(" ");
            docs.push(json!({"id": format!("f{i}"), "title": title, "description": "d"}));
        }
        for i in 0..block1_count {
            docs.push(json!({"id": format!("t{i}"), "title": "mouse", "description": "d"}));
        }
        docs
    }

    /// Same cliff shape as [`skewed_block_corpus`], but with two tokens that
    /// always co-occur at the same term frequency — for a two-token `All`
    /// mode query whose intersection is the whole corpus, so a genuine
    /// undercount can only come from the driver's own pruning, never from
    /// the intersection legitimately excluding anything.
    fn skewed_block_corpus_two_tokens(
        filler_docs: usize,
        block0_count: usize,
        high_tf: usize,
        block1_count: usize,
    ) -> Vec<serde_json::Value> {
        let mut docs = Vec::new();
        for i in 0..block0_count {
            let title = vec!["mouse pad"; high_tf].join(" ");
            docs.push(json!({"id": format!("k{i}"), "title": title, "description": "d", "popularity": 3000}));
        }
        for i in 0..filler_docs {
            let title = (0..20).map(|w| format!("filler{w}")).collect::<Vec<_>>().join(" ");
            docs.push(json!({"id": format!("f{i}"), "title": title, "description": "d"}));
        }
        for i in 0..block1_count {
            docs.push(json!({"id": format!("t{i}"), "title": "mouse pad", "description": "d"}));
        }
        docs
    }

    #[test]
    fn soundness_under_a_full_window_in_both_match_modes() {
        // A corpus spanning several blocks — a stronger soundness check than
        // the single-block `pruning_returns_the_identical_top_k...` above —
        // with `limit` == every match, so pruning never engages in either
        // driver and `found_is_exact` must come back `true` for both modes.
        let docs = skewed_block_corpus(1000, 128, 20, 122);
        let f = SegmentFixture::new(&docs);

        for mode in ["any", "all"] {
            let out = f.search(SearchParams {
                q: Some("mouse".into()),
                prefix: Some(false),
                query_by: Some("title".into()),
                match_mode: Some(mode.into()),
                limit: Some(250),
                ..Default::default()
            });
            assert_eq!(out.found, 250, "mode {mode}: every kept/tail document contains \"mouse\"");
            assert!(out.found_is_exact, "mode {mode}: a full window must never leave found_is_exact false");
        }
    }

    #[test]
    fn disjunctive_pruning_genuinely_undercounts_a_hopeless_tail() {
        // The regression test for the exact gap caught during design review:
        // a single-token query has only one frontier, so without the
        // driver's self-skip branch (the `None`-pivot case in
        // `run_disjunctive`), that frontier's own hopeless block would never
        // be skipped whole — it would always fall back to visiting doc by
        // doc, and `found` would stay exact no matter how small `limit` is.
        let docs = skewed_block_corpus(1000, 128, 20, 122);
        let f = SegmentFixture::new(&docs);
        let small = f.search(SearchParams {
            q: Some("mouse".into()),
            prefix: Some(false),
            query_by: Some("title".into()),
            match_mode: Some("any".into()),
            limit: Some(5),
            ..Default::default()
        });
        let large = f.search(SearchParams {
            q: Some("mouse".into()),
            prefix: Some(false),
            query_by: Some("title".into()),
            match_mode: Some("any".into()),
            limit: Some(250),
            ..Default::default()
        });

        assert_eq!(large.found, 250, "the reference run is unpruned: every kept/tail doc contains \"mouse\"");
        assert!(large.found_is_exact);

        assert!(!small.found_is_exact, "a genuinely hopeless block must have been skipped");
        assert!(
            small.found < large.found,
            "small.found={} must genuinely undercount large.found={}",
            small.found,
            large.found
        );

        // Pruning must not corrupt ranking, only the count of the tail it
        // never visited.
        let small_ids: Vec<DocId> = small.hits.iter().map(|h| h.doc_id).collect();
        let large_top5: Vec<DocId> = large.hits.iter().take(5).map(|h| h.doc_id).collect();
        assert_eq!(small_ids, large_top5);
    }

    #[test]
    fn conjunctive_pruning_genuinely_undercounts_a_hopeless_tail() {
        // Same shape, `All` mode (the default) with two tokens that always
        // co-occur — proving the hybrid leapfrog+bound-skip driver
        // (`run_conjunctive`) also skips and also undercounts, not just
        // `run_disjunctive`. This is the test proving the Context section's
        // decision to extend real pruning to `All` was actually implemented.
        let docs = skewed_block_corpus_two_tokens(1000, 128, 20, 122);
        let f = SegmentFixture::new(&docs);
        let small = f.search(SearchParams {
            q: Some("mouse pad".into()),
            prefix: Some(false),
            query_by: Some("title".into()),
            limit: Some(5),
            ..Default::default()
        });
        let large = f.search(SearchParams {
            q: Some("mouse pad".into()),
            prefix: Some(false),
            query_by: Some("title".into()),
            limit: Some(250),
            ..Default::default()
        });

        assert_eq!(large.found, 250, "every kept/tail doc contains both tokens, always together");
        assert!(large.found_is_exact);
        assert!(!small.found_is_exact, "a genuinely hopeless block must have been skipped");
        assert!(
            small.found < large.found,
            "small.found={} must genuinely undercount large.found={}",
            small.found,
            large.found
        );

        let small_ids: Vec<DocId> = small.hits.iter().map(|h| h.doc_id).collect();
        let large_top5: Vec<DocId> = large.hits.iter().take(5).map(|h| h.doc_id).collect();
        assert_eq!(small_ids, large_top5);
    }

    #[test]
    fn conjunctive_correctness_when_not_skipped() {
        // A moderate, single-block corpus with a large enough window that
        // theta stays -infinity throughout (no bound ever dips below it, so
        // neither the bound-skip nor the leapfrog phase ever needs to
        // reject a genuine candidate) — an exact-intersection sanity check
        // for `All` mode's leapfrog agreement alone.
        let mut docs = Vec::new();
        for i in 0..30 {
            let mut words = Vec::new();
            if i % 2 == 0 {
                words.push("wireless");
            }
            if i % 3 == 0 {
                words.push("mouse");
            }
            if words.is_empty() {
                words.push("filler");
            }
            docs.push(json!({"id": i.to_string(), "title": words.join(" "), "description": "d"}));
        }
        let f = Fixture::new(&docs);
        let out = f.search(SearchParams {
            q: Some("wireless mouse".into()),
            prefix: Some(false),
            query_by: Some("title".into()),
            limit: Some(30),
            ..Default::default()
        });

        let expected: Vec<DocId> = (0..30).filter(|i| i % 6 == 0).collect();
        let mut found_ids: Vec<DocId> = out.matched.iter().collect();
        found_ids.sort_unstable();
        assert_eq!(found_ids, expected, "the hand-computed AND of both per-token match sets");
        assert!(out.found_is_exact);
    }

    #[test]
    fn multi_candidate_aggregation_keeps_max_score_and_min_edits_independently() {
        // One document matched by two candidates of the same query token:
        // the exact term "wireless" (edits=0, tf=1, so a small contribution)
        // and a one-edit typo "wirelesss" (edits=1, tf=10, so a much larger
        // contribution) — both terms occur only in this document, so they
        // share the same idf and the difference is purely from tf. Max BM25
        // must pick the typo'd candidate's larger contribution, but the
        // exact candidate's lower edit distance must still be recorded,
        // independently of which candidate produced the winning score. A
        // single document, single field, single token corpus never fills
        // any window, so this isolates aggregation from any pruning.
        let mut words = vec!["wireless".to_string()];
        words.extend(std::iter::repeat_n("wirelesss".to_string(), 10));
        let f = Fixture::new(&[json!({"id": "1", "title": words.join(" "), "description": ""})]);

        let out = f.search(SearchParams {
            q: Some("wireless".into()),
            prefix: Some(false),
            query_by: Some("title".into()),
            limit: Some(1),
            ..Default::default()
        });

        assert_eq!(out.found, 1);
        assert_eq!(
            out.hits[0].components.typo_penalty, 1.0,
            "min edits (0, from the exact candidate) must win independently of which candidate scored higher"
        );
    }

    #[test]
    fn multi_source_merge_spans_a_segment_and_the_memtable() {
        // A term whose postings span both a segment (the low doc ids, from
        // a prior flush) and the memtable (the high doc ids, written since)
        // — exercising `MergeCursor`'s union merge across a real source
        // boundary, not just within one source.
        let schema = schema();
        let mut flushed = MemTable::new(0, &schema);
        for i in 0..5 {
            flushed.insert(
                ParsedDocument::parse(
                    json!({"id": i.to_string(), "title": "mouse", "description": "d"}),
                    &schema,
                )
                .unwrap(),
            );
        }
        let dir = tempfile::tempdir().unwrap();
        let encoded = tachyon_index::encode(&flushed, &schema).unwrap();
        let paths = tachyon_index::SegmentFilePaths {
            terms: dir.path().join("seg.terms"),
            ids: dir.path().join("seg.ids"),
            post: dir.path().join("seg.post"),
            col: dir.path().join("seg.col"),
            doc: dir.path().join("seg.doc"),
        };
        std::fs::write(&paths.terms, &encoded.terms).unwrap();
        std::fs::write(&paths.ids, &encoded.ids).unwrap();
        std::fs::write(&paths.post, &encoded.post).unwrap();
        std::fs::write(&paths.col, &encoded.col).unwrap();
        std::fs::write(&paths.doc, &encoded.doc).unwrap();
        let segment = tachyon_index::SegmentReader::open(&paths, &schema).unwrap();

        // Doc ids keep incrementing across the flush boundary, never reused.
        let mut live = MemTable::new(5, &schema);
        for i in 5..10 {
            live.insert(
                ParsedDocument::parse(
                    json!({"id": i.to_string(), "title": "mouse", "description": "d"}),
                    &schema,
                )
                .unwrap(),
            );
        }

        let deleted = RoaringBitmap::new();
        let ctx = SearchContext::new(&schema, vec![&live, &segment], &deleted);
        let req = SearchRequest::resolve(
            SearchParams {
                q: Some("mouse".into()),
                prefix: Some(false),
                query_by: Some("title".into()),
                limit: Some(10),
                ..Default::default()
            },
            &schema,
        )
        .unwrap();
        let out = execute(&ctx, &req);

        assert_eq!(out.found, 10);
        assert!(out.found_is_exact);
        let mut ids: Vec<DocId> = out.matched.iter().collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..10).collect::<Vec<DocId>>());
    }

    /// `Collection::merge_locked` (`tachyon-engine`) makes the active
    /// memtable reserve (`MemTable::reserve`) the exact doc id range a
    /// merge's output segment just claimed, so its next real insert doesn't
    /// collide with it — but that reservation makes the memtable *declare*
    /// (`min_doc_id()..end_doc_id()`) the very same range the segment owns,
    /// even though nothing is ever live in the memtable's copy of it.
    /// `SearchContext::is_live`/`field_len` used to stop at the first
    /// range-matching source (see their doc comments), so a document
    /// genuinely live in the second, real-owning source was reported dead
    /// and silently dropped from every search — this reproduces that shape
    /// directly against `SearchContext`, without needing a real merge.
    #[test]
    fn search_context_falls_through_a_hole_only_source_to_the_real_owner() {
        let schema = schema();

        let mut holes = MemTable::new(0, &schema);
        holes.reserve(5); // declares 0..5, nothing live in any of it

        let mut owner = MemTable::new(0, &schema);
        for i in 0..5 {
            owner.insert(
                ParsedDocument::parse(
                    json!({"id": i.to_string(), "title": "mouse", "description": "d"}),
                    &schema,
                )
                .unwrap(),
            );
        }

        let deleted = RoaringBitmap::new();
        // `holes` first, matching `Collection::sources()`'s own
        // memtable-before-every-segment order — the shape that actually
        // broke, since `.find()`/`.any()` walk sources in this order.
        let ctx = SearchContext::new(&schema, vec![&holes, &owner], &deleted);

        for doc_id in 0..5 {
            assert!(ctx.is_live(doc_id), "doc {doc_id} is live in the second, real-owning source");
            assert!(ctx.field_len(doc_id, 0) > 0, "doc {doc_id}'s length must come from the real owner");
            assert!(ctx.value(doc_id, 0).is_some(), "doc {doc_id}'s value must come from the real owner");
        }

        let req = SearchRequest::resolve(
            SearchParams {
                q: Some("mouse".into()),
                prefix: Some(false),
                query_by: Some("title".into()),
                limit: Some(10),
                ..Default::default()
            },
            &schema,
        )
        .unwrap();
        let out = execute(&ctx, &req);
        assert_eq!(out.found, 5, "every document behind the hole-only source must still be found");
    }

    #[test]
    fn found_is_exact_correctness_matrix() {
        // Small corpus, never fills the window: exact in both modes.
        let f = corpus();
        let any = f.search(SearchParams { match_mode: Some("any".into()), ..query("mouse") });
        let all = f.search(SearchParams { match_mode: Some("all".into()), ..query("mouse") });
        assert!(any.found_is_exact, "small corpus, any mode");
        assert!(all.found_is_exact, "small corpus, all mode");

        // A field-only sort disables pruning entirely, regardless of corpus
        // size — reusing `pruning_is_bypassed_for_a_field_only_sort`'s shape.
        let mut docs = vec![json!({
            "id": "winner", "title": "desk accessory", "description": "a mouse pad, barely",
            "popularity": 0,
        })];
        for i in 0..30 {
            docs.push(json!({
                "id": format!("filler{i}"),
                "title": "mouse mouse mouse mouse mouse",
                "description": "mouse mouse mouse",
                "popularity": 100 + i,
            }));
        }
        let sorted_out = Fixture::new(&docs).search(SearchParams {
            sort: Some("popularity:asc".into()),
            limit: Some(3),
            match_mode: Some("any".into()),
            ..query("mouse")
        });
        assert!(sorted_out.found_is_exact, "a field-only sort never prunes");

        // A window-filling query that genuinely skips: false, in either mode.
        let seg = SegmentFixture::new(&skewed_block_corpus(1000, 128, 20, 122));
        for mode in ["any", "all"] {
            let out = seg.search(SearchParams {
                q: Some("mouse".into()),
                prefix: Some(false),
                query_by: Some("title".into()),
                match_mode: Some(mode.into()),
                limit: Some(5),
                ..Default::default()
            });
            assert!(!out.found_is_exact, "mode {mode}: a window-filling query that genuinely skips");
        }
    }
}
