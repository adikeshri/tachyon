//! True block-max WAND: a document-at-a-time postings walk that skips whole
//! blocks — sometimes whole documents — when a sound bound proves they
//! cannot affect the top-K, rather than merely skipping the expensive tail
//! of scoring for documents already visited (that's the older, still-active
//! "scoped" mechanism in `executor.rs`; the two are deliberately named and
//! kept apart — see `executor.rs`'s module doc for how they compose).
//!
//! # Shape
//!
//! ```text
//! per-token candidate terms -> per-source cursors -> MergeCursor (one per
//! candidate, across sources) -> FieldCandidates (one per query_by field,
//! across candidates) -> TokenFrontier (one per query token, across fields)
//! -> a driver (run_disjunctive or run_conjunctive) that decides, block by
//! block, which documents are even worth resolving -> DocEvidence (one
//! visited document's per-field, per-token matches) -> DocScorer (the old
//! finish()'s per-document scoring body, reused verbatim in spirit)
//! ```
//!
//! # Multi-candidate aggregation
//!
//! A query token can expand into several candidate terms (typo correction,
//! prefix completion). When more than one candidate matches the same
//! document in the same field, [`FieldCandidates::resolve`] keeps the
//! *maximum* BM25 contribution, tracks the *minimum* edit distance
//! independently of which candidate produced that best score, and *unions*
//! every matching candidate's positions — bit-for-bit the same rule the
//! pre-pruning accumulator used, so a query too small to trigger any bound
//! check scores identically to before this file existed.

use roaring::RoaringBitmap;

use tachyon_core::{DocId, FieldId};
use tachyon_index::{MergeCursor, PostingCursor};

use crate::bm25::{self, FieldStats};
use crate::executor::{sort_values, Ranked, SearchContext, TermCandidate, TopKByScore};
use crate::query_text::ParsedQuery;
use crate::request::SearchRequest;
use crate::score::{self, ScoreComponents, ScoreWeights};

/// One candidate term's postings, merged across every source, plus what it
/// costs to score a match: `idf` (shared by every doc this candidate
/// matches) and `edits` (this candidate's fixed distance from the original
/// query token).
struct CandidateCursor<'a> {
    cursor: MergeCursor<'a>,
    idf: f32,
    edits: u32,
}

impl CandidateCursor<'_> {
    /// Upper bound on this candidate's own BM25 contribution to whichever
    /// doc it's currently positioned at (or the block it's in, for a
    /// block-structured source).
    fn score_bound(&self) -> f32 {
        bm25::term_score_bound(self.cursor.max_remaining_tf(), self.idf)
    }

    fn contribution(&self, field_len: u32, stats: FieldStats) -> f32 {
        bm25::term_score(self.cursor.term_freq(), field_len, stats, self.idf)
    }
}

/// One query token's evidence in one queried field: the union, across every
/// candidate term for that token, merged by doc id.
struct FieldCandidates<'a> {
    candidates: Vec<CandidateCursor<'a>>,
}

impl FieldCandidates<'_> {
    fn doc_id(&self) -> Option<DocId> {
        self.candidates.iter().filter_map(|c| c.cursor.doc_id()).min()
    }

    fn score_bound(&self) -> f32 {
        self.candidates
            .iter()
            .filter(|c| c.cursor.doc_id().is_some())
            .map(CandidateCursor::score_bound)
            .fold(0.0, f32::max)
    }

    /// Min over every live candidate's own block boundary; `None` if any
    /// live candidate lacks one (its `MergeCursor` has a live memtable
    /// child) — same "any unbounded source makes the whole bound unsound"
    /// rule `MergeCursor` itself follows one level down.
    fn current_block_last_doc_id(&self) -> Option<DocId> {
        let mut min: Option<DocId> = None;
        for c in &self.candidates {
            if c.cursor.doc_id().is_none() {
                continue;
            }
            let last = c.cursor.current_block_last_doc_id()?;
            min = Some(min.map_or(last, |m| m.min(last)));
        }
        min
    }

    /// This token's resolved match in this field at `doc_id`, or `None` if
    /// no candidate is present there. See this module's doc comment for the
    /// exact multi-candidate aggregation rule this implements.
    fn resolve(
        &self,
        doc_id: DocId,
        field_len: u32,
        stats: FieldStats,
        needs_positions: bool,
    ) -> Option<FieldMatch> {
        let mut best_contribution = 0.0f32;
        let mut best_edits = u32::MAX;
        let mut positions = Vec::new();
        let mut matched = false;

        for c in &self.candidates {
            if c.cursor.doc_id() != Some(doc_id) {
                continue;
            }
            matched = true;
            let contribution = c.contribution(field_len, stats);
            if contribution > best_contribution {
                best_contribution = contribution;
            }
            if c.edits < best_edits {
                best_edits = c.edits;
            }
            if needs_positions {
                positions.extend(c.cursor.positions());
            }
        }

        matched.then_some(FieldMatch {
            contribution: best_contribution,
            edits: best_edits,
            positions,
        })
    }
}

/// One query token's evidence across every field in `query_by` — the unit
/// the pruning driver operates on, one per token.
pub(crate) struct TokenFrontier<'a> {
    fields: Vec<FieldCandidates<'a>>,
}

impl TokenFrontier<'_> {
    fn doc_id(&self) -> Option<DocId> {
        self.fields.iter().filter_map(FieldCandidates::doc_id).min()
    }

    /// Max-over-fields of each field's bound: only one field ultimately
    /// wins a document, so summing across fields would be needlessly loose.
    /// Summing THESE per-token bounds across tokens (the drivers' job) is
    /// still sound — max(sum) <= sum(max) — though looser than tracking
    /// per-field consistency across tokens; the scoped mechanism already
    /// accepts the same looseness via its own global `max_field_boost`.
    fn score_bound(&self) -> f32 {
        self.fields.iter().map(FieldCandidates::score_bound).fold(0.0, f32::max)
    }

    /// Min over every currently-live field's block boundary; a field with no
    /// live candidate contributes nothing to a skip target and is excluded,
    /// not treated as "no info".
    fn current_block_last_doc_id(&self) -> Option<DocId> {
        let mut min: Option<DocId> = None;
        for f in &self.fields {
            if f.doc_id().is_none() {
                continue;
            }
            let last = f.current_block_last_doc_id()?;
            min = Some(min.map_or(last, |m| m.min(last)));
        }
        min
    }

    /// Advance every candidate, in every field, currently sitting at this
    /// frontier's own current (smallest) doc id.
    fn advance(&mut self) {
        let Some(current) = self.doc_id() else { return };
        for f in &mut self.fields {
            for c in &mut f.candidates {
                if c.cursor.doc_id() == Some(current) {
                    c.cursor.advance();
                }
            }
        }
    }

    fn advance_to(&mut self, target: DocId) {
        for f in &mut self.fields {
            for c in &mut f.candidates {
                if c.cursor.doc_id().is_some_and(|d| d < target) {
                    c.cursor.advance_to(target);
                }
            }
        }
    }
}

/// Build one frontier per query token. `idf`/candidate resolution mirrors
/// what the pre-pruning walk did inline — only now the result is a cursor to
/// be driven lazily, not an immediate fold into an accumulator. A
/// candidate/field/source combination with no postings anywhere is omitted
/// entirely, same as the old walk's `doc_freq == 0 { continue; }`.
pub(crate) fn build_frontiers<'a>(
    ctx: &'a SearchContext<'a>,
    req: &SearchRequest,
    expansions: &[Vec<TermCandidate>],
) -> Vec<TokenFrontier<'a>> {
    expansions
        .iter()
        .map(|candidates| {
            let fields = req
                .query_by
                .iter()
                .map(|&(field, _boost)| {
                    let stats = ctx.field_stats(field);
                    if stats.doc_count == 0 {
                        return FieldCandidates { candidates: Vec::new() };
                    }

                    let mut field_candidates = Vec::new();
                    for candidate in candidates {
                        let doc_freq: u32 =
                            ctx.sources.iter().map(|s| s.doc_freq(&candidate.term, field)).sum();
                        if doc_freq == 0 {
                            continue;
                        }
                        let idf = bm25::idf(doc_freq, stats.doc_count);
                        let children: Vec<Box<dyn PostingCursor + 'a>> = ctx
                            .sources
                            .iter()
                            .filter_map(|s| s.posting_cursor(&candidate.term, field))
                            .collect();
                        field_candidates.push(CandidateCursor {
                            cursor: MergeCursor::new(children),
                            idf,
                            edits: candidate.edits,
                        });
                    }
                    FieldCandidates { candidates: field_candidates }
                })
                .collect();
            TokenFrontier { fields }
        })
        .collect()
}

/// The bound a pivot/threshold check compares against theta: a real or
/// bound BM25 value, normalized, combined with the maximum possible value
/// of every other `combine()` component. Sound because `combine()` is
/// non-decreasing in each component — shared by both drivers here and by
/// the older scoped-pruning mechanism in `executor.rs`, which uses the
/// identical formula on an already-exact (not bound) BM25 sum.
fn bound_to_combined_score(bm25_bound: f32, scorer: &DocScorer) -> f32 {
    ScoreComponents {
        bm25: score::normalize_bm25(bm25_bound),
        field_boost: scorer.max_field_boost,
        proximity: 1.0,
        typo_penalty: 1.0,
        popularity: 1.0,
    }
    .combine(&scorer.weights)
}

/// One matched (token, field) cell: the winning candidate's contribution and
/// edit distance, and the union of every matching candidate's positions —
/// see this module's doc comment for the exact aggregation rule.
struct FieldMatch {
    contribution: f32,
    edits: u32,
    positions: Vec<u32>,
}

/// One document's evidence, gathered by resolving every token's frontier at
/// that doc id. Mirrors the retired `Accumulator`'s per-slot API, but for a
/// single document — the pruning walk visits one document at a time rather
/// than building a flat table across every match up front.
struct DocEvidence {
    /// `[field_pos][token_idx]`.
    matches: Vec<Vec<Option<FieldMatch>>>,
}

impl DocEvidence {
    fn matched(&self, field: usize, token: usize) -> bool {
        self.matches[field][token].is_some()
    }

    fn field_bm25(&self, field: usize) -> f32 {
        self.matches[field].iter().filter_map(|m| m.as_ref()).map(|m| m.contribution).sum()
    }

    fn field_edits(&self, field: usize) -> u32 {
        self.matches[field].iter().filter_map(|m| m.as_ref()).map(|m| m.edits).sum()
    }

    fn field_matched_tokens(&self, field: usize) -> usize {
        self.matches[field].iter().filter(|m| m.is_some()).count()
    }

    /// Sorted, deduplicated positions of one token in one field. Valid only
    /// after [`DocEvidence::normalize_positions`].
    fn token_positions(&self, field: usize, token: usize) -> &[u32] {
        self.matches[field][token].as_ref().map_or(&[], |m| m.positions.as_slice())
    }

    /// Put every position list into sorted, deduplicated form, same as the
    /// retired accumulator's own pass — positions arrive out of order
    /// because one token can expand into several candidates, each
    /// contributing its own occurrences.
    fn normalize_positions(&mut self) {
        for row in &mut self.matches {
            for m in row.iter_mut().flatten() {
                if m.positions.len() > 1 {
                    m.positions.sort_unstable();
                    m.positions.dedup();
                }
            }
        }
    }
}

/// Everything needed to resolve and score one document, gathered once
/// before either driver runs.
pub(crate) struct DocScorer<'a> {
    ctx: &'a SearchContext<'a>,
    req: &'a SearchRequest,
    filter: Option<&'a RoaringBitmap>,
    weights: ScoreWeights,
    max_boost: f32,
    max_field_boost: f32,
    allowed_edits: u32,
    popularity_field: Option<FieldId>,
    needs_positions: bool,
    num_tokens: usize,
    /// Corpus stats per `query_by` field, computed once — BM25 needs global
    /// numbers, and recomputing them per document would repeat the same
    /// O(num_sources) sum for every match a broad query visits.
    field_stats: Vec<FieldStats>,
}

impl<'a> DocScorer<'a> {
    pub(crate) fn new(
        ctx: &'a SearchContext<'a>,
        req: &'a SearchRequest,
        filter: Option<&'a RoaringBitmap>,
        allowed_edits: u32,
        needs_positions: bool,
        num_tokens: usize,
    ) -> DocScorer<'a> {
        let max_boost = score::max_boost(ctx.schema);
        let max_field_boost = req
            .query_by
            .iter()
            .map(|&(_, boost)| score::normalize_field_boost(boost, max_boost))
            .fold(0.0f32, f32::max);
        let popularity_field = ctx.schema.field(score::POPULARITY_FIELD).map(|(id, _)| id);
        let field_stats = req.query_by.iter().map(|&(field, _)| ctx.field_stats(field)).collect();
        DocScorer {
            ctx,
            req,
            filter,
            weights: ScoreWeights::default(),
            max_boost,
            max_field_boost,
            allowed_edits,
            popularity_field,
            needs_positions,
            num_tokens,
            field_stats,
        }
    }

    /// Resolve every token's frontier at `doc_id` into one document's
    /// evidence. Positions are decoded only for candidates actually
    /// present at this exact doc id — never for a block or document a
    /// driver already decided to skip.
    fn resolve(&self, frontiers: &[TokenFrontier], doc_id: DocId) -> DocEvidence {
        let mut matches = Vec::with_capacity(self.req.query_by.len());
        for (field_pos, &(field, _boost)) in self.req.query_by.iter().enumerate() {
            let field_len = self.ctx.field_len(doc_id, field);
            let stats = self.field_stats[field_pos];
            let row: Vec<Option<FieldMatch>> = frontiers
                .iter()
                .map(|frontier| {
                    frontier.fields[field_pos].resolve(
                        doc_id,
                        field_len,
                        stats,
                        self.needs_positions,
                    )
                })
                .collect();
            matches.push(row);
        }
        let mut evidence = DocEvidence { matches };
        evidence.normalize_positions();
        evidence
    }

    /// The old `finish()`'s per-slot scoring body, unchanged in substance:
    /// best-field selection, proximity, popularity, `combine()`, and a push
    /// into the kept set — still gated by the older *scoped* mechanism's
    /// own exact-bm25-bound check, which this preserves verbatim as an
    /// inner layer beneath the driver's own (block-level, bound-only)
    /// pruning.
    fn score(
        &self,
        doc_id: DocId,
        evidence: &DocEvidence,
        top_k: &mut Option<TopKByScore>,
        candidates: &mut Vec<Ranked>,
    ) {
        let num_fields = self.req.query_by.len();
        let bm25_bound = (0..num_fields).map(|f| evidence.field_bm25(f)).fold(0.0f32, f32::max);

        if let Some(tk) = top_k.as_ref() {
            if let Some(threshold) = tk.threshold() {
                if bound_to_combined_score(bm25_bound, self) < threshold {
                    // Provably cannot beat the current worst-of-the-kept-set;
                    // already counted by the caller.
                    return;
                }
            }
        }

        let mut best_field = 0usize;
        let mut best_bm25 = 0.0f32;
        let mut best_boosted = f32::NEG_INFINITY;
        for field_pos in 0..num_fields {
            let raw = evidence.field_bm25(field_pos);
            let boosted = raw * self.req.query_by[field_pos].1;
            if boosted > best_boosted {
                best_boosted = boosted;
                best_bm25 = raw;
                best_field = field_pos;
            }
        }

        let present: Vec<&[u32]> = (0..self.num_tokens)
            .filter(|&token| evidence.matched(best_field, token))
            .map(|token| evidence.token_positions(best_field, token))
            .collect();

        let matched_here = evidence.field_matched_tokens(best_field);
        let proximity = if matched_here == self.num_tokens {
            score::proximity(&present)
        } else {
            // A partial match in this field cannot claim tight proximity.
            score::proximity(&present) * matched_here as f32 / self.num_tokens as f32
        };

        let popularity = self
            .popularity_field
            .and_then(|f| self.ctx.value(doc_id, f))
            .and_then(|v| v.as_f64())
            .map(|v| score::normalize_popularity(v as f32))
            .unwrap_or(0.0);

        let components = ScoreComponents {
            bm25: score::normalize_bm25(best_bm25),
            field_boost: score::normalize_field_boost(
                self.req.query_by[best_field].1,
                self.max_boost,
            ),
            proximity,
            typo_penalty: score::typo_penalty(evidence.field_edits(best_field), self.allowed_edits),
            popularity,
        };

        let score = components.combine(&self.weights);
        let ranked = Ranked {
            doc_id,
            score,
            components,
            sort_values: sort_values(self.ctx, self.req, doc_id, score),
        };
        match top_k {
            Some(tk) => tk.push(ranked),
            None => candidates.push(ranked),
        }
    }
}

/// Whether some single field places every phrase's tokens consecutively.
///
/// A phrase must be satisfied within one field — `title` ending in "mouse"
/// and `description` starting with "pad" is not the phrase "mouse pad".
fn satisfies_phrases(
    evidence: &DocEvidence,
    req: &SearchRequest,
    phrases: &[(usize, usize)],
) -> bool {
    phrases.iter().all(|&(start, end)| {
        (0..req.query_by.len()).any(|field| phrase_in_field(evidence, field, start, end))
    })
}

fn phrase_in_field(evidence: &DocEvidence, field: usize, start: usize, end: usize) -> bool {
    // Every token of the phrase has to be present in this field at all.
    if (start..=end).any(|token| !evidence.matched(field, token)) {
        return false;
    }

    // Walk the first token's positions; each is a candidate phrase start.
    let first = evidence.token_positions(field, start);

    first.iter().any(|&anchor| {
        (start + 1..=end).enumerate().all(|(offset, token)| {
            let positions = evidence.token_positions(field, token);
            anchor
                .checked_add(offset as u32 + 1)
                .is_some_and(|expected| positions.binary_search(&expected).is_ok())
        })
    })
}

/// Resolve one candidate document: liveness, tombstones, and the request
/// filter first (cheapest checks, applied before any positions are ever
/// decoded), then the phrase gate, then — if it survives both — scoring.
/// Always pushes into `matched_ids` if it clears the phrase gate, matching
/// `found`'s existing definition; a document a bound proved not worth
/// visiting never reaches this function at all, which is the only source of
/// approximation.
#[allow(clippy::too_many_arguments)]
fn visit_and_score(
    doc_id: DocId,
    frontiers: &[TokenFrontier],
    query: &ParsedQuery,
    scorer: &DocScorer,
    matched_ids: &mut Vec<DocId>,
    top_k: &mut Option<TopKByScore>,
    candidates: &mut Vec<Ranked>,
) {
    if !scorer.ctx.is_live(doc_id) || scorer.filter.is_some_and(|f| !f.contains(doc_id)) {
        return;
    }

    let evidence = scorer.resolve(frontiers, doc_id);

    if !query.phrases.is_empty() && !satisfies_phrases(&evidence, scorer.req, &query.phrases) {
        return;
    }

    matched_ids.push(doc_id);
    if scorer.req.window() == 0 {
        return;
    }

    scorer.score(doc_id, &evidence, top_k, candidates);
}

/// `MatchMode::Any`: a document qualifies as soon as ANY one token's
/// frontier reaches it — WAND's classic pivot selection, generalized so the
/// pivot doesn't have to be a genuine intersection candidate, just whichever
/// doc id a running sum of *live* frontiers' bounds first clears theta at.
///
/// Returns the ascending doc ids visited and pushed into `matched_ids`
/// (already reflected in the caller-owned `matched_ids` — see below), and
/// whether any postings were skipped without being visited.
pub(crate) fn run_disjunctive(
    frontiers: &mut [TokenFrontier],
    query: &ParsedQuery,
    scorer: &DocScorer,
    top_k: &mut Option<TopKByScore>,
    candidates: &mut Vec<Ranked>,
) -> (Vec<DocId>, bool) {
    let mut matched_ids = Vec::new();
    let mut any_skip = false;

    loop {
        let mut live: Vec<usize> =
            (0..frontiers.len()).filter(|&i| frontiers[i].doc_id().is_some()).collect();
        if live.is_empty() {
            break;
        }
        live.sort_by_key(|&i| frontiers[i].doc_id().unwrap());

        let theta = top_k.as_ref().and_then(TopKByScore::threshold).unwrap_or(f32::NEG_INFINITY);

        let mut acc_bound = 0.0f32;
        let mut pivot: Option<usize> = None; // index into `live`
        for (pos, &i) in live.iter().enumerate() {
            acc_bound += frontiers[i].score_bound();
            if bound_to_combined_score(acc_bound, scorer) >= theta {
                pivot = Some(pos);
                break;
            }
        }

        match pivot {
            Some(pos) => {
                let pivot_doc = frontiers[live[pos]].doc_id().unwrap();
                if pivot_doc > frontiers[live[0]].doc_id().unwrap() {
                    any_skip = true;
                    for &i in &live[..pos] {
                        frontiers[i].advance_to(pivot_doc);
                    }
                }

                visit_and_score(
                    pivot_doc,
                    frontiers,
                    query,
                    scorer,
                    &mut matched_ids,
                    top_k,
                    candidates,
                );

                for &i in &live {
                    if frontiers[i].doc_id() == Some(pivot_doc) {
                        frontiers[i].advance();
                    }
                }
            }
            None => {
                // Not even summing every live frontier's current-block bound
                // clears theta: nothing up to the nearest block boundary
                // among them can possibly matter. With a SINGLE live
                // frontier (a single-token query, or several tokens whose
                // lists happen to be fully aligned) this is the ONLY way
                // that frontier's own hopeless block ever gets skipped
                // whole, rather than visited document by document.
                any_skip = true;
                let skip_to =
                    live.iter().filter_map(|&i| frontiers[i].current_block_last_doc_id()).min();
                for &i in &live {
                    match skip_to {
                        Some(d) => frontiers[i].advance_to(d.saturating_add(1)),
                        None => {
                            frontiers[i].advance();
                        }
                    }
                }
            }
        }
    }

    (matched_ids, any_skip)
}

/// `MatchMode::All`: every token is required, so a genuine match needs a
/// contribution from every frontier. Two phases run every iteration, in
/// this order:
///
/// 1. **Bound check** — sum every frontier's current-block bound; if that
///    can't clear theta, nothing up to the nearest shared block boundary
///    can possibly matter, so skip ahead without searching for agreement at
///    all.
/// 2. **Exact leapfrog** — find the next doc id every frontier agrees on.
///    Every `advance_to` here targets a KNOWN required position (another
///    frontier's current doc id), so nothing skipped during this phase can
///    be a genuine intersection member; this phase alone is always exact.
///
/// The order matters: checking the bound *before* searching for agreement
/// is what lets a hopeless region be skipped without ever being searched —
/// checking after would mean every visited-but-rejected document still paid
/// the full leapfrog cost, which is the entire latency win for `All`-mode
/// broad queries, including the single-token case (one frontier, its own
/// block bound and block boundary — degenerates correctly, same as
/// [`run_disjunctive`]'s `None` branch).
pub(crate) fn run_conjunctive(
    frontiers: &mut [TokenFrontier],
    query: &ParsedQuery,
    scorer: &DocScorer,
    top_k: &mut Option<TopKByScore>,
    candidates: &mut Vec<Ranked>,
) -> (Vec<DocId>, bool) {
    let mut matched_ids = Vec::new();
    let mut any_skip = false;

    loop {
        if frontiers.iter().any(|f| f.doc_id().is_none()) {
            break; // one required list exhausted -> the intersection is done
        }

        let theta = top_k.as_ref().and_then(TopKByScore::threshold).unwrap_or(f32::NEG_INFINITY);
        let bound_sum: f32 = frontiers.iter().map(TokenFrontier::score_bound).sum();
        if bound_to_combined_score(bound_sum, scorer) < theta {
            any_skip = true;
            let skip_to =
                frontiers.iter().filter_map(TokenFrontier::current_block_last_doc_id).min();
            for f in frontiers.iter_mut() {
                match skip_to {
                    Some(d) => f.advance_to(d.saturating_add(1)),
                    None => {
                        f.advance();
                    }
                }
            }
            continue;
        }

        let max_doc = frontiers.iter().map(|f| f.doc_id().unwrap()).max().unwrap();
        if frontiers.iter().all(|f| f.doc_id() == Some(max_doc)) {
            // Genuine match, always counted: this phase alone is exact.
            visit_and_score(max_doc, frontiers, query, scorer, &mut matched_ids, top_k, candidates);
            for f in frontiers.iter_mut() {
                f.advance();
            }
        } else {
            for f in frontiers.iter_mut() {
                if f.doc_id() != Some(max_doc) {
                    f.advance_to(max_doc);
                }
            }
        }
    }

    (matched_ids, any_skip)
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;
    use tachyon_core::{CollectionSchema, FieldSchema, FieldType, ParsedDocument};
    use tachyon_index::MemTable;

    use crate::query_text;
    use crate::request::{MatchMode, SearchParams};

    fn schema() -> CollectionSchema {
        CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("description", FieldType::Text),
            ],
        )
    }

    fn build(docs: &[serde_json::Value]) -> (MemTable, CollectionSchema) {
        let schema = schema();
        let mut m = MemTable::new(0, &schema);
        for d in docs {
            m.insert(ParsedDocument::parse(d.clone(), &schema).unwrap());
        }
        (m, schema)
    }

    /// Exact-only expansions (no prefix/typo) — isolates frontier/driver
    /// mechanics from query expansion, which the executor's own tests
    /// already cover.
    fn expansions_for(tokens: &[String]) -> Vec<Vec<TermCandidate>> {
        tokens.iter().map(|t| vec![TermCandidate { term: t.clone(), edits: 0 }]).collect()
    }

    struct Outcome {
        matched_ids: Vec<DocId>,
        any_skip: bool,
        hits: Vec<Ranked>,
    }

    fn run(
        m: &MemTable,
        schema: &CollectionSchema,
        q: &str,
        mode: MatchMode,
        limit: usize,
    ) -> Outcome {
        let deleted = RoaringBitmap::new();
        let ctx = SearchContext::new(schema, vec![m], &deleted);
        let match_mode = match mode {
            MatchMode::All => "all",
            MatchMode::Any => "any",
        };
        let req = SearchRequest::resolve(
            SearchParams {
                q: Some(q.into()),
                prefix: Some(false),
                match_mode: Some(match_mode.into()),
                limit: Some(limit),
                ..Default::default()
            },
            schema,
        )
        .unwrap();

        let query = query_text::parse(&req.q);
        let expansions = expansions_for(&query.tokens);
        let mut frontiers = build_frontiers(&ctx, &req, &expansions);
        let needs_positions = query.tokens.len() > 1 || !query.phrases.is_empty();
        let scorer = DocScorer::new(&ctx, &req, None, 0, needs_positions, query.tokens.len());
        let mut top_k = Some(TopKByScore::new(req.window()));
        let mut candidates = Vec::new();

        let (matched_ids, any_skip) = match mode {
            MatchMode::Any => {
                run_disjunctive(&mut frontiers, &query, &scorer, &mut top_k, &mut candidates)
            }
            MatchMode::All => {
                run_conjunctive(&mut frontiers, &query, &scorer, &mut top_k, &mut candidates)
            }
        };

        Outcome { matched_ids, any_skip, hits: top_k.map_or(candidates, TopKByScore::into_vec) }
    }

    fn small_corpus() -> (MemTable, CollectionSchema) {
        build(&[
            json!({"id": "1", "title": "wireless mouse", "description": "a comfortable mouse"}),
            json!({"id": "2", "title": "mechanical keyboard", "description": "loud and tactile"}),
            json!({"id": "3", "title": "mouse pad", "description": "desk mat"}),
            json!({"id": "4", "title": "wireless charger", "description": "charges phones"}),
        ])
    }

    #[test]
    fn disjunctive_matches_every_document_containing_any_token() {
        let (m, schema) = small_corpus();
        let out = run(&m, &schema, "wireless mouse", MatchMode::Any, 10);
        let mut ids = out.matched_ids.clone();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 2, 3], "wireless (0,3) or mouse (0,2) — union is docs 0,2,3");
    }

    #[test]
    fn conjunctive_matches_only_documents_containing_every_token() {
        let (m, schema) = small_corpus();
        let out = run(&m, &schema, "wireless mouse", MatchMode::All, 10);
        assert_eq!(out.matched_ids, vec![0], "only doc 0 has both terms");
    }

    #[test]
    fn a_single_token_query_matches_under_both_modes() {
        let (m, schema) = small_corpus();
        let any = run(&m, &schema, "mouse", MatchMode::Any, 10);
        let all = run(&m, &schema, "mouse", MatchMode::All, 10);
        let mut any_ids = any.matched_ids.clone();
        any_ids.sort_unstable();
        let mut all_ids = all.matched_ids.clone();
        all_ids.sort_unstable();
        assert_eq!(any_ids, vec![0, 2]);
        assert_eq!(all_ids, vec![0, 2]);
    }

    #[test]
    fn a_full_window_never_skips_and_agrees_with_the_hit_count() {
        let (m, schema) = small_corpus();
        let out = run(&m, &schema, "mouse", MatchMode::Any, 10);
        assert!(!out.any_skip, "a corpus this small never fills the window");
        assert_eq!(out.matched_ids.len(), out.hits.len().max(out.matched_ids.len()));
        assert_eq!(out.hits.len(), 2);
    }

    /// Enough documents, with enough score variation, that a small `limit`
    /// genuinely exercises the disjunctive driver's bound checks — even with
    /// no block structure at all (pure memtable), a document's own bound
    /// can still fail to clear an already-full top-K's threshold, which is
    /// what the driver's `None`-pivot branch is for.
    fn broad_corpus(n: usize) -> (MemTable, CollectionSchema) {
        let docs: Vec<serde_json::Value> = (0..n)
            .map(|i| {
                let repeats = (i % 7) + 1;
                let title = vec!["mouse"; repeats].join(" ");
                json!({"id": i.to_string(), "title": title, "description": "a mouse for the desk"})
            })
            .collect();
        build(&docs)
    }

    #[test]
    fn disjunctive_pruning_still_finds_the_correct_top_k() {
        let (m, schema) = broad_corpus(60);
        let small = run(&m, &schema, "mouse", MatchMode::Any, 5);
        let large = run(&m, &schema, "mouse", MatchMode::Any, 60);

        let mut small_ids: Vec<DocId> = small.hits.iter().map(|h| h.doc_id).collect();
        let mut large_top5: Vec<DocId> = large.hits.iter().map(|h| h.doc_id).collect();
        large_top5.sort_by(|a, b| {
            let sa = large.hits.iter().find(|h| h.doc_id == *a).unwrap().score;
            let sb = large.hits.iter().find(|h| h.doc_id == *b).unwrap().score;
            sb.partial_cmp(&sa).unwrap().then(a.cmp(b))
        });
        large_top5.truncate(5);
        small_ids.sort_by(|a, b| {
            let sa = small.hits.iter().find(|h| h.doc_id == *a).unwrap().score;
            let sb = small.hits.iter().find(|h| h.doc_id == *b).unwrap().score;
            sb.partial_cmp(&sa).unwrap().then(a.cmp(b))
        });
        assert_eq!(small_ids, large_top5, "the small-window run must keep the true top 5");
    }

    #[test]
    fn conjunctive_pruning_still_finds_the_correct_top_k() {
        let (m, schema) = broad_corpus(60);
        let small = run(&m, &schema, "mouse", MatchMode::All, 5);
        let large = run(&m, &schema, "mouse", MatchMode::All, 60);

        let top_ids = |o: &Outcome, k: usize| {
            let mut hits = o.hits.clone();
            hits.sort_by(|a, b| {
                b.score.partial_cmp(&a.score).unwrap().then(a.doc_id.cmp(&b.doc_id))
            });
            hits.truncate(k);
            hits.into_iter().map(|h| h.doc_id).collect::<Vec<_>>()
        };
        assert_eq!(top_ids(&small, 5), top_ids(&large, 5));
    }
}
