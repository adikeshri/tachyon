//! Autocomplete (PRD §7.5).
//!
//! `GET /collections/{name}/suggest?q=wir` → the terms a user is most likely to
//! be typing, most popular first.
//!
//! # What a suggestion is
//!
//! A term from the index, not a whole document. Suggestions come from the same
//! dictionary the search reads, so anything suggested is guaranteed to return
//! results — an autocomplete that offers a dead end is worse than none.
//!
//! # Popularity
//!
//! PRD §11 has the autocomplete index storing a term with a popularity score,
//! and §5 wants common suggestions ahead of rare ones. Popularity here is the
//! term's document frequency across the searched fields: the number of
//! documents a user would get if they picked it. That needs no extra structure
//! and cannot drift out of sync with the index.
//!
//! # Ordering
//!
//! Exact prefix matches always come before typo-corrected ones — someone
//! half-way through a word wants the completions of what they typed, not
//! guesses about what they meant. Within each group, higher document frequency
//! wins, and ties break alphabetically so the list is stable.

use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use tachyon_core::{CollectionSchema, Error, FieldId, Result};
use tachyon_index::{tokenizer, FuzzyMatcher};

use crate::executor::SearchContext;

pub const DEFAULT_SUGGEST_LIMIT: usize = 5;
pub const MAX_SUGGEST_LIMIT: usize = 50;

/// Terms examined per source before ranking. Bounds the tail latency of a very
/// short prefix on a large dictionary.
pub const MAX_SUGGEST_CANDIDATES: usize = 1000;

/// See the note on `SearchParams`: `IntoParams` defaults to `Path`, which
/// forces every parameter to be documented as required.
#[derive(Debug, Clone, Default, Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct SuggestParams {
    pub q: Option<String>,
    /// Comma-separated fields whose terms may be suggested.
    pub query_by: Option<String>,
    pub limit: Option<usize>,
    pub typo_tolerance: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct SuggestRequest {
    /// The final token of `q`, normalized — the part being completed.
    pub prefix: String,
    pub fields: Vec<FieldId>,
    pub limit: usize,
    pub typo_tolerance: bool,
}

impl SuggestRequest {
    pub fn resolve(params: SuggestParams, schema: &CollectionSchema) -> Result<SuggestRequest> {
        // Only the last token is being completed; earlier ones are already
        // typed and are not what the user is asking about.
        let prefix = tokenizer::terms(&params.q.unwrap_or_default()).pop().unwrap_or_default();

        let fields = match params.query_by.as_deref() {
            Some(list) => {
                let mut fields = Vec::new();
                for name in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    let (id, field) = schema.field(name).ok_or_else(|| {
                        Error::query(format!("query_by references unknown field `{name}`"))
                    })?;
                    if !field.is_searchable() {
                        return Err(Error::query(format!(
                            "field `{name}` is not searchable, so it has no terms to suggest"
                        )));
                    }
                    fields.push(id);
                }
                if fields.is_empty() {
                    return Err(Error::query("query_by must name at least one field"));
                }
                fields
            }
            None => schema.searchable_field_ids(),
        };

        let limit = params.limit.unwrap_or(DEFAULT_SUGGEST_LIMIT);
        if limit == 0 || limit > MAX_SUGGEST_LIMIT {
            return Err(Error::query(format!(
                "limit must be between 1 and {MAX_SUGGEST_LIMIT}, got {limit}"
            )));
        }

        Ok(SuggestRequest {
            prefix,
            fields,
            limit,
            typo_tolerance: params.typo_tolerance.unwrap_or(schema.typo_tolerance.enabled),
        })
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct Suggestion {
    pub text: String,
    /// Documents containing this term across the searched fields.
    pub count: u64,
    /// Edits away from what the user typed. `0` is a plain completion.
    pub typos: u32,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SuggestResponse {
    pub suggestions: Vec<Suggestion>,
    pub search_time_ms: u64,
}

/// Produce suggestions for a prefix.
pub fn suggest(ctx: &SearchContext, req: &SuggestRequest) -> Vec<Suggestion> {
    if req.prefix.is_empty() {
        return Vec::new();
    }

    let mut candidates: Vec<(String, u32)> = Vec::new();

    // Completions of exactly what was typed.
    let mut prefix_terms = Vec::new();
    for source in &ctx.sources {
        source.collect_terms_with_prefix(&req.prefix, MAX_SUGGEST_CANDIDATES, &mut prefix_terms);
    }
    candidates.extend(prefix_terms.into_iter().map(|term| (term, 0)));

    // …then corrections, for someone who mistyped early in the word and would
    // otherwise see nothing at all.
    if req.typo_tolerance {
        let max_edits = ctx.schema.typo_tolerance.typos_for_length(req.prefix.chars().count());
        if max_edits > 0 {
            let mut matcher = FuzzyMatcher::new(&req.prefix, max_edits as u32);
            let mut matches = Vec::new();
            for source in &ctx.sources {
                source.collect_fuzzy_terms(&mut matcher, &mut matches);
            }
            candidates.extend(matches.into_iter().filter(|(_, edits)| *edits > 0));
        }
    }

    // Keep the cheapest correction for each distinct term.
    candidates.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    candidates.dedup_by(|a, b| a.0 == b.0);

    // Rank cheaply first, on posting-list length, and keep a working set a few
    // times the page. Counting *live* documents means walking posting lists, so
    // it is worth doing for a few dozen terms and not for a thousand.
    //
    // The frequency is attached to each term before sorting rather than read
    // inside the comparator: a comparator runs O(n log n) times, and this one
    // would have done a term-dictionary lookup per source per field on every
    // call — thousands of lookups to order a few hundred candidates.
    let mut ranked: Vec<(u32, u64, String)> = candidates
        .into_iter()
        .map(|(term, typos)| (typos, raw_freq(ctx, req, &term), term))
        .collect();
    ranked.sort_by(|a, b| {
        // Fewest edits first, then most documents, then alphabetically.
        a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)).then_with(|| a.2.cmp(&b.2))
    });
    ranked.truncate(req.limit * 4 + 16);

    let mut suggestions: Vec<Suggestion> = ranked
        .into_iter()
        .map(|(typos, _, term)| {
            let mut count = 0u64;
            for source in &ctx.sources {
                for field in &req.fields {
                    count += source.live_doc_freq(&term, *field, ctx.deleted);
                }
            }
            Suggestion { text: term, count, typos }
        })
        // Drops two kinds of dead end: terms only present in fields the caller
        // did not ask about, and terms whose every document has been deleted.
        .filter(|s| s.count > 0)
        .collect();

    suggestions.sort_by(|a, b| {
        a.typos.cmp(&b.typos).then(b.count.cmp(&a.count)).then_with(|| a.text.cmp(&b.text))
    });
    suggestions.truncate(req.limit);
    suggestions
}

/// Posting-list length across the searched fields, including deleted
/// documents. Only used to order the working set before exact counting.
fn raw_freq(ctx: &SearchContext, req: &SuggestRequest, term: &str) -> u64 {
    let mut total = 0u64;
    for source in &ctx.sources {
        for field in &req.fields {
            total += source.doc_freq(term, *field) as u64;
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;
    use serde_json::json;
    use tachyon_core::{FieldSchema, FieldType, ParsedDocument};
    use tachyon_index::MemTable;

    struct Fixture {
        schema: CollectionSchema,
        memtable: MemTable,
        deleted: RoaringBitmap,
    }

    fn fixture(docs: &[serde_json::Value]) -> Fixture {
        let schema = CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("description", FieldType::Text),
            ],
        );
        let mut memtable = MemTable::new(0, &schema);
        for doc in docs {
            memtable.insert(ParsedDocument::parse(doc.clone(), &schema).unwrap());
        }
        Fixture { schema, memtable, deleted: RoaringBitmap::new() }
    }

    impl Fixture {
        fn suggest(&self, params: SuggestParams) -> Vec<Suggestion> {
            let req = SuggestRequest::resolve(params, &self.schema).unwrap();
            let ctx = SearchContext::new(&self.schema, vec![&self.memtable], &self.deleted);
            suggest(&ctx, &req)
        }

        fn texts(&self, params: SuggestParams) -> Vec<String> {
            self.suggest(params).into_iter().map(|s| s.text).collect()
        }
    }

    fn q(prefix: &str) -> SuggestParams {
        SuggestParams { q: Some(prefix.into()), limit: Some(10), ..Default::default() }
    }

    /// `wireless` appears in three documents, `wired` in two, `wire` in one.
    fn catalogue() -> Fixture {
        fixture(&[
            json!({"id": "1", "title": "wireless mouse"}),
            json!({"id": "2", "title": "wireless keyboard"}),
            json!({"id": "3", "title": "wireless charger"}),
            json!({"id": "4", "title": "wired mouse"}),
            json!({"id": "5", "title": "wired keyboard"}),
            json!({"id": "6", "title": "wire cutter"}),
        ])
    }

    #[test]
    fn completes_a_prefix_ordered_by_popularity() {
        // The PRD's own example: ?q=wir
        let f = catalogue();
        assert_eq!(f.texts(q("wir")), vec!["wireless", "wired", "wire"]);
    }

    #[test]
    fn counts_are_the_documents_behind_each_suggestion() {
        let f = catalogue();
        let suggestions = f.suggest(q("wir"));
        assert_eq!(suggestions[0], Suggestion { text: "wireless".into(), count: 3, typos: 0 });
        assert_eq!(suggestions[1], Suggestion { text: "wired".into(), count: 2, typos: 0 });
        assert_eq!(suggestions[2], Suggestion { text: "wire".into(), count: 1, typos: 0 });
    }

    #[test]
    fn every_suggestion_would_return_results() {
        let f = catalogue();
        for suggestion in f.suggest(q("w")) {
            assert!(suggestion.count > 0, "`{}` leads nowhere", suggestion.text);
        }
    }

    #[test]
    fn limit_is_honoured() {
        let f = catalogue();
        let limited = f.texts(SuggestParams { limit: Some(2), ..q("wir") });
        assert_eq!(limited, vec!["wireless", "wired"]);
    }

    #[test]
    fn only_the_last_token_is_completed() {
        let f = catalogue();
        // "wireless mou" — the user is typing the second word.
        let suggestions = f.texts(q("wireless mou"));
        assert_eq!(suggestions, vec!["mouse"]);
    }

    #[test]
    fn suggestions_are_normalized_like_the_index() {
        let f = catalogue();
        assert_eq!(f.texts(q("WIR")), f.texts(q("wir")));
    }

    #[test]
    fn an_empty_prefix_suggests_nothing() {
        let f = catalogue();
        assert!(f.suggest(q("")).is_empty());
        assert!(f.suggest(q("   ")).is_empty());
        assert!(f.suggest(q("!!!")).is_empty());
    }

    #[test]
    fn an_unmatched_prefix_suggests_nothing() {
        let f = catalogue();
        assert!(f.suggest(q("zzz")).is_empty());
    }

    #[test]
    fn a_mistyped_prefix_still_suggests_something() {
        let f = catalogue();
        // `wirelss` is 7 characters, so one edit is allowed.
        let suggestions = f.suggest(q("wirelss"));
        assert!(!suggestions.is_empty(), "a typo should not produce a dead end");
        assert_eq!(suggestions[0].text, "wireless");
        assert_eq!(suggestions[0].typos, 1);
    }

    #[test]
    fn plain_completions_come_before_corrections() {
        let f = fixture(&[
            json!({"id": "1", "title": "mousepad mousepad mousepad"}),
            json!({"id": "2", "title": "moused"}),
            json!({"id": "3", "title": "house"}),
            json!({"id": "4", "title": "house"}),
            json!({"id": "5", "title": "house"}),
            json!({"id": "6", "title": "house"}),
        ]);
        let suggestions = f.suggest(q("mouse"));
        let plain: Vec<_> =
            suggestions.iter().take_while(|s| s.typos == 0).map(|s| s.text.clone()).collect();
        assert!(plain.contains(&"mousepad".to_string()));
        assert!(
            suggestions.iter().position(|s| s.text == "house").unwrap()
                > suggestions.iter().position(|s| s.text == "mousepad").unwrap(),
            "a popular correction must not displace a real completion: {suggestions:?}"
        );
    }

    #[test]
    fn typo_tolerance_can_be_disabled() {
        let f = catalogue();
        let strict = SuggestParams { typo_tolerance: Some(false), ..q("wirelss") };
        assert!(f.suggest(strict).is_empty());
    }

    #[test]
    fn query_by_restricts_which_fields_contribute() {
        let f = fixture(&[json!({"id": "1", "title": "wireless", "description": "widget"})]);
        assert_eq!(
            f.texts(SuggestParams { query_by: Some("title".into()), ..q("wi") }),
            vec!["wireless"]
        );
        assert_eq!(
            f.texts(SuggestParams { query_by: Some("description".into()), ..q("wi") }),
            vec!["widget"]
        );
    }

    #[test]
    fn resolution_rejects_bad_requests() {
        let f = catalogue();
        for params in [
            SuggestParams { query_by: Some("nope".into()), ..q("w") },
            SuggestParams { limit: Some(0), ..q("w") },
            SuggestParams { limit: Some(MAX_SUGGEST_LIMIT + 1), ..q("w") },
        ] {
            assert!(SuggestRequest::resolve(params, &f.schema).is_err());
        }
    }
}
