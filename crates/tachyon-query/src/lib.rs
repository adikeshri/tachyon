//! The Tachyon query engine: parsing a request, planning it against a schema,
//! walking postings, and ranking what comes back.
//!
//! It holds no state and opens no files — it reads through
//! [`tachyon_index::IndexSource`], so the same code path serves the memtable
//! and every committed segment.

pub mod bm25;
pub mod executor;
pub mod facets;
pub mod filter;
pub mod query_text;
pub mod request;
pub mod score;
pub mod sort;
pub mod suggest;
mod wand;

pub use executor::{execute, ScoredDoc, SearchContext, SearchOutcome};
pub use facets::compute as compute_facets;
pub use filter::{FilterExpr, Predicate};
pub use query_text::ParsedQuery;
pub use request::{Hit, MatchMode, SearchParams, SearchRequest, SearchResponse};
pub use score::{ScoreComponents, ScoreWeights};
pub use sort::{SortClause, SortKey};
pub use suggest::{suggest, SuggestParams, SuggestRequest, SuggestResponse, Suggestion};
