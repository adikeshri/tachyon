//! The composite relevance score (PRD §12).
//!
//! ```text
//! score = 0.45·BM25 + 0.25·field_boost + 0.15·proximity + 0.10·typo_penalty + 0.05·popularity
//! ```
//!
//! Those weights only mean anything if the five components share a scale, so
//! each is normalized into `[0, 1]` before being mixed. The two that are not
//! naturally bounded — BM25 and the field boost — are normalized here, and the
//! reasoning for each is documented at its function.

use tachyon_core::CollectionSchema;

/// Multiplier applied to the weighted sum before it is reported as
/// `text_match`. Purely cosmetic: scores in the hundreds read better in an API
/// response than scores in the hundredths.
pub const TEXT_MATCH_SCALE: f32 = 1000.0;

/// BM25 value at which the normalized component reaches 0.5. Raw BM25 for a
/// solid multi-term match on a small corpus lands in the 5–20 range, so this
/// puts everyday scores in the responsive middle of the curve rather than
/// pinned near 0 or 1.
pub const BM25_HALF_SATURATION: f32 = 10.0;

/// Popularity value at which the normalized component reaches 0.5.
pub const POPULARITY_HALF_SATURATION: f32 = 100.0;

/// Field whose value feeds the popularity component. A collection that does
/// not declare it simply scores 0 there, and the remaining weights dominate.
pub const POPULARITY_FIELD: &str = "popularity";

/// PRD §12 weights.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreWeights {
    pub bm25: f32,
    pub field_boost: f32,
    pub proximity: f32,
    pub typo: f32,
    pub popularity: f32,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        ScoreWeights {
            bm25: 0.45,
            field_boost: 0.25,
            proximity: 0.15,
            typo: 0.10,
            popularity: 0.05,
        }
    }
}

/// The five signals, each already normalized to `[0, 1]`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ScoreComponents {
    pub bm25: f32,
    pub field_boost: f32,
    pub proximity: f32,
    pub typo_penalty: f32,
    pub popularity: f32,
}

impl ScoreComponents {
    pub fn combine(&self, weights: &ScoreWeights) -> f32 {
        let sum = weights.bm25 * self.bm25
            + weights.field_boost * self.field_boost
            + weights.proximity * self.proximity
            + weights.typo * self.typo_penalty
            + weights.popularity * self.popularity;
        sum * TEXT_MATCH_SCALE
    }
}

/// Squash an unbounded, non-negative value into `[0, 1)`.
///
/// `x / (x + half)` is monotonic, has no ceiling to clip against, and keeps
/// relative differences meaningful at both ends — unlike dividing by the
/// maximum score seen in the current result set, which would make a document's
/// score depend on which other documents happened to match.
fn saturate(x: f32, half: f32) -> f32 {
    if x <= 0.0 || !x.is_finite() {
        return 0.0;
    }
    x / (x + half)
}

pub fn normalize_bm25(raw: f32) -> f32 {
    saturate(raw, BM25_HALF_SATURATION)
}

pub fn normalize_popularity(raw: f32) -> f32 {
    saturate(raw, POPULARITY_HALF_SATURATION)
}

/// Normalize a field's boost against the largest boost in the schema, so the
/// component is 1.0 for a hit in the most important field and scales down from
/// there.
pub fn normalize_field_boost(boost: f32, max_boost: f32) -> f32 {
    if max_boost <= 0.0 {
        return 0.0;
    }
    (boost / max_boost).clamp(0.0, 1.0)
}

/// Largest effective boost across a schema's searchable fields, used as the
/// denominator above. Never returns 0, so callers cannot divide by zero.
pub fn max_boost(schema: &CollectionSchema) -> f32 {
    schema
        .fields
        .iter()
        .filter(|f| f.is_searchable())
        .map(|f| f.effective_boost())
        .fold(1.0, f32::max)
}

/// How tightly the query terms sit together, in `[0, 1]`.
///
/// `positions` holds, per query token, the positions at which that token
/// occurs in the field. The score is the ratio of the ideal span (terms
/// adjacent, `n - 1`) to the smallest window actually containing one
/// occurrence of every token: 1.0 for an exact phrase, decaying as the terms
/// spread out.
pub fn proximity(positions: &[Vec<u32>]) -> f32 {
    let n = positions.len();
    if n <= 1 {
        // A single term is trivially adjacent to itself.
        return 1.0;
    }
    if positions.iter().any(|p| p.is_empty()) {
        // Not every token is present in this field; nothing to measure.
        return 0.0;
    }

    match min_window_span(positions) {
        Some(span) if span > 0 => ((n - 1) as f32 / span as f32).clamp(0.0, 1.0),
        // A zero span would mean two tokens at the same position, which the
        // tokenizer cannot produce; treat as a perfect phrase.
        Some(_) => 1.0,
        None => 0.0,
    }
}

/// Smallest `max - min` over any selection of one position from each list.
///
/// Standard k-way sweep: advance the list whose current position is smallest,
/// since that is the only move that can shrink the window.
fn min_window_span(positions: &[Vec<u32>]) -> Option<u32> {
    let mut cursors = vec![0usize; positions.len()];
    let mut best = u32::MAX;

    loop {
        let mut min_value = u32::MAX;
        let mut min_list = 0usize;
        let mut max_value = 0u32;

        for (i, list) in positions.iter().enumerate() {
            let value = *list.get(cursors[i])?;
            if value < min_value {
                min_value = value;
                min_list = i;
            }
            max_value = max_value.max(value);
        }

        best = best.min(max_value - min_value);
        if best == 0 {
            return Some(0);
        }

        cursors[min_list] += 1;
        if cursors[min_list] >= positions[min_list].len() {
            return Some(best);
        }
    }
}

/// Reward exact matches over typo-corrected ones, in `[0, 1]`.
///
/// `edits_used` is the total edit distance spent matching the query; `edits_allowed`
/// is what the typo table would have permitted. A query where nothing needed
/// correcting scores 1.0.
pub fn typo_penalty(edits_used: u32, edits_allowed: u32) -> f32 {
    if edits_allowed == 0 {
        // No typos were permitted, so none were used: this is an exact match.
        return 1.0;
    }
    1.0 - (edits_used as f32 / edits_allowed as f32).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_core::{FieldSchema, FieldType};

    #[test]
    fn saturation_is_monotonic_and_bounded() {
        let mut previous = 0.0;
        for raw in [0.0, 0.5, 1.0, 5.0, 10.0, 100.0, 1e6] {
            let n = normalize_bm25(raw);
            assert!((0.0..=1.0).contains(&n), "{raw} -> {n}");
            assert!(n >= previous, "not monotonic at {raw}");
            previous = n;
        }
        assert!((normalize_bm25(BM25_HALF_SATURATION) - 0.5).abs() < 1e-6);
        assert_eq!(normalize_bm25(-1.0), 0.0);
        assert_eq!(normalize_bm25(f32::NAN), 0.0);
    }

    #[test]
    fn field_boost_normalizes_against_the_schema_maximum() {
        let schema = CollectionSchema::new(
            "c",
            vec![
                FieldSchema::new("title", FieldType::Text), // boost 10
                FieldSchema::new("description", FieldType::Text), // boost 2
            ],
        );
        let max = max_boost(&schema);
        assert_eq!(max, 10.0);
        assert_eq!(normalize_field_boost(10.0, max), 1.0);
        assert_eq!(normalize_field_boost(2.0, max), 0.2);
        assert_eq!(normalize_field_boost(0.0, max), 0.0);
    }

    #[test]
    fn max_boost_never_returns_zero() {
        let schema = CollectionSchema::new(
            "c",
            vec![FieldSchema::new("body", FieldType::Text).with_boost(0.0)],
        );
        assert!(max_boost(&schema) > 0.0);
    }

    #[test]
    fn adjacent_terms_score_a_perfect_proximity() {
        // "wireless mouse" at positions 3 and 4.
        assert_eq!(proximity(&[vec![3], vec![4]]), 1.0);
        // Three adjacent terms.
        assert_eq!(proximity(&[vec![0], vec![1], vec![2]]), 1.0);
    }

    #[test]
    fn spread_out_terms_score_lower() {
        let tight = proximity(&[vec![0], vec![1]]);
        let loose = proximity(&[vec![0], vec![10]]);
        assert!(tight > loose, "{tight} vs {loose}");
        assert!(loose > 0.0);
    }

    #[test]
    fn proximity_finds_the_best_window_among_repeats() {
        // "mouse" at 0 and 50, "pad" at 51: the best window is 50..51.
        assert_eq!(proximity(&[vec![0, 50], vec![51]]), 1.0);
    }

    #[test]
    fn proximity_edge_cases() {
        assert_eq!(proximity(&[]), 1.0, "no terms is not a penalty");
        assert_eq!(proximity(&[vec![7]]), 1.0, "one term is trivially adjacent");
        assert_eq!(proximity(&[vec![1], vec![]]), 0.0, "a missing term scores zero");
    }

    #[test]
    fn typo_penalty_rewards_exact_matches() {
        assert_eq!(typo_penalty(0, 2), 1.0);
        assert_eq!(typo_penalty(1, 2), 0.5);
        assert_eq!(typo_penalty(2, 2), 0.0);
        assert_eq!(typo_penalty(0, 0), 1.0, "no typos allowed means exact");
        assert_eq!(typo_penalty(5, 2), 0.0, "clamped, never negative");
    }

    #[test]
    fn the_prd_weights_sum_to_one() {
        let w = ScoreWeights::default();
        let total = w.bm25 + w.field_boost + w.proximity + w.typo + w.popularity;
        assert!((total - 1.0).abs() < 1e-6, "weights sum to {total}");
    }

    #[test]
    fn a_perfect_document_scores_the_full_scale() {
        let perfect = ScoreComponents {
            bm25: 1.0,
            field_boost: 1.0,
            proximity: 1.0,
            typo_penalty: 1.0,
            popularity: 1.0,
        };
        assert!((perfect.combine(&ScoreWeights::default()) - TEXT_MATCH_SCALE).abs() < 1e-3);
        assert_eq!(ScoreComponents::default().combine(&ScoreWeights::default()), 0.0);
    }

    #[test]
    fn bm25_dominates_the_mix() {
        let w = ScoreWeights::default();
        let strong_text = ScoreComponents { bm25: 1.0, ..Default::default() };
        let popular_only = ScoreComponents { popularity: 1.0, ..Default::default() };
        assert!(strong_text.combine(&w) > popular_only.combine(&w));
    }
}
