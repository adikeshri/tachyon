//! BM25 (PRD §7.3).
//!
//! ```text
//! score(q, d) = Σ_t  idf(t) · (tf · (k1 + 1)) / (tf + k1 · (1 − b + b · |d| / avgdl))
//! idf(t)      = ln(1 + (N − df + 0.5) / (df + 0.5))
//! ```
//!
//! `k1` controls how quickly repeated terms stop helping; `b` how strongly a
//! long field is penalised. The values below are the standard ones and are what
//! the relevance benchmark is tuned against.

/// Term-frequency saturation.
pub const K1: f32 = 1.2;

/// Field-length normalization strength. `0.0` ignores length entirely, `1.0`
/// normalizes fully.
pub const B: f32 = 0.75;

/// Corpus statistics for one field, gathered across every source.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FieldStats {
    /// Documents with any content in this field.
    pub doc_count: u32,
    /// Mean field length in tokens.
    pub avg_len: f32,
}

impl FieldStats {
    pub fn new(doc_count: u32, total_len: u64) -> FieldStats {
        let avg_len = if doc_count > 0 { total_len as f32 / doc_count as f32 } else { 0.0 };
        FieldStats { doc_count, avg_len }
    }
}

/// Inverse document frequency, in the BM25+ form that stays non-negative.
///
/// The `1 +` inside the logarithm is what keeps a term occurring in nearly
/// every document from scoring negative and dragging down documents that
/// legitimately match it.
pub fn idf(doc_freq: u32, num_docs: u32) -> f32 {
    let n = num_docs as f32;
    let df = (doc_freq as f32).min(n);
    (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
}

/// One term's BM25 contribution within one field of one document.
pub fn term_score(tf: u32, field_len: u32, stats: FieldStats, idf: f32) -> f32 {
    if tf == 0 {
        return 0.0;
    }
    let tf = tf as f32;
    // A field with no recorded average (nothing indexed yet) should not blow
    // up the normalizer; fall back to no length normalization.
    let length_norm =
        if stats.avg_len > 0.0 { 1.0 - B + B * (field_len as f32 / stats.avg_len) } else { 1.0 };
    idf * (tf * (K1 + 1.0)) / (tf + K1 * length_norm)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rare_terms_outweigh_common_ones() {
        let rare = idf(1, 1000);
        let common = idf(900, 1000);
        assert!(rare > common, "{rare} should exceed {common}");
        assert!(common >= 0.0, "idf must never go negative, got {common}");
    }

    #[test]
    fn a_term_in_every_document_is_worth_almost_nothing() {
        let everywhere = idf(1000, 1000);
        assert!(everywhere < 0.5, "got {everywhere}");
        assert!(everywhere >= 0.0);
    }

    #[test]
    fn doc_freq_above_the_corpus_size_is_clamped() {
        // Deleted documents linger in posting lists, so df can exceed N.
        assert!(idf(50, 10).is_finite());
        assert!(idf(50, 10) >= 0.0);
    }

    #[test]
    fn term_frequency_saturates() {
        let stats = FieldStats::new(100, 1000); // avg_len 10
        let idf = idf(10, 100);
        let one = term_score(1, 10, stats, idf);
        let two = term_score(2, 10, stats, idf);
        let nine = term_score(9, 10, stats, idf);
        let ten = term_score(10, 10, stats, idf);

        assert!(two > one);
        assert!(ten > nine);
        // The marginal value of one more occurrence shrinks as tf grows.
        assert!(two - one > ten - nine, "tf should saturate");
        // And the whole thing stays bounded by idf * (k1 + 1).
        assert!(term_score(10_000, 10, stats, idf) < idf * (K1 + 1.0));
    }

    #[test]
    fn short_fields_score_higher_for_the_same_term_frequency() {
        let stats = FieldStats::new(100, 1000); // avg_len 10
        let idf = idf(10, 100);
        let short = term_score(1, 3, stats, idf);
        let long = term_score(1, 40, stats, idf);
        assert!(short > long, "a hit in a short field is more significant");
    }

    #[test]
    fn zero_term_frequency_scores_zero() {
        assert_eq!(term_score(0, 10, FieldStats::new(100, 1000), 1.0), 0.0);
    }

    #[test]
    fn an_empty_field_does_not_produce_nan() {
        let stats = FieldStats::new(0, 0);
        let score = term_score(1, 5, stats, idf(0, 0));
        assert!(score.is_finite(), "got {score}");
    }

    #[test]
    fn field_stats_average() {
        assert_eq!(FieldStats::new(4, 40).avg_len, 10.0);
        assert_eq!(FieldStats::new(0, 0).avg_len, 0.0);
    }
}
