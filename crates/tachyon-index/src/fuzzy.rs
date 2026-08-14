//! Damerau-Levenshtein distance for typo tolerance (PRD §7.4).
//!
//! This is the *unrestricted* Damerau-Levenshtein distance: insertion,
//! deletion, substitution, and transposition of two characters that need not
//! be adjacent in the original string. The commonly-implemented "optimal
//! string alignment" variant is cheaper but refuses to edit the same region
//! twice, which makes `cadefghi` → `abcdefghi` cost 3 instead of 2. At the two
//! edits the typo table permits, that difference is reachable, so the real
//! algorithm is what is implemented here.
//!
//! # Cost
//!
//! Matching a query token against a dictionary means running this against many
//! candidates, so [`FuzzyMatcher`] keeps its scratch buffers across calls and
//! rejects candidates in this order:
//!
//! 1. Length differs by more than the budget — impossible, no allocation.
//! 2. Row minimum exceeds the budget — abandon mid-matrix.
//!
//! Most dictionary terms die at step 1.

use std::collections::HashMap;

/// Longest token we will run the matrix against. Beyond this the quadratic
/// cost stops being worth it, and a token this long is not a typo anyway.
pub const MAX_FUZZY_LEN: usize = 64;

/// Matches one query token against many candidate terms.
///
/// Built once per query token, then asked about each candidate; the matrix and
/// the last-occurrence table are reused, so a scan over a large dictionary
/// allocates nothing per candidate.
pub struct FuzzyMatcher {
    query: Vec<char>,
    max: u32,
    /// Row-major `(query_len + 2) x (candidate_len + 2)` scratch matrix.
    matrix: Vec<u32>,
    /// Last row at which each character of the query was seen.
    last_row: HashMap<char, usize>,
    candidate: Vec<char>,
}

impl FuzzyMatcher {
    pub fn new(query: &str, max: u32) -> FuzzyMatcher {
        FuzzyMatcher {
            query: query.chars().take(MAX_FUZZY_LEN).collect(),
            max,
            matrix: Vec::new(),
            last_row: HashMap::new(),
            candidate: Vec::new(),
        }
    }

    pub fn max_distance(&self) -> u32 {
        self.max
    }

    pub fn query_len(&self) -> usize {
        self.query.len()
    }

    /// Edit distance to `candidate`, or `None` if it exceeds the budget.
    pub fn distance(&mut self, candidate: &str) -> Option<u32> {
        if self.max == 0 {
            return (candidate.chars().eq(self.query.iter().copied())).then_some(0);
        }

        // Cheapest possible rejection: a length gap larger than the budget
        // cannot be closed, since each edit changes the length by at most one.
        let candidate_len = candidate.chars().count();
        if candidate_len.abs_diff(self.query.len()) > self.max as usize {
            return None;
        }
        if candidate_len > MAX_FUZZY_LEN {
            return None;
        }

        self.candidate.clear();
        self.candidate.extend(candidate.chars());

        let (m, n) = (self.query.len(), self.candidate.len());
        let width = n + 2;
        let infinity = (m + n) as u32;

        self.matrix.clear();
        self.matrix.resize(width * (m + 2), 0);
        self.last_row.clear();

        // Rows and columns are offset by one so index 0 can hold the sentinel
        // row/column the transposition rule reaches back into.
        let at = |i: usize, j: usize| i * width + j;

        self.matrix[at(0, 0)] = infinity;
        for j in 0..=n {
            self.matrix[at(0, j + 1)] = infinity;
            self.matrix[at(1, j + 1)] = j as u32;
        }
        for i in 0..=m {
            self.matrix[at(i + 1, 0)] = infinity;
            self.matrix[at(i + 1, 1)] = i as u32;
        }

        for i in 1..=m {
            let mut last_match_col = 0usize;
            let mut row_min = u32::MAX;

            for j in 1..=n {
                // Row of the last occurrence in the query of this candidate
                // character, and column of the last match in this row: together
                // they locate the transposition to undo.
                let last_matching_row =
                    self.last_row.get(&self.candidate[j - 1]).copied().unwrap_or(0);
                let cost = if self.query[i - 1] == self.candidate[j - 1] { 0 } else { 1 };

                let substitute = self.matrix[at(i, j)] + cost;
                let insert = self.matrix[at(i + 1, j)] + 1;
                let delete = self.matrix[at(i, j + 1)] + 1;
                let transpose = self.matrix[at(last_matching_row, last_match_col)]
                    .saturating_add((i - last_matching_row) as u32)
                    .saturating_add((j - last_match_col) as u32)
                    .saturating_sub(1);

                let best = substitute.min(insert).min(delete).min(transpose);
                self.matrix[at(i + 1, j + 1)] = best;
                row_min = row_min.min(best);

                if cost == 0 {
                    last_match_col = j;
                }
            }

            self.last_row.insert(self.query[i - 1], i);

            // Every later row is at least this good, so once the whole row is
            // over budget the answer can only be worse. An empty candidate has
            // no columns and so no row minimum to judge.
            if n > 0 && row_min > self.max {
                return None;
            }
        }

        let distance = self.matrix[at(m + 1, n + 1)];
        (distance <= self.max).then_some(distance)
    }
}

/// One-shot distance, for callers not scanning a dictionary.
pub fn distance_within(a: &str, b: &str, max: u32) -> Option<u32> {
    FuzzyMatcher::new(a, max).distance(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(a: &str, b: &str) -> Option<u32> {
        distance_within(a, b, 10)
    }

    #[test]
    fn identical_strings_are_zero_apart() {
        assert_eq!(d("mouse", "mouse"), Some(0));
        assert_eq!(d("", ""), Some(0));
    }

    #[test]
    fn single_edits_cost_one() {
        assert_eq!(d("mouse", "house"), Some(1), "substitution");
        assert_eq!(d("mouse", "mouses"), Some(1), "insertion");
        assert_eq!(d("mouse", "muse"), Some(1), "deletion");
        assert_eq!(d("mouse", "moues"), Some(1), "adjacent transposition");
    }

    #[test]
    fn transposition_is_one_edit_not_two() {
        // The whole point of Damerau over Levenshtein.
        assert_eq!(d("teh", "the"), Some(1));
        assert_eq!(d("wireless", "wierless"), Some(1));
    }

    #[test]
    fn the_unrestricted_variant_reuses_edited_regions() {
        // Optimal string alignment says 3 here; true Damerau-Levenshtein
        // says 2, and this is reachable within the typo table's budget.
        assert_eq!(d("ca", "abc"), Some(2));
        assert_eq!(d("cadefghi", "abcdefghi"), Some(2));
    }

    #[test]
    fn distance_is_symmetric() {
        for (a, b) in [("mouse", "house"), ("teh", "the"), ("kitten", "sitting"), ("ca", "abc")] {
            assert_eq!(d(a, b), d(b, a), "{a} vs {b}");
        }
    }

    #[test]
    fn known_distances() {
        assert_eq!(d("kitten", "sitting"), Some(3));
        assert_eq!(d("saturday", "sunday"), Some(3));
        assert_eq!(d("", "abc"), Some(3));
        assert_eq!(d("abc", ""), Some(3));
    }

    #[test]
    fn the_budget_is_respected() {
        assert_eq!(distance_within("mouse", "house", 1), Some(1));
        assert_eq!(distance_within("mouse", "house", 0), None);
        assert_eq!(distance_within("kitten", "sitting", 2), None);
        assert_eq!(distance_within("kitten", "sitting", 3), Some(3));
    }

    #[test]
    fn a_zero_budget_is_exact_matching() {
        assert_eq!(distance_within("mouse", "mouse", 0), Some(0));
        assert_eq!(distance_within("mouse", "mous", 0), None);
        assert_eq!(distance_within("mouse", "mousey", 0), None);
    }

    #[test]
    fn length_gaps_beyond_the_budget_are_rejected() {
        assert_eq!(distance_within("mouse", "mousepad", 2), None);
        assert_eq!(distance_within("a", "abcdefgh", 2), None);
    }

    #[test]
    fn a_matcher_is_reusable_across_candidates() {
        let mut matcher = FuzzyMatcher::new("mouse", 2);
        assert_eq!(matcher.distance("house"), Some(1));
        assert_eq!(matcher.distance("mouse"), Some(0));
        assert_eq!(matcher.distance("moose"), Some(1));
        assert_eq!(matcher.distance("keyboard"), None);
        // …and still correct after a rejection.
        assert_eq!(matcher.distance("mousy"), Some(1));
        assert_eq!(matcher.query_len(), 5);
        assert_eq!(matcher.max_distance(), 2);
    }

    #[test]
    fn unicode_is_measured_in_characters_not_bytes() {
        assert_eq!(d("café", "cafe"), Some(1));
        assert_eq!(d("naïve", "naive"), Some(1));
        // Four multi-byte characters replaced by four others is four edits,
        // not sixteen.
        assert_eq!(d("鼠标无线", "鼠标有线"), Some(1));
    }

    #[test]
    fn overlong_candidates_are_skipped() {
        let long = "a".repeat(MAX_FUZZY_LEN + 1);
        assert_eq!(distance_within("aaa", &long, 2), None);
    }

    #[test]
    fn realistic_typos_land_inside_the_prd_budget() {
        // Length 8+, so the table allows two edits.
        for typo in ["wireles", "wirelesss", "wirless", "wierless", "wirelss"] {
            assert!(
                distance_within("wireless", typo, 2).is_some(),
                "{typo} should be within two edits of wireless"
            );
        }
        // But an unrelated word is not.
        assert_eq!(distance_within("wireless", "keyboard", 2), None);
    }
}
