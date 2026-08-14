//! Splitting the `q` string into tokens and phrase groups (PRD §7.3, "phrase
//! matching").
//!
//! Double quotes mark a phrase: `wireless "mouse pad"` requires `mouse` and
//! `pad` to be adjacent, in that order, within one field, while `wireless` may
//! appear anywhere. Tokens are flattened into one ordered list — phrases are
//! recorded as index ranges into it — so the executor's postings walk is
//! unchanged and phrases become a verification step over positions it already
//! collects.

use tachyon_index::tokenizer;

/// A query string after tokenization, with its phrase groups marked.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParsedQuery {
    /// Every token, in query order.
    pub tokens: Vec<String>,
    /// Inclusive `(start, end)` index ranges into `tokens` that must appear
    /// consecutively. Single-token quotes are not recorded: there is nothing
    /// to be adjacent to.
    pub phrases: Vec<(usize, usize)>,
}

impl ParsedQuery {
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Whether a token index falls inside any phrase. Prefix expansion is
    /// suppressed for those, since a phrase is by definition a request for
    /// exact adjacent terms.
    pub fn in_phrase(&self, index: usize) -> bool {
        self.phrases.iter().any(|(start, end)| index >= *start && index <= *end)
    }
}

/// Parse a raw query string.
///
/// An unterminated quote is treated as ordinary text rather than an error:
/// this is a search box, and a user half-way through typing a phrase should
/// still get results.
pub fn parse(q: &str) -> ParsedQuery {
    let mut parsed = ParsedQuery::default();

    let mut rest = q;
    while let Some(open) = rest.find('"') {
        // Everything before the quote is ordinary text.
        parsed.tokens.extend(tokenizer::terms(&rest[..open]));

        let after_open = &rest[open + 1..];
        let Some(close) = after_open.find('"') else {
            // Unterminated: the remainder is ordinary text.
            parsed.tokens.extend(tokenizer::terms(after_open));
            return parsed;
        };

        let phrase_tokens = tokenizer::terms(&after_open[..close]);
        if phrase_tokens.len() > 1 {
            let start = parsed.tokens.len();
            parsed.phrases.push((start, start + phrase_tokens.len() - 1));
        }
        parsed.tokens.extend(phrase_tokens);

        rest = &after_open[close + 1..];
    }

    parsed.tokens.extend(tokenizer::terms(rest));
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(q: &str) -> Vec<String> {
        parse(q).tokens
    }

    #[test]
    fn a_plain_query_has_no_phrases() {
        let p = parse("wireless mouse");
        assert_eq!(p.tokens, vec!["wireless", "mouse"]);
        assert!(p.phrases.is_empty());
        assert!(!p.in_phrase(0));
    }

    #[test]
    fn quotes_mark_a_phrase() {
        let p = parse("\"mouse pad\"");
        assert_eq!(p.tokens, vec!["mouse", "pad"]);
        assert_eq!(p.phrases, vec![(0, 1)]);
        assert!(p.in_phrase(0) && p.in_phrase(1));
    }

    #[test]
    fn phrases_mix_with_loose_terms() {
        let p = parse("wireless \"mouse pad\" cheap");
        assert_eq!(p.tokens, vec!["wireless", "mouse", "pad", "cheap"]);
        assert_eq!(p.phrases, vec![(1, 2)]);
        assert!(!p.in_phrase(0));
        assert!(p.in_phrase(1) && p.in_phrase(2));
        assert!(!p.in_phrase(3));
    }

    #[test]
    fn several_phrases_are_tracked_separately() {
        let p = parse("\"mouse pad\" and \"wireless charger\"");
        assert_eq!(p.tokens, vec!["mouse", "pad", "and", "wireless", "charger"]);
        assert_eq!(p.phrases, vec![(0, 1), (3, 4)]);
    }

    #[test]
    fn a_single_word_in_quotes_is_just_a_word() {
        let p = parse("\"mouse\"");
        assert_eq!(p.tokens, vec!["mouse"]);
        assert!(p.phrases.is_empty(), "one token cannot be adjacent to anything");
    }

    #[test]
    fn an_unterminated_quote_degrades_to_plain_text() {
        let p = parse("wireless \"mouse pad");
        assert_eq!(p.tokens, vec!["wireless", "mouse", "pad"]);
        assert!(p.phrases.is_empty());
    }

    #[test]
    fn empty_and_punctuation_only_queries() {
        assert!(parse("").is_empty());
        assert!(parse("   ").is_empty());
        assert!(parse("\"\"").is_empty());
        assert_eq!(tokens("\"  \" mouse"), vec!["mouse"]);
    }

    #[test]
    fn phrase_tokens_are_normalized_like_everything_else() {
        assert_eq!(tokens("\"Café Crème\""), vec!["cafe", "creme"]);
    }
}
