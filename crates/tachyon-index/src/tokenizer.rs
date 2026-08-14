//! Turning text into indexable terms.
//!
//! The same function runs at index time and at query time — that symmetry is
//! what makes a query match the document it should. Anything that changes here
//! invalidates existing segments.
//!
//! # Rules
//!
//! 1. Runs of alphanumeric characters become tokens; everything else is a
//!    separator. `"wireless-mouse (USB)"` → `wireless`, `mouse`, `usb`.
//! 2. Han ideographs each become their own token, since CJK text has no
//!    spaces to split on. Kana and Hangul follow the run rule, which is a known
//!    limitation — proper segmentation for those is post-v1.
//! 3. Tokens are lowercased and stripped of combining marks, so `Café` and
//!    `cafe` are the same term.
//! 4. Tokens longer than [`MAX_TOKEN_LEN`] characters are truncated, capping
//!    what one pathological input can do to the term dictionary.

use unicode_normalization::char::is_combining_mark;
use unicode_normalization::UnicodeNormalization;

/// Longest term kept, in characters after normalization.
pub const MAX_TOKEN_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    /// Normalized term text, as stored in the index.
    pub text: String,
    /// Zero-based ordinal within the field, used for phrase and proximity
    /// matching.
    pub position: u32,
}

/// Whether a character stands alone as a token (CJK ideographs).
fn is_standalone_ideograph(c: char) -> bool {
    matches!(c as u32,
        0x3400..=0x4DBF     // CJK Unified Ideographs Extension A
        | 0x4E00..=0x9FFF   // CJK Unified Ideographs
        | 0xF900..=0xFAFF   // CJK Compatibility Ideographs
        | 0x20000..=0x2A6DF // Extension B
    )
}

/// Normalize one already-split run into its indexed form.
///
/// Returns `None` if nothing survives normalization (for example a run of
/// bare combining marks).
pub fn normalize(run: &str) -> Option<String> {
    // ASCII is its own NFD, carries no combining marks, and lowercases one
    // byte to one byte, so the general path's decomposition and per-character
    // case-mapping iterators can be skipped entirely. This is the overwhelming
    // majority of tokens in practice and the difference is visible in indexing
    // throughput; `query_time_and_index_time_normalization_agree` pins the two
    // paths to the same answer.
    if run.is_ascii() {
        if run.is_empty() {
            return None;
        }
        let truncated = &run[..run.len().min(MAX_TOKEN_LEN)];
        return Some(truncated.to_ascii_lowercase());
    }

    let mut out = String::with_capacity(run.len());
    let mut chars = 0usize;
    for c in run.nfd().filter(|c| !is_combining_mark(*c)) {
        for lowered in c.to_lowercase() {
            if chars >= MAX_TOKEN_LEN {
                return Some(out);
            }
            out.push(lowered);
            chars += 1;
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

/// Split text into positioned tokens.
pub fn tokenize(text: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut run = String::new();
    let mut position = 0u32;
    // Scratch for encoding one ideograph, so splitting CJK text does not
    // allocate a throwaway `String` per character.
    let mut char_buf = [0u8; 4];

    let flush = |run: &mut String, position: &mut u32, tokens: &mut Vec<Token>| {
        if run.is_empty() {
            return;
        }
        if let Some(text) = normalize(run) {
            tokens.push(Token { text, position: *position });
            *position += 1;
        }
        run.clear();
    };

    for c in text.chars() {
        if is_standalone_ideograph(c) {
            flush(&mut run, &mut position, &mut tokens);
            if let Some(text) = normalize(c.encode_utf8(&mut char_buf)) {
                tokens.push(Token { text, position });
                position += 1;
            }
        } else if c.is_alphanumeric() {
            run.push(c);
        } else {
            flush(&mut run, &mut position, &mut tokens);
        }
    }
    flush(&mut run, &mut position, &mut tokens);

    tokens
}

/// Tokenize into bare term strings, for callers that do not need positions.
pub fn terms(text: &str) -> Vec<String> {
    tokenize(text).into_iter().map(|t| t.text).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(s: &str) -> Vec<String> {
        terms(s)
    }

    #[test]
    fn splits_on_punctuation_and_whitespace() {
        assert_eq!(tok("wireless-mouse (USB)"), vec!["wireless", "mouse", "usb"]);
        assert_eq!(tok("a,b;c\td\ne"), vec!["a", "b", "c", "d", "e"]);
        assert_eq!(tok("   spaced   out   "), vec!["spaced", "out"]);
    }

    #[test]
    fn lowercases_and_folds_accents() {
        assert_eq!(tok("Café CRÈME"), vec!["cafe", "creme"]);
        assert_eq!(tok("naïve"), tok("naive"));
        assert_eq!(tok("Übergrößen"), vec!["ubergroßen"]);
    }

    #[test]
    fn keeps_digits_and_alphanumeric_runs_together() {
        assert_eq!(tok("USB3 model X1"), vec!["usb3", "model", "x1"]);
        assert_eq!(tok("3.5mm"), vec!["3", "5mm"]);
    }

    #[test]
    fn positions_are_sequential() {
        let tokens = tokenize("the quick brown fox");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.iter().map(|t| t.position).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
        assert_eq!(tokens[2].text, "brown");
    }

    #[test]
    fn punctuation_does_not_consume_a_position() {
        // "a -- b" is two tokens at positions 0 and 1, not 0 and 2: a phrase
        // query for "a b" should match it.
        let tokens = tokenize("a -- b");
        assert_eq!(tokens.iter().map(|t| t.position).collect::<Vec<_>>(), vec![0, 1]);
    }

    #[test]
    fn empty_and_separator_only_input_yields_nothing() {
        assert!(tok("").is_empty());
        assert!(tok("   ").is_empty());
        assert!(tok("!@#$%^&*()").is_empty());
    }

    #[test]
    fn ideographs_are_split_per_character() {
        assert_eq!(tok("无线鼠标"), vec!["无", "线", "鼠", "标"]);
        // Mixed scripts keep both rules.
        assert_eq!(tok("USB无线"), vec!["usb", "无", "线"]);
    }

    #[test]
    fn long_tokens_are_truncated() {
        let long = "a".repeat(MAX_TOKEN_LEN + 50);
        let tokens = tok(&long);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].chars().count(), MAX_TOKEN_LEN);
    }

    #[test]
    fn query_time_and_index_time_normalization_agree() {
        // The property the whole thing rests on.
        for text in ["Café", "WIRELESS", "Ünïcôde", "Mixed123"] {
            assert_eq!(normalize(text).as_deref(), tok(text).first().map(String::as_str));
        }
    }

    #[test]
    fn the_ascii_fast_path_matches_the_general_one() {
        // `normalize` short-circuits for ASCII. Anything it returns there must
        // be what the decompose-and-fold path would have produced, including
        // at the truncation boundary and for input that normalizes to nothing.
        let long = "A".repeat(MAX_TOKEN_LEN + 10);
        for text in ["Wireless", "USB3", "x", "MiXeD-cAsE", "0123456789", &long] {
            let ascii = normalize(text).expect("non-empty ASCII always normalizes");
            let general: String = text
                .nfd()
                .filter(|c| !is_combining_mark(*c))
                .flat_map(char::to_lowercase)
                .take(MAX_TOKEN_LEN)
                .collect();
            assert_eq!(ascii, general, "fast and general paths disagree on {text:?}");
            assert!(ascii.chars().count() <= MAX_TOKEN_LEN);
        }
        assert_eq!(normalize(""), None);
    }
}
