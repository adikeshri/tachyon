//! Filter expressions (PRD §7.6).
//!
//! ```text
//! brand:=Logitech && price:<5000
//! (brand:=Logitech || brand:=Razer) && price:[1000..5000]
//! brand:=[Logitech,Razer] && in_stock:=true
//! ```
//!
//! # Grammar
//!
//! ```text
//! expr      := or
//! or        := and ( "||" and )*
//! and       := primary ( "&&" primary )*
//! primary   := "(" expr ")" | predicate
//! predicate := ident ":" op? value
//! op        := "=" | "!=" | ">=" | "<=" | ">" | "<"
//! value     := "[" scalar ".." scalar "]"     range, inclusive
//!            | "[" scalar ("," scalar)* "]"   set membership
//!            | scalar
//! scalar    := '"' … '"' | "'" … "'" | bare
//! ```
//!
//! `&&` binds tighter than `||`, as in C. A bare `:` means `:=`.
//!
//! Values are coerced to the field's declared type while parsing, so a filter
//! against a mistyped value fails at request time with a clear message rather
//! than silently matching nothing.
//!
//! # Absent values
//!
//! `brand:!=Razer` returns documents that *have* a brand and it is not Razer.
//! A document with no brand at all is not "not Razer", it is unknown, and
//! returning it from a negation surprises people more often than it helps.

use roaring::RoaringBitmap;

use tachyon_core::{CollectionSchema, Error, FieldId, FieldType, Result, Value};
use tachyon_index::{IndexSource, NumKey};

/// A parsed filter tree.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpr {
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Pred(Predicate),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    pub field: FieldId,
    /// Kept for error messages after parsing.
    pub field_name: String,
    pub op: PredOp,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PredOp {
    Eq(FilterValue),
    Ne(FilterValue),
    Lt(NumKey),
    Le(NumKey),
    Gt(NumKey),
    Ge(NumKey),
    /// Inclusive on both ends.
    Range(NumKey, NumKey),
    In(Vec<FilterValue>),
    NotIn(Vec<FilterValue>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Num(NumKey),
    Text(String),
}

/// Parse a filter expression against a schema.
pub fn parse(input: &str, schema: &CollectionSchema) -> Result<FilterExpr> {
    let mut parser = Parser { input, pos: 0, schema };
    let expr = parser.parse_or()?;
    parser.skip_ws();
    if parser.pos < parser.input.len() {
        return Err(Error::query(format!(
            "unexpected `{}` at position {} in filter",
            parser.input[parser.pos..].chars().next().unwrap_or(' '),
            parser.pos
        )));
    }
    Ok(expr)
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
    schema: &'a CollectionSchema,
}

impl<'a> Parser<'a> {
    fn rest(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn skip_ws(&mut self) {
        let trimmed = self.rest().trim_start();
        self.pos = self.input.len() - trimmed.len();
    }

    fn eat(&mut self, token: &str) -> bool {
        self.skip_ws();
        if self.rest().starts_with(token) {
            self.pos += token.len();
            true
        } else {
            false
        }
    }

    fn parse_or(&mut self) -> Result<FilterExpr> {
        let mut terms = vec![self.parse_and()?];
        while self.eat("||") {
            terms.push(self.parse_and()?);
        }
        Ok(if terms.len() == 1 { terms.pop().expect("checked") } else { FilterExpr::Or(terms) })
    }

    fn parse_and(&mut self) -> Result<FilterExpr> {
        let mut terms = vec![self.parse_primary()?];
        while self.eat("&&") {
            terms.push(self.parse_primary()?);
        }
        Ok(if terms.len() == 1 { terms.pop().expect("checked") } else { FilterExpr::And(terms) })
    }

    fn parse_primary(&mut self) -> Result<FilterExpr> {
        if self.eat("(") {
            let inner = self.parse_or()?;
            if !self.eat(")") {
                return Err(Error::query("unbalanced `(` in filter"));
            }
            return Ok(inner);
        }
        self.parse_predicate().map(FilterExpr::Pred)
    }

    fn parse_predicate(&mut self) -> Result<Predicate> {
        self.skip_ws();
        let name = self.parse_ident()?;

        let (field_id, field) = self
            .schema
            .field(&name)
            .ok_or_else(|| Error::query(format!("filter references unknown field `{name}`")))?;
        if !field.is_filterable() {
            return Err(Error::query(format!(
                "field `{name}` is not filterable: declare it with `filter: true`, `facet: true`, or `sort: true`"
            )));
        }

        if !self.eat(":") {
            return Err(Error::query(format!("expected `:` after `{name}` in filter")));
        }

        // Longest operators first, so `>=` is not read as `>`.
        let op_token = ["!=", ">=", "<=", "=", ">", "<"]
            .into_iter()
            .find(|token| self.eat(token))
            .unwrap_or("=");

        let ty = field.field_type;
        let numeric_only = |op: &str| -> Result<()> {
            if ty.is_numeric() {
                Ok(())
            } else {
                Err(Error::query(format!(
                    "`{op}` needs a numeric field, but `{name}` is `{}`",
                    ty.as_str()
                )))
            }
        };

        self.skip_ws();
        let op = if self.rest().starts_with('[') {
            self.parse_bracketed(&name, ty, op_token)?
        } else {
            let value = self.parse_scalar(&name, ty)?;
            match op_token {
                "=" => PredOp::Eq(value),
                "!=" => PredOp::Ne(value),
                ">" => {
                    numeric_only(">")?;
                    PredOp::Gt(expect_num(value, &name)?)
                }
                ">=" => {
                    numeric_only(">=")?;
                    PredOp::Ge(expect_num(value, &name)?)
                }
                "<" => {
                    numeric_only("<")?;
                    PredOp::Lt(expect_num(value, &name)?)
                }
                "<=" => {
                    numeric_only("<=")?;
                    PredOp::Le(expect_num(value, &name)?)
                }
                other => return Err(Error::query(format!("unsupported operator `{other}`"))),
            }
        };

        Ok(Predicate { field: field_id, field_name: name, op })
    }

    /// `[a..b]` for a range, `[a,b,c]` for set membership.
    fn parse_bracketed(&mut self, name: &str, ty: FieldType, op_token: &str) -> Result<PredOp> {
        if !self.eat("[") {
            return Err(Error::query("expected `[` in filter"));
        }

        let first = self.parse_scalar(name, ty)?;

        if self.eat("..") {
            let second = self.parse_scalar(name, ty)?;
            if !self.eat("]") {
                return Err(Error::query(format!("unbalanced `[` in filter for `{name}`")));
            }
            if !ty.is_numeric() {
                return Err(Error::query(format!(
                    "ranges need a numeric field, but `{name}` is `{}`",
                    ty.as_str()
                )));
            }
            let (lo, hi) = (expect_num(first, name)?, expect_num(second, name)?);
            if lo.cmp_key(&hi).is_gt() {
                return Err(Error::query(format!(
                    "range for `{name}` is inverted: its lower bound exceeds its upper bound"
                )));
            }
            return Ok(PredOp::Range(lo, hi));
        }

        let mut values = vec![first];
        while self.eat(",") {
            values.push(self.parse_scalar(name, ty)?);
        }
        if !self.eat("]") {
            return Err(Error::query(format!("unbalanced `[` in filter for `{name}`")));
        }

        match op_token {
            "=" => Ok(PredOp::In(values)),
            "!=" => Ok(PredOp::NotIn(values)),
            other => Err(Error::query(format!(
                "`{other}` cannot be combined with a `[…]` list for `{name}`"
            ))),
        }
    }

    fn parse_ident(&mut self) -> Result<String> {
        self.skip_ws();
        let rest = self.rest();
        let end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
            .unwrap_or(rest.len());
        if end == 0 {
            return Err(Error::query(format!(
                "expected a field name at position {} in filter",
                self.pos
            )));
        }
        self.pos += end;
        Ok(rest[..end].to_string())
    }

    /// Read one value and coerce it to the field's type.
    fn parse_scalar(&mut self, name: &str, ty: FieldType) -> Result<FilterValue> {
        self.skip_ws();
        let rest = self.rest();
        let mut chars = rest.chars();

        let raw = match chars.next() {
            Some(quote @ ('"' | '\'')) => {
                let body = &rest[quote.len_utf8()..];
                let end = body.find(quote).ok_or_else(|| {
                    Error::query(format!("unterminated quoted value for `{name}` in filter"))
                })?;
                self.pos += quote.len_utf8() * 2 + end;
                body[..end].to_string()
            }
            _ => {
                let end = bare_value_end(rest);
                if end == 0 {
                    return Err(Error::query(format!("expected a value for `{name}` in filter")));
                }
                self.pos += end;
                rest[..end].trim_end().to_string()
            }
        };

        coerce(&raw, ty, name)
    }
}

/// Where a bare (unquoted) value ends: at whitespace, a boolean operator, or
/// any of the list/range punctuation. `.` only terminates as part of `..`, so
/// `1.5` stays one value.
fn bare_value_end(s: &str) -> usize {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\n' | b'\r' | b')' | b']' | b',' => break,
            b'&' | b'|' if bytes.get(i + 1) == Some(&c) => break,
            b'.' if bytes.get(i + 1) == Some(&b'.') => break,
            _ => i += 1,
        }
    }
    i
}

fn coerce(raw: &str, ty: FieldType, name: &str) -> Result<FilterValue> {
    let bad = |expected: &str| {
        Err(Error::query(format!("`{raw}` is not {expected}, which is what `{name}` holds")))
    };

    match ty {
        FieldType::Text | FieldType::Keyword => Ok(FilterValue::Text(raw.to_string())),
        FieldType::Int => match raw.parse::<i64>() {
            Ok(i) => Ok(FilterValue::Num(NumKey::Int(i))),
            Err(_) => bad("an integer"),
        },
        FieldType::Float => match raw.parse::<f64>() {
            Ok(f) if f.is_finite() => Ok(FilterValue::Num(NumKey::Float(f))),
            _ => bad("a finite number"),
        },
        FieldType::Bool => match raw {
            "true" => Ok(FilterValue::Num(NumKey::Int(1))),
            "false" => Ok(FilterValue::Num(NumKey::Int(0))),
            _ => bad("`true` or `false`"),
        },
        FieldType::Date => {
            if let Ok(millis) = raw.parse::<i64>() {
                return Ok(FilterValue::Num(NumKey::Int(millis)));
            }
            match tachyon_core::datetime::parse_rfc3339(raw) {
                Some(millis) => Ok(FilterValue::Num(NumKey::Int(millis))),
                None => bad("an RFC 3339 timestamp or epoch milliseconds"),
            }
        }
    }
}

fn expect_num(value: FilterValue, name: &str) -> Result<NumKey> {
    match value {
        FilterValue::Num(key) => Ok(key),
        FilterValue::Text(_) => {
            Err(Error::query(format!("`{name}` needs a numeric value for this comparison")))
        }
    }
}

/// Evaluate a filter into the set of matching doc ids.
///
/// Each source contributes its own matches and they are unioned, because a doc
/// id belongs to exactly one source.
pub fn evaluate(expr: &FilterExpr, sources: &[&dyn IndexSource]) -> RoaringBitmap {
    match expr {
        FilterExpr::And(terms) => {
            let mut iter = terms.iter();
            let Some(first) = iter.next() else {
                return RoaringBitmap::new();
            };
            let mut acc = evaluate(first, sources);
            for term in iter {
                if acc.is_empty() {
                    // Nothing left to intersect with; skip the remaining work.
                    break;
                }
                acc &= evaluate(term, sources);
            }
            acc
        }
        FilterExpr::Or(terms) => {
            let mut acc = RoaringBitmap::new();
            for term in terms {
                acc |= evaluate(term, sources);
            }
            acc
        }
        FilterExpr::Pred(predicate) => {
            let mut acc = RoaringBitmap::new();
            for source in sources {
                acc |= eval_predicate(predicate, *source);
            }
            acc
        }
    }
}

fn eval_predicate(predicate: &Predicate, source: &dyn IndexSource) -> RoaringBitmap {
    let columns = source.columns();
    let field = predicate.field;

    // A field can have a numeric column or a keyword one, never both.
    if let Some(numeric) = columns.numeric(field) {
        return match &predicate.op {
            PredOp::Eq(FilterValue::Num(k)) => numeric.range(Some(*k), Some(*k)),
            PredOp::Ne(FilterValue::Num(k)) => numeric.not_equal(*k),
            PredOp::Lt(k) => numeric.less_than(*k),
            PredOp::Le(k) => numeric.range(None, Some(*k)),
            PredOp::Gt(k) => numeric.greater_than(*k),
            PredOp::Ge(k) => numeric.range(Some(*k), None),
            PredOp::Range(lo, hi) => numeric.range(Some(*lo), Some(*hi)),
            PredOp::In(values) => {
                let mut acc = RoaringBitmap::new();
                for value in values {
                    if let FilterValue::Num(k) = value {
                        acc |= numeric.range(Some(*k), Some(*k));
                    }
                }
                acc
            }
            PredOp::NotIn(values) => {
                let mut excluded = RoaringBitmap::new();
                for value in values {
                    if let FilterValue::Num(k) = value {
                        excluded |= numeric.range(Some(*k), Some(*k));
                    }
                }
                numeric.present() - excluded
            }
            // Type mismatches are rejected during parsing.
            PredOp::Eq(_) | PredOp::Ne(_) => RoaringBitmap::new(),
        };
    }

    if let Some(keyword) = columns.keyword(field) {
        return match &predicate.op {
            PredOp::Eq(FilterValue::Text(v)) => keyword.equals(v),
            PredOp::Ne(FilterValue::Text(v)) => keyword.not_equal(v),
            PredOp::In(values) => {
                let mut acc = RoaringBitmap::new();
                for value in values {
                    if let FilterValue::Text(v) = value {
                        acc |= keyword.equals(v);
                    }
                }
                acc
            }
            PredOp::NotIn(values) => {
                let mut excluded = RoaringBitmap::new();
                for value in values {
                    if let FilterValue::Text(v) = value {
                        excluded |= keyword.equals(v);
                    }
                }
                keyword.present() - excluded
            }
            _ => RoaringBitmap::new(),
        };
    }

    RoaringBitmap::new()
}

/// The `Value`-level test a predicate applies, exposed for callers that hold a
/// document rather than a column.
pub fn matches_value(op: &PredOp, value: &Value) -> bool {
    let nums = || value.iter_scalars().filter_map(NumKey::from_value);
    let texts = || value.iter_scalars().filter_map(Value::as_str);

    match op {
        PredOp::Eq(FilterValue::Num(k)) => nums().any(|n| n.cmp_key(k).is_eq()),
        PredOp::Eq(FilterValue::Text(t)) => texts().any(|s| s == t),
        PredOp::Ne(FilterValue::Num(k)) => {
            nums().next().is_some() && nums().all(|n| !n.cmp_key(k).is_eq())
        }
        PredOp::Ne(FilterValue::Text(t)) => texts().next().is_some() && texts().all(|s| s != t),
        PredOp::Lt(k) => nums().any(|n| n.cmp_key(k).is_lt()),
        PredOp::Le(k) => nums().any(|n| n.cmp_key(k).is_le()),
        PredOp::Gt(k) => nums().any(|n| n.cmp_key(k).is_gt()),
        PredOp::Ge(k) => nums().any(|n| n.cmp_key(k).is_ge()),
        PredOp::Range(lo, hi) => nums().any(|n| n.cmp_key(lo).is_ge() && n.cmp_key(hi).is_le()),
        PredOp::In(values) => values.iter().any(|v| matches_value(&PredOp::Eq(v.clone()), value)),
        PredOp::NotIn(values) => {
            (nums().next().is_some() || texts().next().is_some())
                && !values.iter().any(|v| matches_value(&PredOp::Eq(v.clone()), value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_core::{FieldSchema, FieldType};

    fn schema() -> CollectionSchema {
        CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("price", FieldType::Int).with_filter(true).with_sort(true),
                FieldSchema::new("rating", FieldType::Float).with_filter(true),
                FieldSchema::new("in_stock", FieldType::Bool).with_filter(true),
                FieldSchema::new("released", FieldType::Date).with_filter(true),
                FieldSchema::new("notes", FieldType::Keyword),
            ],
        )
    }

    fn parse_ok(input: &str) -> FilterExpr {
        parse(input, &schema()).unwrap_or_else(|e| panic!("failed to parse {input:?}: {e}"))
    }

    fn pred(input: &str) -> Predicate {
        match parse_ok(input) {
            FilterExpr::Pred(p) => p,
            other => panic!("expected a predicate, got {other:?}"),
        }
    }

    #[test]
    fn parses_the_prd_example() {
        // PRD §7.6.
        let expr = parse_ok("brand:=Logitech && price:<5000");
        let FilterExpr::And(terms) = expr else { panic!("expected an AND") };
        assert_eq!(terms.len(), 2);
        assert_eq!(
            terms[0],
            FilterExpr::Pred(Predicate {
                field: 1,
                field_name: "brand".into(),
                op: PredOp::Eq(FilterValue::Text("Logitech".into())),
            })
        );
        assert_eq!(
            terms[1],
            FilterExpr::Pred(Predicate {
                field: 2,
                field_name: "price".into(),
                op: PredOp::Lt(NumKey::Int(5000)),
            })
        );
    }

    #[test]
    fn a_bare_colon_means_equality() {
        assert_eq!(pred("brand:Logitech").op, PredOp::Eq(FilterValue::Text("Logitech".into())));
    }

    #[test]
    fn parses_every_comparison_operator() {
        assert_eq!(pred("price:>100").op, PredOp::Gt(NumKey::Int(100)));
        assert_eq!(pred("price:>=100").op, PredOp::Ge(NumKey::Int(100)));
        assert_eq!(pred("price:<100").op, PredOp::Lt(NumKey::Int(100)));
        assert_eq!(pred("price:<=100").op, PredOp::Le(NumKey::Int(100)));
        assert_eq!(pred("price:!=100").op, PredOp::Ne(FilterValue::Num(NumKey::Int(100))));
    }

    #[test]
    fn parses_ranges_and_sets() {
        assert_eq!(pred("price:[100..500]").op, PredOp::Range(NumKey::Int(100), NumKey::Int(500)));
        assert_eq!(
            pred("brand:=[Logitech,Razer]").op,
            PredOp::In(vec![
                FilterValue::Text("Logitech".into()),
                FilterValue::Text("Razer".into())
            ])
        );
        assert!(matches!(pred("brand:!=[Logitech]").op, PredOp::NotIn(_)));
    }

    #[test]
    fn and_binds_tighter_than_or() {
        // a || b && c parses as a || (b && c)
        let expr = parse_ok("price:<10 || price:>100 && brand:=Razer");
        let FilterExpr::Or(terms) = expr else { panic!("expected an OR at the top") };
        assert_eq!(terms.len(), 2);
        assert!(matches!(terms[0], FilterExpr::Pred(_)));
        assert!(matches!(terms[1], FilterExpr::And(_)));
    }

    #[test]
    fn parentheses_override_precedence() {
        let expr = parse_ok("(price:<10 || price:>100) && brand:=Razer");
        let FilterExpr::And(terms) = expr else { panic!("expected an AND at the top") };
        assert!(matches!(terms[0], FilterExpr::Or(_)));
    }

    #[test]
    fn quoted_values_keep_their_spaces_and_punctuation() {
        assert_eq!(
            pred("brand:=\"Logitech G Pro\"").op,
            PredOp::Eq(FilterValue::Text("Logitech G Pro".into()))
        );
        assert_eq!(
            pred("brand:='Razer && Friends'").op,
            PredOp::Eq(FilterValue::Text("Razer && Friends".into()))
        );
    }

    #[test]
    fn coerces_values_to_the_field_type() {
        assert_eq!(pred("rating:>4.5").op, PredOp::Gt(NumKey::Float(4.5)));
        assert_eq!(pred("in_stock:=true").op, PredOp::Eq(FilterValue::Num(NumKey::Int(1))));
        assert_eq!(pred("in_stock:=false").op, PredOp::Eq(FilterValue::Num(NumKey::Int(0))));
        assert_eq!(pred("released:>=1970-01-01T00:00:01Z").op, PredOp::Ge(NumKey::Int(1000)));
        assert_eq!(pred("released:>=1000").op, PredOp::Ge(NumKey::Int(1000)));
        assert_eq!(pred("price:=-50").op, PredOp::Eq(FilterValue::Num(NumKey::Int(-50))));
    }

    #[test]
    fn floats_are_not_mistaken_for_ranges() {
        assert_eq!(
            pred("rating:[1.5..4.5]").op,
            PredOp::Range(NumKey::Float(1.5), NumKey::Float(4.5))
        );
    }

    #[test]
    fn whitespace_is_flexible() {
        let tight = parse_ok("brand:=Logitech&&price:<5000");
        let loose = parse_ok("  brand := Logitech   &&   price : < 5000  ");
        assert_eq!(tight, loose);
    }

    #[test]
    fn rejects_malformed_filters() {
        let cases = [
            ("nope:=1", "unknown field"),
            ("notes:=x", "field is not filterable"),
            ("title:>5", "comparison on a text field"),
            ("price:=abc", "non-numeric value for an int field"),
            ("in_stock:=maybe", "not a boolean"),
            ("released:=yesterday", "not a date"),
            ("brand:>Logitech", "ordering a keyword field"),
            ("brand:[a..b]", "range on a keyword field"),
            ("price:[500..100]", "inverted range"),
            ("(price:<5", "unbalanced parenthesis"),
            ("price:[1,2", "unbalanced bracket"),
            ("brand:=\"unterminated", "unterminated quote"),
            ("price:", "missing value"),
            (":=5", "missing field"),
            ("price:<5 &&", "dangling operator"),
            ("price:<5 price:>1", "two predicates with no operator"),
            ("", "empty filter"),
        ];
        for (input, why) in cases {
            assert!(parse(input, &schema()).is_err(), "should reject {input:?} ({why})");
        }
    }

    #[test]
    fn error_messages_name_the_field() {
        let err = parse("price:=abc", &schema()).unwrap_err().to_string();
        assert!(err.contains("price"), "unhelpful message: {err}");
        let err = parse("notes:=x", &schema()).unwrap_err().to_string();
        assert!(err.contains("notes") && err.contains("filter"), "unhelpful message: {err}");
    }

    #[test]
    fn value_level_matching_agrees_with_the_operators() {
        assert!(matches_value(&PredOp::Gt(NumKey::Int(10)), &Value::Int(11)));
        assert!(!matches_value(&PredOp::Gt(NumKey::Int(10)), &Value::Int(10)));
        assert!(matches_value(&PredOp::Range(NumKey::Int(1), NumKey::Int(5)), &Value::Int(5)));
        assert!(matches_value(
            &PredOp::Eq(FilterValue::Text("a".into())),
            &Value::Array(vec![Value::Str("a".into()), Value::Str("b".into())])
        ));
        // A missing value is never "not equal" — it is unknown.
        assert!(!matches_value(&PredOp::Ne(FilterValue::Text("a".into())), &Value::Null));
    }
}
