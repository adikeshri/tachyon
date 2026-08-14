//! Sort expressions (PRD §7.8).
//!
//! ```text
//! sort=_text_match:desc,price:asc
//! ```
//!
//! Clauses are applied left to right; the first one that distinguishes two
//! documents decides. Relevance is addressable as the pseudo-field
//! `_text_match`, so "cheapest among the good matches" and "best match among
//! the cheap ones" are both expressible.
//!
//! Every sort ends with an implicit tie-break on doc id, so a result set is
//! stable across identical requests and pagination never skips or repeats a
//! document.

use std::cmp::Ordering;

use tachyon_core::{CollectionSchema, Error, FieldId, Result, Value, TEXT_MATCH_FIELD};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortKey {
    /// Relevance score.
    TextMatch,
    Field(FieldId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortClause {
    pub key: SortKey,
    pub descending: bool,
}

/// Parse a comma-separated sort expression.
pub fn parse(input: &str, schema: &CollectionSchema) -> Result<Vec<SortClause>> {
    let mut clauses = Vec::new();

    for part in input.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (name, direction) = match part.rsplit_once(':') {
            Some((name, direction)) => (name.trim(), direction.trim()),
            // Relevance is usually wanted descending and a value ascending,
            // but guessing per field would be a trap; require the direction.
            None => {
                return Err(Error::query(format!(
                    "sort clause `{part}` needs a direction, as in `{part}:asc`"
                )))
            }
        };

        let descending = match direction.to_ascii_lowercase().as_str() {
            "asc" => false,
            "desc" => true,
            other => {
                return Err(Error::query(format!(
                    "sort direction must be `asc` or `desc`, got `{other}`"
                )))
            }
        };

        let key = if name == TEXT_MATCH_FIELD {
            SortKey::TextMatch
        } else {
            let (id, field) = schema
                .field(name)
                .ok_or_else(|| Error::query(format!("sort references unknown field `{name}`")))?;
            if !field.sort {
                return Err(Error::query(format!(
                    "field `{name}` is not sortable: declare it with `sort: true`"
                )));
            }
            SortKey::Field(id)
        };

        if clauses.iter().any(|c: &SortClause| c.key == key) {
            return Err(Error::query(format!("sort names `{name}` more than once")));
        }
        clauses.push(SortClause { key, descending });
    }

    if clauses.is_empty() {
        return Err(Error::query("sort expression is empty"));
    }
    Ok(clauses)
}

/// One document's values for the sort clauses, in clause order.
#[derive(Debug, Clone, PartialEq)]
pub enum SortValue {
    Score(f32),
    Value(Value),
    /// The document has no value for this field. Always sorts last, in both
    /// directions — "missing" is not smaller than everything, it is unknown,
    /// and burying it is what people expect either way.
    Missing,
}

impl SortValue {
    fn cmp_asc(&self, other: &SortValue) -> Ordering {
        match (self, other) {
            (SortValue::Missing, SortValue::Missing) => Ordering::Equal,
            (SortValue::Missing, _) => Ordering::Greater,
            (_, SortValue::Missing) => Ordering::Less,
            (SortValue::Score(a), SortValue::Score(b)) => a.total_cmp(b),
            (SortValue::Value(a), SortValue::Value(b)) => a.cmp_ordered(b),
            // Mixed kinds cannot occur: a clause is either _text_match or a
            // field, for every document.
            _ => Ordering::Equal,
        }
    }
}

/// Reduce a field value to the scalar it sorts by. A multi-valued field sorts
/// by its smallest value, so ascending order means "the cheapest option this
/// document offers".
pub fn sort_value(value: Option<&Value>) -> SortValue {
    match value {
        None | Some(Value::Null) => SortValue::Missing,
        Some(Value::Array(items)) => {
            let min = items.iter().filter(|v| !v.is_null()).min_by(|a, b| a.cmp_ordered(b));
            match min {
                Some(v) => SortValue::Value(v.clone()),
                None => SortValue::Missing,
            }
        }
        Some(v) => SortValue::Value(v.clone()),
    }
}

/// Compare two documents under a sort specification.
///
/// Returns the order in which they should be *returned*: `Less` means first.
pub fn compare(
    clauses: &[SortClause],
    a: &[SortValue],
    b: &[SortValue],
    a_doc: u32,
    b_doc: u32,
) -> Ordering {
    for (i, clause) in clauses.iter().enumerate() {
        let (Some(left), Some(right)) = (a.get(i), b.get(i)) else {
            continue;
        };
        let order = left.cmp_asc(right);
        // A missing value stays last regardless of direction, so it must not
        // be flipped along with the real comparison.
        let order = if clause.descending
            && !matches!(left, SortValue::Missing)
            && !matches!(right, SortValue::Missing)
        {
            order.reverse()
        } else {
            order
        };
        if order != Ordering::Equal {
            return order;
        }
    }
    a_doc.cmp(&b_doc)
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
                FieldSchema::new("price", FieldType::Int).with_sort(true),
                FieldSchema::new("rating", FieldType::Float).with_sort(true),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
            ],
        )
    }

    #[test]
    fn parses_the_prd_example() {
        let clauses = parse("_text_match:desc,price:asc", &schema()).unwrap();
        assert_eq!(
            clauses,
            vec![
                SortClause { key: SortKey::TextMatch, descending: true },
                SortClause { key: SortKey::Field(1), descending: false },
            ]
        );
    }

    #[test]
    fn direction_is_case_insensitive_and_whitespace_tolerant() {
        let clauses = parse("  price : DESC , rating:Asc ", &schema()).unwrap();
        assert!(clauses[0].descending);
        assert!(!clauses[1].descending);
    }

    #[test]
    fn rejects_bad_sort_expressions() {
        for input in [
            "price",                // no direction
            "price:sideways",       // not a direction
            "nope:asc",             // unknown field
            "title:asc",            // text field, not sortable
            "brand:asc",            // not declared sortable
            "price:asc,price:desc", // duplicate key
            "",                     // empty
            "   ",
        ] {
            assert!(parse(input, &schema()).is_err(), "should reject {input:?}");
        }
    }

    fn score(v: f32) -> SortValue {
        SortValue::Score(v)
    }

    fn int(v: i64) -> SortValue {
        SortValue::Value(Value::Int(v))
    }

    #[test]
    fn ascending_and_descending_order_correctly() {
        let asc = vec![SortClause { key: SortKey::Field(1), descending: false }];
        assert_eq!(compare(&asc, &[int(1)], &[int(2)], 0, 1), Ordering::Less);

        let desc = vec![SortClause { key: SortKey::Field(1), descending: true }];
        assert_eq!(compare(&desc, &[int(1)], &[int(2)], 0, 1), Ordering::Greater);
    }

    #[test]
    fn later_clauses_break_earlier_ties() {
        let clauses = vec![
            SortClause { key: SortKey::TextMatch, descending: true },
            SortClause { key: SortKey::Field(1), descending: false },
        ];
        // Same score, cheaper price wins.
        assert_eq!(
            compare(&clauses, &[score(5.0), int(10)], &[score(5.0), int(20)], 0, 1),
            Ordering::Less
        );
        // Better score wins regardless of price.
        assert_eq!(
            compare(&clauses, &[score(9.0), int(99)], &[score(5.0), int(1)], 0, 1),
            Ordering::Less
        );
    }

    #[test]
    fn doc_id_is_the_final_tie_break() {
        let clauses = vec![SortClause { key: SortKey::Field(1), descending: false }];
        assert_eq!(compare(&clauses, &[int(1)], &[int(1)], 3, 7), Ordering::Less);
        assert_eq!(compare(&clauses, &[int(1)], &[int(1)], 7, 3), Ordering::Greater);
    }

    #[test]
    fn missing_values_sort_last_in_both_directions() {
        let asc = vec![SortClause { key: SortKey::Field(1), descending: false }];
        let desc = vec![SortClause { key: SortKey::Field(1), descending: true }];

        assert_eq!(compare(&asc, &[SortValue::Missing], &[int(5)], 0, 1), Ordering::Greater);
        assert_eq!(compare(&desc, &[SortValue::Missing], &[int(5)], 0, 1), Ordering::Greater);
        assert_eq!(compare(&asc, &[int(5)], &[SortValue::Missing], 0, 1), Ordering::Less);
        assert_eq!(compare(&desc, &[int(5)], &[SortValue::Missing], 0, 1), Ordering::Less);
    }

    #[test]
    fn multi_valued_fields_sort_by_their_minimum() {
        let value = Value::Array(vec![Value::Int(50), Value::Int(10), Value::Int(30)]);
        assert_eq!(sort_value(Some(&value)), SortValue::Value(Value::Int(10)));
    }

    #[test]
    fn null_and_absent_are_both_missing() {
        assert_eq!(sort_value(None), SortValue::Missing);
        assert_eq!(sort_value(Some(&Value::Null)), SortValue::Missing);
        assert_eq!(sort_value(Some(&Value::Array(vec![Value::Null]))), SortValue::Missing);
    }
}
