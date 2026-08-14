//! The value model documents are coerced into once validated against a schema.
//!
//! This is deliberately narrower than `serde_json::Value`: there are no maps
//! (nested documents are a post-v1 feature) and arrays cannot nest. Every value
//! that reaches the index has already been checked against a `FieldType`, so
//! downstream code can match on the variant it expects without re-validating.

use std::cmp::Ordering;
use std::fmt;

use crate::datetime::parse_rfc3339;
use crate::error::{Error, Result};
use crate::schema::FieldType;

/// A single, schema-validated field value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    /// Also carries `date` fields, as epoch milliseconds.
    Int(i64),
    Float(f64),
    Str(String),
    /// A multi-valued field. Never nested: elements are always scalars.
    Array(Vec<Value>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Numeric view of a scalar, for numeric indexes and range filters.
    /// `bool` counts as numeric (false = 0, true = 1) so it can be sorted.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Iterate a value as a sequence, so callers can treat single- and
    /// multi-valued fields uniformly.
    pub fn iter_scalars(&self) -> Box<dyn Iterator<Item = &Value> + '_> {
        match self {
            Value::Array(items) => Box::new(items.iter()),
            Value::Null => Box::new(std::iter::empty()),
            other => Box::new(std::iter::once(other)),
        }
    }

    /// Total order used for sorting. Numbers compare numerically across
    /// `Int`/`Float`/`Bool`; strings compare bytewise; `Null` sorts last.
    /// NaN is treated as equal to NaN so the ordering stays total.
    pub fn cmp_ordered(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            (Value::Str(a), Value::Str(b)) => a.cmp(b),
            _ => match (self.as_f64(), other.as_f64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                _ => Ordering::Equal,
            },
        }
    }

    /// Convert back to JSON, e.g. for facet keys and debug output.
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::Value as J;
        match self {
            Value::Null => J::Null,
            Value::Bool(b) => J::Bool(*b),
            Value::Int(i) => J::from(*i),
            Value::Float(f) => serde_json::Number::from_f64(*f).map(J::Number).unwrap_or(J::Null),
            Value::Str(s) => J::String(s.clone()),
            Value::Array(items) => J::Array(items.iter().map(Value::to_json).collect()),
        }
    }

    /// Coerce a raw JSON value into this field's declared type.
    ///
    /// Coercion is intentionally strict — a string in an `int` field is an
    /// error, not a silent parse — with two exceptions that are unambiguous and
    /// routinely useful: integers widen into `float` fields, and `date` fields
    /// accept RFC 3339 strings alongside epoch milliseconds.
    pub fn coerce(raw: &serde_json::Value, ty: FieldType, field: &str) -> Result<Value> {
        use serde_json::Value as J;
        match raw {
            J::Null => Ok(Value::Null),
            J::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    if item.is_array() {
                        return Err(Error::validation(format!(
                            "field `{field}`: nested arrays are not supported in v1"
                        )));
                    }
                    out.push(Value::coerce(item, ty, field)?);
                }
                Ok(Value::Array(out))
            }
            _ => Self::coerce_scalar(raw, ty, field),
        }
    }

    fn coerce_scalar(raw: &serde_json::Value, ty: FieldType, field: &str) -> Result<Value> {
        use serde_json::Value as J;
        let mismatch = |want: &str| {
            Err(Error::validation(format!(
                "field `{field}`: expected {want}, got {}",
                json_type_name(raw)
            )))
        };

        match ty {
            FieldType::Text | FieldType::Keyword => match raw {
                J::String(s) => Ok(Value::Str(s.clone())),
                _ => mismatch("a string"),
            },
            FieldType::Int => match raw {
                J::Number(n) => n.as_i64().map(Value::Int).ok_or_else(|| {
                    Error::validation(format!("field `{field}`: {n} is not a 64-bit integer"))
                }),
                _ => mismatch("an integer"),
            },
            FieldType::Float => match raw {
                J::Number(n) => n.as_f64().map(Value::Float).ok_or_else(|| {
                    Error::validation(format!(
                        "field `{field}`: {n} is not representable as a float"
                    ))
                }),
                _ => mismatch("a number"),
            },
            FieldType::Bool => match raw {
                J::Bool(b) => Ok(Value::Bool(*b)),
                _ => mismatch("a boolean"),
            },
            FieldType::Date => match raw {
                J::Number(n) => n.as_i64().map(Value::Int).ok_or_else(|| {
                    Error::validation(format!("field `{field}`: {n} is not epoch milliseconds"))
                }),
                J::String(s) => parse_rfc3339(s).map(Value::Int).ok_or_else(|| {
                    Error::validation(format!(
                        "field `{field}`: `{s}` is not an RFC 3339 timestamp or epoch milliseconds"
                    ))
                }),
                _ => mismatch("an RFC 3339 timestamp or epoch milliseconds"),
            },
        }
    }
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(n) if n.is_f64() => "a float",
        serde_json::Value::Number(_) => "an integer",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(x) => write!(f, "{x}"),
            Value::Str(s) => f.write_str(s),
            Value::Array(items) => {
                f.write_str("[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{item}")?;
                }
                f.write_str("]")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn coerces_scalars() {
        assert_eq!(
            Value::coerce(&json!("hi"), FieldType::Text, "t").unwrap(),
            Value::Str("hi".into())
        );
        assert_eq!(Value::coerce(&json!(7), FieldType::Int, "n").unwrap(), Value::Int(7));
        assert_eq!(Value::coerce(&json!(7), FieldType::Float, "n").unwrap(), Value::Float(7.0));
        assert_eq!(Value::coerce(&json!(true), FieldType::Bool, "b").unwrap(), Value::Bool(true));
    }

    #[test]
    fn rejects_type_mismatches() {
        assert!(Value::coerce(&json!("7"), FieldType::Int, "n").is_err());
        assert!(Value::coerce(&json!(7), FieldType::Text, "t").is_err());
        assert!(Value::coerce(&json!(7.5), FieldType::Int, "n").is_err());
        assert!(Value::coerce(&json!({"a": 1}), FieldType::Text, "t").is_err());
    }

    #[test]
    fn dates_accept_both_forms() {
        assert_eq!(
            Value::coerce(&json!("1970-01-01T00:00:01Z"), FieldType::Date, "d").unwrap(),
            Value::Int(1000)
        );
        assert_eq!(Value::coerce(&json!(1000), FieldType::Date, "d").unwrap(), Value::Int(1000));
        assert!(Value::coerce(&json!("not a date"), FieldType::Date, "d").is_err());
    }

    #[test]
    fn arrays_are_multi_valued_but_flat() {
        let v = Value::coerce(&json!(["a", "b"]), FieldType::Keyword, "tags").unwrap();
        assert_eq!(v.iter_scalars().count(), 2);
        assert!(Value::coerce(&json!([["a"]]), FieldType::Keyword, "tags").is_err());
    }

    #[test]
    fn null_sorts_last_and_numbers_compare_across_types() {
        assert_eq!(Value::Int(2).cmp_ordered(&Value::Float(2.5)), Ordering::Less);
        assert_eq!(Value::Int(1).cmp_ordered(&Value::Null), Ordering::Less);
        assert_eq!(Value::Null.cmp_ordered(&Value::Int(1)), Ordering::Greater);
        assert_eq!(Value::Bool(false).cmp_ordered(&Value::Bool(true)), Ordering::Less);
    }
}
