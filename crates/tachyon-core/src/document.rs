//! Documents, before and after schema validation.

use serde_json::Value as Json;

use crate::error::{Error, Result};
use crate::schema::{CollectionSchema, FieldId, ID_FIELD};
use crate::value::Value;

/// Dense internal document identifier, assigned per collection in insertion
/// order. Postings lists, bitmaps, and columns are all keyed by this.
pub type DocId = u32;

/// Upper bound on a user-supplied document id. Keeps ids cheap to store and
/// stops a pathological key from bloating the id map.
pub const MAX_ID_LEN: usize = 256;

/// A document that has been checked against a schema and is ready to index.
#[derive(Debug, Clone)]
pub struct ParsedDocument {
    /// The user-facing `id`, unique within the collection.
    pub id: String,
    /// One entry per schema field, positionally aligned with
    /// `CollectionSchema::fields`. Absent fields are `Value::Null`.
    pub values: Vec<Value>,
    /// The document exactly as submitted, returned verbatim in search hits.
    pub source: Json,
}

impl ParsedDocument {
    pub fn value(&self, field: FieldId) -> &Value {
        self.values.get(field as usize).unwrap_or(&Value::Null)
    }

    /// Validate and coerce a raw JSON document.
    ///
    /// Fields not declared in the schema are kept in `source` and returned in
    /// search hits, but are not indexed, filterable, or sortable. This keeps
    /// ingest forgiving for documents carrying extra payload.
    pub fn parse(raw: Json, schema: &CollectionSchema) -> Result<ParsedDocument> {
        let Json::Object(obj) = &raw else {
            return Err(Error::validation("a document must be a JSON object"));
        };

        let id = match obj.get(ID_FIELD) {
            Some(Json::String(s)) => s.clone(),
            // Numeric ids are common in relational exports; accept and stringify.
            Some(Json::Number(n)) => n.to_string(),
            Some(other) => {
                return Err(Error::validation(format!("`id` must be a string, got {other}")))
            }
            None => return Err(Error::validation("document is missing required field `id`")),
        };
        if id.is_empty() {
            return Err(Error::validation("`id` must not be empty"));
        }
        if id.len() > MAX_ID_LEN {
            return Err(Error::validation(format!(
                "`id` must be at most {MAX_ID_LEN} bytes, got {}",
                id.len()
            )));
        }

        let mut values = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            let value = match obj.get(&field.name) {
                None | Some(Json::Null) => {
                    if !field.optional {
                        return Err(Error::validation(format!(
                            "document `{id}` is missing required field `{}`",
                            field.name
                        )));
                    }
                    Value::Null
                }
                Some(raw_value) => {
                    let coerced = Value::coerce(raw_value, field.field_type, &field.name)?;
                    if !field.optional && coerced.is_null() {
                        return Err(Error::validation(format!(
                            "document `{id}`: required field `{}` must not be null",
                            field.name
                        )));
                    }
                    coerced
                }
            };
            values.push(value);
        }

        Ok(ParsedDocument { id, values, source: raw })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldSchema, FieldType};
    use serde_json::json;

    fn schema() -> CollectionSchema {
        CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text).required(),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("price", FieldType::Int).with_filter(true).with_sort(true),
            ],
        )
    }

    #[test]
    fn parses_a_well_formed_document() {
        let doc = ParsedDocument::parse(
            json!({"id": "1", "title": "Wireless Mouse", "brand": "Logitech", "price": 2999}),
            &schema(),
        )
        .unwrap();
        assert_eq!(doc.id, "1");
        assert_eq!(doc.value(0), &Value::Str("Wireless Mouse".into()));
        assert_eq!(doc.value(2), &Value::Int(2999));
    }

    #[test]
    fn missing_optional_fields_become_null() {
        let doc = ParsedDocument::parse(json!({"id": "1", "title": "Mouse"}), &schema()).unwrap();
        assert!(doc.value(1).is_null());
        assert!(doc.value(2).is_null());
    }

    #[test]
    fn undeclared_fields_are_kept_in_source() {
        let doc =
            ParsedDocument::parse(json!({"id": "1", "title": "Mouse", "sku": "X-1"}), &schema())
                .unwrap();
        assert_eq!(doc.source["sku"], json!("X-1"));
        assert_eq!(doc.values.len(), 3);
    }

    #[test]
    fn numeric_ids_are_stringified() {
        let doc = ParsedDocument::parse(json!({"id": 42, "title": "Mouse"}), &schema()).unwrap();
        assert_eq!(doc.id, "42");
    }

    #[test]
    fn rejects_bad_documents() {
        let s = schema();
        for bad in [
            json!({"title": "Mouse"}),                      // no id
            json!({"id": "", "title": "Mouse"}),            // empty id
            json!({"id": ["x"], "title": "Mouse"}),         // non-scalar id
            json!({"id": "1"}),                             // missing required title
            json!({"id": "1", "title": null}),              // required field null
            json!({"id": "1", "title": "M", "price": "5"}), // wrong type
            json!(["not", "an", "object"]),
        ] {
            assert!(ParsedDocument::parse(bad.clone(), &s).is_err(), "should reject {bad}");
        }
    }
}
