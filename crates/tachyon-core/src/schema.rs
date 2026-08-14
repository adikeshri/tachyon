//! Collection schemas.
//!
//! A schema is fixed at creation time (PRD §7.1: "immutable field types after
//! creation") and is persisted next to the collection's data, so every other
//! subsystem can assume field ids are stable for the life of the collection.
//! A field's id is simply its index in [`CollectionSchema::fields`].

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::datetime::now_millis;
use crate::error::{Error, Result};

/// Fields are addressed by their position in the schema. `u16` caps a
/// collection at 65,536 fields, which is far beyond anything sane.
pub type FieldId = u16;

/// The document identifier field. Always present, always a string, never
/// declared explicitly in the schema.
pub const ID_FIELD: &str = "id";

/// Pseudo-field naming relevance score in `sort` expressions (PRD §7.8).
pub const TEXT_MATCH_FIELD: &str = "_text_match";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    /// Tokenized and full-text searchable.
    Text,
    /// Stored verbatim; matched exactly. For facets, filters, and IDs.
    Keyword,
    Int,
    Float,
    Bool,
    /// Epoch milliseconds; also accepts RFC 3339 strings on ingest.
    Date,
}

impl FieldType {
    /// Whether values are tokenized into the inverted index.
    pub fn is_full_text(self) -> bool {
        matches!(self, FieldType::Text)
    }

    /// Whether values live in a numeric column (`i64`/`f64` backed).
    pub fn is_numeric(self) -> bool {
        matches!(self, FieldType::Int | FieldType::Float | FieldType::Date | FieldType::Bool)
    }

    pub fn supports_facet(self) -> bool {
        // Floats are excluded: faceting on a continuous value produces a bucket
        // per document. Range facets are a post-v1 feature.
        matches!(self, FieldType::Keyword | FieldType::Bool | FieldType::Int | FieldType::Date)
    }

    pub fn supports_sort(self) -> bool {
        self.is_numeric()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            FieldType::Text => "text",
            FieldType::Keyword => "keyword",
            FieldType::Int => "int",
            FieldType::Float => "float",
            FieldType::Bool => "bool",
            FieldType::Date => "date",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FieldSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldType,

    /// Build a facet column for this field (PRD §7.7).
    #[serde(default)]
    pub facet: bool,

    /// Build a filter column for this field (PRD §7.6). Faceted fields are
    /// implicitly filterable — the facet column already supports it, and the
    /// PRD's own example filters on a field marked only `facet: true`.
    #[serde(default)]
    pub filter: bool,

    /// Build a sort column for this field (PRD §7.8).
    #[serde(default)]
    pub sort: bool,

    /// Include in the inverted index. Only meaningful for `text` fields; set
    /// `false` for large blobs you want returned but never searched.
    #[serde(default = "default_true")]
    pub index: bool,

    /// Allow documents that omit this field.
    #[serde(default = "default_true")]
    pub optional: bool,

    /// Per-field relevance multiplier. Defaults follow PRD §12 by field name
    /// (`title` 10, `brand` 6, `description` 2) and 1.0 for anything else;
    /// set explicitly to override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
}

fn default_true() -> bool {
    true
}

impl FieldSchema {
    /// Convenience constructor for tests and examples.
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        FieldSchema {
            name: name.into(),
            field_type,
            facet: false,
            filter: false,
            sort: false,
            index: true,
            optional: true,
            boost: None,
        }
    }

    pub fn with_facet(mut self, yes: bool) -> Self {
        self.facet = yes;
        self
    }

    pub fn with_filter(mut self, yes: bool) -> Self {
        self.filter = yes;
        self
    }

    pub fn with_sort(mut self, yes: bool) -> Self {
        self.sort = yes;
        self
    }

    pub fn with_boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn required(mut self) -> Self {
        self.optional = false;
        self
    }

    /// Effective boost, applying the PRD's name-based defaults.
    pub fn effective_boost(&self) -> f32 {
        self.boost.unwrap_or_else(|| default_boost_for(&self.name))
    }

    /// Whether this field is searchable by `query_by`.
    pub fn is_searchable(&self) -> bool {
        self.index && self.field_type.is_full_text()
    }

    /// Whether filter expressions may reference this field. A facet column is
    /// enough to answer a filter, so `facet` implies filterable.
    pub fn is_filterable(&self) -> bool {
        self.filter || self.facet || self.sort
    }

    /// Whether a columnar store is needed for this field at all.
    pub fn needs_column(&self) -> bool {
        self.facet || self.filter || self.sort
    }
}

/// PRD §12 default field boosts, applied when a field declares none.
pub fn default_boost_for(field_name: &str) -> f32 {
    match field_name {
        "title" => 10.0,
        "brand" => 6.0,
        "description" => 2.0,
        _ => 1.0,
    }
}

/// Typo tolerance settings (PRD §7.4). Defaults reproduce the PRD's table:
/// 1–3 characters allow no typos, 4–7 allow one, 8+ allow two.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TypoConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_one_typo_min_len")]
    pub one_typo_min_len: usize,
    #[serde(default = "default_two_typo_min_len")]
    pub two_typo_min_len: usize,
    /// Hard ceiling regardless of token length. Capped at 2: the automaton
    /// cost grows quickly and recall past 2 edits is mostly noise.
    #[serde(default = "default_max_typos")]
    pub max_typos: u8,
}

fn default_one_typo_min_len() -> usize {
    4
}

fn default_two_typo_min_len() -> usize {
    8
}

fn default_max_typos() -> u8 {
    2
}

impl Default for TypoConfig {
    fn default() -> Self {
        TypoConfig {
            enabled: true,
            one_typo_min_len: default_one_typo_min_len(),
            two_typo_min_len: default_two_typo_min_len(),
            max_typos: default_max_typos(),
        }
    }
}

impl TypoConfig {
    /// Edits permitted for a token of `len` characters, honouring [`Self::enabled`].
    pub fn allowed_typos(&self, len: usize) -> u8 {
        if !self.enabled {
            return 0;
        }
        self.typos_for_length(len)
    }

    /// Edits the length table permits, ignoring [`Self::enabled`].
    ///
    /// The query layer needs this because `enabled` is only the *default* for
    /// a request's `typo_tolerance` flag. Consulting it again after the
    /// request has been resolved would double-apply it, and a collection that
    /// defaults typos off could never be searched with them on.
    pub fn typos_for_length(&self, len: usize) -> u8 {
        let allowed = if len >= self.two_typo_min_len {
            2
        } else if len >= self.one_typo_min_len {
            1
        } else {
            0
        };
        allowed.min(self.max_typos).min(2)
    }

    fn validate(&self) -> Result<()> {
        if self.one_typo_min_len == 0 || self.two_typo_min_len == 0 {
            return Err(Error::schema("typo_tolerance minimum lengths must be positive"));
        }
        if self.two_typo_min_len < self.one_typo_min_len {
            return Err(Error::schema(
                "typo_tolerance.two_typo_min_len must be >= one_typo_min_len",
            ));
        }
        if self.max_typos > 2 {
            return Err(Error::schema("typo_tolerance.max_typos must be 0, 1, or 2"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CollectionSchema {
    pub name: String,
    pub fields: Vec<FieldSchema>,
    #[serde(default)]
    pub typo_tolerance: TypoConfig,
    /// Tie-breaker applied when no `sort` is given and scores are equal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_sorting_field: Option<String>,
    #[serde(default = "now_millis")]
    pub created_at: i64,
}

impl CollectionSchema {
    pub fn new(name: impl Into<String>, fields: Vec<FieldSchema>) -> Self {
        CollectionSchema {
            name: name.into(),
            fields,
            typo_tolerance: TypoConfig::default(),
            default_sorting_field: None,
            created_at: now_millis(),
        }
    }

    pub fn field(&self, name: &str) -> Option<(FieldId, &FieldSchema)> {
        self.fields.iter().position(|f| f.name == name).map(|i| (i as FieldId, &self.fields[i]))
    }

    pub fn field_by_id(&self, id: FieldId) -> Option<&FieldSchema> {
        self.fields.get(id as usize)
    }

    /// Field ids of every full-text searchable field, used as the default
    /// `query_by` when a search request omits it.
    pub fn searchable_field_ids(&self) -> Vec<FieldId> {
        self.fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.is_searchable())
            .map(|(i, _)| i as FieldId)
            .collect()
    }

    /// Reject schemas we cannot honour, with a message naming the offending
    /// field. Called before a collection is created; never on the hot path.
    pub fn validate(&self) -> Result<()> {
        validate_ident(&self.name, "collection name")?;
        self.typo_tolerance.validate()?;

        if self.fields.is_empty() {
            return Err(Error::schema("a collection needs at least one field"));
        }

        for (i, field) in self.fields.iter().enumerate() {
            validate_ident(&field.name, "field name")?;

            if field.name == ID_FIELD {
                return Err(Error::schema(
                    "`id` is reserved: every document has one implicitly and it is always a string",
                ));
            }
            if field.name.starts_with('_') {
                return Err(Error::schema(format!(
                    "field `{}`: names starting with `_` are reserved",
                    field.name
                )));
            }
            if self.fields[..i].iter().any(|other| other.name == field.name) {
                return Err(Error::schema(format!("duplicate field `{}`", field.name)));
            }
            if field.facet && !field.field_type.supports_facet() {
                return Err(Error::schema(format!(
                    "field `{}`: `{}` fields cannot be faceted",
                    field.name,
                    field.field_type.as_str()
                )));
            }
            if field.sort && !field.field_type.supports_sort() {
                return Err(Error::schema(format!(
                    "field `{}`: `{}` fields cannot be sorted on",
                    field.name,
                    field.field_type.as_str()
                )));
            }
            if let Some(boost) = field.boost {
                if !boost.is_finite() || boost < 0.0 {
                    return Err(Error::schema(format!(
                        "field `{}`: boost must be a non-negative finite number",
                        field.name
                    )));
                }
            }
        }

        if self.searchable_field_ids().is_empty() {
            return Err(Error::schema(
                "a collection needs at least one indexed `text` field to be searchable",
            ));
        }

        if let Some(sort_field) = &self.default_sorting_field {
            match self.field(sort_field) {
                None => {
                    return Err(Error::schema(format!(
                        "default_sorting_field `{sort_field}` is not a declared field"
                    )))
                }
                Some((_, f)) if !f.sort => {
                    return Err(Error::schema(format!(
                        "default_sorting_field `{sort_field}` must be declared with `sort: true`"
                    )))
                }
                Some(_) => {}
            }
        }

        Ok(())
    }
}

/// Names must be safe to use as path components and unambiguous in filter
/// expressions, which rules out most punctuation.
fn validate_ident(name: &str, what: &str) -> Result<()> {
    if name.is_empty() || name.len() > 64 {
        return Err(Error::schema(format!("{what} must be 1-64 characters, got {:?}", name)));
    }
    let mut chars = name.chars();
    let first = chars.next().expect("non-empty checked above");
    if !first.is_ascii_alphabetic() {
        return Err(Error::schema(format!("{what} `{name}` must start with a letter")));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return Err(Error::schema(format!(
            "{what} `{name}` may only contain letters, digits, `_` and `-`"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn products() -> CollectionSchema {
        CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text).required(),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("price", FieldType::Int).with_filter(true).with_sort(true),
                FieldSchema::new("description", FieldType::Text),
            ],
        )
    }

    #[test]
    fn prd_example_schema_is_valid() {
        products().validate().unwrap();
    }

    #[test]
    fn field_ids_are_positional() {
        let s = products();
        assert_eq!(s.field("title").unwrap().0, 0);
        assert_eq!(s.field("description").unwrap().0, 3);
        assert_eq!(s.searchable_field_ids(), vec![0, 3]);
    }

    #[test]
    fn prd_default_boosts_apply() {
        let s = products();
        assert_eq!(s.field("title").unwrap().1.effective_boost(), 10.0);
        assert_eq!(s.field("brand").unwrap().1.effective_boost(), 6.0);
        assert_eq!(s.field("description").unwrap().1.effective_boost(), 2.0);
        assert_eq!(s.field("price").unwrap().1.effective_boost(), 1.0);
        let overridden = FieldSchema::new("title", FieldType::Text).with_boost(3.0);
        assert_eq!(overridden.effective_boost(), 3.0);
    }

    #[test]
    fn faceted_fields_are_filterable() {
        let s = products();
        assert!(s.field("brand").unwrap().1.is_filterable());
    }

    #[test]
    fn rejects_bad_schemas() {
        let cases: Vec<(CollectionSchema, &str)> = vec![
            (CollectionSchema::new("products", vec![]), "no fields"),
            (
                CollectionSchema::new("products", vec![FieldSchema::new("id", FieldType::Keyword)]),
                "reserved id",
            ),
            (
                CollectionSchema::new("products", vec![FieldSchema::new("_x", FieldType::Text)]),
                "underscore prefix",
            ),
            (
                CollectionSchema::new(
                    "products",
                    vec![
                        FieldSchema::new("title", FieldType::Text),
                        FieldSchema::new("title", FieldType::Text),
                    ],
                ),
                "duplicate field",
            ),
            (
                CollectionSchema::new(
                    "products",
                    vec![FieldSchema::new("title", FieldType::Text).with_facet(true)],
                ),
                "text facet",
            ),
            (
                CollectionSchema::new(
                    "products",
                    vec![FieldSchema::new("brand", FieldType::Keyword)],
                ),
                "no searchable field",
            ),
            (
                CollectionSchema::new(
                    "1products",
                    vec![FieldSchema::new("title", FieldType::Text)],
                ),
                "bad collection name",
            ),
        ];
        for (schema, why) in cases {
            assert!(schema.validate().is_err(), "should reject: {why}");
        }
    }

    #[test]
    fn default_sorting_field_must_be_sortable() {
        let mut s = products();
        s.default_sorting_field = Some("price".into());
        s.validate().unwrap();

        s.default_sorting_field = Some("brand".into());
        assert!(s.validate().is_err());

        s.default_sorting_field = Some("nope".into());
        assert!(s.validate().is_err());
    }

    #[test]
    fn typo_table_matches_prd() {
        let c = TypoConfig::default();
        for len in 1..=3 {
            assert_eq!(c.allowed_typos(len), 0, "len {len}");
        }
        for len in 4..=7 {
            assert_eq!(c.allowed_typos(len), 1, "len {len}");
        }
        for len in [8, 12, 40] {
            assert_eq!(c.allowed_typos(len), 2, "len {len}");
        }

        let off = TypoConfig { enabled: false, ..Default::default() };
        assert_eq!(off.allowed_typos(20), 0);

        let capped = TypoConfig { max_typos: 1, ..Default::default() };
        assert_eq!(capped.allowed_typos(20), 1);
    }
}
