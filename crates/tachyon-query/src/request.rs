//! Search request parsing and the response shape (PRD §7.3, §13).

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use utoipa::{IntoParams, ToSchema};

use tachyon_core::{CollectionSchema, Error, FieldId, Result};

use crate::filter::{self, FilterExpr};
use crate::sort::{self, SortClause};

/// Default page size when the caller does not say.
pub const DEFAULT_LIMIT: usize = 10;

/// Largest page we will build in one response.
pub const MAX_LIMIT: usize = 250;

/// Deepest page we will serve. Beyond this the top-K scan stops being cheap
/// and the caller wants a different access pattern.
pub const MAX_WINDOW: usize = 10_000;

/// How many query tokens must match for a document to qualify.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MatchMode {
    /// Every token must be present. The default: precision over recall.
    #[default]
    All,
    /// Any token qualifies the document; more matches simply score higher.
    Any,
}

impl MatchMode {
    fn parse(s: &str) -> Result<MatchMode> {
        match s.trim().to_ascii_lowercase().as_str() {
            "all" => Ok(MatchMode::All),
            "any" => Ok(MatchMode::Any),
            other => Err(Error::query(format!("match_mode must be `all` or `any`, got `{other}`"))),
        }
    }
}

/// Query-string parameters, exactly as they arrive over HTTP.
///
/// `parameter_in = Query` is not optional decoration: `IntoParams` defaults to
/// `Path`, and OpenAPI requires path parameters to be `required`, so without it
/// every field here is documented as mandatory no matter how `Option` it is.
#[derive(Debug, Clone, Default, Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct SearchParams {
    pub q: Option<String>,
    /// Comma-separated field names to search.
    pub query_by: Option<String>,
    pub filter: Option<String>,
    pub sort: Option<String>,
    pub facet: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// Prefix-match the final token, for search-as-you-type. Defaults to true.
    pub prefix: Option<bool>,
    /// Allow typo correction. Defaults to the collection's setting.
    pub typo_tolerance: Option<bool>,
    pub match_mode: Option<String>,
}

/// A request resolved against a schema: field names are field ids, defaults
/// are filled in, and everything has been range-checked.
#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub q: String,
    /// Fields to search, with the boost each contributes.
    pub query_by: Vec<(FieldId, f32)>,
    pub limit: usize,
    pub offset: usize,
    pub prefix: bool,
    pub typo_tolerance: bool,
    pub match_mode: MatchMode,
    /// Filter expression as written, kept for logging and analytics.
    pub filter: Option<String>,
    /// The same filter, parsed and type-checked against the schema.
    pub filter_expr: Option<FilterExpr>,
    pub sort: Option<String>,
    /// The same sort, parsed and validated. `None` means rank by relevance.
    pub sort_clauses: Option<Vec<SortClause>>,
    pub facet_by: Vec<FieldId>,
}

impl SearchRequest {
    pub fn resolve(params: SearchParams, schema: &CollectionSchema) -> Result<SearchRequest> {
        let q = params.q.unwrap_or_default();

        let query_by = match params.query_by.as_deref() {
            Some(list) => {
                let mut fields = Vec::new();
                for name in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    let (id, field) = schema.field(name).ok_or_else(|| {
                        Error::query(format!("query_by references unknown field `{name}`"))
                    })?;
                    if !field.is_searchable() {
                        return Err(Error::query(format!(
                            "field `{name}` is not searchable: only indexed `text` fields can be queried"
                        )));
                    }
                    fields.push((id, field.effective_boost()));
                }
                if fields.is_empty() {
                    return Err(Error::query("query_by must name at least one field"));
                }
                fields
            }
            // Default to every searchable field, which is what someone trying
            // the API for the first time expects.
            None => schema
                .searchable_field_ids()
                .into_iter()
                .map(|id| {
                    let boost = schema.field_by_id(id).expect("id came from the schema");
                    (id, boost.effective_boost())
                })
                .collect(),
        };

        let limit = params.limit.unwrap_or(DEFAULT_LIMIT);
        if limit > MAX_LIMIT {
            return Err(Error::query(format!("limit must be at most {MAX_LIMIT}, got {limit}")));
        }
        let offset = params.offset.unwrap_or(0);
        // Checked, not plain, addition: both operands come straight off the
        // query string, and `?offset=18446744073709551615` must be a 400 rather
        // than an overflow panic in a debug build or a wrapped window in a
        // release one.
        let window = offset.checked_add(limit).filter(|window| *window <= MAX_WINDOW);
        let Some(window) = window else {
            return Err(Error::query(format!(
                "offset + limit must be at most {MAX_WINDOW}, got {offset} + {limit}"
            )));
        };
        debug_assert!(window <= MAX_WINDOW);

        let facet_by = match params.facet.as_deref() {
            Some(list) => {
                let mut fields = Vec::new();
                for name in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    let (id, field) = schema.field(name).ok_or_else(|| {
                        Error::query(format!("facet references unknown field `{name}`"))
                    })?;
                    if !field.facet {
                        return Err(Error::query(format!(
                            "field `{name}` was not declared with `facet: true`"
                        )));
                    }
                    fields.push(id);
                }
                fields
            }
            None => Vec::new(),
        };

        let match_mode = match params.match_mode.as_deref() {
            Some(s) => MatchMode::parse(s)?,
            None => MatchMode::default(),
        };

        // Parse filter and sort up front so a malformed expression is a 400
        // rather than a query that quietly matches nothing.
        let filter = params.filter.filter(|s| !s.trim().is_empty());
        let filter_expr = match &filter {
            Some(expr) => Some(filter::parse(expr, schema)?),
            None => None,
        };

        let sort = params.sort.filter(|s| !s.trim().is_empty());
        let sort_clauses = match &sort {
            Some(expr) => Some(sort::parse(expr, schema)?),
            None => None,
        };

        Ok(SearchRequest {
            q,
            query_by,
            limit,
            offset,
            prefix: params.prefix.unwrap_or(true),
            typo_tolerance: params.typo_tolerance.unwrap_or(schema.typo_tolerance.enabled),
            match_mode,
            filter,
            filter_expr,
            sort,
            sort_clauses,
            facet_by,
        })
    }

    /// Documents to collect before paginating.
    ///
    /// [`SearchRequest::resolve`] has already bounded this by [`MAX_WINDOW`];
    /// the saturating add covers a request assembled by hand, where wrapping
    /// would silently produce a tiny window instead of an obvious error.
    pub fn window(&self) -> usize {
        self.offset.saturating_add(self.limit)
    }
}

/// One result (PRD §13).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Hit {
    #[schema(value_type = Object)]
    pub document: Json,
    pub text_match: f32,
}

/// The search response (PRD §13).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SearchResponse {
    pub found: usize,
    /// `false` iff pruning skipped at least one block of at least one
    /// term's postings while answering this query — for EITHER match mode;
    /// the exactness guarantee is tied to whether a skip occurred, not to
    /// `match_mode`. Conservative: may occasionally read `false` when a
    /// skipped region held no additional matches, never `true` when a skip
    /// occurred. Always serialized (no `skip_serializing_if`) — unlike
    /// `facets`, this is meaningful on every response.
    pub found_is_exact: bool,
    pub search_time_ms: u64,
    pub hits: Vec<Hit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub facets: Option<serde_json::Map<String, Json>>,
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
                FieldSchema::new("description", FieldType::Text),
            ],
        )
    }

    fn params(query_by: Option<&str>) -> SearchParams {
        SearchParams {
            q: Some("wireless mouse".into()),
            query_by: query_by.map(str::to_owned),
            ..Default::default()
        }
    }

    #[test]
    fn defaults_to_every_searchable_field_with_prd_boosts() {
        let req = SearchRequest::resolve(params(None), &schema()).unwrap();
        assert_eq!(req.query_by, vec![(0, 10.0), (3, 2.0)]);
        assert_eq!(req.limit, DEFAULT_LIMIT);
        assert_eq!(req.offset, 0);
        assert!(req.prefix, "search-as-you-type is the default");
        assert_eq!(req.match_mode, MatchMode::All);
    }

    #[test]
    fn resolves_named_fields_in_order() {
        let req = SearchRequest::resolve(params(Some("description, title")), &schema()).unwrap();
        assert_eq!(req.query_by, vec![(3, 2.0), (0, 10.0)]);
    }

    #[test]
    fn rejects_unsearchable_and_unknown_fields() {
        assert!(SearchRequest::resolve(params(Some("brand")), &schema()).is_err());
        assert!(SearchRequest::resolve(params(Some("price")), &schema()).is_err());
        assert!(SearchRequest::resolve(params(Some("nope")), &schema()).is_err());
        assert!(SearchRequest::resolve(params(Some(" , ")), &schema()).is_err());
    }

    #[test]
    fn enforces_pagination_bounds() {
        let over_limit = SearchParams { limit: Some(MAX_LIMIT + 1), ..params(None) };
        assert!(SearchRequest::resolve(over_limit, &schema()).is_err());

        let too_deep = SearchParams { offset: Some(MAX_WINDOW), limit: Some(10), ..params(None) };
        assert!(SearchRequest::resolve(too_deep, &schema()).is_err());

        let ok = SearchParams { offset: Some(20), limit: Some(5), ..params(None) };
        let req = SearchRequest::resolve(ok, &schema()).unwrap();
        assert_eq!(req.window(), 25);
    }

    #[test]
    fn a_pagination_window_that_would_overflow_is_rejected() {
        // Both values come straight off the query string, so their sum must be
        // a 400 rather than an arithmetic overflow.
        let overflowing =
            SearchParams { offset: Some(usize::MAX), limit: Some(10), ..params(None) };
        assert!(SearchRequest::resolve(overflowing, &schema()).is_err());

        let huge = SearchParams { offset: Some(usize::MAX), limit: Some(0), ..params(None) };
        assert!(SearchRequest::resolve(huge, &schema()).is_err());
    }

    #[test]
    fn facet_fields_must_be_declared_facetable() {
        let good = SearchParams { facet: Some("brand".into()), ..params(None) };
        assert_eq!(SearchRequest::resolve(good, &schema()).unwrap().facet_by, vec![1]);

        let bad = SearchParams { facet: Some("title".into()), ..params(None) };
        assert!(SearchRequest::resolve(bad, &schema()).is_err());
    }

    #[test]
    fn match_mode_parsing() {
        let any = SearchParams { match_mode: Some("ANY".into()), ..params(None) };
        assert_eq!(SearchRequest::resolve(any, &schema()).unwrap().match_mode, MatchMode::Any);

        let bad = SearchParams { match_mode: Some("some".into()), ..params(None) };
        assert!(SearchRequest::resolve(bad, &schema()).is_err());
    }

    #[test]
    fn blank_filter_and_sort_are_treated_as_absent() {
        let p = SearchParams { filter: Some("   ".into()), sort: Some("".into()), ..params(None) };
        let req = SearchRequest::resolve(p, &schema()).unwrap();
        assert!(req.filter.is_none() && req.filter_expr.is_none());
        assert!(req.sort.is_none() && req.sort_clauses.is_none());
    }

    #[test]
    fn filter_and_sort_are_parsed_during_resolution() {
        let p = SearchParams {
            filter: Some("brand:=Logitech && price:<5000".into()),
            sort: Some("_text_match:desc,price:asc".into()),
            ..params(None)
        };
        let req = SearchRequest::resolve(p, &schema()).unwrap();
        assert!(req.filter_expr.is_some());
        assert_eq!(req.sort_clauses.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn a_malformed_filter_or_sort_fails_the_request() {
        let bad_filter = SearchParams { filter: Some("brand:".into()), ..params(None) };
        assert!(SearchRequest::resolve(bad_filter, &schema()).is_err());

        let bad_sort = SearchParams { sort: Some("price".into()), ..params(None) };
        assert!(SearchRequest::resolve(bad_sort, &schema()).is_err());

        let unsortable = SearchParams { sort: Some("title:asc".into()), ..params(None) };
        assert!(SearchRequest::resolve(unsortable, &schema()).is_err());
    }
}
