//! OpenAPI spec and Swagger UI.

#![allow(dead_code)]

use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};
use utoipa::{Modify, OpenApi};

use crate::analytics::{LatencySummary, QueryStat};
use crate::auth::API_KEY_HEADER;
use crate::error::{ErrorBody, ErrorResponse};
use crate::routes::analytics::{AnalyticsParams, LatencyReport, QueryList};
use crate::routes::collections::CollectionView;
use crate::routes::health::Health;
use tachyon_core::CollectionSchema;
use tachyon_engine::BatchReport;
use tachyon_query::{SearchParams, SearchResponse, SuggestParams, SuggestResponse};

/// Attach the API-key security scheme to the generated spec.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "api_key",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new(API_KEY_HEADER))),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    info(
        title = "Tachyon API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Typo-tolerant full-text search over HTTP. See the repository for the full API guide."
    ),
    tags(
        (name = "health", description = "Liveness and readiness"),
        (name = "collections", description = "Collection schema management"),
        (name = "documents", description = "Document indexing and retrieval"),
        (name = "search", description = "Full-text search and autocomplete"),
        (name = "analytics", description = "Query analytics"),
        (name = "operations", description = "Operational endpoints"),
    ),
    paths(
        health,
        create_collection,
        list_collections,
        get_collection,
        drop_collection,
        index_documents,
        get_document,
        delete_document,
        search,
        suggest,
        analytics_top,
        analytics_zero_results,
        analytics_latency,
        metrics,
    ),
    components(schemas(
        Health,
        CollectionSchema,
        CollectionView,
        BatchReport,
        SearchParams,
        SearchResponse,
        SuggestParams,
        SuggestResponse,
        QueryList,
        QueryStat,
        LatencySummary,
        LatencyReport,
        ErrorResponse,
        ErrorBody,
    ))
)]
pub struct ApiDoc;

/// `GET /health`
#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "Process is healthy", body = Health),
    )
)]
fn health() {}

/// `POST /collections`
#[utoipa::path(
    post,
    path = "/collections",
    tag = "collections",
    request_body = CollectionSchema,
    responses(
        (status = 201, description = "Collection created", body = CollectionView),
        (status = 400, description = "Invalid schema", body = ErrorResponse),
        (status = 409, description = "Collection already exists", body = ErrorResponse),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
        (status = 403, description = "Search key attempted a write", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn create_collection() {}

/// `GET /collections`
#[utoipa::path(
    get,
    path = "/collections",
    tag = "collections",
    responses(
        (status = 200, description = "All collections", body = [CollectionView]),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn list_collections() {}

/// `GET /collections/{name}`
#[utoipa::path(
    get,
    path = "/collections/{name}",
    tag = "collections",
    params(("name" = String, Path, description = "Collection name")),
    responses(
        (status = 200, description = "Collection details", body = CollectionView),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn get_collection() {}

/// `DELETE /collections/{name}`
#[utoipa::path(
    delete,
    path = "/collections/{name}",
    tag = "collections",
    params(("name" = String, Path, description = "Collection name")),
    responses(
        (status = 204, description = "Collection dropped"),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
        (status = 403, description = "Search key attempted a write", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn drop_collection() {}

/// `POST /collections/{name}/documents`
#[utoipa::path(
    post,
    path = "/collections/{name}/documents",
    tag = "documents",
    params(("name" = String, Path, description = "Collection name")),
    request_body(
        description = "A JSON array of documents, or a single document object",
        content = Object,
    ),
    responses(
        (status = 200, description = "Per-document indexing results", body = BatchReport),
        (status = 400, description = "Invalid JSON or document", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
        (status = 403, description = "Search key attempted a write", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn index_documents() {}

/// `GET /collections/{name}/documents/{id}`
#[utoipa::path(
    get,
    path = "/collections/{name}/documents/{id}",
    tag = "documents",
    params(
        ("name" = String, Path, description = "Collection name"),
        ("id" = String, Path, description = "Document id"),
    ),
    responses(
        (status = 200, description = "Stored document", body = Object),
        (status = 404, description = "Document not found", body = ErrorResponse),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn get_document() {}

/// `DELETE /collections/{name}/documents/{id}`
#[utoipa::path(
    delete,
    path = "/collections/{name}/documents/{id}",
    tag = "documents",
    params(
        ("name" = String, Path, description = "Collection name"),
        ("id" = String, Path, description = "Document id"),
    ),
    responses(
        (status = 204, description = "Document deleted"),
        (status = 404, description = "Document not found", body = ErrorResponse),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
        (status = 403, description = "Search key attempted a write", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn delete_document() {}

/// `GET /collections/{name}/search`
#[utoipa::path(
    get,
    path = "/collections/{name}/search",
    tag = "search",
    params(
        ("name" = String, Path, description = "Collection name"),
        SearchParams,
    ),
    responses(
        (status = 200, description = "Search results", body = SearchResponse),
        (status = 400, description = "Invalid query", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn search() {}

/// `GET /collections/{name}/suggest`
#[utoipa::path(
    get,
    path = "/collections/{name}/suggest",
    tag = "search",
    params(
        ("name" = String, Path, description = "Collection name"),
        SuggestParams,
    ),
    responses(
        (status = 200, description = "Autocomplete suggestions", body = SuggestResponse),
        (status = 400, description = "Invalid query", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn suggest() {}

/// `GET /analytics/top`
#[utoipa::path(
    get,
    path = "/analytics/top",
    tag = "analytics",
    params(AnalyticsParams),
    responses(
        (status = 200, description = "Most frequent queries", body = QueryList),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn analytics_top() {}

/// `GET /analytics/zero-results`
#[utoipa::path(
    get,
    path = "/analytics/zero-results",
    tag = "analytics",
    params(AnalyticsParams),
    responses(
        (status = 200, description = "Queries that often return nothing", body = QueryList),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn analytics_zero_results() {}

/// `GET /analytics/latency`
#[utoipa::path(
    get,
    path = "/analytics/latency",
    tag = "analytics",
    responses(
        (status = 200, description = "Search latency percentiles", body = LatencyReport),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn analytics_latency() {}

/// `GET /metrics`
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "operations",
    responses(
        (status = 200, description = "Prometheus exposition format", content_type = "text/plain"),
        (status = 401, description = "Missing or invalid API key", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
fn metrics() {}
