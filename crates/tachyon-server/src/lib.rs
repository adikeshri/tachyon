//! The Tachyon REST API.
//!
//! [`build_router`] returns the whole API as a `tower` service, which is what
//! the binary serves and what the integration tests drive in-process — no
//! sockets, no ports, no flakiness.

pub mod analytics;
pub mod auth;
pub mod error;
pub mod openapi;
pub mod routes;
pub mod state;

use axum::routing::{get, post};
use axum::Router;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::openapi::ApiDoc;

pub use auth::Auth;
pub use state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/health", get(routes::health::health))
        .route("/collections", post(routes::collections::create).get(routes::collections::list))
        .route(
            "/collections/{name}",
            get(routes::collections::get).delete(routes::collections::drop),
        )
        .route("/collections/{name}/documents", post(routes::documents::index))
        .route("/collections/{name}/search", get(routes::search::search))
        .route("/collections/{name}/suggest", get(routes::search::suggest))
        .route("/analytics/top", get(routes::analytics::top))
        .route("/analytics/zero-results", get(routes::analytics::zero_results))
        .route("/analytics/latency", get(routes::analytics::latency))
        .route("/metrics", get(routes::metrics::metrics))
        .route(
            "/collections/{name}/documents/{id}",
            get(routes::documents::get).delete(routes::documents::delete),
        )
        // Applied last so it wraps every route above it, including ones
        // added later — a new endpoint is guarded by default, not by memory.
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth::require_api_key))
        .with_state(state)
}
