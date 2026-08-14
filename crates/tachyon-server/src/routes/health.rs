//! Liveness endpoint.

use axum::extract::State;
use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;

use crate::state::AppState;

#[derive(Debug, Serialize, ToSchema)]
pub struct Health {
    pub ok: bool,
    pub version: &'static str,
    pub uptime_seconds: u64,
    pub num_collections: usize,
}

/// `GET /health`
pub async fn health(State(state): State<AppState>) -> Json<Health> {
    Json(Health {
        ok: true,
        version: env!("CARGO_PKG_VERSION"),
        uptime_seconds: state.started_at.elapsed().as_secs(),
        num_collections: state.engine.list_collections().len(),
    })
}
