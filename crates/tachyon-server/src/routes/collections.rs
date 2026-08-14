//! Collection endpoints (PRD §7.1).

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;

use tachyon_core::CollectionSchema;
use tachyon_engine::CollectionStats;

use crate::error::ApiResult;
use crate::state::AppState;

#[derive(Debug, Serialize, ToSchema)]
pub struct CollectionView {
    #[serde(flatten)]
    pub schema: CollectionSchema,
    pub num_documents: u64,
    pub num_segments: usize,
}

fn view(schema: &CollectionSchema, stats: &CollectionStats) -> CollectionView {
    CollectionView {
        schema: schema.clone(),
        num_documents: stats.num_documents,
        num_segments: stats.num_segments,
    }
}

/// `POST /collections`
pub async fn create(
    State(state): State<AppState>,
    Json(schema): Json<CollectionSchema>,
) -> ApiResult<(StatusCode, Json<CollectionView>)> {
    let collection = state.engine.create_collection(schema)?;
    let stats = collection.stats();
    Ok((StatusCode::CREATED, Json(view(collection.schema(), &stats))))
}

/// `GET /collections`
pub async fn list(State(state): State<AppState>) -> ApiResult<Json<Vec<CollectionView>>> {
    let mut out = Vec::new();
    for stats in state.engine.list_collections() {
        // The collection cannot disappear between listing and lookup in
        // practice, but a concurrent drop is possible; skip rather than fail.
        if let Ok(collection) = state.engine.collection(&stats.name) {
            out.push(view(collection.schema(), &stats));
        }
    }
    Ok(Json(out))
}

/// `GET /collections/{name}`
pub async fn get(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<CollectionView>> {
    let collection = state.engine.collection(&name)?;
    let stats = collection.stats();
    Ok(Json(view(collection.schema(), &stats)))
}

/// `DELETE /collections/{name}`
pub async fn drop(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<StatusCode> {
    state.engine.drop_collection(&name)?;
    Ok(StatusCode::NO_CONTENT)
}
