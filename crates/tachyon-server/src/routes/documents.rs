//! Document endpoints (PRD §7.2).

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::Value as Json_;

use tachyon_engine::BatchReport;

use crate::error::ApiResult;
use crate::state::AppState;

/// The PRD specifies a JSON array; accepting a bare object too costs nothing
/// and removes a papercut for anyone indexing one document at a time.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum DocumentPayload {
    Many(Vec<Json_>),
    One(Json_),
}

impl DocumentPayload {
    fn into_vec(self) -> Vec<Json_> {
        match self {
            DocumentPayload::Many(docs) => docs,
            DocumentPayload::One(doc) => vec![doc],
        }
    }
}

/// `POST /collections/{name}/documents`
///
/// Always 200 when the request itself is well-formed: individual documents can
/// fail without failing their neighbours, so per-document status lives in the
/// body (PRD §7.2, "atomic per document").
pub async fn index(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(payload): Json<DocumentPayload>,
) -> ApiResult<Json<BatchReport>> {
    let collection = state.engine.collection(&name)?;
    let report = collection.upsert_batch(payload.into_vec())?;
    Ok(Json(report))
}

/// `GET /collections/{name}/documents/{id}`
pub async fn get(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> ApiResult<Json<Json_>> {
    let collection = state.engine.collection(&name)?;
    Ok(Json(collection.get(&id)?))
}

/// `DELETE /collections/{name}/documents/{id}`
pub async fn delete(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let collection = state.engine.collection(&name)?;
    if collection.delete(&id)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(tachyon_core::Error::DocumentNotFound { collection: name, id }.into())
    }
}
