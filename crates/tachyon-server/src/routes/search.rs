//! Search and autocomplete endpoints (PRD §7.3, §7.5).

use axum::extract::{Path, Query, State};
use axum::Json;

use tachyon_query::{SearchParams, SearchResponse, SuggestParams, SuggestResponse};

use crate::error::ApiResult;
use crate::state::AppState;

/// `GET /collections/{name}/search`
pub async fn search(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<SearchParams>,
) -> ApiResult<Json<SearchResponse>> {
    let collection = state.engine.collection(&name)?;
    // Timed here rather than inside the engine so the recorded latency is what
    // the caller experienced, and at microsecond resolution — the response
    // field is milliseconds, which would collapse most searches to zero.
    let query = params.q.clone().unwrap_or_default();
    let started = std::time::Instant::now();
    let response = collection.search(params)?;
    let elapsed = started.elapsed().as_micros() as u64;

    state.analytics.record_search(&name, &query, response.found, elapsed);
    Ok(Json(response))
}

/// `GET /collections/{name}/suggest`
pub async fn suggest(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<SuggestParams>,
) -> ApiResult<Json<SuggestResponse>> {
    let collection = state.engine.collection(&name)?;
    // Deliberately not recorded: a keystroke is not a search, and counting
    // every prefix would bury the queries people actually ran.
    Ok(Json(collection.suggest(params)?))
}
