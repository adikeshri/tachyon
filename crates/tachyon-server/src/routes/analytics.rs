//! Analytics endpoints (PRD §7.9).

use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::analytics::{LatencySummary, QueryStat};
use crate::error::ApiResult;
use crate::state::AppState;

pub const DEFAULT_ANALYTICS_LIMIT: usize = 20;
pub const MAX_ANALYTICS_LIMIT: usize = 500;

/// `IntoParams` defaults to `parameter_in = Path`, which OpenAPI then requires
/// to be mandatory; these are optional query parameters.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AnalyticsParams {
    /// Restrict to one collection. Omitted means every collection.
    pub collection: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QueryList {
    pub queries: Vec<QueryStat>,
    /// Distinct queries currently tracked, across all collections.
    pub tracked_queries: usize,
    /// Queries discarded to stay within the tracking cap.
    pub dropped_queries: u64,
}

fn limit(params: &AnalyticsParams) -> usize {
    params.limit.unwrap_or(DEFAULT_ANALYTICS_LIMIT).clamp(1, MAX_ANALYTICS_LIMIT)
}

/// `GET /analytics/top`
pub async fn top(
    State(state): State<AppState>,
    Query(params): Query<AnalyticsParams>,
) -> ApiResult<Json<QueryList>> {
    let queries = state.analytics.top_queries(params.collection.as_deref(), limit(&params));
    Ok(Json(QueryList {
        queries,
        tracked_queries: state.analytics.tracked_queries(),
        dropped_queries: state.analytics.dropped_queries(),
    }))
}

/// `GET /analytics/zero-results`
pub async fn zero_results(
    State(state): State<AppState>,
    Query(params): Query<AnalyticsParams>,
) -> ApiResult<Json<QueryList>> {
    let queries = state.analytics.zero_result_queries(params.collection.as_deref(), limit(&params));
    Ok(Json(QueryList {
        queries,
        tracked_queries: state.analytics.tracked_queries(),
        dropped_queries: state.analytics.dropped_queries(),
    }))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct LatencyReport {
    #[serde(flatten)]
    pub latency: LatencySummary,
    pub total_searches: u64,
    pub uptime_seconds: u64,
    /// Mean searches per second since start.
    pub queries_per_second: f64,
}

/// `GET /analytics/latency`
pub async fn latency(State(state): State<AppState>) -> ApiResult<Json<LatencyReport>> {
    let uptime = state.started_at.elapsed().as_secs_f64();
    let total = state.analytics.total_searches();
    Ok(Json(LatencyReport {
        latency: state.analytics.latency(),
        total_searches: total,
        uptime_seconds: uptime as u64,
        queries_per_second: if uptime > 0.0 { total as f64 / uptime } else { 0.0 },
    }))
}
