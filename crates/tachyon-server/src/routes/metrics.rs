//! Prometheus metrics (PRD §15).
//!
//! Rendered on demand from state that is already being maintained — the
//! analytics histogram and each collection's stats — so scraping costs a read
//! lock and some string formatting, and nothing has to be updated on the hot
//! path to keep this endpoint honest.

use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use std::fmt::Write;

use crate::state::AppState;

/// The exposition format Prometheus expects, and the content type it needs to
/// see to parse it.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// `GET /metrics`
pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let mut out = String::with_capacity(2048);
    let uptime = state.started_at.elapsed().as_secs_f64();

    gauge(&mut out, "tachyon_uptime_seconds", "Seconds since the process started", uptime);

    let latency = state.analytics.latency();
    counter(
        &mut out,
        "tachyon_search_requests_total",
        "Searches served since start",
        state.analytics.total_searches() as f64,
    );
    gauge(
        &mut out,
        "tachyon_search_queries_per_second",
        "Mean searches per second since start",
        if uptime > 0.0 { state.analytics.total_searches() as f64 / uptime } else { 0.0 },
    );

    // Percentiles as a summary, which is how Prometheus expects pre-computed
    // quantiles to arrive.
    writeln!(out, "# HELP tachyon_search_latency_seconds Search latency").ok();
    writeln!(out, "# TYPE tachyon_search_latency_seconds summary").ok();
    for (quantile, value) in
        [("0.5", latency.p50_ms), ("0.95", latency.p95_ms), ("0.99", latency.p99_ms)]
    {
        writeln!(
            out,
            "tachyon_search_latency_seconds{{quantile=\"{quantile}\"}} {}",
            value / 1000.0
        )
        .ok();
    }
    writeln!(out, "tachyon_search_latency_seconds_count {}", latency.count).ok();
    writeln!(out).ok();

    gauge(
        &mut out,
        "tachyon_analytics_tracked_queries",
        "Distinct query strings currently tracked",
        state.analytics.tracked_queries() as f64,
    );

    // Per-collection gauges.
    let collections = state.engine.list_collections();
    gauge(
        &mut out,
        "tachyon_collections",
        "Collections open on this node",
        collections.len() as f64,
    );

    for (name, help, values) in [
        (
            "tachyon_collection_documents",
            "Live documents in a collection",
            collections
                .iter()
                .map(|s| (s.name.clone(), s.num_documents as f64))
                .collect::<Vec<_>>(),
        ),
        (
            "tachyon_collection_segments",
            "Committed segments in a collection",
            collections.iter().map(|s| (s.name.clone(), s.num_segments as f64)).collect(),
        ),
        (
            "tachyon_collection_memtable_bytes",
            "Approximate heap held by a collection's memtable",
            collections.iter().map(|s| (s.name.clone(), s.memtable_bytes as f64)).collect(),
        ),
        (
            "tachyon_collection_wal_bytes",
            "Size of a collection's write-ahead log",
            collections.iter().map(|s| (s.name.clone(), s.wal_bytes as f64)).collect(),
        ),
    ] {
        writeln!(out, "# HELP {name} {help}").ok();
        writeln!(out, "# TYPE {name} gauge").ok();
        for (collection, value) in values {
            writeln!(out, "{name}{{collection=\"{}\"}} {value}", escape(&collection)).ok();
        }
        writeln!(out).ok();
    }

    ([(CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)], out)
}

fn gauge(out: &mut String, name: &str, help: &str, value: f64) {
    writeln!(out, "# HELP {name} {help}").ok();
    writeln!(out, "# TYPE {name} gauge").ok();
    writeln!(out, "{name} {value}").ok();
    writeln!(out).ok();
}

fn counter(out: &mut String, name: &str, help: &str, value: f64) {
    writeln!(out, "# HELP {name} {help}").ok();
    writeln!(out, "# TYPE {name} counter").ok();
    writeln!(out, "{name} {value}").ok();
    writeln!(out).ok();
}

/// Escape a label value per the exposition format. Collection names are
/// already restricted to letters, digits, `_` and `-`, so this is belt and
/// braces against a future rule change.
fn escape(value: &str) -> String {
    value.replace('\\', r"\\").replace('"', "\\\"").replace('\n', r"\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_values_are_escaped() {
        assert_eq!(escape("plain"), "plain");
        assert_eq!(escape(r#"a"b"#), r#"a\"b"#);
        assert_eq!(escape("a\\b"), r"a\\b");
        assert_eq!(escape("a\nb"), r"a\nb");
    }
}
