//! End-to-end tests for facets, analytics, authentication, and metrics
//! (PRD §7.7, §7.9, §14, §15).

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

use tachyon_engine::{Engine, EngineConfig};
use tachyon_server::{build_router, AppState, Auth};

struct Harness {
    _dir: tempfile::TempDir,
    app: Router,
    /// Sent as `X-TACHYON-API-KEY` when set.
    key: Option<String>,
}

impl Harness {
    /// Build a harness and seed it. `seed_key` is used for the seeding writes,
    /// so a read-only harness still has data to read; `key` is what the test
    /// itself sends.
    async fn with_auth_seeded(auth: Auth, seed_key: Option<&str>, key: Option<&str>) -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(EngineConfig::new(dir.path())).unwrap();
        let mut h = Harness {
            _dir: dir,
            app: build_router(AppState::with_auth(Arc::new(engine), auth)),
            key: seed_key.map(str::to_owned),
        };
        h.seed().await;
        h.key = key.map(str::to_owned);
        h
    }

    async fn with_auth(auth: Auth, key: Option<&str>) -> Harness {
        Harness::with_auth_seeded(auth, key, key).await
    }

    async fn open() -> Harness {
        Harness::with_auth(Auth::open(), None).await
    }

    async fn seed(&self) {
        self.post(
            "/collections",
            json!({
                "name": "products",
                "fields": [
                    {"name": "title", "type": "text"},
                    {"name": "brand", "type": "keyword", "facet": true},
                    {"name": "year", "type": "int", "facet": true, "filter": true},
                    {"name": "price", "type": "int", "filter": true, "sort": true}
                ]
            }),
        )
        .await;

        self.post(
            "/collections/products/documents",
            json!([
                {"id": "1", "title": "Wireless Mouse", "brand": "Logitech", "year": 2024, "price": 2999},
                {"id": "2", "title": "Gaming Mouse", "brand": "Razer", "year": 2024, "price": 5999},
                {"id": "3", "title": "Mouse Pad", "brand": "Logitech", "year": 2023, "price": 999},
                {"id": "4", "title": "Silent Mouse", "brand": "Logitech", "year": 2024, "price": 1999},
                {"id": "5", "title": "Keyboard", "brand": "Razer", "year": 2023, "price": 8999}
            ]),
        )
        .await;
    }

    async fn send(&self, method: Method, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(key) = &self.key {
            builder = builder.header("x-tachyon-api-key", key);
        }
        let request = match body {
            Some(value) => builder
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&value).unwrap()))
                .unwrap(),
            None => builder.body(Body::empty()).unwrap(),
        };
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, value)
    }

    async fn post(&self, uri: &str, body: Value) -> (StatusCode, Value) {
        self.send(Method::POST, uri, Some(body)).await
    }

    async fn get(&self, uri: &str) -> (StatusCode, Value) {
        self.send(Method::GET, uri, None).await
    }

    async fn search(&self, query: &str) -> Value {
        let (status, body) = self.get(&format!("/collections/products/search?{query}")).await;
        assert_eq!(status, StatusCode::OK, "search failed: {body}");
        body
    }

    async fn text(&self, uri: &str) -> (StatusCode, String) {
        let mut builder = Request::builder().method(Method::GET).uri(uri);
        if let Some(key) = &self.key {
            builder = builder.header("x-tachyon-api-key", key);
        }
        let response =
            self.app.clone().oneshot(builder.body(Body::empty()).unwrap()).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }
}

// --- Facets (PRD §7.7) -----------------------------------------------------

#[tokio::test]
async fn facets_match_the_prd_response_shape() {
    let h = Harness::open().await;
    let body = h.search("q=mouse&prefix=false&facet=brand").await;
    assert_eq!(body["facets"]["brand"], json!({"Logitech": 3, "Razer": 1}));
}

#[tokio::test]
async fn facet_counts_are_accurate_after_filters() {
    let h = Harness::open().await;
    // Everything from 2024, faceted by brand.
    let body = h.search("q=&facet=brand&filter=year%3A%3D2024").await;
    assert_eq!(body["found"], json!(3));
    assert_eq!(body["facets"]["brand"], json!({"Logitech": 2, "Razer": 1}));
}

#[tokio::test]
async fn facet_counts_cover_every_match_not_just_the_page() {
    let h = Harness::open().await;
    let body = h.search("q=&facet=brand&limit=1").await;
    assert_eq!(body["hits"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["facets"]["brand"],
        json!({"Logitech": 3, "Razer": 2}),
        "a facet UI needs the totals, not the page"
    );
}

#[tokio::test]
async fn several_fields_can_be_faceted_at_once() {
    let h = Harness::open().await;
    let body = h.search("q=&facet=brand%2Cyear").await;
    assert_eq!(body["facets"]["brand"], json!({"Logitech": 3, "Razer": 2}));
    assert_eq!(body["facets"]["year"], json!({"2024": 3, "2023": 2}));
}

#[tokio::test]
async fn no_facet_parameter_means_no_facets_key() {
    let h = Harness::open().await;
    let body = h.search("q=mouse&prefix=false").await;
    assert!(body.get("facets").is_none() || body["facets"].is_null());
}

// --- Analytics (PRD §7.9) --------------------------------------------------

#[tokio::test]
async fn top_queries_are_recorded_and_ranked() {
    let h = Harness::open().await;
    for _ in 0..3 {
        h.search("q=mouse&prefix=false").await;
    }
    h.search("q=keyboard&prefix=false").await;

    let (status, body) = h.get("/analytics/top").await;
    assert_eq!(status, StatusCode::OK);
    let queries = body["queries"].as_array().unwrap();
    assert_eq!(queries[0]["query"], json!("mouse"));
    assert_eq!(queries[0]["count"], json!(3));
    assert_eq!(queries[0]["collection"], json!("products"));
    assert!(queries[0]["last_seen"].as_i64().unwrap() > 0);
    assert_eq!(queries[1]["query"], json!("keyboard"));
}

#[tokio::test]
async fn zero_result_queries_are_reported() {
    let h = Harness::open().await;
    h.search("q=helicopter&prefix=false").await;
    h.search("q=helicopter&prefix=false").await;
    h.search("q=mouse&prefix=false").await;

    let (status, body) = h.get("/analytics/zero-results").await;
    assert_eq!(status, StatusCode::OK);
    let queries = body["queries"].as_array().unwrap();
    assert_eq!(queries.len(), 1, "only the query that found nothing");
    assert_eq!(queries[0]["query"], json!("helicopter"));
    assert_eq!(queries[0]["zero_result_count"], json!(2));
}

#[tokio::test]
async fn latency_percentiles_are_reported() {
    let h = Harness::open().await;
    for _ in 0..20 {
        h.search("q=mouse&prefix=false").await;
    }

    let (status, body) = h.get("/analytics/latency").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], json!(20));
    assert_eq!(body["total_searches"], json!(20));
    for key in ["p50_ms", "p95_ms", "p99_ms", "mean_ms", "max_ms"] {
        assert!(body[key].is_number(), "missing {key} in {body}");
    }
    let (p50, p95, p99) = (
        body["p50_ms"].as_f64().unwrap(),
        body["p95_ms"].as_f64().unwrap(),
        body["p99_ms"].as_f64().unwrap(),
    );
    assert!(p50 <= p95 && p95 <= p99, "percentiles out of order: {p50} {p95} {p99}");
}

#[tokio::test]
async fn analytics_can_be_scoped_to_a_collection() {
    let h = Harness::open().await;
    h.search("q=mouse&prefix=false").await;

    assert_eq!(
        h.get("/analytics/top?collection=products").await.1["queries"].as_array().unwrap().len(),
        1
    );
    assert!(h.get("/analytics/top?collection=other").await.1["queries"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn analytics_start_empty() {
    let h = Harness::open().await;
    assert!(h.get("/analytics/top").await.1["queries"].as_array().unwrap().is_empty());
    assert_eq!(h.get("/analytics/latency").await.1["count"], json!(0));
}

// --- Authentication (PRD §14) ---------------------------------------------

#[tokio::test]
async fn without_keys_every_endpoint_is_open() {
    let h = Harness::open().await;
    assert_eq!(h.get("/health").await.0, StatusCode::OK);
    assert_eq!(h.get("/collections").await.0, StatusCode::OK);
    assert_eq!(h.search("q=mouse").await["found"], json!(4));
}

#[tokio::test]
async fn the_admin_key_can_read_and_write() {
    let auth = Auth::new(Some("admin".into()), Some("search".into()));
    let h = Harness::with_auth(auth, Some("admin")).await;

    assert_eq!(h.get("/collections").await.0, StatusCode::OK);
    let (status, _) =
        h.post("/collections/products/documents", json!({"id": "99", "title": "New Mouse"})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        h.send(Method::DELETE, "/collections/products/documents/99", None).await.0,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn the_search_key_can_read_but_not_write() {
    let auth = Auth::new(Some("admin".into()), Some("search".into()));
    let h = Harness::with_auth_seeded(auth, Some("admin"), Some("search")).await;

    assert_eq!(h.get("/collections").await.0, StatusCode::OK);
    assert_eq!(h.search("q=mouse&prefix=false").await["found"], json!(4));

    let (status, body) =
        h.post("/collections/products/documents", json!({"id": "99", "title": "Nope"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("forbidden"));

    let (status, _) = h.send(Method::DELETE, "/collections/products/documents/1", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_missing_or_wrong_key_is_rejected() {
    let auth = Auth::new(Some("admin".into()), Some("search".into()));

    let no_key = Harness::with_auth_seeded(auth.clone(), Some("admin"), None).await;
    let (status, body) = no_key.get("/collections").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"]["code"], json!("unauthorized"));

    let wrong = Harness::with_auth_seeded(auth, Some("admin"), Some("hunter2")).await;
    assert_eq!(wrong.get("/collections").await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn health_stays_reachable_without_a_key() {
    let auth = Auth::new(Some("admin".into()), None);
    let h = Harness::with_auth_seeded(auth, Some("admin"), None).await;
    // A load balancer holds no key but must still be able to probe.
    assert_eq!(h.get("/health").await.0, StatusCode::OK);
    assert_eq!(h.get("/metrics").await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn docs_are_reachable_without_a_key() {
    let auth = Auth::new(Some("admin".into()), None);
    let h = Harness::with_auth_seeded(auth, Some("admin"), None).await;

    let (status, body) = h.text("/docs/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("swagger"), "expected Swagger UI HTML at /docs");

    let (status, body) = h.text("/api-docs/openapi.json").await;
    assert_eq!(status, StatusCode::OK);
    let spec: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(spec["info"]["title"], "Tachyon API");
    assert!(spec["paths"]["/health"].is_object());
}

// --- Metrics (PRD §15) -----------------------------------------------------

#[tokio::test]
async fn metrics_are_prometheus_formatted() {
    let h = Harness::open().await;
    h.search("q=mouse&prefix=false").await;

    let (status, body) = h.text("/metrics").await;
    assert_eq!(status, StatusCode::OK);

    for metric in [
        "tachyon_uptime_seconds",
        "tachyon_search_requests_total",
        "tachyon_search_latency_seconds",
        "tachyon_collections",
        "tachyon_collection_documents",
        "tachyon_collection_segments",
        "tachyon_collection_wal_bytes",
        "tachyon_collection_memtable_bytes",
    ] {
        assert!(body.contains(&format!("# TYPE {metric}")), "missing {metric} in:\n{body}");
    }

    assert!(body.contains("tachyon_collection_documents{collection=\"products\"} 5"));
    assert!(body.contains("tachyon_search_requests_total 1"));
    assert!(body.contains("quantile=\"0.95\""));
}

#[tokio::test]
async fn metrics_have_the_prometheus_content_type() {
    let h = Harness::open().await;
    let request =
        Request::builder().method(Method::GET).uri("/metrics").body(Body::empty()).unwrap();
    let response = h.app.clone().oneshot(request).await.unwrap();
    let content_type = response.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(content_type.starts_with("text/plain"), "got {content_type}");
    assert!(content_type.contains("version=0.0.4"));
}

#[tokio::test]
async fn metrics_track_documents_as_they_change() {
    let h = Harness::open().await;
    assert!(h
        .text("/metrics")
        .await
        .1
        .contains("tachyon_collection_documents{collection=\"products\"} 5"));

    h.send(Method::DELETE, "/collections/products/documents/1", None).await;
    assert!(h
        .text("/metrics")
        .await
        .1
        .contains("tachyon_collection_documents{collection=\"products\"} 4"));
}
