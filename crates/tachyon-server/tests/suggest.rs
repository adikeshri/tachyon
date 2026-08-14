//! End-to-end autocomplete and typo-tolerance tests (PRD §7.4, §7.5).

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

use tachyon_engine::{Engine, EngineConfig};
use tachyon_server::{build_router, AppState};

struct Harness {
    _dir: tempfile::TempDir,
    app: Router,
}

impl Harness {
    async fn new(docs: Value) -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(EngineConfig::new(dir.path())).unwrap();
        let h = Harness { _dir: dir, app: build_router(AppState::new(Arc::new(engine))) };

        h.post(
            "/collections",
            json!({
                "name": "products",
                "fields": [
                    {"name": "title", "type": "text"},
                    {"name": "description", "type": "text"}
                ]
            }),
        )
        .await;
        h.post("/collections/products/documents", docs).await;
        h
    }

    /// A catalogue where `wireless` is the most common term starting with
    /// `wir`, then `wired`, then `wire`.
    async fn catalogue() -> Harness {
        Harness::new(json!([
            {"id": "1", "title": "Wireless Mouse"},
            {"id": "2", "title": "Wireless Keyboard"},
            {"id": "3", "title": "Wireless Charger"},
            {"id": "4", "title": "Wired Mouse"},
            {"id": "5", "title": "Wired Keyboard"},
            {"id": "6", "title": "Wire Cutter"}
        ]))
        .await
    }

    async fn send(&self, method: Method, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
        let builder = Request::builder().method(method).uri(uri);
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

    async fn suggest(&self, query: &str) -> Value {
        let (status, body) =
            self.send(Method::GET, &format!("/collections/products/suggest?{query}"), None).await;
        assert_eq!(status, StatusCode::OK, "suggest failed: {body}");
        body
    }

    async fn search(&self, query: &str) -> Value {
        let (status, body) =
            self.send(Method::GET, &format!("/collections/products/search?{query}"), None).await;
        assert_eq!(status, StatusCode::OK, "search failed: {body}");
        body
    }
}

fn texts(body: &Value) -> Vec<String> {
    body["suggestions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["text"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn the_prd_suggest_example_works() {
    // PRD §7.5: GET /collections/{name}/suggest?q=wir
    let h = Harness::catalogue().await;
    let body = h.suggest("q=wir").await;

    assert_eq!(texts(&body), vec!["wireless", "wired", "wire"]);
    assert!(body["search_time_ms"].is_number());
}

#[tokio::test]
async fn suggestions_are_ordered_by_popularity() {
    let h = Harness::catalogue().await;
    let suggestions = h.suggest("q=wir").await;
    let counts: Vec<u64> = suggestions["suggestions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["count"].as_u64().unwrap())
        .collect();
    assert_eq!(counts, vec![3, 2, 1]);
}

#[tokio::test]
async fn top_k_is_configurable() {
    let h = Harness::catalogue().await;
    assert_eq!(texts(&h.suggest("q=wir&limit=2").await), vec!["wireless", "wired"]);
    assert_eq!(texts(&h.suggest("q=wir&limit=1").await), vec!["wireless"]);
}

#[tokio::test]
async fn a_mistyped_prefix_still_suggests() {
    let h = Harness::catalogue().await;
    let body = h.suggest("q=wirelss").await;
    let suggestions = body["suggestions"].as_array().unwrap();
    assert!(!suggestions.is_empty(), "a typo should not be a dead end");
    assert_eq!(suggestions[0]["text"], json!("wireless"));
    assert_eq!(suggestions[0]["typos"], json!(1));
}

#[tokio::test]
async fn an_empty_or_unmatched_prefix_returns_an_empty_list() {
    let h = Harness::catalogue().await;
    assert!(h.suggest("q=").await["suggestions"].as_array().unwrap().is_empty());
    assert!(h.suggest("q=zzzz").await["suggestions"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn suggestions_follow_the_index_as_it_changes() {
    let h = Harness::catalogue().await;
    assert_eq!(texts(&h.suggest("q=wir&limit=1").await), vec!["wireless"]);

    // Delete every wireless product; the suggestion must follow.
    for id in ["1", "2", "3"] {
        h.send(Method::DELETE, &format!("/collections/products/documents/{id}"), None).await;
    }
    assert_eq!(texts(&h.suggest("q=wir&limit=1").await), vec!["wired"]);
}

#[tokio::test]
async fn invalid_suggest_requests_are_rejected() {
    let h = Harness::catalogue().await;
    for query in ["q=w&query_by=nope", "q=w&limit=0", "q=w&limit=999"] {
        let (status, body) =
            h.send(Method::GET, &format!("/collections/products/suggest?{query}"), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query} should be rejected");
        assert_eq!(body["error"]["code"], json!("invalid_query"));
    }
}

#[tokio::test]
async fn suggesting_on_an_unknown_collection_is_not_found() {
    let h = Harness::catalogue().await;
    let (status, body) = h.send(Method::GET, "/collections/ghost/suggest?q=w", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], json!("collection_not_found"));
}

#[tokio::test]
async fn a_typo_in_a_search_still_finds_the_product() {
    let h = Harness::new(json!([
        {"id": "1", "title": "Wireless Mouse", "description": "A comfortable pointing device"},
        {"id": "2", "title": "Mechanical Keyboard", "description": "Loud and tactile"}
    ]))
    .await;

    for typo in ["wirelss", "wierless", "wireles"] {
        let body = h.search(&format!("q={typo}&prefix=false")).await;
        assert_eq!(body["found"], json!(1), "`{typo}` should still find the mouse");
        assert_eq!(body["hits"][0]["document"]["id"], json!("1"));
    }
}

#[tokio::test]
async fn the_typo_budget_follows_the_prd_table() {
    let h = Harness::new(json!([
        {"id": "short", "title": "cat"},
        {"id": "medium", "title": "mouse"},
        {"id": "long", "title": "wireless"}
    ]))
    .await;

    // The budget is set by the length of the token the user typed.
    //
    // 1-3 characters: no typos.
    assert_eq!(h.search("q=bat&prefix=false").await["found"], json!(0));
    assert_eq!(h.search("q=cat&prefix=false").await["found"], json!(1));

    // 4-7 characters: one typo, but not two.
    assert_eq!(h.search("q=mouze&prefix=false").await["found"], json!(1));
    assert_eq!(h.search("q=mozze&prefix=false").await["found"], json!(0));

    // 8+ characters: two typos, but not three.
    assert_eq!(h.search("q=wirelezz&prefix=false").await["found"], json!(1));
    assert_eq!(h.search("q=wirelzzz&prefix=false").await["found"], json!(0));
}

#[tokio::test]
async fn exact_matches_rank_above_corrected_ones() {
    let h = Harness::new(json!([
        {"id": "corrected", "title": "Moose Tracker"},
        {"id": "exact", "title": "Mouse Tracker"}
    ]))
    .await;

    let body = h.search("q=mouse&prefix=false").await;
    assert_eq!(body["found"], json!(2), "both match once typos are allowed");
    assert_eq!(body["hits"][0]["document"]["id"], json!("exact"));
    let scores: Vec<f64> = body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["text_match"].as_f64().unwrap())
        .collect();
    assert!(scores[0] > scores[1], "the exact match must score higher: {scores:?}");
}

#[tokio::test]
async fn typo_tolerance_can_be_turned_off_per_request() {
    let h = Harness::new(json!([{"id": "1", "title": "Wireless Mouse"}])).await;
    assert_eq!(h.search("q=wirelss&prefix=false").await["found"], json!(1));
    assert_eq!(h.search("q=wirelss&prefix=false&typo_tolerance=false").await["found"], json!(0));
}

#[tokio::test]
async fn typo_settings_can_be_configured_per_collection() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineConfig::new(dir.path())).unwrap();
    let h = Harness { _dir: dir, app: build_router(AppState::new(Arc::new(engine))) };

    h.post(
        "/collections",
        json!({
            "name": "products",
            "fields": [{"name": "title", "type": "text"}],
            "typo_tolerance": {"enabled": false}
        }),
    )
    .await;
    h.post("/collections/products/documents", json!([{"id": "1", "title": "Wireless Mouse"}]))
        .await;

    assert_eq!(h.search("q=wirelss&prefix=false").await["found"], json!(0));
    // …and a request can still opt back in.
    assert_eq!(h.search("q=wirelss&prefix=false&typo_tolerance=true").await["found"], json!(1));
}
