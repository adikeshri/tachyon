//! End-to-end tests against the REST surface.
//!
//! These drive the router in-process with `tower::ServiceExt::oneshot`: the
//! full request path — extractors, handlers, engine, WAL, disk — with no
//! socket, no port allocation, and no timing races.

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
    fn new() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(EngineConfig::new(dir.path())).unwrap();
        let app = build_router(AppState::new(Arc::new(engine)));
        Harness { _dir: dir, app }
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

    async fn get(&self, uri: &str) -> (StatusCode, Value) {
        self.send(Method::GET, uri, None).await
    }

    async fn delete(&self, uri: &str) -> (StatusCode, Value) {
        self.send(Method::DELETE, uri, None).await
    }

    /// The PRD §7.1 example schema.
    async fn create_products(&self) -> (StatusCode, Value) {
        self.post(
            "/collections",
            json!({
                "name": "products",
                "fields": [
                    {"name": "title", "type": "text"},
                    {"name": "brand", "type": "keyword", "facet": true},
                    {"name": "price", "type": "int", "filter": true, "sort": true},
                    {"name": "description", "type": "text"}
                ]
            }),
        )
        .await
    }
}

#[tokio::test]
async fn health_reports_ready() {
    let h = Harness::new();
    let (status, body) = h.get("/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["num_collections"], json!(0));
}

#[tokio::test]
async fn creates_the_prd_example_collection() {
    let h = Harness::new();
    let (status, body) = h.create_products().await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["name"], json!("products"));
    assert_eq!(body["fields"].as_array().unwrap().len(), 4);
    assert_eq!(body["num_documents"], json!(0));
}

#[tokio::test]
async fn rejects_a_duplicate_collection() {
    let h = Harness::new();
    h.create_products().await;
    let (status, body) = h.create_products().await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("collection_exists"));
}

#[tokio::test]
async fn rejects_an_invalid_schema() {
    let h = Harness::new();
    let (status, body) = h
        .post("/collections", json!({"name": "bad", "fields": [{"name": "id", "type": "text"}]}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], json!("invalid_schema"));
}

#[tokio::test]
async fn lists_and_fetches_collections() {
    let h = Harness::new();
    h.create_products().await;

    let (status, body) = h.get("/collections").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap().len(), 1);

    let (status, body) = h.get("/collections/products").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], json!("products"));

    let (status, body) = h.get("/collections/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], json!("collection_not_found"));
}

#[tokio::test]
async fn drops_a_collection() {
    let h = Harness::new();
    h.create_products().await;

    let (status, _) = h.delete("/collections/products").await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = h.get("/collections/products").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = h.delete("/collections/products").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn indexes_a_batch_and_reads_documents_back() {
    let h = Harness::new();
    h.create_products().await;

    let (status, body) = h
        .post(
            "/collections/products/documents",
            json!([
                {"id": "1", "title": "Wireless Mouse", "brand": "Logitech", "price": 2999},
                {"id": "2", "title": "Mechanical Keyboard", "brand": "Razer", "price": 8999}
            ]),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["num_indexed"], json!(2));
    assert_eq!(body["num_failed"], json!(0));

    let (status, body) = h.get("/collections/products/documents/1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["title"], json!("Wireless Mouse"));

    let (status, body) = h.get("/collections/products").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["num_documents"], json!(2));
}

#[tokio::test]
async fn accepts_a_single_document_object() {
    let h = Harness::new();
    h.create_products().await;

    let (status, body) = h
        .post(
            "/collections/products/documents",
            json!({"id": "1", "title": "Wireless Mouse", "brand": "Logitech", "price": 2999}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["num_indexed"], json!(1));
}

#[tokio::test]
async fn partial_batch_failures_are_reported_per_document() {
    let h = Harness::new();
    h.create_products().await;

    let (status, body) = h
        .post(
            "/collections/products/documents",
            json!([
                {"id": "1", "title": "Wireless Mouse", "price": 2999},
                {"id": "2", "title": "Bad Price", "price": "cheap"},
                {"title": "No Id At All"},
                {"id": "4", "title": "Monitor", "price": 19999}
            ]),
        )
        .await;

    assert_eq!(status, StatusCode::OK, "a partly-bad batch is still a well-formed request");
    assert_eq!(body["num_indexed"], json!(2));
    assert_eq!(body["num_failed"], json!(2));

    let results = body["results"].as_array().unwrap();
    assert_eq!(results[0]["success"], json!(true));
    assert_eq!(results[1]["success"], json!(false));
    assert_eq!(results[1]["code"], json!("invalid_document"));
    assert_eq!(results[2]["success"], json!(false));
    assert_eq!(results[3]["success"], json!(true));

    // The good ones really landed.
    assert_eq!(h.get("/collections/products/documents/1").await.0, StatusCode::OK);
    assert_eq!(h.get("/collections/products/documents/4").await.0, StatusCode::OK);
    assert_eq!(h.get("/collections/products/documents/2").await.0, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn upsert_replaces_an_existing_document() {
    let h = Harness::new();
    h.create_products().await;

    h.post("/collections/products/documents", json!({"id": "1", "title": "Mouse", "price": 2999}))
        .await;
    h.post(
        "/collections/products/documents",
        json!({"id": "1", "title": "Better Mouse", "price": 3999}),
    )
    .await;

    let (_, body) = h.get("/collections/products/documents/1").await;
    assert_eq!(body["title"], json!("Better Mouse"));
    let (_, body) = h.get("/collections/products").await;
    assert_eq!(body["num_documents"], json!(1));
}

#[tokio::test]
async fn deletes_a_document() {
    let h = Harness::new();
    h.create_products().await;
    h.post("/collections/products/documents", json!({"id": "1", "title": "Mouse"})).await;

    let (status, _) = h.delete("/collections/products/documents/1").await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = h.get("/collections/products/documents/1").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], json!("document_not_found"));

    let (status, _) = h.delete("/collections/products/documents/1").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn documents_for_an_unknown_collection_are_not_found() {
    let h = Harness::new();
    let (status, body) =
        h.post("/collections/ghost/documents", json!([{"id": "1", "title": "x"}])).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], json!("collection_not_found"));
}

#[tokio::test]
async fn malformed_json_is_a_bad_request() {
    let h = Harness::new();
    h.create_products().await;

    let request = Request::builder()
        .method(Method::POST)
        .uri("/collections/products/documents")
        .header("content-type", "application/json")
        .body(Body::from("{not json"))
        .unwrap();
    let response = h.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn data_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = Engine::open(EngineConfig::new(dir.path())).unwrap();
        let app = build_router(AppState::new(Arc::new(engine)));
        let h = Harness { _dir: tempfile::tempdir().unwrap(), app };
        h.create_products().await;
        h.post(
            "/collections/products/documents",
            json!([
                {"id": "1", "title": "Wireless Mouse", "brand": "Logitech", "price": 2999},
                {"id": "2", "title": "Keyboard", "brand": "Razer", "price": 8999}
            ]),
        )
        .await;
        h.delete("/collections/products/documents/2").await;
    }

    // A fresh engine over the same directory, as a process restart would give.
    let engine = Engine::open(EngineConfig::new(dir.path())).unwrap();
    let app = build_router(AppState::new(Arc::new(engine)));
    let h = Harness { _dir: tempfile::tempdir().unwrap(), app };

    let (status, body) = h.get("/collections/products/documents/1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["title"], json!("Wireless Mouse"));
    assert_eq!(h.get("/collections/products/documents/2").await.0, StatusCode::NOT_FOUND);

    let (_, body) = h.get("/collections/products").await;
    assert_eq!(body["num_documents"], json!(1));
    // The schema came back intact, not defaulted.
    assert_eq!(body["fields"][1]["facet"], json!(true));
}
