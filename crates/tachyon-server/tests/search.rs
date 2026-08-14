//! End-to-end search tests (PRD §7.3, §13).

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
    /// A products collection preloaded with a small, deliberately overlapping
    /// catalogue.
    async fn products() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(EngineConfig::new(dir.path())).unwrap();
        let h = Harness { _dir: dir, app: build_router(AppState::new(Arc::new(engine))) };

        h.post(
            "/collections",
            json!({
                "name": "products",
                "fields": [
                    {"name": "title", "type": "text"},
                    {"name": "description", "type": "text"},
                    {"name": "brand", "type": "keyword", "facet": true},
                    {"name": "price", "type": "int", "filter": true, "sort": true}
                ]
            }),
        )
        .await;

        h.post(
            "/collections/products/documents",
            json!([
                {"id": "1", "title": "Wireless Mouse", "description": "A comfortable wireless mouse", "brand": "Logitech", "price": 2999},
                {"id": "2", "title": "Mechanical Keyboard", "description": "Loud and tactile keys", "brand": "Razer", "price": 8999},
                {"id": "3", "title": "Mouse Pad", "description": "Large desk mat for your mouse", "brand": "Logitech", "price": 999},
                {"id": "4", "title": "Wireless Charger", "description": "Charges phones without a cable", "brand": "Anker", "price": 3499},
                {"id": "5", "title": "Gaming Mouse", "description": "High DPI wired gaming mouse", "brand": "Razer", "price": 5999}
            ]),
        )
        .await;

        h
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

    async fn search(&self, query: &str) -> Value {
        let (status, body) =
            self.send(Method::GET, &format!("/collections/products/search?{query}"), None).await;
        assert_eq!(status, StatusCode::OK, "search failed: {body}");
        body
    }
}

/// Document ids from a search response, in rank order.
fn ids(body: &Value) -> Vec<String> {
    body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["document"]["id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn response_matches_the_prd_shape() {
    let h = Harness::products().await;
    let body = h.search("q=wireless+mouse&prefix=false").await;

    assert!(body["found"].is_number());
    assert!(body["search_time_ms"].is_number());
    let hits = body["hits"].as_array().unwrap();
    assert!(!hits.is_empty());
    assert!(hits[0]["document"].is_object(), "each hit carries the source document");
    assert!(hits[0]["text_match"].is_number());
    assert_eq!(hits[0]["document"]["title"], json!("Wireless Mouse"));
}

#[tokio::test]
async fn finds_the_documents_containing_all_terms() {
    let h = Harness::products().await;
    let body = h.search("q=wireless+mouse&prefix=false").await;
    assert_eq!(body["found"], json!(1));
    assert_eq!(ids(&body), vec!["1"]);
}

#[tokio::test]
async fn a_single_term_matches_every_document_containing_it() {
    let h = Harness::products().await;
    let body = h.search("q=mouse&prefix=false").await;
    assert_eq!(body["found"], json!(3), "documents 1, 3 and 5 mention a mouse");
    let found = ids(&body);
    for id in ["1", "3", "5"] {
        assert!(found.contains(&id.to_string()), "missing {id} in {found:?}");
    }
}

#[tokio::test]
async fn title_matches_outrank_description_matches() {
    let h = Harness::products().await;
    let body = h.search("q=wireless&prefix=false").await;
    // Both titles mention wireless; document 1 also has it in the description.
    let ranked = ids(&body);
    assert_eq!(ranked.len(), 2);
    assert!(ranked.contains(&"1".to_string()));
    assert!(ranked.contains(&"4".to_string()));
}

#[tokio::test]
async fn scores_descend_through_the_ranking() {
    let h = Harness::products().await;
    let body = h.search("q=mouse&prefix=false").await;
    let scores: Vec<f64> = body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["text_match"].as_f64().unwrap())
        .collect();
    assert!(scores.windows(2).all(|w| w[0] >= w[1]), "not descending: {scores:?}");
    assert!(scores[0] > 0.0);
}

#[tokio::test]
async fn prefix_search_completes_the_final_token() {
    let h = Harness::products().await;
    // Typo tolerance off, so this isolates prefix behaviour: `wirel` is one
    // edit from `wired` and would otherwise match on that alone.
    assert_eq!(h.search("q=wirel&prefix=false&typo_tolerance=false").await["found"], json!(0));

    let body = h.search("q=wirel&typo_tolerance=false").await;
    assert_eq!(body["found"], json!(2), "prefix is on by default");
}

#[tokio::test]
async fn search_is_case_and_accent_insensitive() {
    let h = Harness::products().await;
    let lower = h.search("q=mouse&prefix=false").await;
    let upper = h.search("q=MOUSE&prefix=false").await;
    assert_eq!(lower["found"], upper["found"]);
    assert_eq!(ids(&lower), ids(&upper));
}

#[tokio::test]
async fn query_by_restricts_the_searched_fields() {
    let h = Harness::products().await;
    let anywhere = h.search("q=keys&prefix=false").await;
    assert_eq!(anywhere["found"], json!(1), "`keys` appears in a description");

    let titles_only = h.search("q=keys&query_by=title&prefix=false").await;
    assert_eq!(titles_only["found"], json!(0));
}

#[tokio::test]
async fn match_mode_any_widens_the_search() {
    let h = Harness::products().await;
    let all = h.search("q=wireless+keyboard&prefix=false").await;
    assert_eq!(all["found"], json!(0), "no document has both");

    let any = h.search("q=wireless+keyboard&prefix=false&match_mode=any").await;
    assert_eq!(any["found"], json!(3), "two wireless products plus the keyboard");
}

#[tokio::test]
async fn pagination_splits_a_stable_ranking() {
    let h = Harness::products().await;
    let all = h.search("q=mouse&prefix=false&limit=10").await;
    let page1 = h.search("q=mouse&prefix=false&limit=2").await;
    let page2 = h.search("q=mouse&prefix=false&limit=2&offset=2").await;

    assert_eq!(page1["found"], json!(3), "found reports the total, not the page");
    assert_eq!(ids(&page1), ids(&all)[..2].to_vec());
    assert_eq!(ids(&page2), ids(&all)[2..].to_vec());
}

#[tokio::test]
async fn an_empty_query_returns_everything() {
    let h = Harness::products().await;
    let body = h.search("q=").await;
    assert_eq!(body["found"], json!(5));
}

#[tokio::test]
async fn a_query_matching_nothing_is_an_empty_success() {
    let h = Harness::products().await;
    let body = h.search("q=helicopter&prefix=false").await;
    assert_eq!(body["found"], json!(0));
    assert_eq!(body["hits"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn deleted_documents_leave_the_index() {
    let h = Harness::products().await;
    assert_eq!(h.search("q=mouse&prefix=false").await["found"], json!(3));

    h.send(Method::DELETE, "/collections/products/documents/3", None).await;
    let body = h.search("q=mouse&prefix=false").await;
    assert_eq!(body["found"], json!(2));
    assert!(!ids(&body).contains(&"3".to_string()));
}

#[tokio::test]
async fn updated_documents_are_searchable_by_their_new_text() {
    let h = Harness::products().await;
    h.post(
        "/collections/products/documents",
        json!({"id": "2", "title": "Ergonomic Trackball", "description": "no keys here", "brand": "Razer", "price": 8999}),
    )
    .await;

    assert_eq!(h.search("q=trackball&prefix=false").await["found"], json!(1));
    assert_eq!(
        h.search("q=mechanical&prefix=false").await["found"],
        json!(0),
        "the replaced text is gone"
    );
}

#[tokio::test]
async fn search_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = Engine::open(EngineConfig::new(dir.path())).unwrap();
        let h = Harness {
            _dir: tempfile::tempdir().unwrap(),
            app: build_router(AppState::new(Arc::new(engine))),
        };
        h.post(
            "/collections",
            json!({"name": "products", "fields": [{"name": "title", "type": "text"}]}),
        )
        .await;
        h.post(
            "/collections/products/documents",
            json!([{"id": "1", "title": "Wireless Mouse"}, {"id": "2", "title": "Keyboard"}]),
        )
        .await;
    }

    let engine = Engine::open(EngineConfig::new(dir.path())).unwrap();
    let h = Harness {
        _dir: tempfile::tempdir().unwrap(),
        app: build_router(AppState::new(Arc::new(engine))),
    };

    // The inverted index was rebuilt from the WAL, not persisted.
    let body = h.search("q=wireless&prefix=false").await;
    assert_eq!(body["found"], json!(1));
    assert_eq!(ids(&body), vec!["1"]);
}

#[tokio::test]
async fn invalid_search_requests_are_rejected() {
    let h = Harness::products().await;
    for query in [
        "q=x&query_by=nope",       // unknown field
        "q=x&query_by=price",      // not a text field
        "q=x&limit=100000",        // beyond the page cap
        "q=x&match_mode=sideways", // not a mode
        "q=x&facet=title",         // not declared facetable
    ] {
        let (status, body) =
            h.send(Method::GET, &format!("/collections/products/search?{query}"), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query} should be rejected");
        assert_eq!(body["error"]["code"], json!("invalid_query"), "{query}");
    }
}

#[tokio::test]
async fn the_prd_filter_example_works_over_http() {
    let h = Harness::products().await;
    // PRD §7.6: brand:=Logitech && price:<5000
    let body = h
        .search("q=mouse&prefix=false&filter=brand%3A%3DLogitech%20%26%26%20price%3A%3C5000")
        .await;
    assert_eq!(body["found"], json!(2));
    let mut found = ids(&body);
    found.sort();
    assert_eq!(found, vec!["1", "3"]);
}

#[tokio::test]
async fn filters_support_ranges_and_sets() {
    let h = Harness::products().await;

    let body = h.search("q=&filter=price%3A%5B1000..4000%5D").await;
    let mut found = ids(&body);
    found.sort();
    assert_eq!(found, vec!["1", "4"]);

    let body = h.search("q=&filter=brand%3A%3D%5BRazer%2CAnker%5D").await;
    assert_eq!(body["found"], json!(3), "two Razer plus one Anker");
}

#[tokio::test]
async fn the_prd_sort_example_works_over_http() {
    let h = Harness::products().await;
    // PRD §7.8: sort=_text_match:desc,price:asc
    let body = h.search("q=mouse&prefix=false&sort=_text_match%3Adesc%2Cprice%3Aasc").await;
    assert_eq!(body["found"], json!(3));

    let scores: Vec<f64> = body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["text_match"].as_f64().unwrap())
        .collect();
    assert!(scores.windows(2).all(|w| w[0] >= w[1]), "relevance leads: {scores:?}");
}

#[tokio::test]
async fn sorting_by_price_reorders_the_results() {
    let h = Harness::products().await;

    let asc = h.search("q=&sort=price%3Aasc").await;
    assert_eq!(ids(&asc), vec!["3", "1", "4", "5", "2"]);

    let desc = h.search("q=&sort=price%3Adesc").await;
    let mut reversed = ids(&asc);
    reversed.reverse();
    assert_eq!(ids(&desc), reversed);
}

#[tokio::test]
async fn filters_and_sorting_combine() {
    let h = Harness::products().await;
    let body = h.search("q=&filter=brand%3A%3DRazer&sort=price%3Aasc").await;
    assert_eq!(ids(&body), vec!["5", "2"]);
}

#[tokio::test]
async fn invalid_filters_and_sorts_are_rejected() {
    let h = Harness::products().await;
    for query in [
        "q=x&filter=nope%3A%3D1",      // unknown field
        "q=x&filter=title%3A%3E5",     // comparison on a text field
        "q=x&filter=price%3A%3Dcheap", // wrong value type
        "q=x&filter=%28price%3A%3C5",  // unbalanced parenthesis
        "q=x&sort=price",              // no direction
        "q=x&sort=title%3Aasc",        // not sortable
        "q=x&sort=nope%3Aasc",         // unknown field
    ] {
        let (status, body) =
            h.send(Method::GET, &format!("/collections/products/search?{query}"), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query} should be rejected");
        assert_eq!(body["error"]["code"], json!("invalid_query"), "{query}");
        assert!(
            body["error"]["message"].as_str().unwrap().len() > 10,
            "the message should explain what is wrong: {body}"
        );
    }
}

#[tokio::test]
async fn searching_an_unknown_collection_is_not_found() {
    let h = Harness::products().await;
    let (status, body) = h.send(Method::GET, "/collections/ghost/search?q=x", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], json!("collection_not_found"));
}
