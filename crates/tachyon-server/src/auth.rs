//! API key authentication (PRD §14).
//!
//! Two kinds of key, matching the two things people do with a search engine:
//!
//! - an **admin key**, which can do anything, and
//! - a **search key**, which can only read — safe to ship in a browser bundle,
//!   which is the whole reason the split exists.
//!
//! Explicitly *not* in v1 (PRD §14): RBAC, multi-tenant isolation, OIDC. A
//! search key can read every collection.
//!
//! # No keys configured
//!
//! The server runs open. That is the right default for `docker run` and a
//! five-minute quickstart, and the wrong one for the public internet, so it is
//! logged loudly at startup rather than made silent.
//!
//! # Comparison
//!
//! Keys are compared in constant time. The comparison is not the weak point in
//! this design, but a timing oracle on a credential is the kind of thing that
//! should never be knowingly written.

use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use tachyon_core::Error;

use crate::error::ApiError;
use crate::state::AppState;

/// Header carrying the key.
pub const API_KEY_HEADER: &str = "x-tachyon-api-key";

/// What a caller is allowed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Reads and writes.
    Admin,
    /// Reads only.
    Search,
}

#[derive(Debug, Clone, Default)]
pub struct Auth {
    admin_key: Option<String>,
    search_key: Option<String>,
}

impl Auth {
    /// No authentication: every request is admin.
    pub fn open() -> Auth {
        Auth::default()
    }

    pub fn new(admin_key: Option<String>, search_key: Option<String>) -> Auth {
        Auth {
            admin_key: admin_key.filter(|k| !k.is_empty()),
            search_key: search_key.filter(|k| !k.is_empty()),
        }
    }

    /// Whether any key is configured. When false the server is wide open.
    pub fn is_enabled(&self) -> bool {
        self.admin_key.is_some() || self.search_key.is_some()
    }

    /// Resolve a presented key to its access level.
    pub fn access_for(&self, presented: Option<&str>) -> Option<Access> {
        if !self.is_enabled() {
            return Some(Access::Admin);
        }

        let presented = presented?;
        // Both keys are always checked, so the time taken does not reveal
        // which one matched.
        let is_admin =
            self.admin_key.as_deref().is_some_and(|key| constant_time_eq(key, presented));
        let is_search =
            self.search_key.as_deref().is_some_and(|key| constant_time_eq(key, presented));

        if is_admin {
            Some(Access::Admin)
        } else if is_search {
            Some(Access::Search)
        } else {
            None
        }
    }
}

/// Compare two strings without leaking their common prefix length.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    // The length itself is not secret, and bailing here avoids indexing games.
    if a.len() != b.len() {
        return false;
    }
    let mut difference = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        difference |= x ^ y;
    }
    difference == 0
}

/// Whether a request only reads.
///
/// Judged by method rather than by path, so a route added later is guarded
/// automatically instead of silently defaulting to readable.
fn is_read_only(method: &Method) -> bool {
    matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// Paths reachable without a key, so a load balancer can health-check a server
/// whose key it does not hold, and so API docs are browsable without one.
fn is_public(path: &str) -> bool {
    path == "/health"
        || path == "/docs"
        || path.starts_with("/docs/")
        || path == "/api-docs/openapi.json"
}

/// Middleware enforcing the key policy.
pub async fn require_api_key(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if !state.auth.is_enabled() || is_public(request.uri().path()) {
        return next.run(request).await;
    }

    let presented = request.headers().get(API_KEY_HEADER).and_then(|value| value.to_str().ok());

    let Some(access) = state.auth.access_for(presented) else {
        let message = if presented.is_some() {
            "the API key presented is not valid"
        } else {
            "this endpoint requires an API key in the X-TACHYON-API-KEY header"
        };
        return ApiError(Error::Unauthorized(message.into())).into_response();
    };

    if access == Access::Search && !is_read_only(request.method()) {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({
                "error": {
                    "code": "forbidden",
                    "message": "this API key is read-only; writes need the admin key"
                }
            })),
        )
            .into_response();
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unconfigured_server_is_open() {
        let auth = Auth::open();
        assert!(!auth.is_enabled());
        assert_eq!(auth.access_for(None), Some(Access::Admin));
        assert_eq!(auth.access_for(Some("anything")), Some(Access::Admin));
    }

    #[test]
    fn keys_map_to_their_access_level() {
        let auth = Auth::new(Some("admin-key".into()), Some("search-key".into()));
        assert!(auth.is_enabled());
        assert_eq!(auth.access_for(Some("admin-key")), Some(Access::Admin));
        assert_eq!(auth.access_for(Some("search-key")), Some(Access::Search));
        assert_eq!(auth.access_for(Some("wrong")), None);
        assert_eq!(auth.access_for(None), None);
    }

    #[test]
    fn either_key_alone_still_enables_authentication() {
        let admin_only = Auth::new(Some("admin".into()), None);
        assert!(admin_only.is_enabled());
        assert_eq!(admin_only.access_for(Some("admin")), Some(Access::Admin));
        assert_eq!(admin_only.access_for(Some("")), None);

        let search_only = Auth::new(None, Some("search".into()));
        assert!(search_only.is_enabled());
        assert_eq!(search_only.access_for(Some("search")), Some(Access::Search));
        assert_eq!(search_only.access_for(Some("admin")), None);
    }

    #[test]
    fn empty_keys_are_treated_as_unset() {
        let auth = Auth::new(Some(String::new()), Some(String::new()));
        assert!(!auth.is_enabled(), "an empty key must not lock everyone out");
    }

    #[test]
    fn read_only_methods_are_recognised() {
        assert!(is_read_only(&Method::GET));
        assert!(is_read_only(&Method::HEAD));
        assert!(!is_read_only(&Method::POST));
        assert!(!is_read_only(&Method::DELETE));
        assert!(!is_read_only(&Method::PATCH));
        assert!(!is_read_only(&Method::PUT));
    }

    #[test]
    fn health_and_docs_are_public() {
        assert!(is_public("/health"));
        assert!(is_public("/docs"));
        assert!(is_public("/docs/"));
        assert!(is_public("/api-docs/openapi.json"));
        assert!(!is_public("/collections"));
        assert!(!is_public("/metrics"));
    }

    #[test]
    fn constant_time_comparison_is_still_a_correct_comparison() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "abcd"));
        assert!(!constant_time_eq("", "a"));
        assert!(constant_time_eq("", ""));
    }
}
