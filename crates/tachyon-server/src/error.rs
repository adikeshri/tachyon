//! Turning [`tachyon_core::Error`] into an HTTP response.
//!
//! The status and the machine-readable `code` both come from the core error
//! type, so the API contract lives in one place and handlers can simply `?`.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;

use tachyon_core::Error;

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

/// Newtype so we can implement `IntoResponse` for the core error.
#[derive(Debug)]
pub struct ApiError(pub Error);

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.0.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

        // 5xx means we broke something; log it with the full chain. 4xx is the
        // client's business and would be log spam.
        if status.is_server_error() {
            tracing::error!(error = %self.0, code = self.0.code(), "request failed");
        }

        let body =
            ErrorResponse { error: ErrorBody { code: self.0.code(), message: self.0.to_string() } };
        (status, Json(body)).into_response()
    }
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_status_and_code() {
        let r = ApiError(Error::CollectionNotFound("x".into())).into_response();
        assert_eq!(r.status(), StatusCode::NOT_FOUND);

        let r = ApiError(Error::CollectionExists("x".into())).into_response();
        assert_eq!(r.status(), StatusCode::CONFLICT);

        let r = ApiError(Error::validation("nope")).into_response();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        let r = ApiError(Error::internal("boom")).into_response();
        assert_eq!(r.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
