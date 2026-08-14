//! Error taxonomy shared by every Tachyon crate.
//!
//! Each variant maps onto exactly one HTTP status so the REST layer never has
//! to guess. Keep the mapping here rather than in the server: it is part of the
//! public API contract.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("collection `{0}` already exists")]
    CollectionExists(String),

    #[error("collection `{0}` not found")]
    CollectionNotFound(String),

    #[error("document `{id}` not found in collection `{collection}`")]
    DocumentNotFound { collection: String, id: String },

    /// The submitted schema is not a legal schema.
    #[error("invalid schema: {0}")]
    Schema(String),

    /// A document does not conform to the collection's schema.
    #[error("invalid document: {0}")]
    Validation(String),

    /// A search request could not be parsed or planned.
    #[error("invalid query: {0}")]
    Query(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// On-disk state failed its integrity checks.
    #[error("corrupt data: {0}")]
    Corruption(String),

    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// HTTP status for this error. Part of the API contract; see `docs/api.md`.
    pub fn status_code(&self) -> u16 {
        match self {
            Error::CollectionExists(_) => 409,
            Error::CollectionNotFound(_) | Error::DocumentNotFound { .. } => 404,
            Error::Schema(_) | Error::Validation(_) | Error::Query(_) | Error::Json(_) => 400,
            Error::Unauthorized(_) => 401,
            Error::Corruption(_) | Error::Io(_) | Error::Internal(_) => 500,
        }
    }

    /// Stable machine-readable code, so clients can branch without string
    /// matching on `message`.
    pub fn code(&self) -> &'static str {
        match self {
            Error::CollectionExists(_) => "collection_exists",
            Error::CollectionNotFound(_) => "collection_not_found",
            Error::DocumentNotFound { .. } => "document_not_found",
            Error::Schema(_) => "invalid_schema",
            Error::Validation(_) => "invalid_document",
            Error::Query(_) => "invalid_query",
            Error::Unauthorized(_) => "unauthorized",
            Error::Corruption(_) => "corrupt_data",
            Error::Io(_) => "io_error",
            Error::Json(_) => "invalid_json",
            Error::Internal(_) => "internal_error",
        }
    }

    pub fn schema(msg: impl Into<String>) -> Self {
        Error::Schema(msg.into())
    }

    pub fn validation(msg: impl Into<String>) -> Self {
        Error::Validation(msg.into())
    }

    pub fn query(msg: impl Into<String>) -> Self {
        Error::Query(msg.into())
    }

    pub fn corruption(msg: impl Into<String>) -> Self {
        Error::Corruption(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal(msg.into())
    }
}
