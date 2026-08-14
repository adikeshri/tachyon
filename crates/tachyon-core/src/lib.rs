//! Core types shared by every Tachyon crate: schemas, values, documents, and
//! the error taxonomy.
//!
//! This crate holds no state and touches no I/O. Anything that opens a file
//! belongs in `tachyon-storage`; anything that builds a posting list belongs in
//! `tachyon-index`.

pub mod datetime;
pub mod document;
pub mod error;
pub mod schema;
pub mod value;

pub use document::{DocId, ParsedDocument, MAX_ID_LEN};
pub use error::{Error, Result};
pub use schema::{
    CollectionSchema, FieldId, FieldSchema, FieldType, TypoConfig, ID_FIELD, TEXT_MATCH_FIELD,
};
pub use value::Value;
