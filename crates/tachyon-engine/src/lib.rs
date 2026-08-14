//! Collection lifecycle: the write path, crash recovery, and the handles the
//! REST layer talks to.
//!
//! This is the crate that ties [`tachyon_storage`] (durability) to
//! [`tachyon_index`] (structures). Nothing above it should have to know that a
//! WAL exists.

pub mod collection;
pub mod config;
pub mod engine;

pub use collection::{BatchReport, Collection, CollectionStats, DocOutcome};
pub use config::EngineConfig;
pub use engine::Engine;
