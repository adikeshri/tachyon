//! Durable storage for Tachyon.
//!
//! Three concerns, deliberately kept apart from the index structures:
//!
//! - [`layout`] — where files live and how to replace one atomically
//! - [`wal`] — the append-only log that makes an acknowledged write durable
//! - [`meta`] — the collection schema and the commit state that ties segments
//!   and WAL together
//!
//! Nothing here knows what a posting list is; segment *contents* are the
//! index crate's business, segment *lifecycle* is the engine's.

pub mod layout;
pub mod meta;
pub mod wal;

pub use layout::{write_atomic, Layout, STORE_FORMAT_VERSION};
pub use meta::{CollectionState, SegmentRef};
pub use wal::{SyncPolicy, Wal, WalRecord, WalRecordRef, WalScan};
