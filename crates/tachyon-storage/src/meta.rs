//! Collection metadata: the immutable schema and the mutable commit state.
//!
//! `schema.json` is written once at creation and never again — PRD §7.1 makes
//! field types immutable. `state.json` is rewritten atomically at every flush
//! and is the single source of truth for what is durable: which segments are
//! committed, and how far the WAL has been consumed.

use serde::{Deserialize, Serialize};

use tachyon_core::{CollectionSchema, DocId, Error, Result};

use crate::layout::{write_atomic, Layout};

/// A segment that is committed and safe to read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRef {
    pub id: u64,
    /// Number of documents written into the segment, including ones since
    /// deleted.
    pub doc_count: u32,
    /// Inclusive range of internal doc ids the segment covers. Doc ids are
    /// globally increasing within a collection and never reassigned, so a
    /// segment's range is fixed for its lifetime. A merge does *not* reuse
    /// the ranges of the segments it retires — it allocates a fresh range
    /// the same way a flush does, which is what lets it rebuild a segment
    /// through the ordinary insert/encode path instead of merging two
    /// already-encoded segments together. The old ids are simply never
    /// claimed by anything again.
    pub min_doc_id: DocId,
    pub max_doc_id: DocId,
}

/// Everything needed to reconstruct a collection after a restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionState {
    pub format_version: u32,
    /// Next WAL sequence number to hand out.
    pub next_seq: u64,
    /// Every mutation with `seq <= applied_seq` is durably captured in a
    /// segment. Replay skips them.
    pub applied_seq: u64,
    /// Oldest WAL generation not yet fully captured in a committed segment.
    /// Everything below it has already been superseded by a segment and is
    /// safe to delete. This is *not* necessarily the generation currently
    /// being appended to — an in-flight off-lock flush seals the memtable
    /// and rolls a fresh generation for new writes before that flush
    /// commits, so recovery may need to replay this generation and the one
    /// above it, in order, to reconstruct exactly what a crash mid-flush
    /// left behind. See `Collection::open`'s WAL replay for the chain walk,
    /// and `Collection::commit_flush_locked` for where this advances.
    pub wal_generation: u64,
    /// Next internal doc id to assign.
    pub next_doc_id: DocId,
    pub next_segment_id: u64,
    pub segments: Vec<SegmentRef>,
    /// Internal doc ids of deleted documents, held here so a restart does not
    /// resurrect them. Serialized as a sorted list; the in-memory form is a
    /// roaring bitmap.
    #[serde(default)]
    pub deleted: Vec<DocId>,
}

impl Default for CollectionState {
    fn default() -> Self {
        CollectionState {
            format_version: crate::layout::STORE_FORMAT_VERSION,
            next_seq: 1,
            applied_seq: 0,
            wal_generation: 1,
            next_doc_id: 0,
            next_segment_id: 1,
            segments: Vec::new(),
            deleted: Vec::new(),
        }
    }
}

impl CollectionState {
    /// Total live documents across committed segments.
    pub fn committed_doc_count(&self) -> u64 {
        let total: u64 = self.segments.iter().map(|s| s.doc_count as u64).sum();
        total.saturating_sub(self.deleted.len() as u64)
    }
}

/// Persist a collection's schema. Refuses to overwrite: schemas are immutable.
pub fn write_schema(layout: &Layout, schema: &CollectionSchema) -> Result<()> {
    let path = layout.schema_file(&schema.name);
    if path.exists() {
        return Err(Error::CollectionExists(schema.name.clone()));
    }
    std::fs::create_dir_all(layout.collection_dir(&schema.name))?;
    write_atomic(&path, &serde_json::to_vec_pretty(schema)?)?;
    Ok(())
}

pub fn read_schema(layout: &Layout, name: &str) -> Result<CollectionSchema> {
    let path = layout.schema_file(name);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Error::CollectionNotFound(name.to_string()))
        }
        Err(e) => return Err(e.into()),
    };
    let schema: CollectionSchema = serde_json::from_slice(&bytes)
        .map_err(|e| Error::corruption(format!("{}: unreadable schema: {e}", path.display())))?;
    if schema.name != name {
        return Err(Error::corruption(format!(
            "{}: schema names collection `{}` but lives under `{name}`",
            path.display(),
            schema.name
        )));
    }
    Ok(schema)
}

/// Replace the commit state. The rename is the commit point: a crash before it
/// leaves the previous state intact and the new segment files orphaned.
pub fn write_state(layout: &Layout, name: &str, state: &CollectionState) -> Result<()> {
    write_atomic(&layout.state_file(name), &serde_json::to_vec_pretty(state)?)
}

/// Read the commit state, defaulting for a collection that has never flushed.
pub fn read_state(layout: &Layout, name: &str) -> Result<CollectionState> {
    let path = layout.state_file(name);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(CollectionState::default()),
        Err(e) => return Err(e.into()),
    };
    let state: CollectionState = serde_json::from_slice(&bytes).map_err(|e| {
        Error::corruption(format!("{}: unreadable collection state: {e}", path.display()))
    })?;
    if state.format_version != crate::layout::STORE_FORMAT_VERSION {
        return Err(Error::corruption(format!(
            "{}: state format version {}, this build understands {}",
            path.display(),
            state.format_version,
            crate::layout::STORE_FORMAT_VERSION
        )));
    }
    Ok(state)
}

/// Delete a collection and everything under it.
pub fn drop_collection(layout: &Layout, name: &str) -> Result<()> {
    let dir = layout.collection_dir(name);
    if !layout.schema_file(name).exists() {
        return Err(Error::CollectionNotFound(name.to_string()));
    }
    std::fs::remove_dir_all(&dir)?;
    crate::layout::sync_dir(&layout.collections_dir())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_core::{FieldSchema, FieldType};

    fn schema() -> CollectionSchema {
        CollectionSchema::new("products", vec![FieldSchema::new("title", FieldType::Text)])
    }

    #[test]
    fn schema_round_trips_and_is_write_once() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();

        write_schema(&layout, &schema()).unwrap();
        let loaded = read_schema(&layout, "products").unwrap();
        assert_eq!(loaded.name, "products");
        assert_eq!(loaded.fields.len(), 1);
        assert_eq!(loaded.fields[0].field_type, FieldType::Text);

        let err = write_schema(&layout, &schema()).unwrap_err();
        assert!(matches!(err, Error::CollectionExists(_)));
    }

    #[test]
    fn reading_an_unknown_collection_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        assert!(matches!(read_schema(&layout, "nope").unwrap_err(), Error::CollectionNotFound(_)));
    }

    #[test]
    fn state_defaults_before_the_first_flush() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        let state = read_state(&layout, "products").unwrap();
        assert_eq!(state.next_seq, 1);
        assert_eq!(state.applied_seq, 0);
        assert!(state.segments.is_empty());
    }

    #[test]
    fn state_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        write_schema(&layout, &schema()).unwrap();

        let mut state = CollectionState {
            applied_seq: 12,
            next_seq: 13,
            next_doc_id: 9,
            deleted: vec![3, 5],
            ..Default::default()
        };
        state.segments.push(SegmentRef { id: 1, doc_count: 9, min_doc_id: 0, max_doc_id: 8 });
        write_state(&layout, "products", &state).unwrap();

        let loaded = read_state(&layout, "products").unwrap();
        assert_eq!(loaded.applied_seq, 12);
        assert_eq!(loaded.segments.len(), 1);
        assert_eq!(loaded.committed_doc_count(), 7);
    }

    #[test]
    fn dropping_removes_everything() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        write_schema(&layout, &schema()).unwrap();
        std::fs::create_dir_all(layout.wal_dir("products")).unwrap();

        drop_collection(&layout, "products").unwrap();
        assert!(!layout.collection_dir("products").exists());
        assert!(matches!(
            drop_collection(&layout, "products").unwrap_err(),
            Error::CollectionNotFound(_)
        ));
    }
}
