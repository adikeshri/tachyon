//! A single collection: its schema, its durable state, and the write path.
//!
//! # Write path (PRD §10)
//!
//! 1. Validate the document against the schema — outside any lock.
//! 2. Append to the WAL and fsync per the sync policy.
//! 3. Apply to the memtable. This step is infallible by construction: every
//!    fallible check happened in step 1, so a write that reached the log always
//!    reaches memory.
//! 4. Acknowledge.
//!
//! # Recovery
//!
//! `state.json` records the sequence number up to which mutations are captured
//! in committed segments. On open, every WAL record beyond it is replayed in
//! order. Because doc ids are assigned sequentially from `state.next_doc_id`,
//! replay reconstructs exactly the ids the crashed process had assigned.

use std::sync::Arc;

use parking_lot::RwLock;
use roaring::RoaringBitmap;
use serde::Serialize;
use serde_json::Value as Json;
use utoipa::ToSchema;

use tachyon_core::{CollectionSchema, DocId, Error, ParsedDocument, Result};
use tachyon_index::{encode, IndexSource, MemTable, SegmentFilePaths, SegmentReader};
use tachyon_query::{
    compute_facets, execute, suggest, Hit, SearchContext, SearchParams, SearchRequest,
    SearchResponse, SuggestParams, SuggestRequest, SuggestResponse,
};
use tachyon_storage::layout::sync_dir;
use tachyon_storage::{
    meta, wal, write_atomic, CollectionState, Layout, SegmentRef, Wal, WalRecord, WalRecordRef,
};

use crate::config::EngineConfig;

/// Path of every file one segment id is stored as, under this collection's
/// `segments/` directory.
fn segment_file_paths(layout: &Layout, name: &str, id: u64) -> SegmentFilePaths {
    SegmentFilePaths {
        terms: layout.segment_file(name, id, "terms"),
        ids: layout.segment_file(name, id, "ids"),
        post: layout.segment_file(name, id, "post"),
        col: layout.segment_file(name, id, "col"),
        doc: layout.segment_file(name, id, "doc"),
    }
}

/// Per-document result of a batch write. A batch is not all-or-nothing: PRD
/// §7.2 requires atomicity *per document*, so one malformed document does not
/// reject its neighbours.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DocOutcome {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    pub code: Option<&'static str>,
}

impl DocOutcome {
    fn ok(id: String) -> Self {
        DocOutcome { success: true, id: Some(id), error: None, code: None }
    }

    fn failed(err: &Error) -> Self {
        DocOutcome {
            success: false,
            id: None,
            error: Some(err.to_string()),
            code: Some(err.code()),
        }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct BatchReport {
    pub num_indexed: usize,
    pub num_failed: usize,
    pub results: Vec<DocOutcome>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CollectionStats {
    pub name: String,
    pub num_documents: u64,
    pub num_segments: usize,
    pub memtable_documents: usize,
    pub memtable_bytes: usize,
    pub wal_bytes: u64,
    pub created_at: i64,
}

struct Inner {
    state: CollectionState,
    wal: Wal,
    memtable: MemTable,
    /// Committed segments, oldest first. Consulted newest-first (`.rev()`)
    /// after a memtable miss, since a later segment's version of an id, if
    /// any, is the current one.
    segments: Vec<Arc<SegmentReader>>,
    /// Tombstones for doc ids that live in committed segments. Memtable
    /// documents are deleted in place, so they never appear here.
    deleted: RoaringBitmap,
    /// Next WAL sequence number to hand out.
    next_seq: u64,
}

impl std::fmt::Debug for Collection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The WAL handle inside is not printable and not interesting; identify
        // the collection instead.
        f.debug_struct("Collection").field("name", &self.schema.name).finish_non_exhaustive()
    }
}

pub struct Collection {
    schema: CollectionSchema,
    layout: Layout,
    config: EngineConfig,
    inner: RwLock<Inner>,
}

impl Collection {
    /// Create a brand new collection, persisting its schema.
    pub fn create(
        layout: &Layout,
        schema: CollectionSchema,
        config: &EngineConfig,
    ) -> Result<Collection> {
        schema.validate()?;
        meta::write_schema(layout, &schema)?;
        Collection::open(layout, &schema.name, config)
    }

    /// Open an existing collection, replaying any WAL tail.
    pub fn open(layout: &Layout, name: &str, config: &EngineConfig) -> Result<Collection> {
        let schema = meta::read_schema(layout, name)?;
        let state = meta::read_state(layout, name)?;

        let mut deleted = RoaringBitmap::new();
        for id in &state.deleted {
            deleted.insert(*id);
        }

        // Segments before replay: a replayed upsert or delete may reference a
        // document whose only prior version already lives in one of them.
        let mut segments = Vec::with_capacity(state.segments.len());
        for seg_ref in &state.segments {
            let paths = segment_file_paths(layout, name, seg_ref.id);
            segments.push(Arc::new(SegmentReader::open(&paths, &schema)?));
        }

        let mut memtable = MemTable::new(state.next_doc_id, &schema);
        let mut next_seq = state.next_seq;

        let wal_path = layout.wal_file(name, state.wal_generation);
        let scan = wal::read(&wal_path)?;
        let mut replayed = 0usize;

        for record in scan.records {
            if record.seq() <= state.applied_seq {
                // Already durable in a segment.
                continue;
            }
            next_seq = next_seq.max(record.seq() + 1);
            replayed += 1;

            match record {
                WalRecord::Upsert { doc, seq } => {
                    // The document passed validation before it was logged, so
                    // a failure here means the log and the schema disagree.
                    let parsed = ParsedDocument::parse(doc, &schema).map_err(|e| {
                        Error::corruption(format!(
                            "{}: record seq {seq} does not match the collection schema: {e}",
                            wal_path.display()
                        ))
                    })?;
                    Self::apply_upsert(&mut memtable, &mut deleted, &segments, parsed);
                }
                WalRecord::Delete { id, .. } => {
                    Self::apply_delete(&mut memtable, &mut deleted, &segments, &id);
                }
            }
        }

        if replayed > 0 || scan.truncated_at.is_some() {
            tracing::info!(
                collection = name,
                replayed,
                torn_tail = scan.truncated_at.is_some(),
                live_documents = memtable.len(),
                "replayed write-ahead log"
            );
        }

        let wal = Wal::open(&wal_path, config.sync_policy)?;

        Ok(Collection {
            schema,
            layout: layout.clone(),
            config: config.clone(),
            inner: RwLock::new(Inner { state, wal, memtable, segments, deleted, next_seq }),
        })
    }

    pub fn name(&self) -> &str {
        &self.schema.name
    }

    pub fn schema(&self) -> &CollectionSchema {
        &self.schema
    }

    /// Insert or replace documents. Returns one outcome per input document, in
    /// input order.
    pub fn upsert_batch(&self, raw_docs: Vec<Json>) -> Result<BatchReport> {
        // Validate before taking the lock: parsing is the expensive part and
        // it needs nothing but the schema.
        let mut results: Vec<Option<DocOutcome>> = vec![None; raw_docs.len()];
        let mut accepted: Vec<(usize, ParsedDocument)> = Vec::with_capacity(raw_docs.len());
        for (i, raw) in raw_docs.into_iter().enumerate() {
            match ParsedDocument::parse(raw, &self.schema) {
                Ok(parsed) => accepted.push((i, parsed)),
                Err(e) => results[i] = Some(DocOutcome::failed(&e)),
            }
        }

        if !accepted.is_empty() {
            let mut inner = self.inner.write();

            let first_seq = inner.next_seq;
            let records: Vec<WalRecordRef> = accepted
                .iter()
                .enumerate()
                .map(|(n, (_, parsed))| WalRecordRef::Upsert {
                    seq: first_seq + n as u64,
                    doc: &parsed.source,
                })
                .collect();

            // Durable before acknowledged. If this fails nothing has been
            // applied, so the batch is cleanly rejected.
            inner.wal.append_batch(&records)?;
            inner.next_seq = first_seq + accepted.len() as u64;

            {
                let Inner { memtable, deleted, segments, .. } = &mut *inner;
                for (i, parsed) in accepted {
                    let id = parsed.id.clone();
                    Self::apply_upsert(memtable, deleted, segments, parsed);
                    results[i] = Some(DocOutcome::ok(id));
                }
            }

            if Self::needs_flush_locked(&inner, &self.config) {
                Self::flush_locked(&mut inner, &self.schema, &self.layout, &self.config)?;
            }
        }

        let results: Vec<DocOutcome> = results
            .into_iter()
            .map(|r| r.expect("every input document produces an outcome"))
            .collect();
        let num_indexed = results.iter().filter(|r| r.success).count();

        Ok(BatchReport { num_failed: results.len() - num_indexed, num_indexed, results })
    }

    /// Convenience wrapper for a single document.
    pub fn upsert(&self, doc: Json) -> Result<DocOutcome> {
        let mut report = self.upsert_batch(vec![doc])?;
        let outcome = report.results.pop().expect("one input, one outcome");
        Ok(outcome)
    }

    /// Delete by id. Returns `false` if no such document existed.
    pub fn delete(&self, id: &str) -> Result<bool> {
        let mut inner = self.inner.write();
        if Self::lookup(&inner, id).is_none() {
            return Ok(false);
        }

        let seq = inner.next_seq;
        inner.wal.append(&WalRecordRef::Delete { seq, id })?;
        inner.next_seq = seq + 1;

        let removed = {
            let Inner { memtable, deleted, segments, .. } = &mut *inner;
            Self::apply_delete(memtable, deleted, segments, id)
        };

        if Self::needs_flush_locked(&inner, &self.config) {
            Self::flush_locked(&mut inner, &self.schema, &self.layout, &self.config)?;
        }

        Ok(removed)
    }

    /// Fetch a document's source by id.
    pub fn get(&self, id: &str) -> Result<Json> {
        let inner = self.inner.read();
        Self::lookup(&inner, id).and_then(|doc_id| Self::doc_source(&inner, doc_id)).ok_or_else(
            || Error::DocumentNotFound { collection: self.schema.name.clone(), id: id.to_string() },
        )
    }

    /// Run a search (PRD §7.3).
    ///
    /// Holds only a read lock, so searches run concurrently with each other
    /// and are blocked only for the moment a write is applied to the memtable.
    pub fn search(&self, params: SearchParams) -> Result<SearchResponse> {
        let started = std::time::Instant::now();
        let request = SearchRequest::resolve(params, &self.schema)?;

        let inner = self.inner.read();
        let ctx = SearchContext::new(&self.schema, Self::sources(&inner), &inner.deleted);
        let outcome = execute(&ctx, &request);

        let hits = outcome
            .hits
            .iter()
            .filter_map(|scored| {
                // A hit whose document cannot be fetched would mean the index
                // and the doc store disagree; skip rather than serve a hole.
                let document = Self::doc_source(&inner, scored.doc_id)?;
                Some(Hit { document, text_match: scored.score })
            })
            .collect();

        // Facets are counted over every match, not the page, so the numbers
        // answer "what would remain if I also picked this value".
        let facets = (!request.facet_by.is_empty())
            .then(|| compute_facets(&ctx, &request.facet_by, &outcome.matched));

        Ok(SearchResponse {
            found: outcome.found,
            search_time_ms: started.elapsed().as_millis() as u64,
            hits,
            facets,
        })
    }

    /// Autocomplete suggestions for a prefix (PRD §7.5).
    pub fn suggest(&self, params: SuggestParams) -> Result<SuggestResponse> {
        let started = std::time::Instant::now();
        let request = SuggestRequest::resolve(params, &self.schema)?;

        let inner = self.inner.read();
        let ctx = SearchContext::new(&self.schema, Self::sources(&inner), &inner.deleted);
        let suggestions = suggest(&ctx, &request);

        Ok(SuggestResponse { suggestions, search_time_ms: started.elapsed().as_millis() as u64 })
    }

    /// Force everything appended so far to durable storage. Only meaningful
    /// under a relaxed [`tachyon_storage::SyncPolicy`].
    pub fn sync(&self) -> Result<()> {
        self.inner.write().wal.sync()
    }

    pub fn stats(&self) -> CollectionStats {
        let inner = self.inner.read();
        // Not `inner.state.committed_doc_count()`: that subtracts
        // `state.deleted`, the on-disk snapshot as of the *last* flush.
        // `inner.deleted` is the live tombstone set, which grows on every
        // delete or upsert-supersedes-a-segment-doc between flushes — using
        // the stale snapshot here would overcount until the next flush
        // happens to catch up.
        let segment_docs: u64 = inner.state.segments.iter().map(|s| s.doc_count as u64).sum();
        let num_documents =
            segment_docs.saturating_sub(inner.deleted.len()) + inner.memtable.len() as u64;
        CollectionStats {
            name: self.schema.name.clone(),
            num_documents,
            num_segments: inner.state.segments.len(),
            memtable_documents: inner.memtable.len(),
            memtable_bytes: inner.memtable.heap_bytes(),
            wal_bytes: inner.wal.size(),
            created_at: self.schema.created_at,
        }
    }

    /// Whether the memtable has grown past its flush thresholds.
    pub fn needs_flush(&self) -> bool {
        Self::needs_flush_locked(&self.inner.read(), &self.config)
    }

    /// Flush the memtable into a new immutable segment, if anything has been
    /// allocated since the last flush. Returns whether a flush happened.
    ///
    /// Ordinarily triggered automatically once a write crosses
    /// [`EngineConfig::max_memtable_docs`] or `max_memtable_bytes`; exposed
    /// directly for tests and for an operator forcing an early flush.
    pub fn flush(&self) -> Result<bool> {
        let mut inner = self.inner.write();
        Self::flush_locked(&mut inner, &self.schema, &self.layout, &self.config)
    }

    /// Data directory this collection occupies.
    pub fn directory(&self) -> std::path::PathBuf {
        self.layout.collection_dir(&self.schema.name)
    }

    // --- internals -------------------------------------------------------

    fn needs_flush_locked(inner: &Inner, config: &EngineConfig) -> bool {
        inner.memtable.len() >= config.max_memtable_docs
            || inner.memtable.heap_bytes() >= config.max_memtable_bytes
    }

    /// Runs under the same write-lock acquisition as the mutation that
    /// triggered it — encoding, committing, and retiring the old WAL
    /// generation all happen before any other write can observe `inner`.
    /// Splitting this into separate lock acquisitions would let a concurrent
    /// write's WAL record land with a `seq` at or below the `applied_seq`
    /// this flush is about to commit, silently excluding it from replay
    /// forever.
    fn flush_locked(
        inner: &mut Inner,
        schema: &CollectionSchema,
        layout: &Layout,
        config: &EngineConfig,
    ) -> Result<bool> {
        // Not `memtable.is_empty()`: a memtable can hold zero *live* documents
        // (everything since the last flush was also deleted) while still
        // holding postings and columns that need reclaiming — exactly the
        // case `heap_bytes()`-driven flushing exists to catch.
        if inner.memtable.next_doc_id() == inner.memtable.base() {
            return Ok(false);
        }

        let segment_id = inner.state.next_segment_id;
        let encoded = encode(&inner.memtable, schema)?;
        let paths = segment_file_paths(layout, &schema.name, segment_id);

        write_atomic(&paths.terms, &encoded.terms)?;
        write_atomic(&paths.ids, &encoded.ids)?;
        write_atomic(&paths.post, &encoded.post)?;
        write_atomic(&paths.col, &encoded.col)?;
        write_atomic(&paths.doc, &encoded.doc)?;
        sync_dir(&layout.segments_dir(&schema.name))?;

        let new_wal_generation = inner.state.wal_generation + 1;
        let new_wal =
            Wal::open(layout.wal_file(&schema.name, new_wal_generation), config.sync_policy)?;

        let mut new_state = inner.state.clone();
        new_state.next_segment_id += 1;
        new_state.next_doc_id = inner.memtable.next_doc_id();
        new_state.applied_seq = inner.next_seq - 1;
        new_state.wal_generation = new_wal_generation;
        new_state.deleted = inner.deleted.iter().collect();
        new_state.segments.push(SegmentRef {
            id: segment_id,
            doc_count: inner.memtable.len() as u32,
            min_doc_id: inner.memtable.base(),
            max_doc_id: inner.memtable.next_doc_id() - 1,
        });

        // The commit point: a crash before this leaves the previous state
        // intact and the segment files just written orphaned but harmless,
        // since nothing outside `state.json` ever names a segment id.
        meta::write_state(layout, &schema.name, &new_state)?;

        // Everything below is the in-memory mirror of what was just made
        // durable. `inner.state` in particular must be kept current, not
        // just written to disk — the *next* flush reads `inner.state` to
        // compute `next_segment_id` and to extend `segments`, and a stale
        // read here would silently drop this segment's `SegmentRef` from
        // that later write even though its files are still on disk.
        inner.state = new_state;
        inner.segments.push(Arc::new(SegmentReader::open(&paths, schema)?));
        inner.memtable = MemTable::new(inner.state.next_doc_id, schema);
        let old_wal = std::mem::replace(&mut inner.wal, new_wal);

        // Last: losing this before the state.json rename would be
        // unrecoverable (the WAL is the only durable copy), but losing it
        // after is just a harmless leftover file.
        old_wal.remove()?;

        Ok(true)
    }

    /// Every source a search or suggest request reads through: the memtable,
    /// then every committed segment, oldest first — matching the executor's
    /// "memtable first, then committed segments" contract.
    fn sources(inner: &Inner) -> Vec<&dyn IndexSource> {
        let mut sources: Vec<&dyn IndexSource> = Vec::with_capacity(1 + inner.segments.len());
        sources.push(&inner.memtable);
        sources.extend(inner.segments.iter().map(|s| s.as_ref() as &dyn IndexSource));
        sources
    }

    /// A document's stored source, wherever it lives.
    fn doc_source(inner: &Inner, doc_id: DocId) -> Option<Json> {
        if let Some(doc) = inner.memtable.get(doc_id) {
            return Some(doc.source.clone());
        }
        inner.segments.iter().rev().find_map(|segment| segment.get(doc_id))
    }

    /// Resolve a user-facing id to an internal doc id.
    ///
    /// The memtable holds the newest version of everything written since the
    /// last flush, so it is consulted first; only then do committed segments,
    /// newest first. A segment-resident id tombstoned by a later delete must
    /// never be returned.
    fn lookup(inner: &Inner, id: &str) -> Option<DocId> {
        if let Some(doc_id) = inner.memtable.lookup(id) {
            return Some(doc_id);
        }
        inner.segments.iter().rev().find_map(|segment| segment.lookup_id(id)).filter(|doc_id| {
            !inner.deleted.contains(*doc_id)
        })
    }

    /// Insert a document, retiring any previous version of the same id —
    /// whether that version lives in the memtable or a committed segment.
    fn apply_upsert(
        memtable: &mut MemTable,
        deleted: &mut RoaringBitmap,
        segments: &[Arc<SegmentReader>],
        doc: ParsedDocument,
    ) {
        let id = doc.id.clone();
        let previous_in_memtable = memtable.lookup(&id);
        // Insert first: the memtable keys ids to the newest doc id, and
        // removing the old version afterwards must not clear that mapping.
        memtable.insert(doc);
        if let Some(old) = previous_in_memtable {
            memtable.remove(old);
        } else {
            // The previous version, if any, lives in a committed segment —
            // tombstone it there so the same id doesn't resolve to two
            // documents (stale in the segment, fresh in the memtable).
            if let Some(old_doc_id) =
                segments.iter().rev().find_map(|segment| segment.lookup_id(&id))
            {
                deleted.insert(old_doc_id);
            }
        }
    }

    /// Retire the current version of `id`, if any. Returns whether it existed.
    fn apply_delete(
        memtable: &mut MemTable,
        deleted: &mut RoaringBitmap,
        segments: &[Arc<SegmentReader>],
        id: &str,
    ) -> bool {
        if let Some(doc_id) = memtable.lookup(id) {
            return memtable.remove(doc_id);
        }
        let Some(doc_id) = segments.iter().rev().find_map(|segment| segment.lookup_id(id)) else {
            return false;
        };
        if deleted.contains(doc_id) {
            return false; // already tombstoned — deleting twice is not an error
        }
        deleted.insert(doc_id);
        true
    }

    #[cfg(test)]
    fn read_inner(&self) -> parking_lot::RwLockReadGuard<'_, Inner> {
        self.inner.read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tachyon_core::{FieldSchema, FieldType};

    fn schema() -> CollectionSchema {
        CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text).required(),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("price", FieldType::Int).with_filter(true).with_sort(true),
            ],
        )
    }

    struct Harness {
        dir: tempfile::TempDir,
        layout: Layout,
        config: EngineConfig,
    }

    impl Harness {
        fn new() -> Harness {
            let dir = tempfile::tempdir().unwrap();
            let layout = Layout::new(dir.path());
            layout.initialize().unwrap();
            let config = EngineConfig::new(dir.path());
            Harness { dir, layout, config }
        }

        fn create(&self) -> Collection {
            Collection::create(&self.layout, schema(), &self.config).unwrap()
        }

        /// Reopen from disk, as a restart would.
        fn reopen(&self) -> Collection {
            Collection::open(&self.layout, "products", &self.config).unwrap()
        }
    }

    fn product(id: &str, title: &str, price: i64) -> Json {
        json!({ "id": id, "title": title, "brand": "Logitech", "price": price })
    }

    #[test]
    fn indexes_and_reads_back() {
        let h = Harness::new();
        let c = h.create();
        let report = c
            .upsert_batch(vec![
                product("1", "Wireless Mouse", 2999),
                product("2", "Keyboard", 4999),
            ])
            .unwrap();
        assert_eq!(report.num_indexed, 2);
        assert_eq!(report.num_failed, 0);
        assert_eq!(c.get("1").unwrap()["title"], json!("Wireless Mouse"));
        assert_eq!(c.stats().num_documents, 2);
    }

    #[test]
    fn bad_documents_fail_individually() {
        let h = Harness::new();
        let c = h.create();
        let report = c
            .upsert_batch(vec![
                product("1", "Mouse", 2999),
                json!({ "id": "2" }),        // missing required title
                json!({ "title": "no id" }), // missing id
                json!({ "id": "3", "title": "M", "price": "cheap" }), // wrong type
                product("4", "Keyboard", 4999),
            ])
            .unwrap();

        assert_eq!(report.num_indexed, 2);
        assert_eq!(report.num_failed, 3);
        assert!(report.results[0].success);
        assert!(!report.results[1].success);
        assert_eq!(report.results[1].code, Some("invalid_document"));
        assert!(!report.results[2].success);
        assert!(!report.results[3].success);
        assert!(report.results[4].success);
        assert_eq!(c.stats().num_documents, 2);
        // The neighbours really did land.
        assert!(c.get("1").is_ok());
        assert!(c.get("4").is_ok());
        assert!(c.get("2").is_err());
    }

    #[test]
    fn upsert_replaces_by_id() {
        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "Mouse", 2999)).unwrap();
        c.upsert(product("1", "Better Mouse", 3999)).unwrap();

        assert_eq!(c.get("1").unwrap()["title"], json!("Better Mouse"));
        assert_eq!(c.stats().num_documents, 1, "a replacement is not a second document");
    }

    #[test]
    fn duplicate_ids_within_one_batch_keep_the_last() {
        let h = Harness::new();
        let c = h.create();
        c.upsert_batch(vec![product("1", "First", 1), product("1", "Second", 2)]).unwrap();
        assert_eq!(c.get("1").unwrap()["title"], json!("Second"));
        assert_eq!(c.stats().num_documents, 1);
    }

    #[test]
    fn deletes_are_reported_and_idempotent() {
        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "Mouse", 2999)).unwrap();

        assert!(c.delete("1").unwrap());
        assert!(!c.delete("1").unwrap(), "deleting twice is not an error");
        assert!(!c.delete("never-existed").unwrap());
        assert!(matches!(c.get("1"), Err(Error::DocumentNotFound { .. })));
        assert_eq!(c.stats().num_documents, 0);
    }

    #[test]
    fn state_survives_a_restart() {
        let h = Harness::new();
        {
            let c = h.create();
            c.upsert_batch(vec![
                product("1", "Wireless Mouse", 2999),
                product("2", "Keyboard", 4999),
                product("3", "Monitor", 19999),
            ])
            .unwrap();
            c.upsert(product("2", "Mechanical Keyboard", 8999)).unwrap();
            c.delete("3").unwrap();
        }

        let c = h.reopen();
        assert_eq!(c.stats().num_documents, 2);
        assert_eq!(c.get("1").unwrap()["title"], json!("Wireless Mouse"));
        assert_eq!(c.get("2").unwrap()["title"], json!("Mechanical Keyboard"));
        assert!(c.get("3").is_err(), "the delete survived the restart");
    }

    #[test]
    fn replay_reassigns_the_same_doc_ids() {
        let h = Harness::new();
        let before: Vec<DocId> = {
            let c = h.create();
            c.upsert_batch(vec![product("1", "a", 1), product("2", "b", 2)]).unwrap();
            let inner = c.read_inner();
            vec![inner.memtable.lookup("1").unwrap(), inner.memtable.lookup("2").unwrap()]
        };

        let c = h.reopen();
        let inner = c.read_inner();
        let after = vec![inner.memtable.lookup("1").unwrap(), inner.memtable.lookup("2").unwrap()];
        assert_eq!(before, after);
    }

    #[test]
    fn writes_continue_after_a_restart() {
        let h = Harness::new();
        {
            let c = h.create();
            c.upsert(product("1", "Mouse", 2999)).unwrap();
        }
        {
            let c = h.reopen();
            c.upsert(product("2", "Keyboard", 4999)).unwrap();
        }
        let c = h.reopen();
        assert_eq!(c.stats().num_documents, 2);
        assert!(c.get("1").is_ok());
        assert!(c.get("2").is_ok());
    }

    #[test]
    fn survives_a_torn_wal_tail() {
        use std::io::Write;

        let h = Harness::new();
        {
            let c = h.create();
            c.upsert_batch(vec![product("1", "Mouse", 2999), product("2", "Keyboard", 4999)])
                .unwrap();
        }

        // Simulate the process dying part-way through appending a third record.
        let wal_path = h.layout.wal_file("products", 1);
        let mut file = std::fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[200u8, 0, 0, 0, 7, 7, 7, 7, 1, 2, 3]).unwrap();
        drop(file);

        let c = h.reopen();
        assert_eq!(c.stats().num_documents, 2, "acknowledged writes survived");
        // And the collection is writable again.
        c.upsert(product("3", "Monitor", 19999)).unwrap();
        assert_eq!(h.reopen().stats().num_documents, 3);
    }

    #[test]
    fn creating_twice_conflicts() {
        let h = Harness::new();
        let _c = h.create();
        let err = Collection::create(&h.layout, schema(), &h.config).unwrap_err();
        assert!(matches!(err, Error::CollectionExists(_)));
    }

    #[test]
    fn an_invalid_schema_is_rejected_before_anything_is_written() {
        let h = Harness::new();
        let bad = CollectionSchema::new("bad", vec![]);
        assert!(Collection::create(&h.layout, bad, &h.config).is_err());
        assert!(!h.layout.collection_dir("bad").join("schema.json").exists());
        let _ = &h.dir;
    }

    #[test]
    fn a_relaxed_sync_policy_still_recovers_after_an_explicit_sync() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        let config =
            EngineConfig::new(dir.path()).with_sync_policy(tachyon_storage::SyncPolicy::Never);

        {
            let c = Collection::create(&layout, schema(), &config).unwrap();
            c.upsert(product("1", "Mouse", 2999)).unwrap();
            c.sync().unwrap();
        }
        let c = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(c.stats().num_documents, 1);
    }

    // --- segment flush -----------------------------------------------------

    #[test]
    fn flush_produces_a_segment_and_empties_the_memtable() {
        let h = Harness::new();
        let c = h.create();
        c.upsert_batch(vec![product("1", "Wireless Mouse", 2999), product("2", "Keyboard", 4999)])
            .unwrap();

        assert!(c.flush().unwrap());
        assert_eq!(c.stats().num_segments, 1);
        assert_eq!(c.stats().memtable_documents, 0);
        assert_eq!(c.stats().num_documents, 2);
        assert_eq!(c.get("1").unwrap()["title"], json!("Wireless Mouse"));

        assert!(!c.flush().unwrap(), "nothing allocated since the last flush");
    }

    #[test]
    fn flush_triggers_automatically_past_the_doc_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        let config = EngineConfig::new(dir.path()).with_max_memtable_docs(2);

        let c = Collection::create(&layout, schema(), &config).unwrap();
        c.upsert_batch(vec![product("1", "a", 1), product("2", "b", 2)]).unwrap();

        assert_eq!(c.stats().num_segments, 1, "crossing the threshold must flush automatically");
        assert_eq!(c.stats().memtable_documents, 0);
        assert_eq!(c.get("1").unwrap()["title"], json!("a"));
    }

    #[test]
    fn flushed_data_survives_a_restart_with_replay_bounded_to_post_flush_records() {
        let h = Harness::new();
        {
            let c = h.create();
            c.upsert_batch(vec![product("1", "a", 1), product("2", "b", 2)]).unwrap();
            assert!(c.flush().unwrap());
            c.upsert(product("3", "c", 3)).unwrap(); // one record after the flush
        }

        let c = h.reopen();
        assert_eq!(c.stats().num_documents, 3);
        assert_eq!(c.get("1").unwrap()["title"], json!("a"));
        assert_eq!(c.get("3").unwrap()["title"], json!("c"));

        // Replay must not need anything the segment already captured.
        let generation = c.read_inner().state.wal_generation;
        let records = wal::read(&h.layout.wal_file("products", generation)).unwrap().records;
        assert_eq!(records.len(), 1, "only the post-flush record should remain in the WAL");
    }

    #[test]
    fn upserting_a_segment_resident_document_is_visible_exactly_once() {
        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "Old", 100)).unwrap();
        assert!(c.flush().unwrap());

        c.upsert(product("1", "New", 200)).unwrap();

        assert_eq!(c.get("1").unwrap()["title"], json!("New"));
        assert_eq!(
            c.stats().num_documents,
            1,
            "the segment's stale copy must be tombstoned, not left visible alongside the new one"
        );
    }

    #[test]
    fn deleting_a_segment_resident_document_is_durable_across_a_restart() {
        let h = Harness::new();
        {
            let c = h.create();
            c.upsert(product("1", "Mouse", 100)).unwrap();
            assert!(c.flush().unwrap());

            assert!(c.delete("1").unwrap());
            assert!(!c.delete("1").unwrap(), "deleting an already-tombstoned segment doc is a no-op");
            assert!(matches!(c.get("1"), Err(Error::DocumentNotFound { .. })));
        }

        let c = h.reopen();
        assert!(matches!(c.get("1"), Err(Error::DocumentNotFound { .. })));
        assert_eq!(c.stats().num_documents, 0);
    }

    #[test]
    fn two_flushes_in_a_row_both_survive_a_restart() {
        // Regression test: a flush that forgets to keep `inner.state` current
        // in memory would compute the second flush's `SegmentRef` list from
        // the stale pre-first-flush state, silently dropping the first
        // segment from state.json even though its files are still on disk.
        let h = Harness::new();
        {
            let c = h.create();
            c.upsert(product("1", "a", 1)).unwrap();
            assert!(c.flush().unwrap());
            c.upsert(product("2", "b", 2)).unwrap();
            assert!(c.flush().unwrap());
            assert_eq!(c.stats().num_segments, 2);
        }

        let c = h.reopen();
        assert_eq!(c.stats().num_segments, 2);
        assert_eq!(c.stats().num_documents, 2);
        assert_eq!(c.get("1").unwrap()["title"], json!("a"));
        assert_eq!(c.get("2").unwrap()["title"], json!("b"));
    }

    #[test]
    fn an_all_deleted_memtable_still_flushes_cleanly() {
        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "a", 1)).unwrap();
        c.delete("1").unwrap();

        assert!(c.flush().unwrap(), "a doc id was allocated even though nothing is live");
        assert_eq!(c.stats().num_segments, 1);
        assert_eq!(c.stats().num_documents, 0);
        assert!(matches!(c.get("1"), Err(Error::DocumentNotFound { .. })));

        let reopened = h.reopen();
        assert_eq!(reopened.stats().num_segments, 1);
        assert_eq!(reopened.stats().num_documents, 0);
    }

    #[test]
    fn a_stale_orphaned_segment_id_is_safely_overwritten_by_a_real_flush() {
        // No directory scan ever runs on open — only ids named in
        // `state.json` are opened — so files left behind by a crash between
        // writing segment data and committing state.json are inert, and a
        // later flush reusing that id must simply overwrite them.
        let h = Harness::new();
        let c = h.create();

        std::fs::create_dir_all(h.layout.segments_dir("products")).unwrap();
        for ext in ["terms", "ids", "post", "col", "doc"] {
            std::fs::write(h.layout.segment_file("products", 1, ext), b"garbage").unwrap();
        }

        c.upsert(product("1", "Mouse", 100)).unwrap();
        assert!(c.flush().unwrap());
        assert_eq!(c.stats().num_segments, 1);
        assert_eq!(c.get("1").unwrap()["title"], json!("Mouse"));

        let reopened = h.reopen();
        assert_eq!(reopened.stats().num_segments, 1);
        assert_eq!(reopened.get("1").unwrap()["title"], json!("Mouse"));
    }

    #[test]
    fn search_and_suggest_see_documents_in_both_memtable_and_segments() {
        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "Wireless Mouse", 2999)).unwrap();
        assert!(c.flush().unwrap());
        c.upsert(product("2", "Mechanical Keyboard", 8999)).unwrap();

        let results = c
            .search(SearchParams {
                q: Some("mouse keyboard".into()),
                match_mode: Some("any".into()),
                ..Default::default()
            })
            .unwrap();
        let ids: std::collections::HashSet<_> =
            results.hits.iter().map(|h| h.document["id"].as_str().unwrap().to_string()).collect();
        assert!(ids.contains("1"), "the segment-resident document must be found");
        assert!(ids.contains("2"), "the memtable-resident document must be found");

        let suggestions =
            c.suggest(SuggestParams { q: Some("mo".into()), ..Default::default() }).unwrap();
        assert!(!suggestions.suggestions.is_empty());
    }
}
