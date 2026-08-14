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

use parking_lot::RwLock;
use roaring::RoaringBitmap;
use serde::Serialize;
use serde_json::Value as Json;
use utoipa::ToSchema;

use tachyon_core::{CollectionSchema, DocId, Error, ParsedDocument, Result};
use tachyon_index::MemTable;
use tachyon_query::{
    compute_facets, execute, suggest, Hit, SearchContext, SearchParams, SearchRequest,
    SearchResponse, SuggestParams, SuggestRequest, SuggestResponse,
};
use tachyon_storage::{meta, wal, CollectionState, Layout, Wal, WalRecord, WalRecordRef};

use crate::config::EngineConfig;

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
                    Self::apply_upsert(&mut memtable, parsed);
                }
                WalRecord::Delete { id, .. } => {
                    Self::apply_delete(&mut memtable, &mut deleted, &id);
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
            inner: RwLock::new(Inner { state, wal, memtable, deleted, next_seq }),
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

            let memtable = &mut inner.memtable;
            for (i, parsed) in accepted {
                let id = parsed.id.clone();
                Self::apply_upsert(memtable, parsed);
                results[i] = Some(DocOutcome::ok(id));
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

        let Inner { memtable, deleted, .. } = &mut *inner;
        Ok(Self::apply_delete(memtable, deleted, id))
    }

    /// Fetch a document's source by id.
    pub fn get(&self, id: &str) -> Result<Json> {
        let inner = self.inner.read();
        match Self::lookup(&inner, id).and_then(|doc_id| inner.memtable.get(doc_id)) {
            Some(doc) => Ok(doc.source.clone()),
            None => Err(Error::DocumentNotFound {
                collection: self.schema.name.clone(),
                id: id.to_string(),
            }),
        }
    }

    /// Run a search (PRD §7.3).
    ///
    /// Holds only a read lock, so searches run concurrently with each other
    /// and are blocked only for the moment a write is applied to the memtable.
    pub fn search(&self, params: SearchParams) -> Result<SearchResponse> {
        let started = std::time::Instant::now();
        let request = SearchRequest::resolve(params, &self.schema)?;

        let inner = self.inner.read();
        let ctx = SearchContext::new(&self.schema, vec![&inner.memtable], &inner.deleted);
        let outcome = execute(&ctx, &request);

        let hits = outcome
            .hits
            .iter()
            .filter_map(|scored| {
                // A hit whose document cannot be fetched would mean the index
                // and the doc store disagree; skip rather than serve a hole.
                let doc = inner.memtable.get(scored.doc_id)?;
                Some(Hit { document: doc.source.clone(), text_match: scored.score })
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
        let ctx = SearchContext::new(&self.schema, vec![&inner.memtable], &inner.deleted);
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
        CollectionStats {
            name: self.schema.name.clone(),
            num_documents: inner.state.committed_doc_count() + inner.memtable.len() as u64,
            num_segments: inner.state.segments.len(),
            memtable_documents: inner.memtable.len(),
            memtable_bytes: inner.memtable.heap_bytes(),
            wal_bytes: inner.wal.size(),
            created_at: self.schema.created_at,
        }
    }

    /// Whether the memtable has grown past its flush thresholds.
    pub fn needs_flush(&self) -> bool {
        let inner = self.inner.read();
        inner.memtable.len() >= self.config.max_memtable_docs
            || inner.memtable.heap_bytes() >= self.config.max_memtable_bytes
    }

    /// Data directory this collection occupies.
    pub fn directory(&self) -> std::path::PathBuf {
        self.layout.collection_dir(&self.schema.name)
    }

    // --- internals -------------------------------------------------------

    /// Resolve a user-facing id to an internal doc id.
    ///
    /// The memtable holds the newest version of everything written since the
    /// last flush, so it is consulted first; only then do committed segments
    /// matter.
    fn lookup(inner: &Inner, id: &str) -> Option<DocId> {
        if let Some(doc_id) = inner.memtable.lookup(id) {
            return Some(doc_id);
        }
        // Segments are searched newest-first once they exist (M2); a tombstoned
        // doc id must never be returned.
        let _ = &inner.deleted;
        None
    }

    /// Insert a document, retiring any previous version of the same id.
    fn apply_upsert(memtable: &mut MemTable, doc: ParsedDocument) {
        let previous = memtable.lookup(&doc.id);
        // Insert first: the memtable keys ids to the newest doc id, and
        // removing the old version afterwards must not clear that mapping.
        memtable.insert(doc);
        if let Some(old) = previous {
            memtable.remove(old);
        }
    }

    /// Retire the current version of `id`, if any. Returns whether it existed.
    fn apply_delete(memtable: &mut MemTable, deleted: &mut RoaringBitmap, id: &str) -> bool {
        match memtable.lookup(id) {
            Some(doc_id) => memtable.remove(doc_id),
            None => {
                // Nothing in memory; once segments exist the id is resolved
                // against them and tombstoned here.
                let _ = deleted;
                false
            }
        }
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
}
