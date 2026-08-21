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

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use roaring::RoaringBitmap;
use serde::Serialize;
use serde_json::Value as Json;
use utoipa::ToSchema;

use tachyon_core::{CollectionSchema, DocId, Error, ParsedDocument, Result};
use tachyon_index::{
    encode_streaming, merge_segments, IndexSource, MemTable, MergeInput, MergeStats,
    SegmentFilePaths, SegmentReader,
};
use tachyon_query::{
    compute_facets, execute, suggest, Hit, SearchContext, SearchParams, SearchRequest,
    SearchResponse, SuggestParams, SuggestRequest, SuggestResponse,
};
use tachyon_storage::layout::sync_dir;
use tachyon_storage::{
    meta, wal, CollectionState, Layout, SegmentRef, Wal, WalRecord, WalRecordRef,
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

/// `.tmp`-suffixed sibling of a segment file path, used while its bytes are
/// still streaming to disk. Distinct from `write_atomic`'s own `.tmp`
/// convention (which replaces the extension, fine for one file written and
/// renamed at a time) because [`SegmentFiles`] has all five of one segment's
/// files open and being streamed into at once — replacing the extension
/// would collide every one of them on the same `<id>.tmp` path.
fn segment_tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Buffer size for each of the five open files' `BufWriter`. `encode_streaming`
/// and `merge_segments` write in small increments — a `u32` or `u64` field at
/// a time — so wrapping the raw `File` matters a great deal here: without it,
/// every one of those tiny writes is its own `write(2)` syscall, which turned
/// out to cost over an order of magnitude of indexing throughput when this
/// was benchmarked unbuffered. 256 KiB comfortably holds even a wide term's
/// or a large document's worth of writes before it needs to flush.
const SEGMENT_WRITE_BUF_SIZE: usize = 256 * 1024;

/// How far past its ordinary flush thresholds the active memtable is allowed
/// to grow while a flush's build phase is already in progress before a
/// write blocks on `flush_gate` instead of returning immediately. Without
/// this, ingest that outruns the build phase would let the memtable grow
/// without bound — `maybe_flush`'s ordinary `try_lock`-and-skip is meant to
/// avoid serializing writers behind a flush that's already running, not to
/// let one run forever behind an ever-growing memtable.
const FLUSH_BACKPRESSURE_MULTIPLE: usize = 2;

/// The five open files one segment's bytes stream into — the same durability
/// shape `write_atomic` gives a single file (write to a temp sibling, fsync,
/// rename, fsync the directory), just committed as one group of five rather
/// than one file at a time, since [`tachyon_index::encode_streaming`] and
/// [`tachyon_index::merge_segments`] write across all five in a single pass
/// rather than producing one finished blob per file.
struct SegmentFiles {
    terms: BufWriter<File>,
    ids: BufWriter<File>,
    post: BufWriter<File>,
    col: BufWriter<File>,
    doc: BufWriter<File>,
    tmp: SegmentFilePaths,
    dest: SegmentFilePaths,
}

impl SegmentFiles {
    fn create(paths: &SegmentFilePaths) -> Result<SegmentFiles> {
        let parent = paths
            .terms
            .parent()
            .ok_or_else(|| Error::internal("segment path has no parent directory"))?;
        std::fs::create_dir_all(parent)?;
        let tmp = SegmentFilePaths {
            terms: segment_tmp_path(&paths.terms),
            ids: segment_tmp_path(&paths.ids),
            post: segment_tmp_path(&paths.post),
            col: segment_tmp_path(&paths.col),
            doc: segment_tmp_path(&paths.doc),
        };
        let buffered = |path: &Path| -> Result<BufWriter<File>> {
            Ok(BufWriter::with_capacity(SEGMENT_WRITE_BUF_SIZE, File::create(path)?))
        };
        Ok(SegmentFiles {
            terms: buffered(&tmp.terms)?,
            ids: buffered(&tmp.ids)?,
            post: buffered(&tmp.post)?,
            col: buffered(&tmp.col)?,
            doc: buffered(&tmp.doc)?,
            tmp,
            dest: paths.clone(),
        })
    }

    /// Flush each `BufWriter`'s remaining bytes, fsync every underlying
    /// file, rename each into place, then fsync the directory — the commit
    /// point: a crash before the renames leaves the previous state intact
    /// and these temp files orphaned but harmless, since nothing outside
    /// `state.json` ever names a segment id.
    fn commit(self, segments_dir: &Path) -> Result<()> {
        fn finish(mut w: BufWriter<File>) -> Result<()> {
            w.flush()?;
            w.into_inner()
                .map_err(|e| Error::internal(format!("flushing segment file: {e}")))?
                .sync_all()?;
            Ok(())
        }
        finish(self.terms)?;
        finish(self.ids)?;
        finish(self.post)?;
        finish(self.col)?;
        finish(self.doc)?;
        std::fs::rename(&self.tmp.terms, &self.dest.terms)?;
        std::fs::rename(&self.tmp.ids, &self.dest.ids)?;
        std::fs::rename(&self.tmp.post, &self.dest.post)?;
        std::fs::rename(&self.tmp.col, &self.dest.col)?;
        std::fs::rename(&self.tmp.doc, &self.dest.doc)?;
        sync_dir(segments_dir)?;
        Ok(())
    }
}

/// Everything an off-lock merge's build phase (`Collection::build_merge`)
/// needs, captured under the write lock (`Collection::snapshot_merge_locked`)
/// so nothing about which documents are being merged, or which output ids
/// they'll land at, can change while that phase runs without holding it.
struct MergeSnapshot {
    /// Reserved from `state.next_segment_id` at snapshot time — see
    /// `snapshot_merge_locked`'s doc comment for why that can't wait.
    segment_id: u64,
    /// Reserved from the active memtable's `next_doc_id()` at snapshot
    /// time, for the same reason.
    merge_base: DocId,
    /// Total live documents across every victim, as of the snapshot —
    /// `sum(live[i].len())`. Also the size of the range reserved at
    /// `merge_base`, whether or not a document later gets tombstoned before
    /// the merge commits (see `swap_merge_locked`'s tombstone remap).
    claimed: DocId,
    victim_refs: Vec<SegmentRef>,
    /// Kept alive so the build phase can read from them; also pins the
    /// underlying files against deletion, though nothing else in this
    /// codebase ever deletes a segment's files except a merge retiring its
    /// own victims, and `merge_gate` guarantees only one merge is ever in
    /// flight.
    victim_readers: Vec<Arc<SegmentReader>>,
    /// Each victim's own surviving doc ids as of this snapshot — needed
    /// again at swap time to remap any tombstone that lands on one of them
    /// while the build phase is running (see `swap_merge_locked`).
    live: Vec<RoaringBitmap>,
}

/// Everything an off-lock flush's build phase (`Collection::build_flush`)
/// needs, captured under the write lock (`Collection::snapshot_flush_locked`)
/// so nothing about which documents are being flushed, or which segment id
/// they'll land at, can change while that phase runs without holding it.
///
/// Unlike a merge's snapshot, this one's `memtable` is exactly what the
/// output segment will contain — a flush preserves doc ids rather than
/// renumbering them, so nothing here needs the rank-based tombstone remap
/// `MergeSnapshot`/`swap_merge_locked` carry for merges; a delete or
/// supersede landing on a sealed id during the build is just a tombstone
/// under that same id (see `apply_delete`/`apply_upsert`'s `frozen` branch).
struct FlushSnapshot {
    segment_id: u64,
    /// The sealed memtable. Also installed at `inner.frozen` for the
    /// duration of the build, so searches, lookups, and deletes keep seeing
    /// it exactly like a committed segment.
    memtable: Arc<MemTable>,
    /// `next_seq - 1` as of the seal — the WAL checkpoint this flush will
    /// commit. Captured here, not recomputed at commit time: by commit time
    /// a concurrent write's record may already live in the *new* WAL
    /// generation, and recomputing from `inner.next_seq` then would
    /// silently claim credit for a record this flush never captured.
    applied_seq: u64,
    /// The generation new writes land in while this flush is in flight —
    /// becomes `state.wal_generation` at commit, and every generation below
    /// it is deleted then (its records are, by construction, all durably
    /// captured either in this segment or an earlier one).
    new_wal_generation: u64,
    doc_count: u32,
    min_doc_id: DocId,
    max_doc_id: DocId,
    /// `memtable.next_doc_id()` as of the seal, folded into the committed
    /// state via `max` at commit — see `commit_flush_locked` for why a
    /// plain assignment would be wrong.
    next_doc_id: DocId,
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
    /// Sealed by an in-flight off-lock flush's snapshot stage
    /// (`Collection::snapshot_flush_locked`), immutable from that moment
    /// until the flush commits (`Collection::commit_flush_locked`, which
    /// clears this) or its build fails and the sealed memtable is handed
    /// back for retry. At most one can ever exist at once — `flush_gate`
    /// guarantees it. Consulted between the active memtable and committed
    /// segments everywhere a lookup, read, or delete walks `Inner`'s
    /// sources, exactly like one more (not-yet-durable) segment.
    frozen: Option<Arc<MemTable>>,
    /// Committed segments, oldest first. Consulted newest-first (`.rev()`)
    /// after a memtable miss, since a later segment's version of an id, if
    /// any, is the current one.
    segments: Vec<Arc<SegmentReader>>,
    /// Tombstones for doc ids that live in a frozen memtable or a committed
    /// segment. The active memtable's own documents are deleted in place,
    /// so they never appear here.
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
    /// Held for the duration of a merge's build phase (see `run_merge`), so
    /// at most one merge is ever in flight — `inner`'s write lock is
    /// deliberately *not* held for that whole duration, which is the entire
    /// point of an off-lock merge, so it can no longer serialize merges
    /// against each other the way it used to. `merge()` (an explicit,
    /// caller-requested merge) blocks on this; `maybe_merge()` (the
    /// automatic post-write check) uses `try_lock` and simply skips if a
    /// merge is already running — the next write will check again.
    merge_gate: Mutex<()>,
    /// Held for the duration of a flush's build phase, the same role
    /// `merge_gate` plays for a merge — see that field's doc comment for
    /// why the write lock is deliberately not held there instead. Because a
    /// flush's snapshot stage populates `inner.frozen`, this also caps the
    /// WAL generation chain `Collection::open` must be able to replay at
    /// two: at most one flush is ever sealed-but-uncommitted at a time.
    ///
    /// Doubles as where a failed build's retry state lives: if
    /// `Collection::build_flush` errors, the sealed `FlushSnapshot` is
    /// handed back into this `Option` instead of being discarded (the
    /// memtable it points to stays live at `inner.frozen` throughout), and
    /// the next call to acquire this gate resumes from it — rebuilding the
    /// same segment — rather than sealing a second one.
    flush_gate: Mutex<Option<FlushSnapshot>>,
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
        let mut state = meta::read_state(layout, name)?;

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

        // Normally exactly one generation exists: `state.wal_generation`
        // itself. A crash between an off-lock flush sealing a generation and
        // committing its segment leaves that one plus the fresh one it
        // rolled to — both still needed to reconstruct the memtable the
        // never-committed segment would have held. `state.wal_generation`
        // (see its doc comment) is defined as the *oldest* generation not
        // yet captured, which is exactly the lower bound replay needs; any
        // generation below it on disk is a leftover from a flush that *did*
        // commit but crashed before its own cleanup, already durably
        // superseded and safe to delete outright.
        let all_gens = layout.list_wal_generations(name)?;
        for &gen in &all_gens {
            if gen < state.wal_generation {
                let _ = std::fs::remove_file(layout.wal_file(name, gen));
            }
        }
        let mut generations: Vec<u64> =
            all_gens.into_iter().filter(|g| *g >= state.wal_generation).collect();
        if generations.is_empty() {
            // A collection that has never flushed has no generation file on
            // disk yet at all; `Wal::open` below creates it.
            generations.push(state.wal_generation);
        }
        generations.sort_unstable();
        let highest_generation = *generations.last().expect("just ensured non-empty");

        let mut replayed = 0usize;
        let mut any_truncated = false;
        for &gen in &generations {
            let wal_path = layout.wal_file(name, gen);
            let scan = wal::read(&wal_path)?;
            if scan.truncated_at.is_some() && gen != highest_generation {
                // Every generation below the highest was fsynced in full
                // before a newer one was ever rolled (the seal-time
                // `wal.sync()` in `snapshot_flush_locked`) — a torn tail
                // there means on-disk corruption, not a mid-write crash, and
                // silently truncating it could discard an acknowledged
                // write that a later generation's records still depend on.
                return Err(Error::corruption(format!(
                    "{}: torn tail in a non-final WAL generation",
                    wal_path.display()
                )));
            }
            any_truncated |= scan.truncated_at.is_some();

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
                        Self::apply_upsert(&mut memtable, None, &mut deleted, &segments, parsed);
                    }
                    WalRecord::Delete { id, .. } => {
                        Self::apply_delete(&mut memtable, None, &mut deleted, &segments, &id);
                    }
                }
            }
        }

        // A crash between a flush sealing a memtable and committing its
        // segment leaves that memtable's documents replayed above at fresh
        // ids (the WAL, not the never-committed segment, is authoritative) —
        // orphaning any tombstone recorded against their *old* ids. Harmless
        // for visibility (nothing owns those old ids any more, live or
        // dead), but `stats()` subtracts `deleted.len()` from segment doc
        // counts, and a stale entry here would silently undercount forever.
        let mut owned_by_a_segment = RoaringBitmap::new();
        for segment in &segments {
            owned_by_a_segment |= segment.presence();
        }
        deleted &= owned_by_a_segment;

        if replayed > 0 || any_truncated {
            tracing::info!(
                collection = name,
                replayed,
                torn_tail = any_truncated,
                live_documents = memtable.len(),
                "replayed write-ahead log"
            );
        }

        let wal = Wal::open(layout.wal_file(name, highest_generation), config.sync_policy)?;
        // In memory only: this is the generation actually open for
        // appending now, which is what `snapshot_flush_locked` must seal
        // next. Persisting it is `commit_flush_locked`'s job, the same as
        // it always was — if this process crashes again before a flush
        // commits, the next open recomputes the same chain and replays it
        // the same way, redoing this reconstruction from scratch rather
        // than trusting a value nothing has made durable yet.
        state.wal_generation = highest_generation;

        Ok(Collection {
            schema,
            layout: layout.clone(),
            config: config.clone(),
            inner: RwLock::new(Inner {
                state,
                wal,
                memtable,
                frozen: None,
                segments,
                deleted,
                next_seq,
            }),
            merge_gate: Mutex::new(()),
            flush_gate: Mutex::new(None),
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
            {
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

                let Inner { memtable, frozen, deleted, segments, .. } = &mut *inner;
                for (i, parsed) in accepted {
                    let id = parsed.id.clone();
                    Self::apply_upsert(memtable, frozen.as_deref(), deleted, segments, parsed);
                    results[i] = Some(DocOutcome::ok(id));
                }
            } // write lock released before a flush or merge is even considered

            self.maybe_flush();
            self.maybe_merge();
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
        let removed = {
            let mut inner = self.inner.write();
            if Self::lookup(&inner, id).is_none() {
                return Ok(false);
            }

            let seq = inner.next_seq;
            inner.wal.append(&WalRecordRef::Delete { seq, id })?;
            inner.next_seq = seq + 1;

            let Inner { memtable, frozen, deleted, segments, .. } = &mut *inner;
            Self::apply_delete(memtable, frozen.as_deref(), deleted, segments, id)
        }; // write lock released before a flush or merge is even considered

        self.maybe_flush();
        self.maybe_merge();
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
            found_is_exact: outcome.found_is_exact,
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
        //
        // `inner.deleted` only ever tombstones ids owned by a frozen
        // memtable or a committed segment (the active memtable deletes in
        // place instead), so summing those two doc counts before
        // subtracting it — rather than subtracting from each separately —
        // gives an exact answer without needing a range-restricted count.
        let segment_docs: u64 = inner.state.segments.iter().map(|s| s.doc_count as u64).sum();
        let frozen_docs = inner.frozen.as_ref().map_or(0, |f| f.len() as u64);
        let frozen_bytes = inner.frozen.as_ref().map_or(0, |f| f.heap_bytes());
        let num_documents = (segment_docs + frozen_docs).saturating_sub(inner.deleted.len())
            + inner.memtable.len() as u64;
        CollectionStats {
            name: self.schema.name.clone(),
            num_documents,
            num_segments: inner.state.segments.len(),
            memtable_documents: inner.memtable.len() + frozen_docs as usize,
            memtable_bytes: inner.memtable.heap_bytes() + frozen_bytes,
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
    ///
    /// Blocks on `flush_gate` rather than skipping if one is already in
    /// flight, for the same reason `merge()` blocks on `merge_gate`: a
    /// caller reaching for this method explicitly wants a flush to have
    /// happened by the time it returns.
    pub fn flush(&self) -> Result<bool> {
        let mut gate = self.flush_gate.lock();
        let flushed = self.run_flush(&mut gate)?;
        drop(gate); // released before a merge is even considered
        self.maybe_merge();
        Ok(flushed)
    }

    /// Force a merge right now, regardless of `merge_trigger_segments`
    /// (though a merge still requires at least `merge_fan_in` segments to
    /// have anything to fold together) — exposed directly for tests and for
    /// an operator forcing one early. Returns whether a merge actually
    /// happened.
    ///
    /// Blocks on `merge_gate` rather than skipping if one is already in
    /// flight: unlike the automatic post-write check, a caller reaching for
    /// this method explicitly wants a merge to have happened by the time it
    /// returns, so waiting for an in-progress one and then deciding fresh
    /// whether another is still needed is the right behavior here, not
    /// giving up.
    pub fn merge(&self) -> Result<bool> {
        let _gate = self.merge_gate.lock();
        self.run_merge()
    }

    /// Data directory this collection occupies.
    pub fn directory(&self) -> std::path::PathBuf {
        self.layout.collection_dir(&self.schema.name)
    }

    // --- internals -------------------------------------------------------

    /// A flush is due either because the active memtable has grown past its
    /// thresholds, or because a previous flush attempt sealed a memtable and
    /// then failed to build it — `inner.frozen` still holds that work, and
    /// nothing else will retry it except another flush attempt.
    fn needs_flush_locked(inner: &Inner, config: &EngineConfig) -> bool {
        inner.frozen.is_some()
            || inner.memtable.len() >= config.max_memtable_docs
            || inner.memtable.heap_bytes() >= config.max_memtable_bytes
    }

    /// Best-effort: run a flush if one is due and none is already in flight.
    /// Called after every write's own lock scope has ended, mirroring
    /// `maybe_merge` — see its doc comment for why errors are logged rather
    /// than propagated here.
    ///
    /// Unlike `maybe_merge`, this does not simply skip when the gate is
    /// held if the active memtable is *severely* over threshold
    /// (`FLUSH_BACKPRESSURE_MULTIPLE`×): ingest outrunning the build phase
    /// would otherwise let the memtable grow without bound while a flush is
    /// perpetually "in progress" elsewhere. In that case this blocks on the
    /// gate instead, applying backpressure to the writer that triggered it.
    fn maybe_flush(&self) {
        let (needs_flush, over_hard_limit) = {
            let inner = self.inner.read();
            (
                Self::needs_flush_locked(&inner, &self.config),
                // `saturating_mul`: `max_memtable_docs`/`max_memtable_bytes`
                // can be `usize::MAX` (an effectively unbounded threshold —
                // `tachyon-bench`'s default), and a plain `*` would overflow.
                inner.memtable.len()
                    >= self.config.max_memtable_docs.saturating_mul(FLUSH_BACKPRESSURE_MULTIPLE)
                    || inner.memtable.heap_bytes()
                        >= self
                            .config
                            .max_memtable_bytes
                            .saturating_mul(FLUSH_BACKPRESSURE_MULTIPLE),
            )
        };
        if !needs_flush {
            return;
        }

        let mut gate = if over_hard_limit {
            self.flush_gate.lock()
        } else {
            match self.flush_gate.try_lock() {
                Some(gate) => gate,
                None => return,
            }
        };
        if let Err(e) = self.run_flush(&mut gate) {
            tracing::error!(
                collection = %self.schema.name,
                error = %e,
                "background segment flush failed; the collection remains correct, just holding \
                 more memory in an unflushed memtable until a later write triggers another attempt"
            );
        }
    }

    /// Runs one flush, seal → build → commit, start to finish — or, if a
    /// previous attempt's build phase failed, resumes the memtable it
    /// already sealed rather than sealing a second one (see `flush_gate`'s
    /// doc comment). Assumes the caller already holds `flush_gate`.
    ///
    /// Only the seal and commit stages hold `inner`'s write lock; the build
    /// stage — encoding the sealed memtable into a segment, the expensive
    /// part this three-stage split exists for — runs with no lock held at
    /// all, so searches and writes proceed normally while it's in progress,
    /// reading the sealed memtable through `inner.frozen` exactly like one
    /// more (not yet durable) segment. See `snapshot_flush_locked` and
    /// `commit_flush_locked` for how the two locked stages stay correct
    /// despite everything that can happen to the collection in between:
    /// merges committing, documents being deleted or superseded, and so on.
    fn run_flush(&self, gate: &mut Option<FlushSnapshot>) -> Result<bool> {
        let snapshot = match gate.take() {
            Some(snapshot) => snapshot,
            None => {
                let mut inner = self.inner.write();
                match Self::snapshot_flush_locked(
                    &mut inner,
                    &self.schema,
                    &self.layout,
                    &self.config,
                )? {
                    Some(snapshot) => snapshot,
                    None => return Ok(false),
                }
            }
        };

        let paths = match Self::build_flush(&snapshot, &self.schema, &self.layout) {
            Ok(paths) => paths,
            Err(e) => {
                // The sealed memtable stays live at `inner.frozen`
                // regardless — only the gate's copy of the snapshot needs
                // restoring, so the next attempt resumes from here instead
                // of losing this work or re-sealing over it.
                *gate = Some(snapshot);
                return Err(e);
            }
        };

        let mut inner = self.inner.write();
        Self::commit_flush_locked(&mut inner, &self.schema, &self.layout, snapshot, paths)?;
        Ok(true)
    }

    /// Stage 1: seal the active memtable — install it at `inner.frozen`
    /// (still visible to every reader and still deletable, just no longer
    /// growable) and start a fresh, empty one in its place — and reserve
    /// everything a concurrent flush attempt could otherwise race with,
    /// before the lock is released for the build phase.
    ///
    /// Returns `Ok(None)` if nothing has been written since the last flush.
    /// Nothing here can leave `inner` partially mutated: every fallible step
    /// (fsyncing the outgoing generation, opening the new one) runs before
    /// anything is actually sealed.
    fn snapshot_flush_locked(
        inner: &mut Inner,
        schema: &CollectionSchema,
        layout: &Layout,
        config: &EngineConfig,
    ) -> Result<Option<FlushSnapshot>> {
        // Not `memtable.is_empty()`: a memtable can hold zero *live* documents
        // (everything since the last flush was also deleted) while still
        // holding postings and columns that need reclaiming — exactly the
        // case `heap_bytes()`-driven flushing exists to catch.
        if inner.memtable.next_doc_id() == inner.memtable.base() {
            return Ok(None);
        }
        debug_assert!(
            inner.frozen.is_none(),
            "flush_gate guarantees at most one sealed-but-uncommitted memtable at a time"
        );

        // Durable before sealing: under a relaxed sync policy the outgoing
        // WAL generation may hold acknowledged-but-unsynced records, and
        // this is the last moment anything forces them to disk before that
        // generation is (eventually, at commit) retired — after which the
        // segment this flush produces is their only remaining durable copy.
        inner.wal.sync()?;

        let sealed_generation = inner.state.wal_generation;
        let new_wal_generation = sealed_generation + 1;
        let new_wal =
            Wal::open(layout.wal_file(&schema.name, new_wal_generation), config.sync_policy)?;

        let segment_id = inner.state.next_segment_id;
        inner.state.next_segment_id += 1;

        // The memtable's own `base()`/`next_doc_id()` span every id it was
        // ever handed, live or not — including holes an off-lock merge
        // reserved on it (`snapshot_merge_locked`) that this flush's own
        // inserts never filled. Declaring those as part of this segment's
        // range would make it overlap another segment's — the merge output
        // that reserved them declares the very same ids as its own. A
        // single forward pass over what's actually live finds the true
        // bounds; an entirely dead memtable (every doc since the last
        // flush was also deleted — `heap_bytes()`-driven flushing can still
        // trigger this) has no live doc to anchor on, so it falls back to
        // the full span, which is harmless there since nothing is ever
        // live in it for another segment to collide with.
        let mut live_span: Option<(DocId, DocId)> = None;
        for (id, _) in inner.memtable.iter() {
            live_span = Some(match live_span {
                Some((first, _)) => (first, id),
                None => (id, id),
            });
        }
        let (min_doc_id, max_doc_id) =
            live_span.unwrap_or((inner.memtable.base(), inner.memtable.next_doc_id() - 1));
        let doc_count = inner.memtable.len() as u32;
        let next_doc_id = inner.memtable.next_doc_id();

        let sealed =
            Arc::new(std::mem::replace(&mut inner.memtable, MemTable::new(next_doc_id, schema)));
        inner.frozen = Some(Arc::clone(&sealed));
        // The outgoing generation's file is deliberately left on disk here
        // (unlike the old single-stage flush, which removed it immediately)
        // — it is still the only durable copy of `sealed`'s records until
        // this flush commits. `commit_flush_locked` removes it.
        let _ = std::mem::replace(&mut inner.wal, new_wal);

        Ok(Some(FlushSnapshot {
            segment_id,
            memtable: sealed,
            applied_seq: inner.next_seq - 1,
            new_wal_generation,
            doc_count,
            min_doc_id,
            max_doc_id,
            next_doc_id,
        }))
    }

    /// Stage 2: encode the sealed memtable into the output segment's five
    /// files. No lock held, no `Inner` reference anywhere in this
    /// function's signature: everything it touches either came from the
    /// snapshot (immutable once captured — `snapshot.memtable` is an `Arc`
    /// with no interior mutability) or is freshly created (the output
    /// files).
    fn build_flush(
        snapshot: &FlushSnapshot,
        schema: &CollectionSchema,
        layout: &Layout,
    ) -> Result<SegmentFilePaths> {
        let paths = segment_file_paths(layout, &schema.name, snapshot.segment_id);
        let mut files = SegmentFiles::create(&paths)?;
        encode_streaming(
            &snapshot.memtable,
            schema,
            &mut files.terms,
            &mut files.ids,
            &mut files.post,
            &mut files.col,
            &mut files.doc,
        )?;
        files.commit(&layout.segments_dir(&schema.name))?;
        Ok(paths)
    }

    /// Stage 3: commit the flush, re-derived from whatever `inner.state`
    /// looks like *now* — a concurrent merge may have committed while the
    /// build phase ran without the lock, and every bit of that must survive
    /// into this flush's own commit rather than being silently discarded by
    /// committing a state clone taken at seal time.
    ///
    /// Unlike a merge, a flush never renumbers doc ids or needs to remap a
    /// tombstone — the sealed memtable's ids are exactly the output
    /// segment's ids, so anything landing on them during the build (a
    /// delete, an upsert's supersede) is already a valid tombstone under
    /// the id this segment is about to claim.
    fn commit_flush_locked(
        inner: &mut Inner,
        schema: &CollectionSchema,
        layout: &Layout,
        snapshot: FlushSnapshot,
        paths: SegmentFilePaths,
    ) -> Result<()> {
        let mut new_state = inner.state.clone();
        new_state.applied_seq = snapshot.applied_seq;
        new_state.wal_generation = snapshot.new_wal_generation;
        // `max`, not a plain assignment: a concurrent merge may already have
        // advanced this further (see `swap_merge_locked`'s identical
        // reasoning) — regressing it would let a future flush's memtable
        // hand out an id a segment already owns.
        new_state.next_doc_id = new_state.next_doc_id.max(snapshot.next_doc_id);
        new_state.deleted = inner.deleted.iter().collect();
        new_state.segments.push(SegmentRef {
            id: snapshot.segment_id,
            doc_count: snapshot.doc_count,
            min_doc_id: snapshot.min_doc_id,
            max_doc_id: snapshot.max_doc_id,
        });

        // The commit point: a crash before this leaves the previous state
        // intact and the segment files just written, plus the sealed WAL
        // generation, orphaned but harmless — the next open replays the
        // sealed generation from scratch, redoing this same flush.
        meta::write_state(layout, &schema.name, &new_state)?;

        // Everything below is the in-memory mirror of what was just made
        // durable. `inner.state` in particular must be kept current, not
        // just written to disk — the *next* flush or merge reads
        // `inner.state` to compute its own ids, and a stale read here would
        // silently drop this segment's `SegmentRef` from that later write
        // even though its files are still on disk.
        inner.state = new_state;
        inner.segments.push(Arc::new(SegmentReader::open(&paths, schema)?));
        inner.frozen = None;

        // Cleanup, strictly after the commit: losing this before the
        // state.json rename would be unrecoverable for a relaxed sync
        // policy (a lower generation may be the only durable copy of
        // records this segment now also holds); losing it after is just a
        // harmless leftover file the next open would otherwise replay for
        // nothing. Removes every generation below the new one, not just the
        // one this flush itself sealed, so a leftover from an earlier
        // failed attempt gets swept up too.
        for gen in layout.list_wal_generations(&schema.name)? {
            if gen < snapshot.new_wal_generation {
                let _ = std::fs::remove_file(layout.wal_file(&schema.name, gen));
            }
        }
        let _ = sync_dir(&layout.wal_dir(&schema.name));

        Ok(())
    }

    fn needs_merge_locked(inner: &Inner, config: &EngineConfig) -> bool {
        inner.state.segments.len() > config.merge_trigger_segments
    }

    /// Best-effort: run a merge if one is due and none is already in
    /// flight. Called after every write's own lock scope has ended — never
    /// from inside one, and never while `inner`'s write lock is held (see
    /// `run_merge`'s doc comment for why that matters).
    ///
    /// Errors are logged, not propagated. By the time this runs, the write
    /// that triggered the check has already durably succeeded (its WAL
    /// record is fsynced, its memtable insert applied, any flush it needed
    /// committed) — a merge is background bookkeeping on top of that, and a
    /// failure in it must not make an already-successful write look like it
    /// failed to the caller. `merge()` (the explicit, caller-requested
    /// form) still returns its `Result` directly, for exactly the opposite
    /// reason: a caller reaching for that method wants to know.
    fn maybe_merge(&self) {
        // Cheap precheck under a read lock, before even touching
        // `merge_gate`: the common case, after most writes, is that no
        // merge is due at all, and this avoids taking the gate mutex (and
        // paying for the `Ordering::SeqCst`-ish RMW a `try_lock` costs) on
        // every single write for nothing.
        if !Self::needs_merge_locked(&self.inner.read(), &self.config) {
            return;
        }
        let Some(_gate) = self.merge_gate.try_lock() else { return };
        if let Err(e) = self.run_merge() {
            tracing::error!(
                collection = %self.schema.name,
                error = %e,
                "background segment merge failed; the collection remains correct, just with \
                 more segments than ideal until a later write triggers another attempt"
            );
        }
    }

    /// Runs one merge, snapshot → build → swap, start to finish. Assumes
    /// the caller already holds `merge_gate` — `merge()` and `maybe_merge()`
    /// are the only two callers, and both take it before calling this.
    ///
    /// Only the snapshot and swap stages hold `inner`'s write lock; the
    /// build stage — encoding the merged segment, the expensive part this
    /// whole three-stage split exists for — runs with no lock held at all,
    /// so searches and writes proceed normally while it's in progress. See
    /// `snapshot_merge_locked` and `swap_merge_locked` for how the two
    /// locked stages stay correct despite everything that can happen to the
    /// collection in between: flushes committing, documents being deleted
    /// or superseded, and so on.
    fn run_merge(&self) -> Result<bool> {
        let snapshot = {
            let mut inner = self.inner.write();
            Self::snapshot_merge_locked(&mut inner, &self.config)
        };
        let Some(snapshot) = snapshot else { return Ok(false) };

        let built = Self::build_merge(&snapshot, &self.schema, &self.layout)?;

        let mut inner = self.inner.write();
        Self::swap_merge_locked(&mut inner, &self.schema, &self.layout, snapshot, built)?;
        Ok(true)
    }

    /// Stage 1: pick the `merge_fan_in` smallest segments (by document
    /// count, ties broken by id for a deterministic choice) and reserve
    /// everything a concurrent write could otherwise race with, before the
    /// lock is released for the build phase:
    ///
    /// - **The output doc id range.** `merge_base` is the active memtable's
    ///   own `next_doc_id()` — reserved immediately via `MemTable::reserve`,
    ///   under this same lock hold, so no concurrent insert can land inside
    ///   `[merge_base, merge_base + claimed)` no matter how long the build
    ///   phase takes without the lock. This reservation survives any number
    ///   of intervening flushes on its own: a flush that replaces
    ///   `inner.memtable` starts the new one from `state.next_doc_id`, which
    ///   it sets from the *old* memtable's `next_doc_id()` — already past
    ///   this reservation by construction.
    /// - **The output segment id.** `inner.state.next_segment_id` is bumped
    ///   right here, the same way `flush_locked` bumps it for its own
    ///   segment — so a concurrent flush's `SegmentRef` can never collide
    ///   with this merge's.
    ///
    /// Returns `None` if there are fewer than `merge_fan_in` segments to
    /// work with — the same no-op condition the single-lock `merge_locked`
    /// used to check, just renamed since there's no longer one `_locked`
    /// function to check it inside of.
    fn snapshot_merge_locked(inner: &mut Inner, config: &EngineConfig) -> Option<MergeSnapshot> {
        if inner.state.segments.len() < config.merge_fan_in {
            return None;
        }

        // `inner.segments` and `inner.state.segments` are always the same
        // length and order — both only ever change together, here and in
        // `flush_locked` — so an index into one is valid for the other.
        let mut by_size: Vec<usize> = (0..inner.state.segments.len()).collect();
        by_size.sort_by_key(|&i| (inner.state.segments[i].doc_count, inner.state.segments[i].id));
        let mut victims: Vec<usize> = by_size.into_iter().take(config.merge_fan_in).collect();
        victims.sort_unstable(); // ascending index order, i.e. creation order

        let victim_refs: Vec<SegmentRef> =
            victims.iter().map(|&i| inner.state.segments[i].clone()).collect();
        let victim_readers: Vec<Arc<SegmentReader>> =
            victims.iter().map(|&i| Arc::clone(&inner.segments[i])).collect();

        // Each victim's own surviving doc ids as of *now*: its presence
        // bitmap (already excludes anything that was a hole when it was
        // flushed) minus whatever the collection has tombstoned so far.
        // Kept in the snapshot (not just used to compute `claimed`) because
        // `swap_merge_locked` needs this exact bitmap again, to tell which
        // of these ids the build phase copied into the merge output before
        // reproducing `tachyon_index::merge_segments`'s own rank-based
        // remap for any of them that got tombstoned while that ran.
        let live: Vec<RoaringBitmap> = victim_readers
            .iter()
            .map(|reader| {
                let mut live = reader.presence().clone();
                live -= &inner.deleted;
                live
            })
            .collect();
        let claimed: DocId = live.iter().map(|l| l.len() as DocId).sum();

        // See "The output doc id range" above for why this reservation
        // happens here, under the lock, rather than after the build phase.
        let merge_base = inner.memtable.next_doc_id();
        inner.memtable.reserve(claimed as usize);

        // See "The output segment id" above.
        let segment_id = inner.state.next_segment_id;
        inner.state.next_segment_id += 1;

        Some(MergeSnapshot { segment_id, merge_base, claimed, victim_refs, victim_readers, live })
    }

    /// Stage 2: the actual merge — decoding, re-blocking, and streaming the
    /// victims' postings, columns, and doc stores into the output segment's
    /// five files. No lock held, no `Inner` reference anywhere in this
    /// function's signature: everything it touches either came from the
    /// snapshot (immutable once captured) or is freshly created (the output
    /// files). Returns `None` if every snapshotted document turned out to
    /// be dead — nothing to write, the victims will simply disappear with
    /// no replacement once `swap_merge_locked` commits that.
    fn build_merge(
        snapshot: &MergeSnapshot,
        schema: &CollectionSchema,
        layout: &Layout,
    ) -> Result<Option<(SegmentFilePaths, MergeStats)>> {
        if snapshot.claimed == 0 {
            return Ok(None);
        }

        let merge_inputs: Vec<MergeInput> = snapshot
            .victim_readers
            .iter()
            .zip(&snapshot.live)
            .map(|(reader, live)| MergeInput { reader: reader.as_ref(), live: live.clone() })
            .collect();

        let paths = segment_file_paths(layout, &schema.name, snapshot.segment_id);
        let mut files = SegmentFiles::create(&paths)?;
        let stats = merge_segments(
            &merge_inputs,
            schema,
            snapshot.merge_base,
            &mut files.terms,
            &mut files.ids,
            &mut files.post,
            &mut files.col,
            &mut files.doc,
        )?;
        files.commit(&layout.segments_dir(&schema.name))?;
        debug_assert_eq!(stats.doc_count as DocId, snapshot.claimed);

        Ok(Some((paths, stats)))
    }

    /// Stage 3: commit the merge, re-validated against whatever changed
    /// concurrently while the build phase ran without the lock — a flush
    /// committing a new segment, a document being deleted or superseded,
    /// or both.
    ///
    /// A merge renumbers rather than preserves doc ids — see
    /// `tachyon_index::merge_segments`'s own doc comment for the exact
    /// rank-based mapping, and `snapshot_merge_locked`'s doc comment for why
    /// the range it claims can never collide with anything else. Doc ids
    /// retired this way are simply never claimed again — no segment or
    /// memtable ever covers that range afterward.
    fn swap_merge_locked(
        inner: &mut Inner,
        schema: &CollectionSchema,
        layout: &Layout,
        snapshot: MergeSnapshot,
        built: Option<(SegmentFilePaths, MergeStats)>,
    ) -> Result<()> {
        // Freshly cloned *now*, not reused from anything computed at
        // snapshot time: a concurrent flush may have committed a new
        // segment, advanced `next_doc_id`, or added tombstones while the
        // build phase above ran without the lock, and every bit of that
        // must survive into this merge's own commit rather than being
        // silently discarded by committing a state clone taken before it
        // happened.
        let mut new_state = inner.state.clone();

        // Every id this snapshot considered live that has since been
        // deleted — by a plain delete, or by an upsert superseding it with
        // a new version — needs a tombstone under its *new* id: the
        // document was already copied into the merge output at snapshot
        // time (`build_merge` had no way to know it wouldn't be), and
        // nothing un-copies it now. `snapshot.live` is exactly the bitmap
        // `tachyon_index::merge_segments` remapped from, so intersecting it
        // against the tombstones that have landed since snapshot, then
        // reproducing that same rank-based formula, recovers precisely
        // which new ids need tombstoning.
        let mut new_tombstones = RoaringBitmap::new();
        if let Some((_, stats)) = &built {
            for (i, live) in snapshot.live.iter().enumerate() {
                let now_dead = live & &inner.deleted;
                for old_id in now_dead.iter() {
                    let new_id = stats.new_base[i] + (live.rank(old_id) - 1) as DocId;
                    new_tombstones.insert(new_id);
                }
            }
        }

        // Tombstones for ids no segment will ever claim again — the merge
        // output starts fresh, so nothing later needs to know these were
        // once deleted. Pruned from the live bitmap too, not just the
        // serialized snapshot: `stats()` subtracts `inner.deleted.len()`
        // from the summed segment doc counts, and a stale entry for an id
        // no segment counts anymore would silently undercount forever.
        //
        // Pruned by each victim's own *presence* bitmap, not its declared
        // `[min_doc_id, max_doc_id]` range: a `SegmentRef`'s range can be
        // wider than what it actually holds. `flush_locked` narrows a
        // fresh segment's range to its true live span, but a memtable can
        // have live documents both *before and after* a hole an off-lock
        // merge reserved on it (`snapshot_merge_locked`) — a single
        // contiguous range fundamentally cannot describe "content flanking
        // someone else's reservation", so the two segments' declared
        // ranges can still overlap even though neither one's *presence*
        // ever does (doc ids are global and never reused, so a presence
        // bit is only ever set in the one segment that legitimately owns
        // it). Pruning by range here would delete a tombstone that
        // actually belongs to a completely different, unrelated segment
        // sharing numeric territory with a victim — silently resurrecting
        // whatever document that tombstone was protecting.
        for reader in &snapshot.victim_readers {
            inner.deleted -= reader.presence();
        }
        inner.deleted |= &new_tombstones;
        new_state.deleted = inner.deleted.iter().collect();

        // Re-locate every victim by id, not by the index captured at
        // snapshot time: a concurrent flush only ever appends, so today
        // those indices would in fact still be valid, but nothing here
        // should depend on that staying true. A victim that's vanished
        // (impossible today — `merge_gate` guarantees only this merge ever
        // removes a segment, and nothing else does) is a bug elsewhere, not
        // a condition to paper over.
        let mut victim_indices: Vec<usize> = snapshot
            .victim_refs
            .iter()
            .map(|v| {
                new_state.segments.iter().position(|s| s.id == v.id).ok_or_else(|| {
                    Error::internal(format!(
                        "merge: victim segment {} vanished before this merge could commit — \
                         merge_gate should make that unreachable",
                        v.id
                    ))
                })
            })
            .collect::<Result<_>>()?;
        victim_indices.sort_unstable();
        for &i in victim_indices.iter().rev() {
            new_state.segments.remove(i);
        }

        let mut new_reader = None;
        if let Some((paths, stats)) = &built {
            // `max`, not a plain assignment: a concurrent flush may have
            // already advanced this past what this merge alone would set it
            // to, and this must never regress it — a smaller-than-reality
            // `next_doc_id` would let a future flush's memtable hand out an
            // id that segment or memtable already owns.
            new_state.next_doc_id =
                new_state.next_doc_id.max(snapshot.merge_base + snapshot.claimed);
            // `victim_indices[0]` is the smallest victim index, which — now
            // that every victim has been removed — equals the number of
            // untouched segments that came before it. Inserting there puts
            // the merged segment exactly where the earliest victim used to
            // sit, preserving relative recency against every segment not
            // involved in this merge.
            new_state.segments.insert(
                victim_indices[0],
                SegmentRef {
                    id: snapshot.segment_id,
                    doc_count: stats.doc_count as u32,
                    min_doc_id: snapshot.merge_base,
                    max_doc_id: snapshot.merge_base + snapshot.claimed - 1,
                },
            );
            new_reader = Some(Arc::new(SegmentReader::open(paths, schema)?));
        }

        // The commit point: a crash before this leaves the previous state
        // intact and any new segment files just written orphaned but
        // harmless, for exactly the same reason a flush's are.
        meta::write_state(layout, &schema.name, &new_state)?;

        inner.state = new_state;
        for &i in victim_indices.iter().rev() {
            inner.segments.remove(i);
        }
        if let Some(reader) = new_reader {
            inner.segments.insert(victim_indices[0], reader);
        }

        // Cleanup, strictly after the commit. Safe with no concurrent-access
        // hazard: a search only ever borrows a source for the duration of
        // one read-lock hold — the borrow checker ties `&dyn IndexSource` to
        // that guard's lifetime — and every victim removed here has already
        // been swapped out of `inner.segments` above, under the exclusive
        // write lock this function holds throughout; `snapshot.victim_readers`
        // is this call's own `Arc` clones, kept alive until they drop at the
        // end of this function regardless.
        for old in &snapshot.victim_refs {
            for ext in ["terms", "ids", "post", "col", "doc"] {
                let _ = std::fs::remove_file(layout.segment_file(&schema.name, old.id, ext));
            }
        }

        Ok(())
    }

    /// Every source a search or suggest request reads through: the active
    /// memtable, then a sealed-but-uncommitted one if a flush is mid-build,
    /// then every committed segment, oldest first — matching the executor's
    /// "memtable first, then committed segments" contract, with the frozen
    /// memtable slotting in exactly where its eventual segment will.
    fn sources(inner: &Inner) -> Vec<&dyn IndexSource> {
        let mut sources: Vec<&dyn IndexSource> = Vec::with_capacity(2 + inner.segments.len());
        sources.push(&inner.memtable);
        if let Some(frozen) = &inner.frozen {
            sources.push(frozen.as_ref());
        }
        sources.extend(inner.segments.iter().map(|s| s.as_ref() as &dyn IndexSource));
        sources
    }

    /// A document's stored source, wherever it lives.
    fn doc_source(inner: &Inner, doc_id: DocId) -> Option<Json> {
        if let Some(doc) = inner.memtable.get(doc_id) {
            return Some(doc.source.clone());
        }
        if let Some(doc) = inner.frozen.as_ref().and_then(|f| f.get(doc_id)) {
            return Some(doc.source.clone());
        }
        inner.segments.iter().rev().find_map(|segment| segment.get(doc_id))
    }

    /// Resolve a user-facing id to an internal doc id.
    ///
    /// The active memtable holds the newest version of everything written
    /// since the last flush, so it is consulted first; then a
    /// sealed-but-uncommitted memtable, if a flush is mid-build; only then
    /// committed segments, newest first. A tombstoned id must never be
    /// returned.
    ///
    /// A frozen-memtable hit — tombstoned or not — never falls through to
    /// segments: unlike the active memtable (which never holds a tombstoned
    /// entry, since `MemTable::remove` deletes in place), a frozen entry can
    /// be tombstoned by a delete or an upsert's supersede landing after it
    /// was sealed. But whenever this id was first written into what is now
    /// the frozen memtable, `apply_upsert` already tombstoned whatever
    /// segment copy existed at that time — so a live frozen entry means no
    /// segment can also hold this id live, and a tombstoned one means
    /// nothing does.
    fn lookup(inner: &Inner, id: &str) -> Option<DocId> {
        if let Some(doc_id) = inner.memtable.lookup(id) {
            return Some(doc_id);
        }
        if let Some(frozen) = &inner.frozen {
            if let Some(doc_id) = frozen.lookup(id) {
                return (!inner.deleted.contains(doc_id)).then_some(doc_id);
            }
        }
        inner
            .segments
            .iter()
            .rev()
            .find_map(|segment| segment.lookup_id(id))
            .filter(|doc_id| !inner.deleted.contains(*doc_id))
    }

    /// Insert a document, retiring any previous version of the same id —
    /// whether that version lives in the active memtable, a frozen one, or a
    /// committed segment.
    fn apply_upsert(
        memtable: &mut MemTable,
        frozen: Option<&MemTable>,
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
        } else if let Some(old_doc_id) = frozen.and_then(|f| f.lookup(&id)) {
            // The previous version lives in a memtable a flush has already
            // sealed — immutable, so it cannot be removed in place; tombstone
            // it the same way a committed segment's copy would be.
            deleted.insert(old_doc_id);
        } else if let Some(old_doc_id) =
            segments.iter().rev().find_map(|segment| segment.lookup_id(&id))
        {
            // The previous version lives in a committed segment — tombstone
            // it there so the same id doesn't resolve to two documents
            // (stale in the segment, fresh in the memtable).
            deleted.insert(old_doc_id);
        }
    }

    /// Retire the current version of `id`, if any. Returns whether it existed.
    fn apply_delete(
        memtable: &mut MemTable,
        frozen: Option<&MemTable>,
        deleted: &mut RoaringBitmap,
        segments: &[Arc<SegmentReader>],
        id: &str,
    ) -> bool {
        if let Some(doc_id) = memtable.lookup(id) {
            return memtable.remove(doc_id);
        }
        let doc_id = frozen
            .and_then(|f| f.lookup(id))
            .or_else(|| segments.iter().rev().find_map(|segment| segment.lookup_id(id)));
        let Some(doc_id) = doc_id else {
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
    fn an_unbounded_memtable_threshold_does_not_overflow_the_backpressure_check() {
        // `maybe_flush`'s backpressure check multiplies the configured
        // threshold by `FLUSH_BACKPRESSURE_MULTIPLE` — `usize::MAX` (an
        // effectively unbounded threshold, e.g. `tachyon-bench`'s default)
        // must not overflow that multiplication.
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        let config = EngineConfig::new(dir.path()).with_max_memtable_docs(usize::MAX);

        let c = Collection::create(&layout, schema(), &config).unwrap();
        c.upsert(product("1", "a", 1)).unwrap();
        assert_eq!(c.stats().num_segments, 0, "far below any threshold, unbounded or not");
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
            assert!(
                !c.delete("1").unwrap(),
                "deleting an already-tombstoned segment doc is a no-op"
            );
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

    // --- off-lock flush: WAL generation chain ---------------------------

    #[test]
    fn a_crash_between_a_flushs_seal_and_commit_replays_both_wal_generations() {
        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "Mouse", 100)).unwrap();
        c.upsert(product("2", "Keyboard", 200)).unwrap();

        // Seal and build, but never commit — simulating a crash after the
        // WAL generation rolled and the segment was written to disk, but
        // before state.json ever named it. A third document lands in the
        // *new* generation while the "old" one is still sealed, exactly
        // like a write racing a real flush's build phase. `flush_gate` is
        // held throughout, exactly as `run_flush`'s real callers do, so the
        // upsert below can't trigger its own (confusingly interleaved)
        // automatic flush.
        let gate = c.flush_gate.lock();
        let snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_flush_locked(&mut inner, c.schema(), &h.layout, &h.config)
                .unwrap()
                .unwrap()
        };
        assert_eq!(snapshot.doc_count, 2);
        Collection::build_flush(&snapshot, c.schema(), &h.layout).unwrap();
        c.upsert(product("3", "Monitor", 300)).unwrap();
        drop(snapshot);
        drop(gate);
        drop(c); // the "crash": nothing was ever committed

        // Two WAL generations exist on disk; state.json still names only
        // the first (a flush's commit is what would have advanced it).
        assert!(h.layout.wal_file("products", 1).exists());
        assert!(h.layout.wal_file("products", 2).exists());

        let reopened = h.reopen();
        assert_eq!(
            reopened.stats().num_segments,
            0,
            "the never-committed segment must not be counted"
        );
        assert_eq!(reopened.stats().num_documents, 3, "every acknowledged write survived");
        for (id, title) in [("1", "Mouse"), ("2", "Keyboard"), ("3", "Monitor")] {
            assert_eq!(reopened.get(id).unwrap()["title"], json!(title));
        }

        // The collection is fully writable and flushes cleanly from here —
        // redoing the same flush the crash interrupted.
        assert!(reopened.flush().unwrap());
        assert_eq!(reopened.stats().num_segments, 1);
        assert_eq!(reopened.stats().num_documents, 3);

        let reopened_again = h.reopen();
        assert_eq!(reopened_again.stats().num_documents, 3);
        assert_eq!(reopened_again.stats().num_segments, 1);
        for (id, title) in [("1", "Mouse"), ("2", "Keyboard"), ("3", "Monitor")] {
            assert_eq!(reopened_again.get(id).unwrap()["title"], json!(title));
        }
    }

    #[test]
    fn a_torn_tail_in_a_non_final_wal_generation_is_corruption() {
        use std::io::Write;

        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "Mouse", 100)).unwrap();

        let snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_flush_locked(&mut inner, c.schema(), &h.layout, &h.config)
                .unwrap()
                .unwrap()
        };
        Collection::build_flush(&snapshot, c.schema(), &h.layout).unwrap();
        drop(snapshot);
        drop(c);

        // Corrupt the tail of the *sealed*, non-final generation — exactly
        // what `snapshot_flush_locked`'s seal-time `wal.sync()` should make
        // impossible in a real crash, since that generation is fsynced
        // whole before a newer one is ever rolled.
        let wal_path = h.layout.wal_file("products", 1);
        let mut file = std::fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[200u8, 0, 0, 0, 7, 7, 7, 7, 1, 2, 3]).unwrap();
        drop(file);

        let err = Collection::open(&h.layout, "products", &h.config).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "got {err:?}");
    }

    // --- off-lock flush races -------------------------------------------
    //
    // A flush's build phase (`Collection::build_flush`) runs with no lock
    // held — the whole point of the off-lock rewrite — so anything that
    // used to be impossible mid-flush (a write or a merge landing while a
    // flush is "in progress") is now routine. These tests drive
    // `snapshot_flush_locked`, `build_flush`, and `commit_flush_locked`
    // directly rather than through `run_flush`, so each one can land an
    // exact, deterministic write in the gap between snapshot and commit
    // instead of hoping a real thread wins a race.
    //
    // Each test locks `flush_gate` itself first, exactly as `run_flush`'s
    // real callers do: without it, the `c.upsert()`/`c.delete()`/`c.merge()`
    // calls used to simulate "activity during the build phase" would let
    // `maybe_flush()` run its own *actual* automatic flush (nothing else is
    // holding the gate to stop it), confusing the very scenario each test
    // is trying to isolate.

    #[test]
    fn a_delete_during_the_flush_build_phase_is_not_resurrected() {
        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "a", 1)).unwrap();
        c.upsert(product("2", "b", 2)).unwrap();

        let _gate = c.flush_gate.lock();
        let snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_flush_locked(&mut inner, c.schema(), &h.layout, &h.config)
                .unwrap()
                .unwrap()
        };
        assert_eq!(snapshot.doc_count, 2);

        // A delete lands "during the build phase": "2" was live when the
        // memtable was sealed and is already baked into the segment
        // `build_flush` is about to produce.
        assert!(c.delete("2").unwrap());
        assert_eq!(c.stats().num_documents, 1, "the frozen copy is tombstoned immediately");

        let paths = Collection::build_flush(&snapshot, c.schema(), &h.layout).unwrap();
        {
            let mut inner = c.inner.write();
            Collection::commit_flush_locked(&mut inner, c.schema(), &h.layout, snapshot, paths)
                .unwrap();
        }
        drop(_gate);

        assert_eq!(c.stats().num_segments, 1);
        assert_eq!(c.stats().num_documents, 1, "\"2\" must not have been resurrected by the flush");
        assert!(matches!(c.get("2"), Err(Error::DocumentNotFound { .. })));
        assert!(c.get("1").is_ok());

        let reopened = h.reopen();
        assert_eq!(reopened.stats().num_documents, 1);
        assert!(matches!(reopened.get("2"), Err(Error::DocumentNotFound { .. })));
    }

    #[test]
    fn an_upsert_during_the_flush_build_phase_supersedes_the_frozen_copy_without_duplication() {
        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "Old", 100)).unwrap();

        let _gate = c.flush_gate.lock();
        let snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_flush_locked(&mut inner, c.schema(), &h.layout, &h.config)
                .unwrap()
                .unwrap()
        };

        // "1"'s old copy is now in the memtable being sealed. This upsert's
        // new doc id must land in the fresh active memtable, not collide
        // with the frozen one.
        c.upsert(product("1", "New", 200)).unwrap();
        assert_eq!(c.get("1").unwrap()["title"], json!("New"));
        assert_eq!(c.stats().num_documents, 1, "the supersede must tombstone the frozen copy");

        let paths = Collection::build_flush(&snapshot, c.schema(), &h.layout).unwrap();
        {
            let mut inner = c.inner.write();
            Collection::commit_flush_locked(&mut inner, c.schema(), &h.layout, snapshot, paths)
                .unwrap();
        }
        drop(_gate);

        assert_eq!(c.get("1").unwrap()["title"], json!("New"));
        assert_eq!(c.stats().num_documents, 1);

        let reopened = h.reopen();
        assert_eq!(reopened.get("1").unwrap()["title"], json!("New"));
        assert_eq!(reopened.stats().num_documents, 1);
    }

    #[test]
    fn search_sees_the_frozen_memtable_during_a_flush_build_phase() {
        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "Wireless Mouse", 100)).unwrap();

        let _gate = c.flush_gate.lock();
        let snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_flush_locked(&mut inner, c.schema(), &h.layout, &h.config)
                .unwrap()
                .unwrap()
        };
        c.upsert(product("2", "Mechanical Keyboard", 200)).unwrap();

        let results = c
            .search(SearchParams {
                q: Some("mouse keyboard".into()),
                match_mode: Some("any".into()),
                ..Default::default()
            })
            .unwrap();
        let ids: std::collections::HashSet<_> =
            results.hits.iter().map(|h| h.document["id"].as_str().unwrap().to_string()).collect();
        assert!(ids.contains("1"), "the frozen (sealed but uncommitted) document must be found");
        assert!(ids.contains("2"), "the active memtable's document must be found");

        let paths = Collection::build_flush(&snapshot, c.schema(), &h.layout).unwrap();
        let mut inner = c.inner.write();
        Collection::commit_flush_locked(&mut inner, c.schema(), &h.layout, snapshot, paths)
            .unwrap();
        drop(inner);
        drop(_gate);
    }

    #[test]
    fn a_merge_committing_during_the_flush_build_phase_does_not_collide_with_it() {
        let (_dir, layout, config) = merge_harness(2, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        // Two segments already committed, eligible for a merge.
        for i in 0..4 {
            c.upsert(product(&(i + 1).to_string(), "x", i)).unwrap();
        }
        assert_eq!(c.stats().num_segments, 2);
        c.upsert(product("5", "e", 5)).unwrap(); // lands in the fresh active memtable

        let flush_gate = c.flush_gate.lock();
        let flush_snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_flush_locked(&mut inner, c.schema(), &layout, &config)
                .unwrap()
                .unwrap()
        };
        assert_eq!(flush_snapshot.doc_count, 1, "only \"5\" had been written since the last flush");

        // A merge runs entirely inside the flush's build phase — folding
        // the two pre-existing segments together while the flush's own
        // segment is neither built nor committed yet.
        let merge_gate = c.merge_gate.lock();
        let merge_snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_merge_locked(&mut inner, &config).unwrap()
        };
        let merge_built = Collection::build_merge(&merge_snapshot, c.schema(), &layout).unwrap();
        {
            let mut inner = c.inner.write();
            Collection::swap_merge_locked(
                &mut inner,
                c.schema(),
                &layout,
                merge_snapshot,
                merge_built,
            )
            .unwrap();
        }
        drop(merge_gate);
        assert_eq!(c.stats().num_segments, 1, "the merge committed on its own");

        let flush_paths = Collection::build_flush(&flush_snapshot, c.schema(), &layout).unwrap();
        {
            let mut inner = c.inner.write();
            Collection::commit_flush_locked(
                &mut inner,
                c.schema(),
                &layout,
                flush_snapshot,
                flush_paths,
            )
            .unwrap();
        }
        drop(flush_gate);

        assert_eq!(c.stats().num_segments, 2, "the merged segment plus the flush's own");
        assert_eq!(c.stats().num_documents, 5);
        for id in ["1", "2", "3", "4", "5"] {
            assert!(c.get(id).is_ok(), "doc {id} must survive");
        }

        // No id collision between the merge output's range and the flush's.
        let inner = c.inner.read();
        let mut ranges: Vec<(DocId, DocId)> =
            inner.state.segments.iter().map(|s| (s.min_doc_id, s.max_doc_id)).collect();
        ranges.sort_unstable();
        for w in ranges.windows(2) {
            assert!(w[0].1 < w[1].0, "overlapping segment ranges {:?} and {:?}", w[0], w[1]);
        }
        drop(inner);

        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(reopened.stats().num_documents, 5);
        for id in ["1", "2", "3", "4", "5"] {
            assert!(reopened.get(id).is_ok());
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_failed_flush_build_leaves_the_sealed_memtable_retryable() {
        use std::os::unix::fs::PermissionsExt;

        let h = Harness::new();
        let c = h.create();
        c.upsert(product("1", "Mouse", 100)).unwrap();

        let segments_dir = h.layout.segments_dir("products");
        std::fs::create_dir_all(&segments_dir).unwrap();
        std::fs::set_permissions(&segments_dir, std::fs::Permissions::from_mode(0o500)).unwrap();

        let err = c.flush();
        std::fs::set_permissions(&segments_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            err.is_err(),
            "the build phase must have failed to write into a read-only directory"
        );

        // The collection is still fully correct — nothing was lost, the
        // sealed memtable's documents are still visible through `frozen`.
        assert_eq!(c.stats().num_segments, 0);
        assert_eq!(c.stats().num_documents, 1);
        assert_eq!(c.get("1").unwrap()["title"], json!("Mouse"));
        assert!(
            c.inner.read().frozen.is_some(),
            "the sealed memtable must not have been discarded"
        );

        // The next flush attempt resumes from the same sealed memtable
        // rather than sealing a second one, and now succeeds.
        assert!(c.flush().unwrap());
        assert_eq!(c.stats().num_segments, 1);
        assert_eq!(c.stats().num_documents, 1);
        assert!(c.inner.read().frozen.is_none());

        let reopened = h.reopen();
        assert_eq!(reopened.stats().num_segments, 1);
        assert_eq!(reopened.get("1").unwrap()["title"], json!("Mouse"));
    }

    #[test]
    fn concurrent_writes_and_flushes_leave_the_collection_internally_consistent() {
        // Real threads, mirroring `concurrent_writes_and_merges_leave_the_
        // collection_internally_consistent` but forcing explicit flushes
        // (which block on `flush_gate`, so several threads calling
        // `c.flush()` concurrently must serialize correctly rather than
        // double-encoding or losing whichever memtable was sealed first) on
        // top of the automatic ones every write already triggers.
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        let config = EngineConfig::new(dir.path())
            .with_max_memtable_docs(3)
            .with_merge_trigger_segments(2)
            .with_merge_fan_in(2);
        let c = Arc::new(Collection::create(&layout, schema(), &config).unwrap());

        const THREADS: usize = 4;
        const PER_THREAD: usize = 50;

        let mut handles = Vec::with_capacity(THREADS);
        for t in 0..THREADS {
            let c = Arc::clone(&c);
            handles.push(std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let id = format!("{t}-{i}");
                    c.upsert(product(&id, "widget", (t * 1000 + i) as i64)).unwrap();
                    if i % 5 == 0 {
                        let victim = format!("{t}-{}", i.saturating_sub(3));
                        c.delete(&victim).unwrap();
                    }
                    if i % 7 == 0 {
                        c.flush().unwrap();
                    }
                    if i % 11 == 0 {
                        c.merge().unwrap();
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        // The same delete pattern each thread ran, replayed here against a
        // plain `HashSet` instead of the collection — an independent
        // reference for what should still be alive.
        let mut expected_alive: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for t in 0..THREADS {
            for i in 0..PER_THREAD {
                expected_alive.insert(format!("{t}-{i}"));
            }
            for i in (0..PER_THREAD).step_by(5) {
                expected_alive.remove(&format!("{t}-{}", i.saturating_sub(3)));
            }
        }

        for id in &expected_alive {
            assert!(c.get(id).is_ok(), "expected {id} to be alive");
        }
        assert_eq!(c.stats().num_documents, expected_alive.len() as u64);

        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(reopened.stats().num_documents, expected_alive.len() as u64);
        for id in &expected_alive {
            assert!(reopened.get(id).is_ok());
        }
    }

    // --- tiered merges -------------------------------------------------

    /// A collection whose every flush produces a 2-document segment, so a
    /// handful of upserts is enough to drive several flushes deterministically.
    fn merge_harness(
        merge_trigger_segments: usize,
        merge_fan_in: usize,
    ) -> (tempfile::TempDir, Layout, EngineConfig) {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        let config = EngineConfig::new(dir.path())
            .with_max_memtable_docs(2)
            .with_merge_trigger_segments(merge_trigger_segments)
            .with_merge_fan_in(merge_fan_in);
        (dir, layout, config)
    }

    #[test]
    fn merging_the_smallest_segments_preserves_every_live_document() {
        let (_dir, layout, config) = merge_harness(2, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        // Three flushes of 2 docs each (max_memtable_docs=2) would normally
        // leave 3 segments, but the third flush's own auto-merge check sees
        // segments.len()=3 > trigger=2 and immediately folds the two
        // smallest (the first two, tied at doc_count=2) into one.
        for i in 0..6 {
            c.upsert(product(&(i + 1).to_string(), &format!("item {i}"), 100 + i)).unwrap();
        }
        assert_eq!(c.stats().num_segments, 2);
        assert_eq!(c.stats().num_documents, 6);

        for i in 1..=6 {
            let doc = c.get(&i.to_string()).unwrap();
            assert_eq!(doc["title"], json!(format!("item {}", i - 1)));
        }

        let results = c
            .search(SearchParams {
                q: Some("item".into()),
                match_mode: Some("any".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(results.found, 6);
    }

    #[test]
    fn merge_triggers_automatically_without_a_manual_call() {
        let (_dir, layout, config) = merge_harness(1, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        // trigger=1: a merge is attempted after every flush past 1 segment.
        for i in 0..8 {
            c.upsert(product(&(i + 1).to_string(), "x", i)).unwrap();
        }
        // 4 flushes of 2 docs; merges should have kept segment count from
        // growing unboundedly rather than sitting at 4.
        assert!(c.stats().num_segments < 4, "merging should have kept segment count down");
        assert_eq!(c.stats().num_documents, 8);
    }

    #[test]
    fn a_document_deleted_before_a_merge_does_not_reappear() {
        let (_dir, layout, config) = merge_harness(2, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        c.upsert(product("1", "a", 1)).unwrap();
        c.upsert(product("2", "b", 2)).unwrap(); // segment 1: [1,2]
        assert!(c.delete("2").unwrap()); // tombstoned in a committed segment

        c.upsert(product("3", "c", 3)).unwrap();
        c.upsert(product("4", "d", 4)).unwrap(); // segment 2: [3,4]
        c.upsert(product("5", "e", 5)).unwrap();
        c.upsert(product("6", "f", 6)).unwrap(); // segment 3: [5,6] -> triggers merge

        assert_eq!(c.stats().num_documents, 5, "\"2\" stays deleted through the merge");
        assert!(matches!(c.get("2"), Err(Error::DocumentNotFound { .. })));
        for id in ["1", "3", "4", "5", "6"] {
            assert!(c.get(id).is_ok(), "doc {id} should have survived the merge");
        }
    }

    #[test]
    fn a_document_from_a_merged_segment_can_still_be_deleted_afterward() {
        let (_dir, layout, config) = merge_harness(2, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        for i in 0..6 {
            c.upsert(product(&(i + 1).to_string(), "x", i)).unwrap();
        }
        assert_eq!(c.stats().num_segments, 2, "the merge already ran");

        // "1" was in the very first (now-merged-away) segment. Deleting it
        // now must resolve through the *new* merged segment's id index.
        assert!(c.delete("1").unwrap());
        assert!(matches!(c.get("1"), Err(Error::DocumentNotFound { .. })));
        assert_eq!(c.stats().num_documents, 5);

        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert!(matches!(reopened.get("1"), Err(Error::DocumentNotFound { .. })));
        assert_eq!(reopened.stats().num_documents, 5);
    }

    #[test]
    fn a_merge_set_that_is_entirely_dead_leaves_no_replacement_segment() {
        let (_dir, layout, config) = merge_harness(2, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        c.upsert(product("1", "a", 1)).unwrap();
        c.upsert(product("2", "b", 2)).unwrap(); // segment 1
        assert!(c.delete("1").unwrap());
        assert!(c.delete("2").unwrap()); // segment 1 is now entirely dead

        c.upsert(product("3", "c", 3)).unwrap();
        c.upsert(product("4", "d", 4)).unwrap(); // segment 2, still fully live

        // Force the merge explicitly rather than relying on a third flush,
        // so this test exercises exactly the two segments above.
        assert!(c.merge().unwrap());

        assert_eq!(
            c.stats().num_segments,
            1,
            "the fully-dead segment vanished with no replacement"
        );
        assert_eq!(c.stats().num_documents, 2);
        assert!(c.get("3").is_ok());
        assert!(c.get("4").is_ok());
    }

    #[test]
    fn merge_survives_a_restart_and_retires_the_old_segment_files() {
        let (_dir, layout, config) = merge_harness(2, 2);
        let old_segment_files: Vec<std::path::PathBuf>;
        {
            let c = Collection::create(&layout, schema(), &config).unwrap();
            for i in 0..6 {
                c.upsert(product(&(i + 1).to_string(), "x", i)).unwrap();
            }
            assert_eq!(c.stats().num_segments, 2);
            // Segment id 1 was one of the two merge victims (smallest/first);
            // its files must be gone once the merge committed.
            old_segment_files = ["terms", "ids", "post", "col", "doc"]
                .iter()
                .map(|ext| layout.segment_file("products", 1, ext))
                .collect();
            for path in &old_segment_files {
                assert!(!path.exists(), "{path:?} should have been retired by the merge");
            }
        }

        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(reopened.stats().num_segments, 2);
        assert_eq!(reopened.stats().num_documents, 6);
        for i in 1..=6 {
            assert!(reopened.get(&i.to_string()).is_ok());
        }
        for path in &old_segment_files {
            assert!(!path.exists());
        }
    }

    #[test]
    fn a_stale_orphaned_merge_output_id_is_safely_overwritten() {
        let (_dir, layout, config) = merge_harness(2, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        c.upsert(product("1", "a", 1)).unwrap();
        c.upsert(product("2", "b", 2)).unwrap(); // segment id 1
        c.upsert(product("3", "c", 3)).unwrap();
        c.upsert(product("4", "d", 4)).unwrap(); // segment id 2

        // The next segment id (3) is what the upcoming merge will claim.
        // Simulate a crash that left garbage there from a previous attempt.
        for ext in ["terms", "ids", "post", "col", "doc"] {
            std::fs::write(layout.segment_file("products", 3, ext), b"garbage").unwrap();
        }

        assert!(c.merge().unwrap(), "the real merge must overwrite the orphaned files at id 3");
        assert_eq!(c.stats().num_segments, 1);
        assert_eq!(c.stats().num_documents, 4);
        for i in 1..=4 {
            assert!(c.get(&i.to_string()).is_ok());
        }

        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(reopened.stats().num_documents, 4);
    }

    /// A merge's output segment must never claim a doc id the *active*
    /// memtable is going to hand out later — regression test for a bug
    /// where `merge_locked` computed its output range from
    /// `state.next_doc_id`, the exact id the fresh post-flush memtable
    /// already started counting from, without telling that memtable to
    /// skip past the range the merge just claimed. Left unfixed, a later
    /// real upsert into the memtable silently reused an id a merged segment
    /// already owned — `MergeCursor` then saw the same doc id surface from
    /// two different sources on the same query, breaking the
    /// ascending-order guarantee `tachyon-query`'s WAND drivers depend on
    /// and panicking `RoaringBitmap::from_sorted_iter` in `executor.rs`.
    ///
    /// Fixing that (`MemTable::reserve`, called from `merge_locked`) traded
    /// one bug for a subtler one: a memtable that reserved a merge's range
    /// still *declares* that range as its own via `base()`/`next_doc_id()`
    /// even though nothing is ever live there, so a doc id can legitimately
    /// fall inside two sources' declared ranges at once now. This is
    /// asserted directly below, and separately the search-side fallout is
    /// checked: `SearchContext::is_live`/`field_len` used to stop at the
    /// *first* range-matching source (`executor.rs`), so a genuinely live
    /// document in the second, real owner was reported dead and silently
    /// excluded from every search. Both fixes are needed together.
    ///
    /// Many small flush+merge rounds are needed to give the bug room to
    /// manifest — a single merge round (every other merge test here) never
    /// exercised more than one live memtable generation past the merge.
    #[test]
    fn repeated_merges_never_hand_the_active_memtable_a_claimed_doc_id() {
        let (_dir, layout, config) = merge_harness(3, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        let n = 200;
        for i in 0..n {
            c.upsert(product(&(i + 1).to_string(), "widget gadget", 100 + i)).unwrap();
        }
        assert!(c.stats().num_segments > 1, "sanity: this many docs must have forced a merge");

        // Segments' own declared ranges must be pairwise disjoint — unlike
        // the memtable, a segment's range is fixed for its whole life at
        // exactly the ids one memtable generation used, so this always held
        // even before the fix; asserted here as a sanity check on the setup.
        let inner = c.inner.read();
        let mut seg_ranges: Vec<(DocId, DocId)> =
            inner.state.segments.iter().map(|s| (s.min_doc_id, s.max_doc_id)).collect();
        seg_ranges.sort_unstable();
        for w in seg_ranges.windows(2) {
            assert!(w[0].1 < w[1].0, "overlapping segment ranges {:?} and {:?}", w[0], w[1]);
        }
        drop(inner);

        // A broad, window-filling `any`-mode query with a small `limit` is
        // exactly what drove pruning through every source in the real
        // crash: this must not panic (the original failure mode). `found`
        // itself is allowed to be an approximate lower bound once pruning
        // engages (by design, see `wand.rs`'s module doc), so it isn't
        // checked here.
        c.search(SearchParams {
            q: Some("widget".into()),
            match_mode: Some("any".into()),
            limit: Some(5),
            ..Default::default()
        })
        .unwrap();

        // A window big enough to hold every match never lets pruning skip
        // anything (same reasoning as this file's other exact-count
        // assertions), so `found` must be exact here — this is what
        // actually confirms no document was lost to a doc-id collision, or
        // to a live document behind a reserved-but-declared memtable range
        // being reported dead.
        let exhaustive = c
            .search(SearchParams {
                q: Some("widget".into()),
                match_mode: Some("any".into()),
                limit: Some(n as usize),
                ..Default::default()
            })
            .unwrap();
        assert!(exhaustive.found_is_exact, "a full window must never skip a block");
        assert_eq!(exhaustive.found, n as usize);

        // A doc-id collision would also silently corrupt content (two
        // documents fighting over the same slot in different sources) —
        // confirm every original document still reads back correctly.
        for i in 0..n {
            let doc = c.get(&(i + 1).to_string()).unwrap();
            assert_eq!(doc["price"], json!(100 + i));
        }
    }

    // --- off-lock merge races -------------------------------------------
    //
    // A merge's build phase (`Collection::build_merge`) runs with no lock
    // held — the whole point of the off-lock rewrite — so anything that
    // used to be impossible mid-merge (a write landing while a merge is "in
    // progress") is now routine. These tests drive `snapshot_merge_locked`,
    // `build_merge`, and `swap_merge_locked` directly rather than through
    // `run_merge`, so each one can land an exact, deterministic write in
    // the gap between snapshot and swap instead of hoping a real thread
    // wins a race — the same three functions `run_merge` itself calls, in
    // the same order, just with a controlled interruption between stages
    // instead of nothing.
    //
    // Each test locks `merge_gate` itself first, exactly as `run_merge`'s
    // real callers do: without it, the `c.flush()`/`c.upsert()`/`c.delete()`
    // calls used to simulate "a write during the build phase" would trigger
    // their own *actual* automatic merge via `maybe_merge()` (nothing else
    // is holding the gate to stop them), which would confuse the very
    // scenario each test is trying to isolate.

    #[test]
    fn a_delete_during_the_merge_build_phase_is_not_resurrected() {
        let (_dir, layout, config) = merge_harness(2, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        c.upsert(product("1", "a", 1)).unwrap();
        c.upsert(product("2", "b", 2)).unwrap(); // segment 1: [1,2]
        c.upsert(product("3", "c", 3)).unwrap();
        c.upsert(product("4", "d", 4)).unwrap(); // segment 2: [3,4]
        assert_eq!(c.stats().num_segments, 2);

        let _gate = c.merge_gate.lock();
        let snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_merge_locked(&mut inner, &config).unwrap()
        };
        assert_eq!(snapshot.claimed, 4, "both victims were fully live at snapshot time");

        // A write lands "during the build phase": "2" was live when the
        // snapshot above was taken and is already baked into the merge
        // output build_merge is about to produce.
        assert!(c.delete("2").unwrap());

        let built = Collection::build_merge(&snapshot, c.schema(), &layout).unwrap();
        {
            let mut inner = c.inner.write();
            Collection::swap_merge_locked(&mut inner, c.schema(), &layout, snapshot, built)
                .unwrap();
        }
        drop(_gate);

        assert_eq!(c.stats().num_segments, 1);
        assert_eq!(c.stats().num_documents, 3, "\"2\" must not have been resurrected by the merge");
        assert!(matches!(c.get("2"), Err(Error::DocumentNotFound { .. })));
        for id in ["1", "3", "4"] {
            assert!(c.get(id).is_ok(), "doc {id} should have survived the merge");
        }

        // The restart path re-derives everything from `state.json` alone —
        // if the tombstone remap were wrong, this is where it would show.
        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(reopened.stats().num_documents, 3);
        assert!(matches!(reopened.get("2"), Err(Error::DocumentNotFound { .. })));
    }

    #[test]
    fn a_delete_during_a_second_merges_build_phase_targeting_an_already_merged_doc_is_not_resurrected(
    ) {
        // Same shape as the test above, but the deleted document has
        // already been through one merge before this second one's build
        // phase starts — its "old id" going into this merge's snapshot is
        // itself a merge output's id, not an original flush's.
        let (_dir, layout, config) = merge_harness(2, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        c.upsert(product("1", "a", 1)).unwrap();
        c.upsert(product("2", "b", 2)).unwrap(); // segment 1: [1,2]
        c.upsert(product("3", "c", 3)).unwrap();
        c.upsert(product("4", "d", 4)).unwrap(); // segment 2: [3,4]
        assert!(c.merge().unwrap(), "first merge: segments 1+2 fold into one");
        assert_eq!(c.stats().num_segments, 1);

        c.upsert(product("5", "e", 5)).unwrap();
        c.upsert(product("6", "f", 6)).unwrap(); // segment 3
        assert_eq!(c.stats().num_segments, 2, "the merged segment plus the new one");

        let _gate = c.merge_gate.lock();
        let snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_merge_locked(&mut inner, &config).unwrap()
        };
        assert_eq!(snapshot.claimed, 6);

        // "2" now lives in the *first* merge's output segment — one of
        // this second merge's two victims.
        assert!(c.delete("2").unwrap());

        let built = Collection::build_merge(&snapshot, c.schema(), &layout).unwrap();
        {
            let mut inner = c.inner.write();
            Collection::swap_merge_locked(&mut inner, c.schema(), &layout, snapshot, built)
                .unwrap();
        }
        drop(_gate);

        assert_eq!(c.stats().num_segments, 1);
        assert_eq!(c.stats().num_documents, 5, "\"2\" must not have been resurrected");
        assert!(matches!(c.get("2"), Err(Error::DocumentNotFound { .. })));
        for id in ["1", "3", "4", "5", "6"] {
            assert!(c.get(id).is_ok(), "doc {id} should have survived both merges");
        }

        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(reopened.stats().num_documents, 5);
        assert!(matches!(reopened.get("2"), Err(Error::DocumentNotFound { .. })));
    }

    #[test]
    fn an_upsert_during_the_merge_build_phase_supersedes_the_old_copy_without_duplication() {
        let (_dir, layout, config) = merge_harness(2, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        c.upsert(product("1", "Old", 100)).unwrap();
        c.upsert(product("2", "b", 2)).unwrap(); // segment 1: [1,2]
        c.upsert(product("3", "c", 3)).unwrap();
        c.upsert(product("4", "d", 4)).unwrap(); // segment 2: [3,4]
        assert_eq!(c.stats().num_segments, 2);

        let _gate = c.merge_gate.lock();
        let snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_merge_locked(&mut inner, &config).unwrap()
        };

        // "1"'s old copy lives in a victim about to be merged away. This
        // upsert's new doc id must land past the snapshot's reservation in
        // the active memtable, not collide with the merge's claimed range.
        c.upsert(product("1", "New", 200)).unwrap();

        let built = Collection::build_merge(&snapshot, c.schema(), &layout).unwrap();
        {
            let mut inner = c.inner.write();
            Collection::swap_merge_locked(&mut inner, c.schema(), &layout, snapshot, built)
                .unwrap();
        }
        drop(_gate);

        assert_eq!(c.get("1").unwrap()["title"], json!("New"));
        assert_eq!(
            c.stats().num_documents,
            4,
            "the merged-away old copy of \"1\" must not be visible alongside the new one"
        );

        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(reopened.get("1").unwrap()["title"], json!("New"));
        assert_eq!(reopened.stats().num_documents, 4);
    }

    #[test]
    fn a_flush_during_the_merge_build_phase_is_not_lost_and_does_not_collide_with_it() {
        let (_dir, layout, config) = merge_harness(2, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        c.upsert(product("1", "a", 1)).unwrap();
        c.upsert(product("2", "b", 2)).unwrap(); // segment 1
        c.upsert(product("3", "c", 3)).unwrap();
        c.upsert(product("4", "d", 4)).unwrap(); // segment 2
        assert_eq!(c.stats().num_segments, 2);

        let _gate = c.merge_gate.lock();
        let snapshot = {
            let mut inner = c.inner.write();
            Collection::snapshot_merge_locked(&mut inner, &config).unwrap()
        };

        // A brand new segment commits while the merge's build phase would
        // be running — an explicit flush, so this test controls exactly
        // when it happens rather than relying on the doc-count threshold.
        c.upsert(product("5", "e", 5)).unwrap();
        assert!(c.flush().unwrap());
        assert_eq!(
            c.stats().num_segments,
            3,
            "the two not-yet-swapped victims plus the concurrently flushed one"
        );

        let built = Collection::build_merge(&snapshot, c.schema(), &layout).unwrap();
        {
            let mut inner = c.inner.write();
            Collection::swap_merge_locked(&mut inner, c.schema(), &layout, snapshot, built)
                .unwrap();
        }
        drop(_gate);

        // Two victims replaced by one merged segment, plus the
        // concurrently flushed one that must have survived the swap.
        assert_eq!(c.stats().num_segments, 2);
        assert_eq!(c.stats().num_documents, 5);
        for id in ["1", "2", "3", "4", "5"] {
            assert!(c.get(id).is_ok(), "doc {id} must survive");
        }

        // No id collision between the merge's output range and the
        // concurrently flushed segment's range.
        let inner = c.inner.read();
        let mut ranges: Vec<(DocId, DocId)> =
            inner.state.segments.iter().map(|s| (s.min_doc_id, s.max_doc_id)).collect();
        ranges.sort_unstable();
        for w in ranges.windows(2) {
            assert!(w[0].1 < w[1].0, "overlapping segment ranges {:?} and {:?}", w[0], w[1]);
        }
        drop(inner);

        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(reopened.stats().num_documents, 5);
        for id in ["1", "2", "3", "4", "5"] {
            assert!(reopened.get(id).is_ok());
        }
    }

    #[test]
    fn a_long_run_of_interleaved_writes_and_merges_never_resurrects_a_deleted_document() {
        // Fully single-threaded and deterministic, but exercises the same
        // pattern the real bug this regression-tests came from: many small,
        // frequent merges (`merge_trigger_segments`/`merge_fan_in` = 2, so
        // almost every flush becomes eligible) interleaved with deletes,
        // over enough iterations that a merge's own reservation
        // (`snapshot_merge_locked`, via `MemTable::reserve`) lands in the
        // *middle* of an actively-written memtable — something the pre-
        // off-lock design could never produce, since its merge always ran
        // immediately after its own triggering flush, on a guaranteed-fresh
        // memtable. That gave a later flush of that same memtable a
        // *wider* declared range than what it actually held live (real
        // documents both before and after the reservation's hole), which
        // in turn made the merge that eventually retired an *unrelated*
        // segment sharing that numeric territory prune a tombstone that
        // didn't belong to it — silently resurrecting a deleted document.
        // Fixed by pruning a retired segment's tombstones through its own
        // presence bitmap (exact, never overlapping) rather than its
        // declared range (can overlap another segment's, since a single
        // contiguous range can't describe "content flanking someone
        // else's reservation").
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        let config = EngineConfig::new(dir.path())
            .with_max_memtable_docs(3)
            .with_merge_trigger_segments(2)
            .with_merge_fan_in(2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        const N: usize = 150;
        let mut expected_alive: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for i in 0..N {
            let id = i.to_string();
            c.upsert(product(&id, "widget", i as i64)).unwrap();
            expected_alive.insert(id);
            if i % 5 == 0 {
                let victim = i.saturating_sub(3).to_string();
                if c.delete(&victim).unwrap() {
                    expected_alive.remove(&victim);
                }
            }
            if i % 11 == 0 {
                c.merge().unwrap();
            }
        }

        let mut mismatches = Vec::new();
        for i in 0..N {
            let id = i.to_string();
            let alive = c.get(&id).is_ok();
            let should_be_alive = expected_alive.contains(&id);
            if alive != should_be_alive {
                mismatches.push((id, alive, should_be_alive));
            }
        }
        assert!(mismatches.is_empty(), "(id, actually_alive, should_be_alive): {mismatches:?}");
        assert_eq!(c.stats().num_documents, expected_alive.len() as u64);

        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(reopened.stats().num_documents, expected_alive.len() as u64);
        for id in &expected_alive {
            assert!(reopened.get(id).is_ok());
        }
    }

    #[test]
    fn a_merge_gate_held_elsewhere_makes_maybe_merge_a_harmless_no_op() {
        // `maybe_merge` (the automatic post-write trigger) must never block
        // on `merge_gate` — it uses `try_lock` and simply skips. Verified
        // directly: hold the gate, perform writes that would ordinarily
        // trigger a merge, and confirm segment count is left untouched by
        // anything other than the flushes those writes themselves caused.
        let (_dir, layout, config) = merge_harness(1, 2);
        let c = Collection::create(&layout, schema(), &config).unwrap();

        let _gate = c.merge_gate.lock();
        for i in 0..6 {
            c.upsert(product(&(i + 1).to_string(), "x", i)).unwrap();
        }
        // trigger=1, max_memtable_docs=2: 3 flushes, and with the gate held
        // throughout, not one of `maybe_merge`'s attempts could have run.
        assert_eq!(c.stats().num_segments, 3, "merging must have been skipped, not blocked on");
        drop(_gate);

        // Once released, the very next write's own `maybe_merge` call
        // catches up.
        c.upsert(product("7", "x", 7)).unwrap();
        assert!(c.stats().num_segments < 3, "merging should resume once the gate is free");
        assert_eq!(c.stats().num_documents, 7);
    }

    #[test]
    fn concurrent_writes_and_merges_leave_the_collection_internally_consistent() {
        // Real threads this time, not the controlled single-threaded
        // interleavings above — a broad smoke test that nothing panics,
        // deadlocks, or corrupts state under genuine concurrent load, with
        // an independently computed expectation to check the result
        // against rather than just "it didn't crash". Each thread owns a
        // disjoint id namespace (`"{thread}-{i}"`) so the expected outcome
        // for a given id never depends on inter-thread ordering — only on
        // that one thread's own sequence, which every thread runs
        // identically regardless of how the others interleave with it.
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.initialize().unwrap();
        let config = EngineConfig::new(dir.path())
            .with_max_memtable_docs(3)
            .with_merge_trigger_segments(2)
            .with_merge_fan_in(2);
        let c = Arc::new(Collection::create(&layout, schema(), &config).unwrap());

        const THREADS: usize = 4;
        // Small enough that the expected-alive count stays under
        // `tachyon_query::request::MAX_LIMIT` (250) — the search assertion
        // below needs a window big enough to hold every match, and the
        // API refuses a `limit` past that cap.
        const PER_THREAD: usize = 50;

        let mut handles = Vec::with_capacity(THREADS);
        for t in 0..THREADS {
            let c = Arc::clone(&c);
            handles.push(std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let id = format!("{t}-{i}");
                    c.upsert(product(&id, "widget", (t * 1000 + i) as i64)).unwrap();
                    if i % 5 == 0 {
                        let victim = format!("{t}-{}", i.saturating_sub(3));
                        c.delete(&victim).unwrap();
                    }
                    if i % 11 == 0 {
                        c.merge().unwrap();
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().expect("writer thread panicked");
        }

        // The same delete pattern each thread ran, replayed here against a
        // plain `HashSet` instead of the collection — an independent
        // reference for what should still be alive.
        let mut expected_alive: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for t in 0..THREADS {
            for i in 0..PER_THREAD {
                expected_alive.insert(format!("{t}-{i}"));
            }
            for i in (0..PER_THREAD).step_by(5) {
                expected_alive.remove(&format!("{t}-{}", i.saturating_sub(3)));
            }
        }

        for id in &expected_alive {
            assert!(c.get(id).is_ok(), "expected {id} to be alive");
        }
        assert_eq!(c.stats().num_documents, expected_alive.len() as u64);

        // Global consistency, not just per-id: a window big enough to hold
        // every match must find exactly the expected count, exactly.
        let found = c
            .search(SearchParams {
                q: Some("widget".into()),
                match_mode: Some("any".into()),
                limit: Some(expected_alive.len() + 10),
                ..Default::default()
            })
            .unwrap();
        assert!(found.found_is_exact, "a full window must never skip a block");
        assert_eq!(found.found, expected_alive.len());

        drop(c);
        let reopened = Collection::open(&layout, "products", &config).unwrap();
        assert_eq!(reopened.stats().num_documents, expected_alive.len() as u64);
        for id in &expected_alive {
            assert!(reopened.get(id).is_ok());
        }
    }
}
