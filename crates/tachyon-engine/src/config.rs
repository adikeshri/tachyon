//! Engine-wide tunables.

use std::path::PathBuf;
use std::time::Duration;

use tachyon_storage::SyncPolicy;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Root of the data directory. Created if absent.
    pub data_dir: PathBuf,
    /// How aggressively the WAL is fsynced. [`SyncPolicy::Always`] is the
    /// default: no acknowledged write is ever lost.
    pub sync_policy: SyncPolicy,
    /// Flush the memtable into a segment once it holds this many documents.
    pub max_memtable_docs: usize,
    /// …or once it occupies roughly this many bytes, whichever comes first.
    /// Counts the stored documents, the inverted index, and the columns — the
    /// index alone is usually the larger half.
    pub max_memtable_bytes: usize,
    /// Attempt a merge once a collection holds more than this many committed
    /// segments. Each flush checks this once, after its own commit — a
    /// search fans out across every committed segment with nothing merging
    /// them back together, so segment count (not corpus size alone) is what
    /// drives query latency up over a collection's life.
    pub merge_trigger_segments: usize,
    /// How many segments one merge folds together — the smallest this many
    /// by document count, size-tiered in spirit. At most one merge runs per
    /// flush, so segment count converges down over several flushes rather
    /// than in one large pause.
    pub merge_fan_in: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            data_dir: PathBuf::from("./data"),
            sync_policy: SyncPolicy::Always,
            // ~100k docs keeps a flush short enough not to stall ingest, while
            // being large enough that segment count stays manageable.
            max_memtable_docs: 100_000,
            max_memtable_bytes: 256 * 1024 * 1024,
            merge_trigger_segments: 8,
            merge_fan_in: 4,
        }
    }
}

impl EngineConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        EngineConfig { data_dir: data_dir.into(), ..Default::default() }
    }

    /// Trade a bounded window of durability for ingest throughput.
    pub fn with_sync_interval(mut self, interval: Duration) -> Self {
        self.sync_policy = SyncPolicy::Interval(interval);
        self
    }

    pub fn with_sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.sync_policy = policy;
        self
    }

    pub fn with_max_memtable_docs(mut self, docs: usize) -> Self {
        self.max_memtable_docs = docs;
        self
    }

    pub fn with_merge_trigger_segments(mut self, segments: usize) -> Self {
        self.merge_trigger_segments = segments;
        self
    }

    pub fn with_merge_fan_in(mut self, fan_in: usize) -> Self {
        self.merge_fan_in = fan_in;
        self
    }
}
