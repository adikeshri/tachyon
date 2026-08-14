//! Query analytics (PRD §7.9).
//!
//! Every search records its text, result count, latency, and timestamp. Two
//! shapes of question follow from that, and they want different structures:
//!
//! - *Which queries are popular, and which return nothing?* — an aggregate per
//!   distinct query string, so a query asked a million times costs one entry.
//! - *How fast are we?* — percentiles, which need a distribution rather than a
//!   per-query average.
//!
//! # Bounded by construction
//!
//! A search engine is a public surface; the set of distinct queries is
//! attacker-controlled and unbounded. The aggregate map is capped at
//! [`MAX_TRACKED_QUERIES`] and compacts by dropping the least-asked half when
//! it fills, so a flood of unique junk cannot grow memory without limit. It
//! also cannot hide a genuinely popular query, which by definition survives
//! the cut.
//!
//! # Not durable
//!
//! Analytics live in memory and start empty after a restart. They are an
//! operational signal, not a system of record, and writing them down would put
//! a disk write on the read path.

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::Serialize;
use utoipa::ToSchema;

use tachyon_core::datetime::now_millis;

/// Distinct queries tracked before compaction.
pub const MAX_TRACKED_QUERIES: usize = 10_000;

/// Latency buckets, in microseconds. Exponential, so the resolution is fine
/// where searches actually land (tens of microseconds to a few milliseconds)
/// and coarse out in the tail where only the magnitude matters.
const NUM_BUCKETS: usize = 32;

/// A latency distribution in fixed exponential buckets.
///
/// Bucket `i` holds observations in `[2^i, 2^(i+1))` microseconds, which spans
/// a microsecond to roughly an hour in 32 buckets — constant memory, no
/// allocation on the record path, and percentiles accurate to within one
/// bucket width.
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    buckets: [u64; NUM_BUCKETS],
    count: u64,
    total_micros: u64,
    max_micros: u64,
}

impl LatencyHistogram {
    pub fn record(&mut self, micros: u64) {
        let bucket = bucket_for(micros);
        self.buckets[bucket] += 1;
        self.count += 1;
        self.total_micros = self.total_micros.saturating_add(micros);
        self.max_micros = self.max_micros.max(micros);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn mean_micros(&self) -> u64 {
        self.total_micros.checked_div(self.count).unwrap_or(0)
    }

    pub fn max_micros(&self) -> u64 {
        self.max_micros
    }

    /// Value at `quantile` (0.0–1.0), in microseconds.
    ///
    /// Returns the upper edge of the bucket the quantile falls in: reporting a
    /// latency budget as met when it might not be is the worse error, so this
    /// rounds pessimistically.
    pub fn quantile(&self, quantile: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = (quantile.clamp(0.0, 1.0) * self.count as f64).ceil().max(1.0) as u64;

        let mut seen = 0u64;
        for (i, bucket) in self.buckets.iter().enumerate() {
            seen += bucket;
            if seen >= target {
                let upper = 1u64 << (i + 1);
                return upper.min(self.max_micros.max(1));
            }
        }
        self.max_micros
    }
}

fn bucket_for(micros: u64) -> usize {
    if micros < 2 {
        return 0;
    }
    // Index of the highest set bit: the exponent of the containing power of two.
    let bucket = (u64::BITS - 1 - micros.leading_zeros()) as usize;
    bucket.min(NUM_BUCKETS - 1)
}

/// What we know about one distinct query string.
#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct QueryStat {
    pub query: String,
    pub collection: String,
    /// Times this query has been run.
    pub count: u64,
    /// Times it returned nothing.
    pub zero_result_count: u64,
    /// Results the most recent run returned.
    pub last_result_count: u64,
    pub avg_latency_ms: f64,
    /// Epoch milliseconds of the most recent run.
    pub last_seen: i64,
    #[serde(skip)]
    total_micros: u64,
}

#[derive(Debug, Default)]
struct Inner {
    queries: HashMap<(String, String), QueryStat>,
    latencies: LatencyHistogram,
    /// Searches recorded since start, including ones dropped by compaction.
    total_searches: u64,
    /// Queries discarded because the map was full, so the caller can tell
    /// "nobody searched that" from "we stopped counting".
    dropped_queries: u64,
}

#[derive(Debug, Default)]
pub struct Analytics {
    inner: RwLock<Inner>,
}

impl Analytics {
    pub fn new() -> Analytics {
        Analytics::default()
    }

    /// Record one search.
    pub fn record_search(
        &self,
        collection: &str,
        query: &str,
        result_count: usize,
        latency_micros: u64,
    ) {
        let mut inner = self.inner.write();
        inner.total_searches += 1;
        inner.latencies.record(latency_micros);

        // An empty query is a browse, not a search anyone typed; counting it
        // would swamp the top-queries list on any UI that lists a collection.
        let query = query.trim();
        if query.is_empty() {
            return;
        }

        let key = (collection.to_string(), query.to_string());
        if !inner.queries.contains_key(&key) && inner.queries.len() >= MAX_TRACKED_QUERIES {
            inner.compact();
            if inner.queries.len() >= MAX_TRACKED_QUERIES {
                inner.dropped_queries += 1;
                return;
            }
        }

        let stat = inner.queries.entry(key).or_insert_with(|| QueryStat {
            query: query.to_string(),
            collection: collection.to_string(),
            ..Default::default()
        });

        stat.count += 1;
        stat.last_result_count = result_count as u64;
        stat.last_seen = now_millis();
        stat.total_micros = stat.total_micros.saturating_add(latency_micros);
        stat.avg_latency_ms = stat.total_micros as f64 / stat.count as f64 / 1000.0;
        if result_count == 0 {
            stat.zero_result_count += 1;
        }
    }

    /// Most-asked queries, most frequent first (PRD `/analytics/top`).
    pub fn top_queries(&self, collection: Option<&str>, limit: usize) -> Vec<QueryStat> {
        self.ranked(collection, limit, |stat| stat.count)
    }

    /// Queries that most often return nothing (PRD `/analytics/zero-results`).
    ///
    /// Ranked by how many times they came back empty, not by whether the last
    /// run did: a query that fails nine times in ten is the one worth fixing.
    pub fn zero_result_queries(&self, collection: Option<&str>, limit: usize) -> Vec<QueryStat> {
        self.ranked(collection, limit, |stat| stat.zero_result_count)
    }

    fn ranked(
        &self,
        collection: Option<&str>,
        limit: usize,
        rank: impl Fn(&QueryStat) -> u64,
    ) -> Vec<QueryStat> {
        let inner = self.inner.read();
        let mut stats: Vec<QueryStat> = inner
            .queries
            .values()
            .filter(|stat| collection.is_none_or(|name| stat.collection == name))
            .filter(|stat| rank(stat) > 0)
            .cloned()
            .collect();

        stats.sort_by(|a, b| {
            rank(b)
                .cmp(&rank(a))
                .then(b.last_seen.cmp(&a.last_seen))
                .then_with(|| a.query.cmp(&b.query))
        });
        stats.truncate(limit);
        stats
    }

    /// Latency percentiles (PRD `/analytics/latency`, §15 metrics).
    pub fn latency(&self) -> LatencySummary {
        let inner = self.inner.read();
        LatencySummary {
            count: inner.latencies.count(),
            mean_ms: inner.latencies.mean_micros() as f64 / 1000.0,
            p50_ms: inner.latencies.quantile(0.50) as f64 / 1000.0,
            p95_ms: inner.latencies.quantile(0.95) as f64 / 1000.0,
            p99_ms: inner.latencies.quantile(0.99) as f64 / 1000.0,
            max_ms: inner.latencies.max_micros() as f64 / 1000.0,
        }
    }

    pub fn total_searches(&self) -> u64 {
        self.inner.read().total_searches
    }

    pub fn tracked_queries(&self) -> usize {
        self.inner.read().queries.len()
    }

    pub fn dropped_queries(&self) -> u64 {
        self.inner.read().dropped_queries
    }
}

impl Inner {
    /// Halve the tracked set, keeping the most-asked queries.
    fn compact(&mut self) {
        let mut counts: Vec<u64> = self.queries.values().map(|stat| stat.count).collect();
        if counts.is_empty() {
            return;
        }
        counts.sort_unstable();
        let cutoff = counts[counts.len() / 2];

        let before = self.queries.len();
        self.queries.retain(|_, stat| stat.count > cutoff);
        self.dropped_queries += (before - self.queries.len()) as u64;

        // Everything sharing the median count is a possibility; if that was
        // all of them, nothing was dropped and the map must still shrink.
        if self.queries.len() >= MAX_TRACKED_QUERIES {
            let keep: Vec<(String, String)> =
                self.queries.keys().take(MAX_TRACKED_QUERIES / 2).cloned().collect();
            let before = self.queries.len();
            self.queries.retain(|key, _| keep.contains(key));
            self.dropped_queries += (before - self.queries.len()) as u64;
        }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LatencySummary {
    pub count: u64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_repeated_queries() {
        let a = Analytics::new();
        for _ in 0..3 {
            a.record_search("products", "wireless mouse", 5, 1_000);
        }
        a.record_search("products", "keyboard", 2, 1_000);

        let top = a.top_queries(None, 10);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].query, "wireless mouse");
        assert_eq!(top[0].count, 3);
        assert_eq!(top[1].query, "keyboard");
        assert_eq!(a.total_searches(), 4);
    }

    #[test]
    fn tracks_zero_result_queries_separately() {
        let a = Analytics::new();
        a.record_search("products", "helicopter", 0, 500);
        a.record_search("products", "helicopter", 0, 500);
        a.record_search("products", "mouse", 7, 500);

        let empty = a.zero_result_queries(None, 10);
        assert_eq!(empty.len(), 1, "only the query that came back empty");
        assert_eq!(empty[0].query, "helicopter");
        assert_eq!(empty[0].zero_result_count, 2);
    }

    #[test]
    fn a_query_that_started_failing_shows_up_in_both_lists() {
        let a = Analytics::new();
        a.record_search("products", "mouse", 7, 500);
        a.record_search("products", "mouse", 0, 500);

        assert_eq!(a.top_queries(None, 10)[0].count, 2);
        let empty = a.zero_result_queries(None, 10);
        assert_eq!(empty[0].zero_result_count, 1);
        assert_eq!(empty[0].last_result_count, 0);
    }

    #[test]
    fn results_can_be_scoped_to_one_collection() {
        let a = Analytics::new();
        a.record_search("products", "mouse", 1, 500);
        a.record_search("articles", "mouse", 1, 500);

        assert_eq!(a.top_queries(None, 10).len(), 2);
        assert_eq!(a.top_queries(Some("products"), 10).len(), 1);
        assert_eq!(a.top_queries(Some("products"), 10)[0].collection, "products");
        assert!(a.top_queries(Some("nothing"), 10).is_empty());
    }

    #[test]
    fn empty_queries_count_towards_latency_but_not_the_top_list() {
        let a = Analytics::new();
        a.record_search("products", "", 100, 500);
        a.record_search("products", "   ", 100, 500);

        assert!(a.top_queries(None, 10).is_empty(), "browsing is not a search term");
        assert_eq!(a.latency().count, 2, "but it is still a request we served");
        assert_eq!(a.total_searches(), 2);
    }

    #[test]
    fn averages_latency_per_query() {
        let a = Analytics::new();
        a.record_search("products", "mouse", 1, 1_000);
        a.record_search("products", "mouse", 1, 3_000);
        let stat = &a.top_queries(None, 1)[0];
        assert!((stat.avg_latency_ms - 2.0).abs() < 1e-9, "got {}", stat.avg_latency_ms);
    }

    #[test]
    fn limit_is_honoured() {
        let a = Analytics::new();
        for i in 0..20 {
            for _ in 0..=i {
                a.record_search("products", &format!("q{i}"), 1, 500);
            }
        }
        assert_eq!(a.top_queries(None, 5).len(), 5);
        assert_eq!(a.top_queries(None, 5)[0].query, "q19", "most frequent first");
    }

    #[test]
    fn percentiles_track_the_distribution() {
        let a = Analytics::new();
        // 99 fast queries and one slow one.
        for _ in 0..99 {
            a.record_search("products", "fast", 1, 1_000); // 1ms
        }
        a.record_search("products", "slow", 1, 500_000); // 500ms

        let latency = a.latency();
        assert_eq!(latency.count, 100);
        assert!(latency.p50_ms <= 4.0, "p50 should sit with the fast queries: {latency:?}");
        assert!(latency.p99_ms >= 1.0);
        assert!(latency.max_ms >= 500.0);
        assert!(latency.p50_ms <= latency.p95_ms && latency.p95_ms <= latency.p99_ms);
    }

    #[test]
    fn an_empty_histogram_reports_zeroes_not_garbage() {
        let a = Analytics::new();
        let latency = a.latency();
        assert_eq!(latency.count, 0);
        assert_eq!(latency.p50_ms, 0.0);
        assert_eq!(latency.p99_ms, 0.0);
        assert_eq!(latency.max_ms, 0.0);
    }

    #[test]
    fn histogram_buckets_are_monotonic() {
        let mut h = LatencyHistogram::default();
        for micros in [1u64, 10, 100, 1_000, 10_000, 100_000] {
            h.record(micros);
        }
        assert!(h.quantile(0.0) <= h.quantile(0.5));
        assert!(h.quantile(0.5) <= h.quantile(0.99));
        assert!(h.quantile(0.99) <= h.max_micros().max(1));
    }

    #[test]
    fn tracking_is_bounded_by_a_flood_of_unique_queries() {
        let a = Analytics::new();

        // One query everybody asks…
        for _ in 0..50 {
            a.record_search("products", "popular", 1, 500);
        }
        // …and a flood of junk nobody repeats.
        for i in 0..(MAX_TRACKED_QUERIES * 2) {
            a.record_search("products", &format!("junk-{i}"), 0, 500);
        }

        assert!(
            a.tracked_queries() <= MAX_TRACKED_QUERIES,
            "tracked {} queries",
            a.tracked_queries()
        );
        assert!(a.dropped_queries() > 0, "compaction should have reported drops");

        // The popular query survived, which is the whole point.
        let top = a.top_queries(None, 1);
        assert_eq!(top[0].query, "popular");
        assert_eq!(top[0].count, 50);
        // And every request is still counted, dropped or not.
        assert_eq!(a.total_searches(), 50 + MAX_TRACKED_QUERIES as u64 * 2);
    }
}
