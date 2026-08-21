//! Benchmark harness (PRD §6, §20).
//!
//! Measures the engine directly rather than over HTTP: the numbers the PRD
//! sets targets for — search latency, indexing throughput, memory — are engine
//! properties, and putting a socket in front of them would measure the socket.
//!
//! ```bash
//! cargo run --release -p tachyon-bench -- --documents 100000 --queries 2000
//! ```

mod corpus;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;

use tachyon_engine::{Collection, EngineConfig};
use tachyon_query::SearchParams;
use tachyon_storage::{Layout, SyncPolicy};

use corpus::Rng;

#[derive(Debug, Parser)]
#[command(name = "tachyon-bench", about = "Indexing and search benchmarks for Tachyon")]
struct Args {
    /// Documents to index.
    #[arg(long, default_value_t = 100_000)]
    documents: usize,

    /// Searches to run when measuring latency.
    #[arg(long, default_value_t = 2_000)]
    queries: usize,

    /// Documents per indexing batch.
    #[arg(long, default_value_t = 1_000)]
    batch_size: usize,

    /// Seed for the corpus generator, so runs are comparable.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// fsync every batch, as production would. Off by default: at these batch
    /// sizes the benchmark would otherwise measure the disk, not the engine.
    #[arg(long)]
    fsync: bool,

    /// Also benchmark filtered, sorted, and faceted searches.
    #[arg(long, default_value_t = true)]
    full: bool,

    /// Also run a flush-under-load scenario: a low `--max-memtable-docs` so
    /// segment flushes happen mid-run, with a background thread searching
    /// continuously, to measure what a flush costs concurrent readers. Since
    /// off-lock flush, that's no longer "however long the whole encode
    /// takes" — the write lock is held only for a brief seal before the
    /// build and an equally brief commit after it, with searches and writes
    /// proceeding normally in between (mirroring off-lock merge). Off by
    /// default: the scenario above stays flush-free on purpose, so its
    /// numbers remain a stable baseline unaffected by the flush path's own
    /// performance.
    #[arg(long)]
    flush_scenario: bool,

    /// Memtable document threshold for `--flush-scenario`, chosen well below
    /// `--documents` so several flushes happen during one run.
    #[arg(long, default_value_t = 5_000)]
    flush_scenario_max_memtable_docs: usize,

    /// Multiplies the corpus's distinct term count by suffixing each base
    /// word with a bucket in `0..vocab_scale`. `1` (the default) reproduces
    /// the original ~64-term corpus, where one query word matches ~6% of the
    /// collection — deliberately broad, the expensive case. Real catalogues
    /// look much more like a higher scale: a query term matching a thin
    /// slice of the corpus rather than a wide swath of it.
    #[arg(long, default_value_t = 1)]
    vocab_scale: usize,

    /// Memtable document threshold for the main Indexing/Search sections
    /// (independent of `--flush-scenario-max-memtable-docs`, which only
    /// applies to `--flush-scenario`). Defaults to unbounded so the main
    /// section keeps measuring the index rather than the flush policy, same
    /// as always — but that also means it never touches `SegmentReader` at
    /// all: every read comes from the one giant memtable, so anything that
    /// only pays off for on-disk segments (column caching, block-structured
    /// postings, the autocomplete fast count) is invisible in the default
    /// run. Set this below `--documents` to measure the collection the way
    /// it actually looks in steady state: mostly segments, not one memtable.
    #[arg(long, default_value_t = usize::MAX)]
    max_memtable_docs: usize,

    /// Also run a sustained-concurrency scenario: this many threads, each
    /// continuously issuing plain-query searches against one shared,
    /// already-indexed collection for `--concurrency-duration-secs`,
    /// reporting aggregate throughput and mean per-query processing time —
    /// the same pair Typesense's own published benchmarks report ("N
    /// concurrent queries per second, average processing time of Xms").
    /// `Collection::search` takes only a read lock, so this needs no engine
    /// change to be safe — `--flush-scenario` above already proves the same
    /// `Arc<Collection>` + multi-thread pattern works. 0 (the default)
    /// skips this: every section above runs single-threaded, which is the
    /// quantity that number is NOT, so without this flag nothing in this
    /// binary's output is directly comparable to it.
    #[arg(long, default_value_t = 0)]
    concurrency: usize,

    /// How long `--concurrency` hammers the shared collection.
    #[arg(long, default_value_t = 10)]
    concurrency_duration_secs: u64,

    /// Attempt a merge once a collection holds more than this many committed
    /// segments — see `EngineConfig::merge_trigger_segments`. Applies to the
    /// main Indexing/Search section and `--flush-scenario`; the default
    /// matches `EngineConfig::default()` so omitting this flag changes
    /// nothing about existing runs.
    #[arg(long, default_value_t = 8)]
    merge_trigger_segments: usize,

    /// How many segments one merge folds together — see
    /// `EngineConfig::merge_fan_in`.
    #[arg(long, default_value_t = 4)]
    merge_fan_in: usize,

    /// Also run a merge scenario: index into a collection with auto-merge
    /// disabled until several segments have accumulated, then explicitly
    /// call `Collection::merge()` in a loop, reporting each merge's own wall
    /// time and RSS delta — the number the streaming merge rewrite exists to
    /// shrink, isolated from indexing and query traffic so it isn't averaged
    /// away against everything else a full run does.
    #[arg(long)]
    merge_scenario: bool,

    /// Documents per segment for `--merge-scenario`'s setup phase (i.e. its
    /// own `--max-memtable-docs`), and the fan-in each explicit merge call
    /// folds together.
    #[arg(long, default_value_t = 100_000)]
    merge_scenario_segment_docs: usize,

    #[arg(long, default_value_t = 4)]
    merge_scenario_fan_in: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let dir = tempfile::tempdir()?;
    let layout = Layout::new(dir.path());
    layout.initialize()?;

    let config = EngineConfig::new(dir.path()).with_sync_policy(if args.fsync {
        SyncPolicy::Always
    } else {
        SyncPolicy::Interval(Duration::from_millis(200))
    });
    // Unbounded by default: the whole corpus stays in one memtable so this
    // measures the index, not the flush policy. Override with
    // `--max-memtable-docs` to measure against on-disk segments instead.
    let config = config
        .with_max_memtable_docs(args.max_memtable_docs)
        .with_merge_trigger_segments(args.merge_trigger_segments)
        .with_merge_fan_in(args.merge_fan_in);

    println!("Tachyon benchmark");
    println!("  documents   {}", args.documents);
    println!("  queries     {}", args.queries);
    println!("  batch size  {}", args.batch_size);
    println!("  fsync       {}", if args.fsync { "every batch" } else { "every 200ms" });
    println!(
        "  vocab scale {}  ({})",
        args.vocab_scale,
        if args.vocab_scale <= 1 {
            "fixed ~64-term corpus, deliberately broad matches"
        } else {
            "widened vocabulary, thinner matches per term"
        }
    );
    println!(
        "  max memtable docs  {}",
        if args.max_memtable_docs == usize::MAX {
            // Still subject to `max_memtable_bytes` (default 256 MiB), so a
            // large `--documents` flushes into segments anyway — this only
            // disables the doc-count trigger, not flushing altogether.
            "unbounded by doc count (256 MiB byte threshold still applies)".to_string()
        } else {
            args.max_memtable_docs.to_string()
        }
    );
    println!();

    let collection = Collection::create(&layout, corpus::schema("products"), &config)?;

    // --- Indexing ---------------------------------------------------------
    let mut rng = Rng::new(args.seed);
    let started = Instant::now();
    let mut indexed = 0usize;

    while indexed < args.documents {
        let this_batch = args.batch_size.min(args.documents - indexed);
        let batch: Vec<_> = (0..this_batch)
            .map(|i| corpus::document(&mut rng, indexed + i, args.vocab_scale))
            .collect();
        let report = collection.upsert_batch(batch)?;
        if report.num_failed > 0 {
            return Err(format!("{} documents failed to index", report.num_failed).into());
        }
        indexed += this_batch;
    }
    collection.sync()?;
    let index_elapsed = started.elapsed();

    let stats = collection.stats();
    println!("Indexing");
    println!("  elapsed        {:.2}s", index_elapsed.as_secs_f64());
    println!(
        "  throughput     {:.0} docs/sec  (target 10,000)",
        args.documents as f64 / index_elapsed.as_secs_f64()
    );
    println!("  documents      {}", stats.num_documents);
    println!("  segments       {}", stats.num_segments);
    println!("  memtable       {}", human_bytes(stats.memtable_bytes));
    println!("  wal            {}", human_bytes(stats.wal_bytes as usize));
    println!("  bytes/doc      {}", human_bytes(stats.memtable_bytes / args.documents.max(1)));
    println!("  rss (peak)     {}", human_bytes(peak_rss_bytes()));
    if let Some(rss) = current_rss_bytes() {
        println!("  rss (steady)   {}", human_bytes(rss));
    }
    println!();

    // --- Search -----------------------------------------------------------
    let mut rng = Rng::new(args.seed ^ 0x5eed);
    let queries = corpus::queries(&mut rng, args.queries, args.vocab_scale);

    let plain = measure(&collection, &queries, |q| SearchParams {
        q: Some(q.to_string()),
        ..Default::default()
    })?;
    report("Search (q only)", &plain, 30.0);

    if args.full {
        let filtered = measure(&collection, &queries, |q| SearchParams {
            q: Some(q.to_string()),
            filter: Some("price:<25000 && rating:>2.0".into()),
            ..Default::default()
        })?;
        report("Search + filter", &filtered, 30.0);

        let sorted = measure(&collection, &queries, |q| SearchParams {
            q: Some(q.to_string()),
            sort: Some("_text_match:desc,price:asc".into()),
            ..Default::default()
        })?;
        report("Search + sort", &sorted, 30.0);

        let faceted = measure(&collection, &queries, |q| SearchParams {
            q: Some(q.to_string()),
            facet: Some("brand,category".into()),
            ..Default::default()
        })?;
        report("Search + facets", &faceted, 30.0);
    }

    // --- Autocomplete -----------------------------------------------------
    let prefixes: Vec<String> =
        queries.iter().map(|q| q.chars().take(3).collect::<String>()).collect();
    let mut suggest_latencies = Vec::with_capacity(prefixes.len());
    let mut suggestions = 0usize;
    for prefix in &prefixes {
        let started = Instant::now();
        let response = collection.suggest(tachyon_query::SuggestParams {
            q: Some(prefix.clone()),
            ..Default::default()
        })?;
        suggest_latencies.push(started.elapsed().as_micros() as u64);
        suggestions += response.suggestions.len();
    }
    let suggest = Measurement { latencies: suggest_latencies, total_hits: suggestions as u64 };
    report("Autocomplete", &suggest, 5.0);

    if args.flush_scenario {
        run_flush_scenario(&args)?;
    }

    if args.concurrency > 0 {
        run_concurrency_scenario(&args)?;
    }

    if args.merge_scenario {
        run_merge_scenario(&args)?;
    }

    Ok(())
}

/// Index enough documents to accumulate several on-disk segments with
/// auto-merge disabled, then call `Collection::merge()` explicitly, once per
/// `--merge-scenario-fan-in` segments, reporting each call's own wall time
/// and RSS delta. Isolates the merge path's cost from indexing throughput
/// and query latency, which the main run's aggregate numbers average it into.
fn run_merge_scenario(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let segments = args.documents.div_ceil(args.merge_scenario_segment_docs).max(2);
    println!("Merge scenario");
    println!("  segments                {segments}");
    println!("  docs/segment            {}", args.merge_scenario_segment_docs);
    println!("  merge fan-in            {}", args.merge_scenario_fan_in);
    println!();

    let dir = tempfile::tempdir()?;
    let layout = Layout::new(dir.path());
    layout.initialize()?;
    let config = EngineConfig::new(dir.path())
        .with_sync_policy(SyncPolicy::Interval(Duration::from_millis(200)))
        .with_max_memtable_docs(args.merge_scenario_segment_docs)
        // Disabled during setup: every flush should produce its own
        // segment, not have some folded together before the loop below
        // gets to measure a merge call's cost in isolation.
        .with_merge_trigger_segments(usize::MAX)
        .with_merge_fan_in(args.merge_scenario_fan_in);

    let collection = Collection::create(&layout, corpus::schema("products"), &config)?;

    // One `flush()` call per segment, explicitly — rather than relying on
    // `max_memtable_docs` to trip mid-batch, which it may not: a flush
    // check that only runs once per `upsert_batch` call (not per document)
    // would otherwise let one large batch land entirely in a single
    // segment regardless of the threshold, defeating the "several separate
    // segments" setup this scenario needs.
    let mut rng = Rng::new(args.seed);
    let mut indexed = 0usize;
    for _ in 0..segments {
        let mut this_segment = 0usize;
        while this_segment < args.merge_scenario_segment_docs {
            let this_batch = args.batch_size.min(args.merge_scenario_segment_docs - this_segment);
            let batch: Vec<_> = (0..this_batch)
                .map(|i| corpus::document(&mut rng, indexed + i, args.vocab_scale))
                .collect();
            let report = collection.upsert_batch(batch)?;
            if report.num_failed > 0 {
                return Err(format!("{} documents failed to index", report.num_failed).into());
            }
            indexed += this_batch;
            this_segment += this_batch;
        }
        collection.flush()?;
    }
    collection.sync()?;
    println!("  segments after setup    {}", collection.stats().num_segments);
    println!();

    let mut merge_times_ms = Vec::new();
    let mut rss_deltas = Vec::new();
    loop {
        let rss_before = current_rss_bytes();
        let started = Instant::now();
        let merged = collection.merge()?;
        let elapsed = started.elapsed();
        if !merged {
            break;
        }
        merge_times_ms.push(elapsed.as_secs_f64() * 1000.0);
        if let (Some(before), Some(after)) = (rss_before, current_rss_bytes()) {
            rss_deltas.push(after as i64 - before as i64);
        }
    }

    if merge_times_ms.is_empty() {
        println!("  no merges ran — need at least `merge_fan_in` segments to fold together");
        println!();
        return Ok(());
    }

    let mean_ms = merge_times_ms.iter().sum::<f64>() / merge_times_ms.len() as f64;
    let max_ms = merge_times_ms.iter().cloned().fold(0.0, f64::max);
    println!("  merges run              {}", merge_times_ms.len());
    println!("  mean merge time         {mean_ms:.1} ms");
    println!("  max merge time          {max_ms:.1} ms");
    if !rss_deltas.is_empty() {
        let mean_delta = rss_deltas.iter().sum::<i64>() as f64 / rss_deltas.len() as f64;
        let max_delta = rss_deltas.iter().cloned().max().unwrap_or(0);
        println!("  mean RSS delta/merge    {}", human_bytes_signed(mean_delta as i64));
        println!("  max RSS delta/merge     {}", human_bytes_signed(max_delta));
    }
    println!("  segments after merging  {}", collection.stats().num_segments);
    println!("  rss (peak)              {}", human_bytes(peak_rss_bytes()));
    println!();

    Ok(())
}

/// Index the corpus into a collection whose memtable threshold is crossed
/// repeatedly, while a background thread searches the whole time, and report
/// search latency under that concurrent flush load. A separate collection and
/// scenario from the one above — the goal here is to see flush's cost, not to
/// fold it into (and skew) the flush-free baseline.
fn run_flush_scenario(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    println!("Flush-under-load scenario");
    println!("  documents               {}", args.documents);
    println!("  max_memtable_docs       {}", args.flush_scenario_max_memtable_docs);
    println!();

    let dir = tempfile::tempdir()?;
    let layout = Layout::new(dir.path());
    layout.initialize()?;
    let config = EngineConfig::new(dir.path())
        .with_sync_policy(SyncPolicy::Interval(Duration::from_millis(200)))
        .with_max_memtable_docs(args.flush_scenario_max_memtable_docs);

    let collection = Arc::new(Collection::create(&layout, corpus::schema("products"), &config)?);

    let mut rng = Rng::new(args.seed ^ 0x5eed);
    let queries = corpus::queries(&mut rng, args.queries.max(200), args.vocab_scale);

    let stop = Arc::new(AtomicBool::new(false));
    let latencies = Arc::new(Mutex::new(Vec::<u64>::new()));
    let total_hits = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let search_thread = {
        let collection = Arc::clone(&collection);
        let stop = Arc::clone(&stop);
        let latencies = Arc::clone(&latencies);
        let total_hits = Arc::clone(&total_hits);
        let queries = queries.clone();
        std::thread::spawn(move || {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let q = &queries[i % queries.len()];
                let started = Instant::now();
                if let Ok(response) =
                    collection.search(SearchParams { q: Some(q.clone()), ..Default::default() })
                {
                    total_hits.fetch_add(response.found as u64, Ordering::Relaxed);
                }
                latencies
                    .lock()
                    .expect("latencies mutex poisoned")
                    .push(started.elapsed().as_micros() as u64);
                i += 1;
            }
        })
    };

    let mut rng = Rng::new(args.seed);
    let started = Instant::now();
    let mut indexed = 0usize;
    while indexed < args.documents {
        let this_batch = args.batch_size.min(args.documents - indexed);
        let batch: Vec<_> = (0..this_batch)
            .map(|i| corpus::document(&mut rng, indexed + i, args.vocab_scale))
            .collect();
        let report = collection.upsert_batch(batch)?;
        if report.num_failed > 0 {
            return Err(format!("{} documents failed to index", report.num_failed).into());
        }
        indexed += this_batch;
    }
    let index_elapsed = started.elapsed();

    stop.store(true, Ordering::Relaxed);
    search_thread.join().expect("search thread panicked");

    let stats = collection.stats();
    println!("Indexing (with flushes)");
    println!("  elapsed                 {:.2}s", index_elapsed.as_secs_f64());
    println!(
        "  throughput              {:.0} docs/sec",
        args.documents as f64 / index_elapsed.as_secs_f64()
    );
    println!("  segments produced       {}", stats.num_segments);
    println!("  documents               {}", stats.num_documents);
    println!();

    let latencies =
        Arc::try_unwrap(latencies).expect("search thread has joined").into_inner().unwrap();
    let total_hits = total_hits.load(Ordering::Relaxed);
    let measurement = Measurement { latencies, total_hits };
    report("Search, concurrent with flushing", &measurement, 60.0);

    Ok(())
}

/// Index a fresh collection, then hammer it with `--concurrency` threads
/// each running the plain-query workload continuously for
/// `--concurrency-duration-secs`, and report aggregate throughput and mean
/// per-query processing time — the pair Typesense's own benchmarks publish.
/// `Collection::search` takes only a read lock (see its doc comment), so
/// many threads calling it concurrently is an already-supported, already-
/// exercised pattern (`run_flush_scenario` above does the same
/// `Arc<Collection>` + `thread::spawn` shape); this needed no engine change.
fn run_concurrency_scenario(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    println!("Concurrency scenario");
    println!("  threads                 {}", args.concurrency);
    println!("  duration                {}s", args.concurrency_duration_secs);
    println!();

    let dir = tempfile::tempdir()?;
    let layout = Layout::new(dir.path());
    layout.initialize()?;
    let config = EngineConfig::new(dir.path())
        .with_sync_policy(SyncPolicy::Interval(Duration::from_millis(200)))
        .with_max_memtable_docs(args.max_memtable_docs);

    let collection = Arc::new(Collection::create(&layout, corpus::schema("products"), &config)?);

    let mut rng = Rng::new(args.seed);
    let mut indexed = 0usize;
    while indexed < args.documents {
        let this_batch = args.batch_size.min(args.documents - indexed);
        let batch: Vec<_> = (0..this_batch)
            .map(|i| corpus::document(&mut rng, indexed + i, args.vocab_scale))
            .collect();
        let report = collection.upsert_batch(batch)?;
        if report.num_failed > 0 {
            return Err(format!("{} documents failed to index", report.num_failed).into());
        }
        indexed += this_batch;
    }
    collection.sync()?;

    let mut rng = Rng::new(args.seed ^ 0x5eed);
    let queries = Arc::new(corpus::queries(&mut rng, args.queries.max(200), args.vocab_scale));

    let stop = Arc::new(AtomicBool::new(false));
    let total_queries = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let errors = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let latencies: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let started = Instant::now();
    let mut handles = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        let collection = Arc::clone(&collection);
        let queries = Arc::clone(&queries);
        let stop = Arc::clone(&stop);
        let total_queries = Arc::clone(&total_queries);
        let errors = Arc::clone(&errors);
        let latencies = Arc::clone(&latencies);
        handles.push(std::thread::spawn(move || {
            let mut i = 0usize;
            let mut local_latencies = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let q = &queries[i % queries.len()];
                let query_started = Instant::now();
                match collection.search(SearchParams { q: Some(q.clone()), ..Default::default() }) {
                    Ok(_) => local_latencies.push(query_started.elapsed().as_micros() as u64),
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                i += 1;
            }
            total_queries.fetch_add(local_latencies.len() as u64, Ordering::Relaxed);
            latencies.lock().expect("latencies mutex poisoned").extend(local_latencies);
        }));
    }

    std::thread::sleep(Duration::from_secs(args.concurrency_duration_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("worker thread panicked");
    }
    let elapsed = started.elapsed();

    let total = total_queries.load(Ordering::Relaxed);
    let errors = errors.load(Ordering::Relaxed);
    let qps = total as f64 / elapsed.as_secs_f64();
    let latencies =
        Arc::try_unwrap(latencies).expect("every worker thread has joined").into_inner().unwrap();
    let mean_ms = latencies.iter().sum::<u64>() as f64 / latencies.len().max(1) as f64 / 1000.0;

    println!("  queries completed       {total}");
    if errors > 0 {
        println!("  errors                  {errors}  (excluded from latency stats)");
    }
    println!("  elapsed                 {:.2}s", elapsed.as_secs_f64());
    println!(
        "  throughput              {qps:.1} concurrent queries/sec  \
         (Typesense reference: 104 @ 4 vCPU / 2.2M docs)"
    );
    println!("  mean processing time    {mean_ms:.2} ms  (Typesense reference: 11 ms)");
    println!();

    Ok(())
}

struct Measurement {
    latencies: Vec<u64>,
    total_hits: u64,
}

fn measure(
    collection: &Collection,
    queries: &[String],
    build: impl Fn(&str) -> SearchParams,
) -> Result<Measurement, Box<dyn std::error::Error>> {
    // One warm-up pass so the first few queries do not pay for cold caches.
    for query in queries.iter().take(20) {
        collection.search(build(query))?;
    }

    let mut latencies = Vec::with_capacity(queries.len());
    let mut total_hits = 0u64;
    for query in queries {
        let started = Instant::now();
        let response = collection.search(build(query))?;
        latencies.push(started.elapsed().as_micros() as u64);
        total_hits += response.found as u64;
    }
    Ok(Measurement { latencies, total_hits })
}

fn report(label: &str, measurement: &Measurement, target_p95_ms: f64) {
    let mut sorted = measurement.latencies.clone();
    sorted.sort_unstable();

    let percentile = |p: f64| -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let index = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
        sorted[index] as f64 / 1000.0
    };

    let p95 = percentile(0.95);
    let mean = sorted.iter().sum::<u64>() as f64 / sorted.len().max(1) as f64 / 1000.0;
    let mean_hits = measurement.total_hits as f64 / sorted.len().max(1) as f64;
    // The cost-per-matched-document ratio: latency should be dominated by work
    // proportional to how many documents a query actually touches, not by
    // corpus size on its own. Comparing this figure across corpus sizes is
    // what tells apart "the walk got more expensive" from "the query just
    // matched more documents" — the two look identical in raw latency alone.
    let ns_per_hit = if mean_hits > 0.0 { mean * 1_000_000.0 / mean_hits } else { 0.0 };

    println!("{label}");
    println!("  queries        {}", sorted.len());
    println!("  mean           {mean:.2} ms");
    println!("  p50            {:.2} ms", percentile(0.50));
    println!(
        "  p95            {p95:.2} ms  (target {target_p95_ms:.0}) {}",
        if p95 <= target_p95_ms { "PASS" } else { "FAIL" }
    );
    println!("  p99            {:.2} ms", percentile(0.99));
    println!("  max            {:.2} ms", percentile(1.0));
    println!("  mean hits      {mean_hits:.0}");
    println!("  ns/matched doc {ns_per_hit:.0}");
    println!();
}

/// Peak resident set size since process start — a kernel-tracked high-water
/// mark, so it catches transient spikes a point-in-time sample would miss.
fn peak_rss_bytes() -> usize {
    let maxrss = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage.ru_maxrss
    };
    // Linux reports ru_maxrss in KiB; Darwin reports it in bytes.
    if cfg!(target_os = "linux") {
        maxrss as usize * 1024
    } else {
        maxrss as usize
    }
}

/// Current resident set size, via `ps` — unlike `peak_rss_bytes`, this can
/// fall as well as rise, so it reflects what's actually resident right now
/// rather than the high-water mark. `None` on platforms without `ps -o rss=`.
fn current_rss_bytes() -> Option<usize> {
    let pid = std::process::id().to_string();
    let output = std::process::Command::new("ps").args(["-o", "rss=", "-p", &pid]).output().ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse::<usize>().ok().map(|kb| kb * 1024)
}

fn human_bytes(bytes: usize) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// Same as [`human_bytes`], but for a signed delta — `current_rss_bytes()`
/// can fall as well as rise between two samples.
fn human_bytes_signed(bytes: i64) -> String {
    if bytes < 0 {
        format!("-{}", human_bytes(bytes.unsigned_abs() as usize))
    } else {
        format!("+{}", human_bytes(bytes as usize))
    }
}
