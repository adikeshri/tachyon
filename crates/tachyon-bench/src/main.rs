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
    /// continuously, to measure what a flush — which holds the write lock
    /// for its full duration — costs concurrent readers. Off by default: the
    /// scenario above stays flush-free on purpose, so its numbers remain a
    /// stable baseline unaffected by the flush path's own performance.
    #[arg(long)]
    flush_scenario: bool,

    /// Memtable document threshold for `--flush-scenario`, chosen well below
    /// `--documents` so several flushes happen during one run.
    #[arg(long, default_value_t = 5_000)]
    flush_scenario_max_memtable_docs: usize,
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
    // The whole corpus stays in one memtable so this measures the index, not
    // the flush policy.
    let config = config.with_max_memtable_docs(usize::MAX);

    println!("Tachyon benchmark");
    println!("  documents   {}", args.documents);
    println!("  queries     {}", args.queries);
    println!("  batch size  {}", args.batch_size);
    println!("  fsync       {}", if args.fsync { "every batch" } else { "every 200ms" });
    println!();

    let collection = Collection::create(&layout, corpus::schema("products"), &config)?;

    // --- Indexing ---------------------------------------------------------
    let mut rng = Rng::new(args.seed);
    let started = Instant::now();
    let mut indexed = 0usize;

    while indexed < args.documents {
        let this_batch = args.batch_size.min(args.documents - indexed);
        let batch: Vec<_> =
            (0..this_batch).map(|i| corpus::document(&mut rng, indexed + i)).collect();
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
    println!("  memtable       {}", human_bytes(stats.memtable_bytes));
    println!("  wal            {}", human_bytes(stats.wal_bytes as usize));
    println!("  bytes/doc      {}", human_bytes(stats.memtable_bytes / args.documents.max(1)));
    println!();

    // --- Search -----------------------------------------------------------
    let mut rng = Rng::new(args.seed ^ 0x5eed);
    let queries = corpus::queries(&mut rng, args.queries);

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
    let queries = corpus::queries(&mut rng, args.queries.max(200));

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
        let batch: Vec<_> =
            (0..this_batch).map(|i| corpus::document(&mut rng, indexed + i)).collect();
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
    println!("  mean hits      {:.0}", measurement.total_hits as f64 / sorted.len().max(1) as f64);
    println!();
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
