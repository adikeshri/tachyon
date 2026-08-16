//! The `tachyon` binary.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tower_http::trace::TraceLayer;

use tachyon_engine::{Engine, EngineConfig};
use tachyon_server::{build_router, AppState, Auth};
use tachyon_storage::SyncPolicy;

/// Tachyon — typo-tolerant full-text search, in a single binary.
#[derive(Debug, Parser)]
#[command(name = "tachyon", version, about)]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "TACHYON_LISTEN", default_value = "0.0.0.0:8108")]
    listen: SocketAddr,

    /// Directory holding collections, segments, and write-ahead logs.
    #[arg(long, env = "TACHYON_DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,

    /// Milliseconds between WAL fsyncs. `0` fsyncs before acknowledging every
    /// write, which is the safe default; a positive value trades a bounded
    /// window of durability for ingest throughput.
    #[arg(long, env = "TACHYON_SYNC_INTERVAL_MS", default_value_t = 0)]
    sync_interval_ms: u64,

    /// Flush a memtable into a segment once it holds this many documents.
    #[arg(long, env = "TACHYON_MAX_MEMTABLE_DOCS", default_value_t = 100_000)]
    max_memtable_docs: usize,

    /// Attempt a merge once a collection holds more than this many committed
    /// segments. Query latency grows with segment count, not just corpus
    /// size, so this bounds how far it's allowed to grow.
    #[arg(long, env = "TACHYON_MERGE_TRIGGER_SEGMENTS", default_value_t = 8)]
    merge_trigger_segments: usize,

    /// How many segments one merge folds together — the smallest this many
    /// by document count.
    #[arg(long, env = "TACHYON_MERGE_FAN_IN", default_value_t = 4)]
    merge_fan_in: usize,

    /// Admin API key: full read and write access. Leave unset to run without
    /// authentication, which is fine locally and not on a public network.
    #[arg(long, env = "TACHYON_ADMIN_KEY")]
    admin_key: Option<String>,

    /// Search-only API key: reads, no writes. Safe to embed in a client.
    #[arg(long, env = "TACHYON_SEARCH_KEY")]
    search_key: Option<String>,

    /// Log filter, in `tracing-subscriber` EnvFilter syntax.
    #[arg(long, env = "TACHYON_LOG", default_value = "info")]
    log: String,

    /// Check that a locally running Tachyon is healthy, then exit.
    /// Used as the Docker HEALTHCHECK so the image doesn't need wget/curl.
    #[arg(long)]
    healthcheck: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.healthcheck {
        return healthcheck(args.listen).await;
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&args.log))
        .with_target(false)
        .init();

    let config = EngineConfig {
        data_dir: args.data_dir.clone(),
        sync_policy: if args.sync_interval_ms == 0 {
            SyncPolicy::Always
        } else {
            SyncPolicy::Interval(Duration::from_millis(args.sync_interval_ms))
        },
        max_memtable_docs: args.max_memtable_docs,
        merge_trigger_segments: args.merge_trigger_segments,
        merge_fan_in: args.merge_fan_in,
        ..Default::default()
    };

    let engine = Arc::new(Engine::open(config)?);
    tracing::info!(
        data_dir = %args.data_dir.display(),
        collections = engine.list_collections().len(),
        "opened data directory"
    );

    let auth = Auth::new(args.admin_key.clone(), args.search_key.clone());
    if auth.is_enabled() {
        tracing::info!(
            admin_key = args.admin_key.is_some(),
            search_key = args.search_key.is_some(),
            "API key authentication is enabled"
        );
    } else {
        tracing::warn!(
            "no API keys configured: every endpoint is open. \
             Set --admin-key (TACHYON_ADMIN_KEY) before exposing this to a network."
        );
    }

    let app = build_router(AppState::with_auth(Arc::clone(&engine), auth))
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(address = %args.listen, "tachyon is listening");

    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await?;

    // A clean shutdown is the one chance to make a relaxed sync policy behave
    // like a strict one.
    tracing::info!("shutting down; flushing write-ahead logs");
    engine.sync_all()?;
    Ok(())
}

/// Raw HTTP/1.1 GET of `/health`, checking for a `200` status. Deliberately
/// avoids pulling in an HTTP client crate or an external tool like wget/curl
/// just to poll ourselves.
async fn healthcheck(listen: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // `listen` is often 0.0.0.0; dial the loopback address instead of the
    // unspecified one.
    let target = if listen.ip().is_unspecified() {
        SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), listen.port())
    } else {
        listen
    };

    let mut stream = tokio::net::TcpStream::connect(target).await?;
    stream
        .write_all(
            format!("GET /health HTTP/1.1\r\nHost: {target}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;

    let status_line = response
        .split(|&b| b == b'\n')
        .next()
        .map(|line| String::from_utf8_lossy(line).into_owned())
        .unwrap_or_default();

    if status_line.starts_with("HTTP/1.1 200") || status_line.starts_with("HTTP/1.0 200") {
        Ok(())
    } else {
        Err(format!("unhealthy: {}", status_line.trim()).into())
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
