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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

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
