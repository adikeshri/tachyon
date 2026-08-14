//! Shared application state handed to every handler.

use std::sync::Arc;
use std::time::Instant;

use tachyon_engine::Engine;

use crate::analytics::Analytics;
use crate::auth::Auth;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Engine>,
    pub analytics: Arc<Analytics>,
    pub auth: Arc<Auth>,
    /// Process start, for `/health` uptime and the metrics endpoint.
    pub started_at: Instant,
}

impl AppState {
    /// State with analytics enabled and authentication open, which is what the
    /// tests and a local first run want.
    pub fn new(engine: Arc<Engine>) -> AppState {
        AppState::with_auth(engine, Auth::open())
    }

    pub fn with_auth(engine: Arc<Engine>, auth: Auth) -> AppState {
        AppState {
            engine,
            analytics: Arc::new(Analytics::new()),
            auth: Arc::new(auth),
            started_at: Instant::now(),
        }
    }
}
