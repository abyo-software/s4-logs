//! s4logs-gateway — Mode B (迂回). Wave 1C implements this crate; wave 3F
//! hardens it (WAL, SigV4, real /ready, pagination, backpressure).
//!
//! Contract: DESIGN.md §8 + §11.1–11.2. AWS JSON 1.1 dispatch on
//! `X-Amz-Target: Logs_20140328.*`, first-match-wins TOML routing,
//! per-log-group `ChunkWriter` buffers with size/age/shutdown flush, CW
//! passthrough, /health /ready /metrics.
//!
//! # Durability / security posture (honest notes)
//!
//! - **SigV4 validation is opt-in** (`GatewayConfig::auth`, DESIGN.md
//!   §11.2). Default [`AuthMode::None`] ignores the `Authorization` header —
//!   deploy behind TLS + a network boundary, README must carry this note.
//! - **Buffer durability** (DESIGN.md §8.3, §11.1): by default events
//!   routed to S3 sit in process memory until a flush trigger fires; a
//!   crash loses up to `flush_bytes` / `flush_interval` worth of events per
//!   (group, date) buffer. With `wal_dir` set, every accepted event is
//!   fsynced to a write-ahead log before the PutLogEvents 200 and replayed
//!   on startup — at-least-once, duplicates possible after a crash (see
//!   `crate::wal` for the precise contract).
//!
//! # Usage (CLI `s4logs serve`)
//!
//! ```ignore
//! let routing = RoutingConfig::from_toml_str(&fs::read_to_string(path)?)?;
//! let gateway = Gateway::new(
//!     GatewayConfig { account, routing, flush_bytes, flush_interval, ..Default::default() },
//!     Arc::new(ProbedStore::new(ObjectStore::new(s3_client, bucket, &prefix))),
//!     Arc::new(SdkCwForward::new(cw_client)),                   // or NoopCwForward
//! );
//! gateway.serve("0.0.0.0:8080".parse()?).await?;                // SIGTERM/ctrl_c → flush → exit
//! ```

pub mod api;
pub mod auth;
pub mod buffer;
pub mod forward;
pub mod handlers;
pub mod registry;
pub mod routing;
pub mod sink;
pub mod wal;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use s4logs_core::chunk::ChunkConfig;
use thiserror::Error;
use tokio::net::TcpListener;

pub use crate::auth::AuthMode;
pub use crate::buffer::{BufferConfig, BufferError, BufferManager};
pub use crate::forward::{CwForward, NoopCwForward, SdkCwForward};
pub use crate::routing::{RouteAction, RoutingConfig, RoutingError};
pub use crate::sink::{GatewaySink, ProbedStore};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GatewayError {
    #[error("gateway server io error")]
    Io(#[from] std::io::Error),
    #[error("final buffer flush failed on shutdown")]
    Flush(#[source] BufferError),
    #[error("wal replay failed on startup")]
    WalReplay(#[source] BufferError),
}

/// Gateway configuration (CLI flags map 1:1 — DESIGN.md §8.3, §11.1–11.2).
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// `--account`; `ChunkLocation::account` partition label.
    pub account: String,
    /// Compiled `--routing-config` TOML (default: everything → s3).
    pub routing: RoutingConfig,
    /// `--flush-bytes` (default 8 MiB uncompressed).
    pub flush_bytes: u64,
    /// `--flush-interval` (default 60 s, oldest-event age).
    pub flush_interval: Duration,
    /// Chunk frame target / zstd level (defaults from s4logs-core).
    pub chunk: ChunkConfig,
    /// `--wal-dir` (default `None` = memory-only buffering, DESIGN.md
    /// §11.1). When set, accepted events are fsynced here before the
    /// PutLogEvents response and replayed on startup.
    pub wal_dir: Option<PathBuf>,
    /// `--auth-mode` / `--auth-access-key` / `--auth-secret` (default
    /// [`AuthMode::None`], DESIGN.md §11.2).
    pub auth: AuthMode,
    /// `--max-buffered-bytes` (default 256 MiB): cap on total uncompressed
    /// buffered bytes; beyond it the largest buffer is force-flushed, then
    /// requests get 503 backpressure.
    pub max_buffered_bytes: u64,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        let b = BufferConfig::default();
        Self {
            account: b.account,
            routing: RoutingConfig::default(),
            flush_bytes: b.flush_bytes,
            flush_interval: b.flush_interval,
            chunk: b.chunk,
            wal_dir: b.wal_dir,
            auth: AuthMode::None,
            max_buffered_bytes: b.max_buffered_bytes,
        }
    }
}

/// The Mode B gateway: axum app + buffer manager + sweep + shutdown flush.
pub struct Gateway {
    state: handlers::AppState,
    buffers: Arc<BufferManager>,
    flush_interval: Duration,
}

impl Gateway {
    pub fn new(
        cfg: GatewayConfig,
        sink: Arc<dyn GatewaySink>,
        forward: Arc<dyn CwForward>,
    ) -> Self {
        let buffers = Arc::new(BufferManager::new(
            BufferConfig {
                account: cfg.account,
                flush_bytes: cfg.flush_bytes,
                flush_interval: cfg.flush_interval,
                chunk: cfg.chunk,
                wal_dir: cfg.wal_dir,
                max_buffered_bytes: cfg.max_buffered_bytes,
            },
            sink,
        ));
        let state = handlers::AppState::new(
            cfg.routing,
            buffers.clone(),
            forward,
            cfg.auth,
            install_metrics(),
        );
        Self {
            state,
            buffers,
            flush_interval: cfg.flush_interval,
        }
    }

    /// The axum application — used directly by tests
    /// (`tower::ServiceExt::oneshot`) and embedders.
    pub fn app(&self) -> axum::Router {
        handlers::build_app(self.state.clone())
    }

    /// Buffer manager handle (tests / embedders: explicit `flush_all`).
    pub fn buffers(&self) -> Arc<BufferManager> {
        self.buffers.clone()
    }

    /// Bind `addr` and serve until SIGTERM / ctrl_c, then stop accepting,
    /// flush all buffers, and return.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), GatewayError> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!(addr = %addr, "s4logs gateway listening");
        self.serve_listener(listener, shutdown_signal()).await
    }

    /// Like [`Gateway::serve`] but with a caller-provided listener and
    /// shutdown future (tests bind port 0 and trigger shutdown manually).
    ///
    /// WAL replay (DESIGN.md §11.1) happens here, before any request is
    /// served — connections arriving meanwhile queue in the accept backlog.
    pub async fn serve_listener<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), GatewayError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.buffers
            .replay_wal()
            .await
            .map_err(GatewayError::WalReplay)?;
        let sweeper = tokio::spawn(sweep_loop(
            self.buffers.clone(),
            sweep_period(self.flush_interval),
        ));
        let served = axum::serve(listener, self.app())
            .with_graceful_shutdown(shutdown)
            .await;
        sweeper.abort();
        tracing::info!("shutdown: flushing all buffers");
        let flushed = self.buffers.flush_all().await;
        served?;
        flushed.map_err(GatewayError::Flush)
    }
}

/// Age-flush sweep cadence: a quarter of the flush interval, clamped to
/// [100 ms, 1 s] so the age bound is honored with ≤1 s slack.
fn sweep_period(flush_interval: Duration) -> Duration {
    (flush_interval / 4).clamp(Duration::from_millis(100), Duration::from_secs(1))
}

async fn sweep_loop(buffers: Arc<BufferManager>, period: Duration) {
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        buffers.sweep_expired().await;
    }
}

/// Resolves on SIGTERM (unix) or ctrl_c.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %err, "ctrl_c handler failed; relying on SIGTERM");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "SIGTERM handler failed; relying on ctrl_c");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutdown signal received");
}

/// Install the process-global Prometheus recorder once. Returns `None` if a
/// different recorder won the race (then `/metrics` renders empty and the
/// host application owns metric export).
fn install_metrics() -> Option<PrometheusHandle> {
    static HANDLE: OnceLock<Option<PrometheusHandle>> = OnceLock::new();
    HANDLE
        .get_or_init(|| match PrometheusBuilder::new().install_recorder() {
            Ok(handle) => Some(handle),
            Err(err) => {
                tracing::warn!(error = %err, "prometheus recorder not installed");
                None
            }
        })
        .clone()
}
