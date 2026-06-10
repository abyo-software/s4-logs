//! s4logs-gateway — Mode B (迂回). Wave 1C implements this crate.
//!
//! Contract: DESIGN.md §8. AWS JSON 1.1 dispatch on `X-Amz-Target:
//! Logs_20140328.*`, first-match-wins TOML routing, per-log-group
//! `ChunkWriter` buffers with size/age/shutdown flush, CW passthrough,
//! /health /ready /metrics.
//!
//! # P1 limitations (documented, by design)
//!
//! - **No SigV4 validation** (DESIGN.md §8.1): the `Authorization` header is
//!   ignored. Deploy behind TLS + a network boundary; static-credential
//!   validation arrives in P3. README must carry this note.
//! - **In-memory buffer durability** (DESIGN.md §8.3): events routed to S3
//!   sit in process memory until a flush trigger fires. A crash loses up to
//!   `flush_bytes` / `flush_interval` worth of events per (group, date)
//!   buffer. WAL is on the roadmap; README Limitations must state this.
//!
//! # Usage (wave 2D CLI `s4logs serve`)
//!
//! ```ignore
//! let routing = RoutingConfig::from_toml_str(&fs::read_to_string(path)?)?;
//! let gateway = Gateway::new(
//!     GatewayConfig { account, routing, flush_bytes, flush_interval, ..Default::default() },
//!     Arc::new(ObjectStore::new(s3_client, bucket, &prefix)),   // s4logs-core
//!     Arc::new(SdkCwForward::new(cw_client)),                   // or NoopCwForward
//! );
//! gateway.serve("0.0.0.0:8080".parse()?).await?;                // SIGTERM/ctrl_c → flush → exit
//! ```

pub mod api;
pub mod buffer;
pub mod forward;
pub mod handlers;
pub mod registry;
pub mod routing;
pub mod sink;

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use s4logs_core::chunk::ChunkConfig;
use thiserror::Error;
use tokio::net::TcpListener;

pub use crate::buffer::{BufferConfig, BufferError, BufferManager};
pub use crate::forward::{CwForward, NoopCwForward, SdkCwForward};
pub use crate::routing::{RouteAction, RoutingConfig, RoutingError};
pub use crate::sink::GatewaySink;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GatewayError {
    #[error("gateway server io error")]
    Io(#[from] std::io::Error),
    #[error("final buffer flush failed on shutdown")]
    Flush(#[source] BufferError),
}

/// Gateway configuration (CLI flags map 1:1 — DESIGN.md §8.3).
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
            },
            sink,
        ));
        let state =
            handlers::AppState::new(cfg.routing, buffers.clone(), forward, install_metrics());
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
    pub async fn serve_listener<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), GatewayError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
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
