//! `s4logs serve` — Mode B gateway wiring (DESIGN.md §8, §9, §11). Runs
//! until SIGTERM / ctrl-c, then flushes all buffers and exits.

use std::sync::Arc;

use anyhow::{Context, Result};
use s4logs_core::chunk::ChunkConfig;
use s4logs_core::store::ObjectStore;
use s4logs_gateway::{
    AuthMode, CwForward, Gateway, GatewayConfig, NoopCwForward, ProbedStore, RoutingConfig,
    SdkCwForward,
};

use crate::aws;
use crate::cli::{AuthModeArg, GlobalArgs, ServeArgs, UsageError};

pub async fn run(global: &GlobalArgs, args: &ServeArgs) -> Result<()> {
    let bucket = global.require_bucket()?;
    let account = global.require_account()?;

    let routing = match &args.routing_config {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading routing config {}", path.display()))?;
            RoutingConfig::from_toml_str(&text)
                .with_context(|| format!("parsing routing config {}", path.display()))?
        }
        None => RoutingConfig::default(), // everything → s3
    };

    let auth = match args.auth_mode {
        AuthModeArg::None => {
            if args.auth_access_key.is_some() || args.auth_secret.is_some() {
                tracing::warn!(
                    "--auth-access-key/--auth-secret are ignored without --auth-mode sigv4"
                );
            }
            AuthMode::None
        }
        AuthModeArg::Sigv4 => match (&args.auth_access_key, &args.auth_secret) {
            (Some(access_key), Some(secret_key)) => AuthMode::SigV4 {
                access_key: access_key.clone(),
                secret_key: secret_key.clone(),
            },
            _ => {
                return Err(UsageError(
                    "--auth-mode sigv4 requires --auth-access-key and --auth-secret \
                     (or S4LOGS_AUTH_ACCESS_KEY / S4LOGS_AUTH_SECRET)"
                        .into(),
                )
                .into());
            }
        },
    };

    let clients = aws::load(global).await;
    let store = ObjectStore::new(clients.s3(), bucket, &global.prefix)
        .with_storage_class(args.storage_class.map(crate::cli::StorageClassArg::to_sdk));
    let forward: Arc<dyn CwForward> = if args.no_cloudwatch {
        tracing::info!("--no-cloudwatch: cloudwatch/both routes are no-ops");
        Arc::new(NoopCwForward)
    } else {
        Arc::new(SdkCwForward::new(clients.cwl()))
    };

    if args.wal_dir.is_none() {
        tracing::warn!(
            "running without --wal-dir: a crash loses buffered events below the flush thresholds"
        );
    }

    let gateway = Gateway::new(
        GatewayConfig {
            account,
            routing,
            flush_bytes: args.flush_bytes,
            flush_interval: args.flush_interval,
            chunk: ChunkConfig::default(),
            wal_dir: args.wal_dir.clone(),
            auth,
            max_buffered_bytes: args.max_buffered_bytes,
        },
        // ProbedStore turns /ready into a real (cached) S3 listing probe.
        Arc::new(ProbedStore::new(store)),
        forward,
    );
    gateway.serve(args.listen).await.context("gateway serve")?;
    Ok(())
}
