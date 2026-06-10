//! `s4logs serve` — Mode B gateway wiring (DESIGN.md §8, §9). Runs until
//! SIGTERM / ctrl-c, then flushes all buffers and exits.

use std::sync::Arc;

use anyhow::{Context, Result};
use s4logs_core::chunk::ChunkConfig;
use s4logs_core::store::ObjectStore;
use s4logs_gateway::{
    CwForward, Gateway, GatewayConfig, NoopCwForward, RoutingConfig, SdkCwForward,
};

use crate::aws;
use crate::cli::{GlobalArgs, ServeArgs};

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

    let clients = aws::load(global).await;
    let store = ObjectStore::new(clients.s3(), bucket, &global.prefix);
    let forward: Arc<dyn CwForward> = if args.no_cloudwatch {
        tracing::info!("--no-cloudwatch: cloudwatch/both routes are no-ops");
        Arc::new(NoopCwForward)
    } else {
        Arc::new(SdkCwForward::new(clients.cwl()))
    };

    let gateway = Gateway::new(
        GatewayConfig {
            account,
            routing,
            flush_bytes: args.flush_bytes,
            flush_interval: args.flush_interval,
            chunk: ChunkConfig::default(),
            // Wave 3F gateway options (WAL / SigV4 / memory cap) keep their
            // defaults until the orchestrator wires the serve flags.
            ..GatewayConfig::default()
        },
        Arc::new(store),
        forward,
    );
    gateway.serve(args.listen).await.context("gateway serve")?;
    Ok(())
}
