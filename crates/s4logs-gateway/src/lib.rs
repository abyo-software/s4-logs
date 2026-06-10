//! s4logs-gateway — Mode B (迂回). Wave 1C implements this crate.
//!
//! Contract: DESIGN.md §8. AWS JSON 1.1 dispatch on `X-Amz-Target:
//! Logs_20140328.*`, first-match-wins TOML routing, per-log-group
//! `ChunkWriter` buffers with size/age/shutdown flush, CW passthrough,
//! /health /ready /metrics.
