//! `GatewaySink` — the gateway's view of a chunk sink: `ChunkSink` (from
//! s4logs-core) plus a cheap readiness probe for `/ready`.

use async_trait::async_trait;
use s4logs_core::sink::{ChunkSink, MemorySink};
use s4logs_core::store::ObjectStore;

/// Chunk sink with a readiness probe. `/ready` reports 503 if either this
/// returns `false` or the most recent flush errored.
#[async_trait]
pub trait GatewaySink: ChunkSink {
    /// Cheap backend health check. Default: always ready.
    async fn ready(&self) -> bool {
        true
    }
}

#[async_trait]
impl GatewaySink for MemorySink {}

/// P1: trivially ready — `/ready` health is carried by the
/// last-flush-succeeded bit in `BufferManager`. Wave 2D can upgrade this to
/// a cheap `ListObjectsV2` (`ObjectStore::list_keys` with a max-1-key probe)
/// per DESIGN.md §8.4 once the store lands.
#[async_trait]
impl GatewaySink for ObjectStore {}
