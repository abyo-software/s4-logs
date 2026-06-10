//! `GatewaySink` — the gateway's view of a chunk sink: `ChunkSink` (from
//! s4logs-core) plus a cheap readiness probe for `/ready`.
//!
//! Production wiring should use [`ProbedStore`] (real `ListObjectsV2`
//! max_keys=1 probe, cached ~10 s); the blanket always-ready impl on the raw
//! `ObjectStore` is kept for embedders that own their own health checking.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use s4logs_core::chunk::EncodedChunk;
use s4logs_core::layout::ChunkLocation;
use s4logs_core::sink::{ChunkSink, MemorySink, PutReceipt, SinkError};
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

/// Trivially ready — kept for embedders with external health checking.
/// Production should wrap the store in [`ProbedStore`] so `/ready` reflects
/// actual bucket reachability (DESIGN.md §8.4).
#[async_trait]
impl GatewaySink for ObjectStore {}

/// How long a probe result (positive or negative) is served from cache.
const PROBE_TTL: Duration = Duration::from_secs(10);

/// `ObjectStore` wrapper whose `/ready` performs a real `ListObjectsV2`
/// (`max_keys=1`) against the bucket + key prefix, cached for ~10 s so
/// readiness polling never hammers S3 (DESIGN.md §8.4).
pub struct ProbedStore {
    store: ObjectStore,
    ttl: Duration,
    /// Last probe `(when, outcome)` — tokio mutex because the probe is
    /// awaited while holding it (collapses concurrent probes into one).
    cache: tokio::sync::Mutex<Option<(Instant, bool)>>,
}

impl ProbedStore {
    pub fn new(store: ObjectStore) -> Self {
        Self::with_ttl(store, PROBE_TTL)
    }

    pub fn with_ttl(store: ObjectStore, ttl: Duration) -> Self {
        Self {
            store,
            ttl,
            cache: tokio::sync::Mutex::new(None),
        }
    }

    async fn probe(&self) -> bool {
        match self
            .store
            .client()
            .list_objects_v2()
            .bucket(self.store.bucket())
            .prefix(self.store.key_prefix())
            .max_keys(1)
            .send()
            .await
        {
            Ok(_) => true,
            Err(err) => {
                tracing::warn!(
                    bucket = %self.store.bucket(),
                    error = %aws_sdk_s3::error::DisplayErrorContext(&err),
                    "/ready S3 probe failed"
                );
                false
            }
        }
    }
}

#[async_trait]
impl ChunkSink for ProbedStore {
    fn key_prefix(&self) -> &str {
        self.store.key_prefix()
    }

    async fn put_chunk(
        &self,
        loc: &ChunkLocation,
        chunk: &EncodedChunk,
    ) -> Result<PutReceipt, SinkError> {
        self.store.put_chunk(loc, chunk).await
    }
}

#[async_trait]
impl GatewaySink for ProbedStore {
    async fn ready(&self) -> bool {
        let mut cache = self.cache.lock().await;
        if let Some((at, ok)) = *cache
            && at.elapsed() < self.ttl
        {
            return ok;
        }
        let ok = self.probe().await;
        *cache = Some((Instant::now(), ok));
        ok
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_smithy_mocks::{RuleMode, mock, mock_client};

    fn probed(client: aws_sdk_s3::Client, ttl: Duration) -> ProbedStore {
        ProbedStore::with_ttl(ObjectStore::new(client, "bucket", "s4logs"), ttl)
    }

    #[tokio::test]
    async fn ready_lists_with_max_keys_1_and_caches() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|inp| inp.prefix() == Some("s4logs/") && inp.max_keys() == Some(1))
            .then_output(|| ListObjectsV2Output::builder().build());
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let store = probed(client, Duration::from_secs(3600));
        assert!(store.ready().await);
        assert!(store.ready().await);
        assert!(store.ready().await);
        assert_eq!(
            rule.num_calls(),
            1,
            "second/third hits must come from cache"
        );
    }

    #[tokio::test]
    async fn ready_probe_failure_is_cached_too() {
        use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
        use aws_sdk_s3::types::error::NoSuchBucket;
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_error(|| ListObjectsV2Error::NoSuchBucket(NoSuchBucket::builder().build()));
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let store = probed(client, Duration::from_secs(3600));
        assert!(!store.ready().await);
        assert!(!store.ready().await);
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test]
    async fn expired_cache_reprobes() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| ListObjectsV2Output::builder().build());
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let store = probed(client, Duration::ZERO);
        assert!(store.ready().await);
        assert!(store.ready().await);
        assert_eq!(rule.num_calls(), 2, "zero ttl must probe every time");
    }
}
