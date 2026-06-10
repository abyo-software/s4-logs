//! Stream-shard parallel paging (wave 4K).
//!
//! The 2026-06-10 real-AWS experiment showed drain speed is **latency-bound**
//! (5 GiB → 94.6 min at `--concurrency 4`, zero throttling): each
//! `FilterLogEvents` page round-trip is the bottleneck, not quota. The
//! parallelism axis inside one window is the **log stream**: FilterLogEvents
//! accepts up to [`MAX_STREAMS_PER_FILTER`] `logStreamNames`, and disjoint
//! stream sets page independently. [`partition_streams`] splits a group's
//! streams round-robin into shards; [`event_pages`] merges the per-shard
//! page streams into one event feed that a single-threaded `ChunkWriter`
//! consumes (`ChunkWriter` needs no ordering — the S4LT sidecar stores
//! min/max per frame, DESIGN.md §5).
//!
//! # Determinism honesty
//!
//! With more than one shard, pages from different shards interleave in
//! completion order, so **data object content is no longer byte-deterministic
//! across runs** (the *record set* is identical; the order inside objects is
//! not). Manifest-skip idempotency is unaffected — it keys on manifest
//! existence, never on object bytes. The base (`shard_streams = 1`) path
//! issues exactly the same single-token page sequence as before and stays
//! byte-deterministic.

use futures::StreamExt;
use futures::stream::BoxStream;

use crate::cw::{CwError, CwEvent, CwSource};
use crate::window::Window;

/// FilterLogEvents accepts at most this many `logStreamNames` per call
/// (AWS API limit).
pub const MAX_STREAMS_PER_FILTER: usize = 100;

/// Partition `streams` round-robin into shards for parallel paging.
///
/// - At most `max_per_shard` names per shard (the API limit): when
///   `requested_shards` shards would overflow it, the shard count **grows**
///   to `ceil(len / max_per_shard)` — a group with more than
///   `100 × requested` streams simply pages with more shards rather than
///   batching sequentially.
/// - Never more shards than streams (no empty shards).
/// - Deterministic: same input, same partition.
pub fn partition_streams(
    streams: &[String],
    requested_shards: usize,
    max_per_shard: usize,
) -> Vec<Vec<String>> {
    if streams.is_empty() {
        return Vec::new();
    }
    let max_per_shard = max_per_shard.max(1);
    let min_shards = streams.len().div_ceil(max_per_shard);
    let n = requested_shards.max(1).max(min_shards).min(streams.len());
    let mut shards: Vec<Vec<String>> = (0..n)
        .map(|i| Vec::with_capacity(streams.len().div_ceil(n) + usize::from(i == 0)))
        .collect();
    for (i, s) in streams.iter().enumerate() {
        shards[i % n].push(s.clone());
    }
    shards
}

/// Sequential page stream for one (window, optional stream filter):
/// repeated `filter_log_events` calls chained on `next_token`.
fn page_stream<'a>(
    cw: &'a dyn CwSource,
    log_group: &'a str,
    w: Window,
    streams: Option<&'a [String]>,
) -> BoxStream<'a, Result<Vec<CwEvent>, CwError>> {
    // State: `Some(token)` = fetch the next page with that continuation
    // token (`Some(None)` = first page); `None` = range exhausted.
    futures::stream::try_unfold(Some(None::<String>), move |state| async move {
        let Some(token) = state else {
            return Ok(None);
        };
        let page = cw
            .filter_log_events(log_group, w.start_ms, w.end_ms, token.as_deref(), streams)
            .await?;
        let next_state = page.next_token.map(Some);
        Ok(Some((page.events, next_state)))
    })
    .boxed()
}

/// All event pages of one window as a single stream.
///
/// - `shards = None` (or fewer than 2 shards): one unsharded pass — the
///   exact pre-wave-4K call sequence.
/// - `shards = Some(..)` with ≥ 2 shards: one [`page_stream`] per shard,
///   merged with `select_all`, so up to `shards.len()` FilterLogEvents
///   requests are in flight concurrently while the consumer holds the
///   single `ChunkWriter`. Pages interleave in completion order (see module
///   docs on determinism).
pub(crate) fn event_pages<'a>(
    cw: &'a dyn CwSource,
    log_group: &'a str,
    w: Window,
    shards: Option<&'a [Vec<String>]>,
) -> BoxStream<'a, Result<Vec<CwEvent>, CwError>> {
    match shards {
        Some(shards) if shards.len() >= 2 => futures::stream::select_all(
            shards
                .iter()
                .map(|s| page_stream(cw, log_group, w, Some(s.as_slice()))),
        )
        .boxed(),
        Some([single]) => page_stream(cw, log_group, w, Some(single.as_slice())),
        _ => page_stream(cw, log_group, w, None),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("stream-{i:04}")).collect()
    }

    #[test]
    fn round_robin_partition_is_balanced_and_complete() {
        let s = names(10);
        let shards = partition_streams(&s, 3, MAX_STREAMS_PER_FILTER);
        assert_eq!(shards.len(), 3);
        assert_eq!(
            shards.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![4, 3, 3]
        );
        // Round-robin: shard i holds indices ≡ i (mod 3).
        assert_eq!(
            shards[0],
            vec!["stream-0000", "stream-0003", "stream-0006", "stream-0009"]
        );
        assert_eq!(shards[1], vec!["stream-0001", "stream-0004", "stream-0007"]);
        // Disjoint + complete.
        let mut all: Vec<String> = shards.into_iter().flatten().collect();
        all.sort();
        assert_eq!(all, s);
    }

    #[test]
    fn shard_count_grows_past_request_to_respect_api_limit() {
        // 250 streams at the 100-name API limit need ≥ 3 shards even when
        // only 2 were requested.
        let s = names(250);
        let shards = partition_streams(&s, 2, MAX_STREAMS_PER_FILTER);
        assert_eq!(shards.len(), 3);
        assert!(shards.iter().all(|sh| sh.len() <= MAX_STREAMS_PER_FILTER));
        assert_eq!(shards.iter().map(Vec::len).sum::<usize>(), 250);
    }

    #[test]
    fn never_more_shards_than_streams() {
        let shards = partition_streams(&names(2), 8, MAX_STREAMS_PER_FILTER);
        assert_eq!(shards.len(), 2);
        assert!(partition_streams(&[], 4, MAX_STREAMS_PER_FILTER).is_empty());
        let one = partition_streams(&names(1), 4, MAX_STREAMS_PER_FILTER);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].len(), 1);
    }

    #[test]
    fn partition_is_deterministic() {
        let s = names(57);
        assert_eq!(
            partition_streams(&s, 4, MAX_STREAMS_PER_FILTER),
            partition_streams(&s, 4, MAX_STREAMS_PER_FILTER)
        );
    }
}
