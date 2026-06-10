//! Buffer manager — per-(log_group, UTC date) `ChunkWriter`s with
//! size / age / shutdown flush (DESIGN.md §8.3), optional WAL (§11.1) and a
//! global memory cap with backpressure.
//!
//! Each incoming event is routed into the writer keyed by
//! `(log_group, layout::date_from_ts_ms(timestamp))`, so a flushed chunk
//! never spans a `dt=` partition (DESIGN.md §3). Flush triggers:
//!
//! 1. uncompressed buffered bytes ≥ `flush_bytes` (default 8 MiB) — checked
//!    inline after every batch;
//! 2. oldest event in the buffer arrived ≥ `flush_interval` ago (default
//!    60 s) — checked by the periodic sweep `Gateway::serve` spawns;
//! 3. graceful shutdown (`flush_all`);
//! 4. memory-cap pressure: when an incoming batch would push the total
//!    uncompressed buffered bytes over `max_buffered_bytes` (default
//!    256 MiB), the largest buffer is flushed immediately; if that is not
//!    enough the batch is rejected with [`BufferError::OverCapacity`]
//!    (→ 503, agents retry) and `s4logs_backpressure_total` is incremented.
//!
//! **Durability**: without a WAL (default), buffers live in process memory
//! only — a crash loses everything not yet flushed (< flush_bytes /
//! flush_interval worth of events per group; README Limitations). With
//! `wal_dir` set, every buffered event is appended to a per-buffer WAL
//! segment and fsynced (group commit per request batch) *before* the
//! `PutLogEvents` response is sent; see `crate::wal` for the exact
//! at-least-once semantics. WAL appends happen while holding the buffer
//! mutex — correctness first; the fsync is one syscall per touched segment
//! per batch, which serializes concurrent batches (acceptable for the
//! gateway's throughput class, and the price of the ordering guarantee).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use metrics::counter;
use s4logs_core::chunk::{ChunkConfig, ChunkError, ChunkWriter};
use s4logs_core::layout::{ChunkLocation, date_from_ts_ms};
use s4logs_core::record::LogRecord;
use s4logs_core::sink::SinkError;
use std::sync::Arc;
use thiserror::Error;

use crate::api::InputLogEvent;
use crate::sink::GatewaySink;
use crate::wal::{self, WalEntry, WalSegment};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BufferError {
    #[error("chunk encode failed")]
    Chunk(#[from] ChunkError),
    #[error("chunk store failed")]
    Sink(#[from] SinkError),
    /// WAL append/fsync failed — the durability promise cannot be kept, so
    /// the request must fail (500 `InternalFailure`) rather than silently
    /// degrade to memory-only buffering.
    #[error("wal write failed")]
    Wal(#[source] std::io::Error),
    /// `max_buffered_bytes` would be exceeded even after flushing the
    /// largest buffer → 503 `ServiceUnavailableException`, clients retry.
    #[error("buffer memory cap exceeded; retry the batch")]
    OverCapacity,
}

#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Account/scope label for `ChunkLocation::account` (`--account`).
    pub account: String,
    /// Flush a buffer once its uncompressed JSONL reaches this size.
    pub flush_bytes: u64,
    /// Flush a buffer once its oldest event is this old.
    pub flush_interval: Duration,
    /// Frame target / zstd level for the chunk writers.
    pub chunk: ChunkConfig,
    /// Write-ahead log directory (DESIGN.md §11.1). `None` = memory-only.
    pub wal_dir: Option<PathBuf>,
    /// Cap on total uncompressed buffered bytes across all buffers
    /// (default 256 MiB). Exceeding it triggers an immediate flush of the
    /// largest buffer, then backpressure (503).
    pub max_buffered_bytes: u64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            account: "000000000000".to_owned(),
            flush_bytes: 8 << 20,
            flush_interval: Duration::from_secs(60),
            chunk: ChunkConfig::default(),
            wal_dir: None,
            max_buffered_bytes: 256 << 20,
        }
    }
}

/// Buffer key: (raw log group, UTC `YYYY-MM-DD` of the event timestamp).
type BufKey = (String, String);

struct Buf {
    writer: ChunkWriter,
    /// Timestamp of the first event pushed — becomes the object name prefix
    /// (`{first_event_ts_ms}-{uuid8}`, DESIGN.md §3).
    first_event_ts_ms: i64,
    /// Wall-clock arrival of that first event (age-flush clock).
    opened_at: Instant,
    /// WAL segment mirroring this buffer (when `wal_dir` is configured).
    /// Deleted after a successful flush; left on disk by a failed one, so a
    /// restart recovers what the failed flush dropped from memory.
    wal: Option<WalSegment>,
}

/// Per-log-group buffering + flush. Shared between the HTTP handlers, the
/// periodic sweep task and graceful shutdown via `Arc`.
pub struct BufferManager {
    sink: Arc<dyn GatewaySink>,
    cfg: BufferConfig,
    buffers: Mutex<HashMap<BufKey, Buf>>,
    /// `/ready` bit: cleared when a flush fails, set again on the next
    /// successful flush.
    last_flush_ok: AtomicBool,
}

impl BufferManager {
    pub fn new(cfg: BufferConfig, sink: Arc<dyn GatewaySink>) -> Self {
        Self {
            sink,
            cfg,
            buffers: Mutex::new(HashMap::new()),
            last_flush_ok: AtomicBool::new(true),
        }
    }

    pub fn config(&self) -> &BufferConfig {
        &self.cfg
    }

    /// `/ready` health: backend probe + last flush outcome.
    pub async fn ready(&self) -> bool {
        self.last_flush_ok.load(Ordering::Relaxed) && self.sink.ready().await
    }

    /// Buffer one PutLogEvents batch. Events are fanned out to per-date
    /// writers so chunks never span a `dt=` partition; any writer that
    /// crossed `flush_bytes` is flushed before returning. With WAL enabled,
    /// every event is appended + group-commit-fsynced before this returns.
    pub async fn push_events(
        &self,
        log_group: &str,
        stream: &str,
        events: &[InputLogEvent],
    ) -> Result<(), BufferError> {
        // Approximate uncompressed JSONL footprint of the incoming batch for
        // the memory-cap check (exact accounting happens via the writers).
        let incoming: u64 = events
            .iter()
            .map(|ev| (ev.message.len() + stream.len() + 64) as u64)
            .sum();
        self.reserve(incoming).await?;
        let due = {
            let mut buffers = self.buffers.lock().unwrap_or_else(|e| e.into_inner());
            for ev in events {
                let rec = LogRecord {
                    timestamp: ev.timestamp,
                    stream: stream.to_owned(),
                    message: ev.message.clone(),
                    ingestion_time: None,
                    event_id: None,
                };
                Self::push_one(&self.cfg, &mut buffers, log_group, &rec)?;
            }
            Self::sync_wal(&mut buffers)?;
            take_matching(&mut buffers, |buf| {
                buf.writer.uncompressed_len() >= self.cfg.flush_bytes
            })
        };
        self.flush_taken(due).await
    }

    /// Memory-cap backpressure (module docs, trigger 4): make room for
    /// `incoming` bytes or reject with [`BufferError::OverCapacity`].
    async fn reserve(&self, incoming: u64) -> Result<(), BufferError> {
        let largest = {
            let mut buffers = self.buffers.lock().unwrap_or_else(|e| e.into_inner());
            if buffered_total(&buffers) + incoming <= self.cfg.max_buffered_bytes {
                return Ok(());
            }
            let key = buffers
                .iter()
                .max_by_key(|(_, buf)| buf.writer.uncompressed_len())
                .map(|(k, _)| k.clone());
            key.and_then(|k| buffers.remove_entry(&k))
        };
        if let Some((key, buf)) = largest {
            tracing::info!(log_group = %key.0, dt = %key.1, "memory cap: flushing largest buffer");
            self.flush_taken(vec![(key, buf)]).await?;
        }
        let still_over = {
            let buffers = self.buffers.lock().unwrap_or_else(|e| e.into_inner());
            buffered_total(&buffers) + incoming > self.cfg.max_buffered_bytes
        };
        if still_over {
            counter!("s4logs_backpressure_total").increment(1);
            return Err(BufferError::OverCapacity);
        }
        Ok(())
    }

    /// Append one record to its buffer (creating buffer + WAL segment on
    /// first touch). WAL append happens *before* the in-memory push so a
    /// WAL failure never leaves an event that exists only in memory.
    fn push_one(
        cfg: &BufferConfig,
        buffers: &mut HashMap<BufKey, Buf>,
        log_group: &str,
        rec: &LogRecord,
    ) -> Result<(), BufferError> {
        let date = date_from_ts_ms(rec.timestamp);
        let buf = match buffers.entry((log_group.to_owned(), date)) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let wal = match &cfg.wal_dir {
                    Some(dir) => Some(
                        WalSegment::create(dir, log_group, &e.key().1).map_err(BufferError::Wal)?,
                    ),
                    None => None,
                };
                e.insert(Buf {
                    writer: ChunkWriter::new(cfg.chunk.clone()),
                    first_event_ts_ms: rec.timestamp,
                    opened_at: Instant::now(),
                    wal,
                })
            }
        };
        if let Some(seg) = &mut buf.wal {
            seg.append(&WalEntry {
                log_group: log_group.to_owned(),
                timestamp: rec.timestamp,
                stream: rec.stream.clone(),
                message: rec.message.clone(),
                ingestion_time: rec.ingestion_time,
            })
            .map_err(BufferError::Wal)?;
        }
        buf.writer.push(rec)?;
        Ok(())
    }

    /// Group-commit fsync of every dirty WAL segment (no-op per clean
    /// segment, so this is O(buffers) bookkeeping + one fsync per segment
    /// actually touched by the current batch).
    fn sync_wal(buffers: &mut HashMap<BufKey, Buf>) -> Result<(), BufferError> {
        for buf in buffers.values_mut() {
            if let Some(seg) = &mut buf.wal {
                seg.sync().map_err(BufferError::Wal)?;
            }
        }
        Ok(())
    }

    /// Startup WAL replay (DESIGN.md §11.1): push every intact event of
    /// every surviving segment back into the buffers (re-appending them to
    /// fresh segments + fsync), delete the old segment files, then flush
    /// whatever already exceeds `flush_bytes`. Normal age/size thresholds
    /// govern the rest. Returns the number of replayed events.
    ///
    /// Crash mid-replay re-replays everything → duplicates possible
    /// (at-least-once; `crate::wal` module docs).
    pub async fn replay_wal(&self) -> Result<u64, BufferError> {
        let Some(dir) = self.cfg.wal_dir.clone() else {
            return Ok(0);
        };
        let segments = wal::scan_dir(&dir).map_err(BufferError::Wal)?;
        if segments.is_empty() {
            return Ok(0);
        }
        let mut replayed = 0u64;
        let due = {
            let mut buffers = self.buffers.lock().unwrap_or_else(|e| e.into_inner());
            for seg in &segments {
                for entry in &seg.entries {
                    let rec = LogRecord {
                        timestamp: entry.timestamp,
                        stream: entry.stream.clone(),
                        message: entry.message.clone(),
                        ingestion_time: entry.ingestion_time,
                        event_id: None,
                    };
                    Self::push_one(&self.cfg, &mut buffers, &entry.log_group, &rec)?;
                    replayed += 1;
                }
            }
            // The replayed events are now durable in the *new* segments;
            // only then drop the old files.
            Self::sync_wal(&mut buffers)?;
            for seg in &segments {
                if let Err(err) = std::fs::remove_file(&seg.path) {
                    tracing::warn!(path = %seg.path.display(), error = %err, "replayed wal segment delete failed");
                }
            }
            take_matching(&mut buffers, |buf| {
                buf.writer.uncompressed_len() >= self.cfg.flush_bytes
            })
        };
        counter!("s4logs_wal_replayed_events_total").increment(replayed);
        tracing::info!(
            events = replayed,
            segments = segments.len(),
            "wal replay complete"
        );
        self.flush_taken(due).await?;
        Ok(replayed)
    }

    /// Flush every buffer whose oldest event exceeded `flush_interval`.
    /// Called by the periodic sweep task; errors are recorded in the ready
    /// bit and logged (there is no client to report them to).
    pub async fn sweep_expired(&self) {
        let due = {
            let mut buffers = self.buffers.lock().unwrap_or_else(|e| e.into_inner());
            take_matching(&mut buffers, |buf| {
                buf.opened_at.elapsed() >= self.cfg.flush_interval
            })
        };
        if let Err(err) = self.flush_taken(due).await {
            tracing::error!(error = %err, "age-based flush failed");
        }
    }

    /// Flush everything (graceful shutdown). All buffers are attempted even
    /// if one fails; the first error is returned.
    pub async fn flush_all(&self) -> Result<(), BufferError> {
        let due = {
            let mut buffers = self.buffers.lock().unwrap_or_else(|e| e.into_inner());
            take_matching(&mut buffers, |_| true)
        };
        self.flush_taken(due).await
    }

    async fn flush_taken(&self, taken: Vec<(BufKey, Buf)>) -> Result<(), BufferError> {
        let mut first_err = None;
        for ((log_group, date), buf) in taken {
            if let Err(err) = self.flush_one(log_group, date, buf).await {
                self.last_flush_ok.store(false, Ordering::Relaxed);
                tracing::error!(error = %err, "chunk flush failed");
                first_err.get_or_insert(err);
            } else {
                self.last_flush_ok.store(true, Ordering::Relaxed);
            }
        }
        match first_err {
            None => Ok(()),
            Some(err) => Err(err),
        }
    }

    async fn flush_one(
        &self,
        log_group: String,
        date: String,
        buf: Buf,
    ) -> Result<(), BufferError> {
        let Some(chunk) = buf.writer.finish()? else {
            // Empty writer — nothing to store; its WAL mirror is empty too.
            if let Some(seg) = buf.wal {
                seg.delete();
            }
            return Ok(());
        };
        let loc = ChunkLocation {
            account: self.cfg.account.clone(),
            log_group,
            date,
            name: format!("{}-{}", buf.first_event_ts_ms, uuid8()),
        };
        // On error the `?` drops `buf.wal` *without* deleting: the segment
        // survives on disk and a restart replays the events this failed
        // flush just dropped from memory.
        let receipt = self.sink.put_chunk(&loc, &chunk).await?;
        // Chunk (data + sidecars) is durable — retire the WAL segment.
        if let Some(seg) = buf.wal {
            seg.delete();
        }
        counter!("s4logs_flush_total").increment(1);
        counter!("s4logs_flush_bytes_total", "kind" => "raw").increment(chunk.uncompressed_bytes);
        counter!("s4logs_flush_bytes_total", "kind" => "compressed")
            .increment(chunk.body.len() as u64);
        tracing::info!(
            key = %receipt.data_key,
            records = chunk.record_count,
            raw_bytes = chunk.uncompressed_bytes,
            compressed_bytes = chunk.body.len(),
            "flushed chunk"
        );
        Ok(())
    }
}

/// Remove and return every buffer matching `pred` (lock must be held by the
/// caller via the `&mut` borrow; flushing happens after the lock is gone).
fn take_matching(
    buffers: &mut HashMap<BufKey, Buf>,
    pred: impl Fn(&Buf) -> bool,
) -> Vec<(BufKey, Buf)> {
    let keys: Vec<BufKey> = buffers
        .iter()
        .filter(|(_, buf)| pred(buf))
        .map(|(k, _)| k.clone())
        .collect();
    keys.into_iter()
        .filter_map(|k| buffers.remove_entry(&k))
        .collect()
}

/// Total uncompressed bytes currently buffered (memory-cap accounting).
fn buffered_total(buffers: &HashMap<BufKey, Buf>) -> u64 {
    buffers.values().map(|b| b.writer.uncompressed_len()).sum()
}

/// 8-hex-char random suffix for gateway object names (DESIGN.md §3) and WAL
/// segment names (`crate::wal`).
pub(crate) fn uuid8() -> String {
    let full = uuid::Uuid::new_v4().simple().to_string();
    full[..8].to_owned()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use s4logs_core::sink::MemorySink;

    fn ev(ts: i64, msg: &str) -> InputLogEvent {
        InputLogEvent {
            timestamp: ts,
            message: msg.to_owned(),
        }
    }

    fn mgr(cfg: BufferConfig) -> (Arc<MemorySink>, BufferManager) {
        let sink = Arc::new(MemorySink::new(""));
        let m = BufferManager::new(cfg, sink.clone());
        (sink, m)
    }

    #[tokio::test]
    async fn explicit_flush_writes_layout_key_named_after_first_event() {
        let (sink, m) = mgr(BufferConfig::default());
        m.push_events("/g", "s", &[ev(1781049600123, "hello")])
            .await
            .unwrap();
        assert!(sink.keys().is_empty(), "below thresholds — nothing flushed");
        m.flush_all().await.unwrap();
        let keys = sink.keys();
        assert_eq!(keys.len(), 3, "data + s4index + s4lts: {keys:?}");
        let data = keys.iter().find(|k| k.ends_with(".jsonl.zst")).unwrap();
        assert!(
            data.starts_with(
                "data/account=000000000000/loggroup=%2Fg/dt=2026-06-10/1781049600123-"
            )
        );
        assert!(m.ready().await);
    }

    #[tokio::test]
    async fn size_threshold_flushes_inline() {
        let (sink, m) = mgr(BufferConfig {
            flush_bytes: 512,
            ..BufferConfig::default()
        });
        let batch: Vec<_> = (0..20)
            .map(|i| ev(1781049600000 + i, &format!("padded message number {i:04}")))
            .collect();
        m.push_events("/g", "s", &batch).await.unwrap();
        assert!(
            sink.keys().iter().any(|k| k.ends_with(".jsonl.zst")),
            "size threshold should have flushed without explicit flush_all"
        );
    }

    #[tokio::test]
    async fn age_sweep_flushes_expired_buffers_only() {
        let (sink, m) = mgr(BufferConfig {
            flush_interval: Duration::from_secs(3600),
            ..BufferConfig::default()
        });
        m.push_events("/g", "s", &[ev(1, "x")]).await.unwrap();
        m.sweep_expired().await;
        assert!(sink.keys().is_empty(), "1h interval — not expired yet");

        let (sink, m) = mgr(BufferConfig {
            flush_interval: Duration::ZERO,
            ..BufferConfig::default()
        });
        m.push_events("/g", "s", &[ev(1, "x")]).await.unwrap();
        m.sweep_expired().await;
        assert!(sink.keys().iter().any(|k| k.ends_with(".jsonl.zst")));
    }

    #[tokio::test]
    async fn date_split_never_spans_dt_partition() {
        let (sink, m) = mgr(BufferConfig::default());
        // 2026-06-09T23:59:59.999Z and 2026-06-10T00:00:00.000Z
        m.push_events("/g", "s", &[ev(1781049599999, "a"), ev(1781049600000, "b")])
            .await
            .unwrap();
        m.flush_all().await.unwrap();
        let data: Vec<_> = sink
            .keys()
            .into_iter()
            .filter(|k| k.ends_with(".jsonl.zst"))
            .collect();
        assert_eq!(data.len(), 2, "{data:?}");
        assert!(data.iter().any(|k| k.contains("/dt=2026-06-09/")));
        assert!(data.iter().any(|k| k.contains("/dt=2026-06-10/")));
    }

    #[tokio::test]
    async fn flush_all_on_empty_manager_is_noop() {
        let (sink, m) = mgr(BufferConfig::default());
        m.flush_all().await.unwrap();
        assert!(sink.keys().is_empty());
    }

    fn wal_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut v: Vec<_> = std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|x| x == "wal"))
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    }

    #[tokio::test]
    async fn wal_segments_split_per_dt_and_are_deleted_on_flush() {
        let dir = tempfile::tempdir().unwrap();
        let (sink, m) = mgr(BufferConfig {
            wal_dir: Some(dir.path().to_owned()),
            ..BufferConfig::default()
        });
        // 2026-06-09T23:59:59.999Z and 2026-06-10T00:00:00.000Z — two buffer
        // keys, two WAL segments.
        m.push_events("/g", "s", &[ev(1781049599999, "a"), ev(1781049600000, "b")])
            .await
            .unwrap();
        assert_eq!(wal_files(dir.path()).len(), 2);
        m.flush_all().await.unwrap();
        assert_eq!(
            wal_files(dir.path()).len(),
            0,
            "flushed segments must be deleted"
        );
        assert_eq!(
            sink.keys()
                .iter()
                .filter(|k| k.ends_with(".jsonl.zst"))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn wal_replay_restores_unflushed_events() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = BufferConfig {
            wal_dir: Some(dir.path().to_owned()),
            ..BufferConfig::default()
        };
        // "Crash": manager dropped while events are still buffered.
        let (sink, m) = mgr(cfg.clone());
        m.push_events("/g", "s", &[ev(1781049600123, "survives")])
            .await
            .unwrap();
        drop(m);
        assert!(sink.keys().is_empty());
        assert_eq!(wal_files(dir.path()).len(), 1);

        let (sink, m) = mgr(cfg);
        assert_eq!(m.replay_wal().await.unwrap(), 1);
        m.flush_all().await.unwrap();
        assert!(sink.keys().iter().any(|k| k.ends_with(".jsonl.zst")));
        assert_eq!(wal_files(dir.path()).len(), 0, "replay + flush retires wal");
    }

    #[tokio::test]
    async fn replay_without_wal_dir_is_noop() {
        let (_sink, m) = mgr(BufferConfig::default());
        assert_eq!(m.replay_wal().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn oversized_batch_is_rejected_with_over_capacity() {
        let (sink, m) = mgr(BufferConfig {
            max_buffered_bytes: 128,
            ..BufferConfig::default()
        });
        let err = m
            .push_events("/g", "s", &[ev(1, &"x".repeat(512))])
            .await
            .unwrap_err();
        assert!(matches!(err, BufferError::OverCapacity), "{err:?}");
        m.flush_all().await.unwrap();
        assert!(sink.keys().is_empty(), "rejected batch must not buffer");
    }

    #[tokio::test]
    async fn cap_pressure_flushes_largest_buffer_first() {
        let (sink, m) = mgr(BufferConfig {
            max_buffered_bytes: 1000,
            ..BufferConfig::default()
        });
        m.push_events("/g", "s", &[ev(1781049600000, &"x".repeat(600))])
            .await
            .unwrap();
        assert!(sink.keys().is_empty(), "first batch fits below the cap");
        // Second batch would exceed the cap → the (only) largest buffer is
        // flushed to make room and the batch is accepted.
        m.push_events("/g", "s", &[ev(1781049600001, &"y".repeat(600))])
            .await
            .unwrap();
        assert_eq!(
            sink.keys()
                .iter()
                .filter(|k| k.ends_with(".jsonl.zst"))
                .count(),
            1,
            "largest buffer must have been force-flushed"
        );
        m.flush_all().await.unwrap();
        assert_eq!(
            sink.keys()
                .iter()
                .filter(|k| k.ends_with(".jsonl.zst"))
                .count(),
            2,
            "second batch was accepted and buffered"
        );
    }
}
