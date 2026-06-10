//! Buffer manager — per-(log_group, UTC date) `ChunkWriter`s with
//! size / age / shutdown flush (DESIGN.md §8.3).
//!
//! Each incoming event is routed into the writer keyed by
//! `(log_group, layout::date_from_ts_ms(timestamp))`, so a flushed chunk
//! never spans a `dt=` partition (DESIGN.md §3). Flush triggers:
//!
//! 1. uncompressed buffered bytes ≥ `flush_bytes` (default 8 MiB) — checked
//!    inline after every batch;
//! 2. oldest event in the buffer arrived ≥ `flush_interval` ago (default
//!    60 s) — checked by the periodic sweep `Gateway::serve` spawns;
//! 3. graceful shutdown (`flush_all`).
//!
//! **Durability (honest P1 note, DESIGN.md §8.3)**: buffers live in process
//! memory only. A crash loses everything not yet flushed (< flush_bytes /
//! flush_interval worth of events per group). A WAL is on the roadmap; the
//! README Limitations section must state this.

use std::collections::HashMap;
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

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BufferError {
    #[error("chunk encode failed")]
    Chunk(#[from] ChunkError),
    #[error("chunk store failed")]
    Sink(#[from] SinkError),
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
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            account: "000000000000".to_owned(),
            flush_bytes: 8 << 20,
            flush_interval: Duration::from_secs(60),
            chunk: ChunkConfig::default(),
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
    /// crossed `flush_bytes` is flushed before returning.
    pub async fn push_events(
        &self,
        log_group: &str,
        stream: &str,
        events: &[InputLogEvent],
    ) -> Result<(), BufferError> {
        let due = {
            let mut buffers = self.buffers.lock().unwrap_or_else(|e| e.into_inner());
            for ev in events {
                let date = date_from_ts_ms(ev.timestamp);
                let buf = buffers
                    .entry((log_group.to_owned(), date))
                    .or_insert_with(|| Buf {
                        writer: ChunkWriter::new(self.cfg.chunk.clone()),
                        first_event_ts_ms: ev.timestamp,
                        opened_at: Instant::now(),
                    });
                buf.writer.push(&LogRecord {
                    timestamp: ev.timestamp,
                    stream: stream.to_owned(),
                    message: ev.message.clone(),
                    ingestion_time: None,
                    event_id: None,
                })?;
            }
            take_matching(&mut buffers, |buf| {
                buf.writer.uncompressed_len() >= self.cfg.flush_bytes
            })
        };
        self.flush_taken(due).await
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
            return Ok(()); // empty writer — nothing to store
        };
        let loc = ChunkLocation {
            account: self.cfg.account.clone(),
            log_group,
            date,
            name: format!("{}-{}", buf.first_event_ts_ms, uuid8()),
        };
        let receipt = self.sink.put_chunk(&loc, &chunk).await?;
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

/// 8-hex-char random suffix for gateway object names (DESIGN.md §3).
fn uuid8() -> String {
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
}
