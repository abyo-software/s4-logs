//! DrainJob — Mode A core loop (DESIGN.md §7).
//!
//! Unit of work = `(log_group, window)`. Per window: skip if a manifest
//! already exists (idempotency), page `FilterLogEvents` through a
//! `ChunkWriter`, rotate to a new object when the uncompressed size reaches
//! `chunk_target_bytes` (object names `{window_start_ms}-{seq:06}` —
//! deterministic, so re-running a window overwrites identical content; with
//! `shard_streams > 1` the names stay deterministic but the *content* byte
//! order does not — see [`crate::shard`]), then write the manifest. Empty
//! windows still get a manifest (empty `objects`) so the retention gate can
//! prove coverage.
//!
//! `--dry-run` reads CW and *compresses* (for an honest savings estimate)
//! but writes nothing — no chunks, no manifests.

use std::sync::Arc;

use futures::StreamExt;
use s4logs_core::chunk::{ChunkConfig, ChunkError, ChunkWriter};
use s4logs_core::layout::{ChunkLocation, date_from_ts_ms, manifest_key};
use s4logs_core::record::LogRecord;
use s4logs_core::sink::{ChunkSink, SinkError};
use thiserror::Error;

use crate::cw::{CwError, CwSource};
use crate::manifest::{
    DRAIN_VERSION, MANIFEST_VERSION, Manifest, ManifestError, ManifestObject, ManifestStore,
};
use crate::progress::{Progress, ProgressEvent};
use crate::shard::{MAX_STREAMS_PER_FILTER, event_pages, partition_streams};
use crate::window::{HOUR_MS, Window, WindowError, windows};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DrainError {
    #[error(transparent)]
    Cw(#[from] CwError),
    #[error("chunk encode failed")]
    Chunk(#[from] ChunkError),
    #[error("chunk store failed")]
    Sink(#[from] SinkError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Window(#[from] WindowError),
    #[error("invalid drain options: {0}")]
    BadOptions(String),
}

/// Everything `s4logs drain` (wave 2D CLI) configures.
#[derive(Debug, Clone)]
pub struct DrainOptions {
    /// AWS account id (or operator-chosen scope label) for the S3 layout.
    pub account: String,
    /// Raw CloudWatch log group name.
    pub log_group: String,
    /// Drain range start (epoch ms). `None` → log group creation time.
    pub from_ms: Option<i64>,
    /// Drain range end, exclusive (epoch ms). `None` → `now - now_cutoff_ms`.
    pub to_ms: Option<i64>,
    /// Safety margin behind `now` when `to_ms` is `None` — CW ingestion
    /// lags, and draining the current instant would archive a still-filling
    /// window. Default 15 min.
    pub now_cutoff_ms: i64,
    /// Window length (UTC-grid aligned, additionally cut at day boundaries).
    /// Default 1h.
    pub window_ms: i64,
    /// Rotate to a new data object once this much *uncompressed* JSONL
    /// accumulated. Default 256 MiB.
    pub chunk_target_bytes: u64,
    /// Frame size / zstd level for `ChunkWriter`.
    pub chunk: ChunkConfig,
    /// Windows processed in parallel. Default 2 (quota-friendly).
    pub concurrency: usize,
    /// Read CW, count + compress, write nothing.
    pub dry_run: bool,
    /// Parallel FilterLogEvents shards *within* one window, partitioned by
    /// log stream (wave 4K; default 1 = the exact pre-4K single-token page
    /// loop). When > 1 the group's streams are listed once per run and
    /// round-robin-partitioned ([`crate::shard::partition_streams`]); total
    /// in-flight FilterLogEvents pressure becomes
    /// `concurrency × shard_streams`.
    ///
    /// HONESTY: with `shard_streams > 1` object **content** is no longer
    /// byte-deterministic across runs (shard pages interleave in completion
    /// order). The archived record *set* per window is identical and
    /// manifest-skip idempotency is unaffected. See [`crate::shard`].
    pub shard_streams: usize,
    /// Progress hooks ([`crate::progress`]); `Progress::none()` = zero
    /// overhead. Shared by drain and reconcile.
    pub progress: Progress,
}

impl DrainOptions {
    pub fn new(account: impl Into<String>, log_group: impl Into<String>) -> Self {
        Self {
            account: account.into(),
            log_group: log_group.into(),
            from_ms: None,
            to_ms: None,
            now_cutoff_ms: 15 * 60_000,
            window_ms: HOUR_MS,
            chunk_target_bytes: 256 << 20,
            chunk: ChunkConfig::default(),
            concurrency: 2,
            dry_run: false,
            shard_streams: 1,
            progress: Progress::none(),
        }
    }
}

/// Summary for `--dry-run` and final output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DrainReport {
    pub log_group: String,
    pub dry_run: bool,
    pub windows_total: u64,
    /// Windows actually paged through CW (includes empty ones).
    pub windows_processed: u64,
    /// Windows skipped because a manifest already existed.
    pub windows_skipped: u64,
    /// Processed windows that contained zero events.
    pub windows_empty: u64,
    pub records: u64,
    /// Events CW returned outside the requested window (defensively
    /// dropped; should be 0 against real AWS).
    pub events_outside_window: u64,
    /// Uncompressed JSONL bytes.
    pub raw_bytes: u64,
    /// zstd bytes (counted in dry-run too; written only in real runs).
    pub compressed_bytes: u64,
    /// Data objects PUT (0 in dry-run).
    pub objects_written: u64,
}

/// CW Logs storage price, USD per GiB-month.
pub const CW_STORAGE_USD_PER_GIB_MONTH: f64 = 0.03;
/// S3 Standard storage price, USD per GiB-month.
pub const S3_STORAGE_USD_PER_GIB_MONTH: f64 = 0.023;
/// CloudWatch bills archived storage on **gzip-level-6 compressed** bytes,
/// not raw (AWS pricing page footnote). We don't know the gzip size of the
/// drained data, so the CW-side estimate assumes this ratio; typical text
/// logs land 3-5x. Without it the savings estimate overstates by ~4x —
/// found during the 2026-06-10 real-AWS controlled experiment.
pub const CW_ASSUMED_GZIP_RATIO: f64 = 4.0;

fn gib(bytes: u64) -> f64 {
    bytes as f64 / (1u64 << 30) as f64
}

impl DrainReport {
    /// What the drained bytes cost per month if left in CloudWatch —
    /// estimated on CW's gzip-compressed billing basis
    /// ([`CW_ASSUMED_GZIP_RATIO`]).
    pub fn cw_monthly_storage_usd(&self) -> f64 {
        gib(self.raw_bytes) / CW_ASSUMED_GZIP_RATIO * CW_STORAGE_USD_PER_GIB_MONTH
    }

    /// What the compressed bytes cost per month in S3 Standard.
    pub fn s3_monthly_storage_usd(&self) -> f64 {
        gib(self.compressed_bytes) * S3_STORAGE_USD_PER_GIB_MONTH
    }

    /// Estimated monthly storage saving (CW raw vs S3 compressed).
    pub fn estimated_monthly_savings_usd(&self) -> f64 {
        self.cw_monthly_storage_usd() - self.s3_monthly_storage_usd()
    }

    fn absorb(&mut self, o: WindowOutcome) {
        if o.skipped {
            self.windows_skipped += 1;
            return;
        }
        self.windows_processed += 1;
        if o.records == 0 {
            self.windows_empty += 1;
        }
        self.records += o.records;
        self.events_outside_window += o.dropped;
        self.raw_bytes += o.raw_bytes;
        self.compressed_bytes += o.compressed_bytes;
        self.objects_written += o.objects_written;
    }
}

#[derive(Debug, Default)]
pub(crate) struct WindowOutcome {
    pub(crate) skipped: bool,
    pub(crate) records: u64,
    pub(crate) dropped: u64,
    pub(crate) raw_bytes: u64,
    pub(crate) compressed_bytes: u64,
    pub(crate) objects_written: u64,
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Names a window's data objects. Base drain objects are
/// `{window_start_ms}-{seq:06}` (6 digits, deterministic — DESIGN.md §7);
/// reconcile-appended objects are `{window_start_ms}-r{attempt:02}{seq:04}`.
/// The two schemes can never collide: a base suffix is exactly six ASCII
/// digits, a reconcile suffix always starts with `r`. Each reconcile run
/// that appends to a window uses the next free `attempt` (01–99, scanned
/// from the manifest), so repeated reconciles never overwrite each other.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ObjectNaming {
    Base,
    Reconcile { attempt: u32 },
}

impl ObjectNaming {
    fn name(&self, window_start_ms: i64, seq: u32) -> String {
        match self {
            Self::Base => format!("{window_start_ms}-{seq:06}"),
            Self::Reconcile { attempt } => format!("{window_start_ms}-r{attempt:02}{seq:04}"),
        }
    }
}

/// Per-window chunk pipeline shared by drain and reconcile: push records,
/// rotate at `chunk_target_bytes`, PUT through the [`ChunkSink`] (unless
/// dry-run), accumulate manifest entries + byte counters, emit
/// `ObjectWritten` progress events.
pub(crate) struct WindowWriter<'a> {
    sink: &'a dyn ChunkSink,
    opts: &'a DrainOptions,
    window_start_ms: i64,
    date: String,
    naming: ObjectNaming,
    writer: ChunkWriter,
    seq: u32,
    pub(crate) objects: Vec<ManifestObject>,
    pub(crate) records: u64,
    pub(crate) raw_bytes: u64,
    pub(crate) compressed_bytes: u64,
    pub(crate) objects_written: u64,
}

impl<'a> WindowWriter<'a> {
    pub(crate) fn new(
        sink: &'a dyn ChunkSink,
        opts: &'a DrainOptions,
        w: Window,
        naming: ObjectNaming,
    ) -> Self {
        Self {
            sink,
            opts,
            window_start_ms: w.start_ms,
            // Windows never span UTC days, so the window start fixes the
            // dt= partition for every event inside it.
            date: date_from_ts_ms(w.start_ms),
            naming,
            writer: ChunkWriter::new(opts.chunk.clone()),
            seq: 0,
            objects: Vec::new(),
            records: 0,
            raw_bytes: 0,
            compressed_bytes: 0,
            objects_written: 0,
        }
    }

    /// Uncompressed JSONL accepted so far (flushed + pending) — the
    /// `bytes_so_far` of `Page` progress events.
    pub(crate) fn bytes_so_far(&self) -> u64 {
        self.raw_bytes + self.writer.uncompressed_len()
    }

    pub(crate) async fn push(&mut self, rec: &LogRecord) -> Result<(), DrainError> {
        self.writer.push(rec)?;
        if self.writer.uncompressed_len() >= self.opts.chunk_target_bytes {
            let full =
                std::mem::replace(&mut self.writer, ChunkWriter::new(self.opts.chunk.clone()));
            self.flush(full).await?;
        }
        Ok(())
    }

    /// Flush the pending partial chunk. Call exactly once, after the last
    /// `push`.
    pub(crate) async fn finish(&mut self) -> Result<(), DrainError> {
        let last = std::mem::replace(&mut self.writer, ChunkWriter::new(self.opts.chunk.clone()));
        self.flush(last).await
    }

    /// Finalize one chunk: count it, and unless dry-run, PUT it and record
    /// its manifest entry. No-op for an empty writer.
    async fn flush(&mut self, writer: ChunkWriter) -> Result<(), DrainError> {
        let Some(chunk) = writer.finish()? else {
            return Ok(());
        };
        self.records += chunk.record_count;
        self.raw_bytes += chunk.uncompressed_bytes;
        self.compressed_bytes += chunk.body.len() as u64;
        if !self.opts.dry_run {
            let loc = ChunkLocation {
                account: self.opts.account.clone(),
                log_group: self.opts.log_group.clone(),
                date: self.date.clone(),
                name: self.naming.name(self.window_start_ms, self.seq),
            };
            let receipt = self.sink.put_chunk(&loc, &chunk).await?;
            self.opts.progress.emit(|| ProgressEvent::ObjectWritten {
                key: receipt.data_key.clone(),
                raw_bytes: chunk.uncompressed_bytes,
                compressed_bytes: receipt.body_len,
            });
            self.objects.push(ManifestObject {
                data_key: receipt.data_key,
                etag: receipt.etag,
                crc32c: receipt.crc32c,
                body_len: receipt.body_len,
                raw_bytes: Some(chunk.uncompressed_bytes),
                record_count: chunk.record_count,
                min_ts: chunk.min_timestamp,
                max_ts: chunk.max_timestamp,
            });
            self.objects_written += 1;
        }
        self.seq += 1;
        Ok(())
    }
}

/// Mode A drain for one log group.
pub struct DrainJob {
    pub(crate) cw: Arc<dyn CwSource>,
    pub(crate) sink: Arc<dyn ChunkSink>,
    pub(crate) manifests: Arc<dyn ManifestStore>,
    pub(crate) opts: DrainOptions,
}

impl DrainJob {
    pub fn new(
        cw: Arc<dyn CwSource>,
        sink: Arc<dyn ChunkSink>,
        manifests: Arc<dyn ManifestStore>,
        opts: DrainOptions,
    ) -> Self {
        Self {
            cw,
            sink,
            manifests,
            opts,
        }
    }

    pub fn options(&self) -> &DrainOptions {
        &self.opts
    }

    /// Resolve the effective `[from, to)` drain range (creation time / now
    /// cutoff defaults). Shared with [`crate::reconcile::ReconcileJob`].
    pub(crate) async fn resolve_range(&self) -> Result<(i64, i64), DrainError> {
        if self.opts.chunk_target_bytes == 0 {
            return Err(DrainError::BadOptions(
                "chunk_target_bytes must be > 0".into(),
            ));
        }
        let from = match self.opts.from_ms {
            Some(f) => f,
            None => {
                self.cw
                    .describe_log_group(&self.opts.log_group)
                    .await?
                    .creation_time_ms
            }
        };
        let to = match self.opts.to_ms {
            Some(t) => t,
            None => now_ms().saturating_sub(self.opts.now_cutoff_ms),
        };
        Ok((from, to))
    }

    /// Stream shards for this run: `None` (page unsharded — the default and
    /// the < 2-shard degenerate cases) or ≥ 2 round-robin stream shards.
    /// Lists the group's streams once; see [`crate::shard`].
    pub(crate) async fn compute_shards(&self) -> Result<Option<Vec<Vec<String>>>, DrainError> {
        if self.opts.shard_streams <= 1 {
            return Ok(None);
        }
        let names = self.cw.list_log_streams(&self.opts.log_group, None).await?;
        let shards = partition_streams(&names, self.opts.shard_streams, MAX_STREAMS_PER_FILTER);
        if shards.len() < 2 {
            // 0 or 1 streams: a stream-name filter would only change the
            // call shape, not the result — keep the exact default behavior.
            return Ok(None);
        }
        tracing::info!(
            log_group = %self.opts.log_group,
            streams = shards.iter().map(Vec::len).sum::<usize>(),
            shards = shards.len(),
            "stream-sharded paging enabled"
        );
        Ok(Some(shards))
    }

    /// Run the drain. Fails fast on the first window error (windows already
    /// completed keep their manifests, so a re-run resumes where it left
    /// off — that is the idempotency story, not partial-failure bookkeeping).
    pub async fn run(&self) -> Result<DrainReport, DrainError> {
        let (from, to) = self.resolve_range().await?;
        let shards = self.compute_shards().await?;
        let wins = windows(from, to, self.opts.window_ms)?;
        let mut report = DrainReport {
            log_group: self.opts.log_group.clone(),
            dry_run: self.opts.dry_run,
            windows_total: wins.len() as u64,
            ..DrainReport::default()
        };
        tracing::info!(
            log_group = %self.opts.log_group,
            from_ms = from,
            to_ms = to,
            windows = wins.len(),
            dry_run = self.opts.dry_run,
            "drain starting"
        );
        let mut stream = futures::stream::iter(
            wins.into_iter()
                .map(|w| self.process_window(w, shards.as_deref())),
        )
        .buffer_unordered(self.opts.concurrency.max(1));
        while let Some(res) = stream.next().await {
            report.absorb(res?);
        }
        Ok(report)
    }

    /// Drain one window (manifest-skip, page → chunk → PUT → manifest).
    /// `shards` comes from [`Self::compute_shards`]; `None` = unsharded.
    pub(crate) async fn process_window(
        &self,
        w: Window,
        shards: Option<&[Vec<String>]>,
    ) -> Result<WindowOutcome, DrainError> {
        let opts = &self.opts;
        let mkey = manifest_key(
            self.sink.key_prefix(),
            &opts.account,
            &opts.log_group,
            w.start_ms,
            w.end_ms,
        );
        if self.manifests.exists(&mkey).await? {
            tracing::debug!(log_group = %opts.log_group, window_start_ms = w.start_ms, "manifest exists; skipping window");
            opts.progress
                .emit(|| ProgressEvent::WindowSkipped { window: w });
            return Ok(WindowOutcome {
                skipped: true,
                ..WindowOutcome::default()
            });
        }
        opts.progress
            .emit(|| ProgressEvent::WindowStarted { window: w });

        let mut out = WindowOutcome::default();
        let mut ww = WindowWriter::new(&*self.sink, opts, w, ObjectNaming::Base);
        {
            let mut pages = event_pages(&*self.cw, &opts.log_group, w, shards);
            while let Some(page) = pages.next().await {
                let events = page?;
                let page_len = events.len() as u64;
                for ev in events {
                    if ev.timestamp < w.start_ms || ev.timestamp >= w.end_ms {
                        tracing::warn!(
                            log_group = %opts.log_group,
                            timestamp = ev.timestamp,
                            window_start_ms = w.start_ms,
                            window_end_ms = w.end_ms,
                            "event outside requested window; dropping (would corrupt dt= partition)"
                        );
                        out.dropped += 1;
                        continue;
                    }
                    ww.push(&LogRecord {
                        timestamp: ev.timestamp,
                        stream: ev.log_stream_name,
                        message: ev.message,
                        ingestion_time: ev.ingestion_time,
                        event_id: ev.event_id,
                    })
                    .await?;
                }
                opts.progress.emit(|| ProgressEvent::Page {
                    window: w,
                    events: page_len,
                    bytes_so_far: ww.bytes_so_far(),
                });
            }
        }
        ww.finish().await?;
        out.records = ww.records;
        out.raw_bytes = ww.raw_bytes;
        out.compressed_bytes = ww.compressed_bytes;
        out.objects_written = ww.objects_written;

        if !opts.dry_run {
            let manifest = Manifest {
                version: MANIFEST_VERSION,
                account: opts.account.clone(),
                log_group: opts.log_group.clone(),
                window_start_ms: w.start_ms,
                window_end_ms: w.end_ms,
                record_count: ww.objects.iter().map(|o| o.record_count).sum(),
                objects: ww.objects,
                completed_at_ms: now_ms(),
                drain_version: DRAIN_VERSION.to_owned(),
                reconciled_at_ms: None,
                reconciled_added: None,
            };
            self.manifests.put(&mkey, manifest.to_json_bytes()?).await?;
        }
        opts.progress.emit(|| ProgressEvent::WindowDone {
            window: w,
            records: out.records,
        });
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Multi-group drain (DESIGN.md §11.4)
// ---------------------------------------------------------------------------

/// One group's outcome inside a multi-group drain.
#[derive(Debug)]
pub struct GroupDrainResult {
    pub log_group: String,
    pub result: Result<DrainReport, DrainError>,
}

/// Outcome of [`drain_groups`]: per-group results, name-sorted. Failed
/// groups are *skipped + reported*, never fatal to their siblings
/// (DESIGN.md §11.4); the CLI maps `any_failed()` to exit code 1.
#[derive(Debug, Default)]
pub struct MultiDrainReport {
    pub groups: Vec<GroupDrainResult>,
}

impl MultiDrainReport {
    pub fn any_failed(&self) -> bool {
        self.groups.iter().any(|g| g.result.is_err())
    }

    /// `(log_group, error)` for every failed group, in name order.
    pub fn failures(&self) -> impl Iterator<Item = (&str, &DrainError)> {
        self.groups
            .iter()
            .filter_map(|g| g.result.as_ref().err().map(|e| (g.log_group.as_str(), e)))
    }

    pub fn succeeded(&self) -> impl Iterator<Item = &DrainReport> {
        self.groups.iter().filter_map(|g| g.result.as_ref().ok())
    }

    /// Sum of all successful per-group reports. `log_group` carries a
    /// "{ok}/{total} log groups" label; `dry_run` is true when every
    /// successful run was a dry run.
    pub fn aggregate(&self) -> DrainReport {
        let ok = self.groups.iter().filter(|g| g.result.is_ok()).count();
        let mut agg = DrainReport {
            log_group: format!("{ok}/{} log groups", self.groups.len()),
            dry_run: self.succeeded().all(|r| r.dry_run) && ok > 0,
            ..DrainReport::default()
        };
        for r in self.succeeded() {
            agg.windows_total += r.windows_total;
            agg.windows_processed += r.windows_processed;
            agg.windows_skipped += r.windows_skipped;
            agg.windows_empty += r.windows_empty;
            agg.records += r.records;
            agg.events_outside_window += r.events_outside_window;
            agg.raw_bytes += r.raw_bytes;
            agg.compressed_bytes += r.compressed_bytes;
            agg.objects_written += r.objects_written;
        }
        agg
    }
}

/// Run one independent [`DrainJob`] per discovered group with bounded
/// group-level parallelism. `base.log_group` is ignored; `base.from_ms`
/// (when `None`) defaults per group to the discovered creation time — no
/// extra `DescribeLogGroups` round-trip inside each job. Window-level
/// parallelism stays `base.concurrency` *per group*, so the total
/// FilterLogEvents pressure is `group_concurrency × concurrency`.
pub async fn drain_groups(
    cw: Arc<dyn CwSource>,
    sink: Arc<dyn ChunkSink>,
    manifests: Arc<dyn ManifestStore>,
    base: &DrainOptions,
    groups: Vec<(String, crate::cw::LogGroupInfo)>,
    group_concurrency: usize,
) -> MultiDrainReport {
    let jobs = groups.into_iter().map(|(name, info)| {
        let mut opts = base.clone();
        opts.log_group = name.clone();
        opts.from_ms = base.from_ms.or(Some(info.creation_time_ms));
        let job = DrainJob::new(cw.clone(), sink.clone(), manifests.clone(), opts);
        async move {
            let result = job.run().await;
            if let Err(err) = &result {
                tracing::error!(log_group = %name, error = %err, "group drain failed; continuing with remaining groups");
            }
            GroupDrainResult {
                log_group: name,
                result,
            }
        }
    });
    let mut results: Vec<GroupDrainResult> = futures::stream::iter(jobs)
        .buffer_unordered(group_concurrency.max(1))
        .collect()
        .await;
    results.sort_by(|a, b| a.log_group.cmp(&b.log_group));
    MultiDrainReport { groups: results }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::manifest::MemoryManifestStore;
    use crate::testutil::MockCw;
    use crate::window::DAY_MS;
    use bytes::Bytes;
    use s4logs_core::sink::MemorySink;
    use std::sync::atomic::Ordering;

    // 2024-06-05T00:00:00Z, an exact UTC day boundary.
    const DAY0: i64 = 1_717_545_600_000;
    const ACCT: &str = "123456789012";
    const GROUP: &str = "/aws/lambda/foo";

    fn job(
        cw: MockCw,
        opts: DrainOptions,
    ) -> (DrainJob, Arc<MemorySink>, Arc<MemoryManifestStore>) {
        let sink = Arc::new(MemorySink::new("s4logs"));
        let manifests = Arc::new(MemoryManifestStore::new());
        let j = DrainJob::new(Arc::new(cw), sink.clone(), manifests.clone(), opts);
        (j, sink, manifests)
    }

    fn opts(from: i64, to: i64) -> DrainOptions {
        DrainOptions {
            from_ms: Some(from),
            to_ms: Some(to),
            ..DrainOptions::new(ACCT, GROUP)
        }
    }

    #[tokio::test]
    async fn pagination_rotation_and_manifest() {
        // 100 events in one hour window, small pages + small chunk target →
        // many FilterLogEvents pages, several object rotations.
        let mut cw = MockCw {
            page_size: 7,
            ..MockCw::default()
        };
        for i in 0..100i64 {
            cw.events.push(crate::testutil::event(
                DAY0 + i * 1000,
                &format!("padded log line number {i:04} {}", "x".repeat(40)),
            ));
        }
        let mut o = opts(DAY0, DAY0 + HOUR_MS);
        o.chunk_target_bytes = 4_000; // force rotation
        let (job, sink, manifests) = job(cw, o);
        let report = job.run().await.unwrap();

        assert_eq!(report.windows_total, 1);
        assert_eq!(report.windows_processed, 1);
        assert_eq!(report.windows_skipped, 0);
        assert_eq!(report.records, 100);
        assert!(
            report.objects_written > 1,
            "expected chunk rotation, got {report:?}"
        );
        assert!(report.raw_bytes > 0 && report.compressed_bytes > 0);

        // Manifest exists at the layout key and is internally consistent.
        let mkey = manifest_key("s4logs", ACCT, GROUP, DAY0, DAY0 + HOUR_MS);
        let m = Manifest::from_json_bytes(&manifests.get(&mkey).await.unwrap().unwrap()).unwrap();
        assert_eq!(m.version, 1);
        assert_eq!(m.log_group, GROUP);
        assert_eq!(m.record_count, 100);
        assert_eq!(m.objects.len() as u64, report.objects_written);
        assert_eq!(m.objects.iter().map(|o| o.record_count).sum::<u64>(), 100);
        // Deterministic object names: {window_start_ms}-{seq:06}.
        for (i, obj) in m.objects.iter().enumerate() {
            assert!(
                obj.data_key.contains(&format!("/{DAY0}-{i:06}.jsonl.zst")),
                "object {i} key {} lacks deterministic name",
                obj.data_key
            );
            assert!(obj.etag.is_some());
            assert!(obj.body_len > 0);
            assert!(
                obj.raw_bytes.unwrap() >= obj.body_len,
                "raw_bytes accounting missing or smaller than compressed"
            );
            assert!(obj.min_ts >= DAY0 && obj.max_ts < DAY0 + HOUR_MS);
            // data object + both sidecars were stored
            assert!(sink.get(&obj.data_key).is_some());
        }
        // 3 keys per object (data + .s4index + .s4lts).
        assert_eq!(sink.keys().len() as u64, 3 * report.objects_written);
    }

    #[tokio::test]
    async fn idempotent_skip_when_manifest_exists() {
        let mut cw = MockCw::default();
        cw.events
            .push(crate::testutil::event(DAY0 + 1, "should not be read"));
        let (job, sink, manifests) = job(cw, opts(DAY0, DAY0 + HOUR_MS));
        manifests
            .put(
                &manifest_key("s4logs", ACCT, GROUP, DAY0, DAY0 + HOUR_MS),
                Bytes::from_static(b"{}"),
            )
            .await
            .unwrap();
        let report = job.run().await.unwrap();
        assert_eq!(report.windows_skipped, 1);
        assert_eq!(report.windows_processed, 0);
        assert_eq!(report.records, 0);
        assert!(sink.keys().is_empty());
    }

    #[tokio::test]
    async fn skip_does_not_touch_cw() {
        let cw = MockCw::default();
        let calls = cw.filter_calls.clone();
        let (job, _sink, manifests) = job(cw, opts(DAY0, DAY0 + HOUR_MS));
        manifests
            .put(
                &manifest_key("s4logs", ACCT, GROUP, DAY0, DAY0 + HOUR_MS),
                Bytes::from_static(b"{}"),
            )
            .await
            .unwrap();
        job.run().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn empty_window_writes_manifest_with_no_objects() {
        let (job, sink, manifests) = job(MockCw::default(), opts(DAY0, DAY0 + HOUR_MS));
        let report = job.run().await.unwrap();
        assert_eq!(report.windows_processed, 1);
        assert_eq!(report.windows_empty, 1);
        assert_eq!(report.objects_written, 0);
        assert!(sink.keys().is_empty());
        let mkey = manifest_key("s4logs", ACCT, GROUP, DAY0, DAY0 + HOUR_MS);
        let m = Manifest::from_json_bytes(&manifests.get(&mkey).await.unwrap().unwrap()).unwrap();
        assert!(m.objects.is_empty());
        assert_eq!(m.record_count, 0);
        assert_eq!(m.window_start_ms, DAY0);
        assert_eq!(m.window_end_ms, DAY0 + HOUR_MS);
    }

    #[tokio::test]
    async fn dry_run_writes_nothing_but_counts_everything() {
        let mut cw = MockCw::default();
        for i in 0..50i64 {
            cw.events.push(crate::testutil::event(
                DAY0 + i * 1000,
                &format!("dry run line {i} {}", "y".repeat(60)),
            ));
        }
        let mut o = opts(DAY0, DAY0 + HOUR_MS);
        o.dry_run = true;
        o.chunk_target_bytes = 2_000;
        let (job, sink, manifests) = job(cw, o);
        let report = job.run().await.unwrap();
        assert!(report.dry_run);
        assert_eq!(report.records, 50);
        assert!(report.raw_bytes > 0);
        assert!(
            report.compressed_bytes > 0,
            "dry-run must still estimate compression"
        );
        assert_eq!(report.objects_written, 0);
        assert!(
            sink.keys().is_empty(),
            "dry-run wrote chunks: {:?}",
            sink.keys()
        );
        assert!(manifests.is_empty(), "dry-run wrote manifests");
        assert!(report.estimated_monthly_savings_usd() > 0.0);
    }

    #[tokio::test]
    async fn day_boundary_windows_never_mix_dt_partitions() {
        // Events at 22:30, 23:15 (day 0) and 00:30, 01:30 (day 1).
        let mut cw = MockCw::default();
        for off in [
            22 * HOUR_MS + 30 * 60_000,
            23 * HOUR_MS + 15 * 60_000,
            DAY_MS + 30 * 60_000,
            DAY_MS + HOUR_MS + 30 * 60_000,
        ] {
            cw.events
                .push(crate::testutil::event(DAY0 + off, "boundary event"));
        }
        let from = DAY0 + 22 * HOUR_MS + 30 * 60_000; // mid-window from
        let to = DAY0 + DAY_MS + 2 * HOUR_MS;
        let (job, sink, _manifests) = job(cw, opts(from, to));
        let report = job.run().await.unwrap();
        assert_eq!(report.windows_total, 4);
        assert_eq!(report.records, 4);
        assert_eq!(report.objects_written, 4);

        let data_keys: Vec<String> = sink
            .keys()
            .into_iter()
            .filter(|k| k.ends_with(".jsonl.zst"))
            .collect();
        assert_eq!(data_keys.len(), 4);
        let day0_date = date_from_ts_ms(DAY0);
        let day1_date = date_from_ts_ms(DAY0 + DAY_MS);
        for key in &data_keys {
            let loc = ChunkLocation::parse_data_key("s4logs", key).unwrap();
            // name prefix = window start; its date must equal the partition.
            let win_start: i64 = loc.name.split('-').next().unwrap().parse().unwrap();
            assert_eq!(
                date_from_ts_ms(win_start),
                loc.date,
                "chunk spans dt= partition: {key}"
            );
            assert!(loc.date == day0_date || loc.date == day1_date);
        }
        assert!(
            data_keys
                .iter()
                .any(|k| k.contains(&format!("dt={day0_date}")))
        );
        assert!(
            data_keys
                .iter()
                .any(|k| k.contains(&format!("dt={day1_date}")))
        );
    }

    #[tokio::test]
    async fn throttling_surfaces_as_typed_error() {
        let cw = MockCw {
            throttle_remaining: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(u32::MAX)),
            ..MockCw::default()
        };
        let (job, sink, manifests) = job(cw, opts(DAY0, DAY0 + HOUR_MS));
        let err = job.run().await.unwrap_err();
        assert!(
            matches!(err, DrainError::Cw(CwError::Throttled { .. })),
            "got {err:?}"
        );
        assert!(sink.keys().is_empty());
        assert!(
            manifests.is_empty(),
            "no manifest may exist for a failed window"
        );
    }

    #[tokio::test]
    async fn out_of_window_events_are_dropped_not_archived() {
        let mut cw = MockCw::default();
        cw.events
            .push(crate::testutil::event(DAY0 + 1, "in window"));
        // Mock that misbehaves: returns an event outside the asked range.
        cw.ignore_range_filter = true;
        cw.events
            .push(crate::testutil::event(DAY0 + HOUR_MS + 1, "outside"));
        let (job, _sink, manifests) = job(cw, opts(DAY0, DAY0 + HOUR_MS));
        let report = job.run().await.unwrap();
        assert_eq!(report.records, 1);
        assert_eq!(report.events_outside_window, 1);
        let mkey = manifest_key("s4logs", ACCT, GROUP, DAY0, DAY0 + HOUR_MS);
        let m = Manifest::from_json_bytes(&manifests.get(&mkey).await.unwrap().unwrap()).unwrap();
        assert_eq!(m.record_count, 1);
    }

    #[tokio::test]
    async fn from_defaults_to_log_group_creation_time() {
        let mut cw = MockCw::default();
        cw.info.creation_time_ms = DAY0 + 30 * 60_000; // mid-window creation
        cw.events
            .push(crate::testutil::event(DAY0 + 45 * 60_000, "early event"));
        let mut o = opts(0, DAY0 + 2 * HOUR_MS);
        o.from_ms = None; // → describe_log_group
        let (job, _sink, manifests) = job(cw, o);
        let report = job.run().await.unwrap();
        // creation at 00:30 aligns down to 00:00 → windows [00,01) and [01,02).
        assert_eq!(report.windows_total, 2);
        assert_eq!(report.records, 1);
        assert!(
            manifests
                .exists(&manifest_key("s4logs", ACCT, GROUP, DAY0, DAY0 + HOUR_MS))
                .await
                .unwrap()
        );
    }

    #[test]
    fn savings_math() {
        let r = DrainReport {
            raw_bytes: 10 * (1u64 << 30),
            compressed_bytes: 1u64 << 30,
            ..DrainReport::default()
        };
        // CW side is estimated on its gzip-compressed billing basis:
        // 10 GiB raw / 4.0 assumed gzip * $0.03 = $0.075.
        assert!((r.cw_monthly_storage_usd() - 0.075).abs() < 1e-9);
        assert!((r.s3_monthly_storage_usd() - 0.023).abs() < 1e-9);
        assert!((r.estimated_monthly_savings_usd() - 0.052).abs() < 1e-9);
    }

    #[tokio::test]
    async fn rejects_zero_chunk_target() {
        let mut o = opts(DAY0, DAY0 + HOUR_MS);
        o.chunk_target_bytes = 0;
        let (job, _sink, _manifests) = job(MockCw::default(), o);
        assert!(matches!(
            job.run().await.unwrap_err(),
            DrainError::BadOptions(_)
        ));
    }

    // -- multi-group drain (DESIGN.md §11.4) --------------------------------

    fn group_info(creation_time_ms: i64) -> crate::cw::LogGroupInfo {
        crate::cw::LogGroupInfo {
            retention_days: None,
            stored_bytes: None,
            creation_time_ms,
        }
    }

    #[tokio::test]
    async fn multi_group_failure_is_skipped_and_aggregated() {
        let mut cw = MockCw::default();
        for g in ["/g/a", "/g/bad", "/g/c"] {
            cw.group_events.insert(
                g.to_owned(),
                vec![crate::testutil::event(DAY0 + 1, &format!("event in {g}"))],
            );
        }
        cw.fail_filter_groups.insert("/g/bad".to_owned());
        let sink = Arc::new(MemorySink::new("s4logs"));
        let manifests = Arc::new(MemoryManifestStore::new());
        let base = opts(DAY0, DAY0 + HOUR_MS);
        let groups = vec![
            ("/g/c".to_owned(), group_info(DAY0)), // unsorted on purpose
            ("/g/a".to_owned(), group_info(DAY0)),
            ("/g/bad".to_owned(), group_info(DAY0)),
        ];
        let multi = drain_groups(
            Arc::new(cw),
            sink.clone(),
            manifests.clone(),
            &base,
            groups,
            2,
        )
        .await;

        assert!(multi.any_failed());
        let names: Vec<&str> = multi.groups.iter().map(|g| g.log_group.as_str()).collect();
        assert_eq!(names, vec!["/g/a", "/g/bad", "/g/c"], "name-sorted output");
        let failed: Vec<&str> = multi.failures().map(|(g, _)| g).collect();
        assert_eq!(failed, vec!["/g/bad"]);

        let agg = multi.aggregate();
        assert_eq!(agg.log_group, "2/3 log groups");
        assert_eq!(agg.records, 2, "only successful groups aggregate");
        assert_eq!(agg.windows_processed, 2);
        assert_eq!(agg.objects_written, 2);

        // Successful groups have manifests; the failed one has none.
        assert!(
            manifests
                .exists(&manifest_key("s4logs", ACCT, "/g/a", DAY0, DAY0 + HOUR_MS))
                .await
                .unwrap()
        );
        assert!(
            manifests
                .exists(&manifest_key("s4logs", ACCT, "/g/c", DAY0, DAY0 + HOUR_MS))
                .await
                .unwrap()
        );
        assert!(
            !manifests
                .exists(&manifest_key(
                    "s4logs",
                    ACCT,
                    "/g/bad",
                    DAY0,
                    DAY0 + HOUR_MS
                ))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn multi_group_defaults_from_to_discovered_creation_time() {
        // No --from: each group must start at its own creation time without
        // re-describing (MockCw's describe fallback would give 0 → a huge
        // window count, so windows_total == 2 proves the info was used).
        let cw = MockCw::default();
        let sink = Arc::new(MemorySink::new("s4logs"));
        let manifests = Arc::new(MemoryManifestStore::new());
        let mut base = opts(0, DAY0 + 2 * HOUR_MS);
        base.from_ms = None;
        let multi = drain_groups(
            Arc::new(cw),
            sink,
            manifests,
            &base,
            vec![("/g".to_owned(), group_info(DAY0 + 30 * 60_000))],
            1,
        )
        .await;
        assert!(!multi.any_failed());
        let report = &multi.groups[0].result.as_ref().unwrap();
        assert_eq!(report.windows_total, 2);
    }

    // -- stream-shard parallel paging (wave 4K) ------------------------------

    /// Decode every data object in the sink into a sorted record list.
    fn archived_records(sink: &MemorySink) -> Vec<(i64, String, String)> {
        let mut out = Vec::new();
        for key in sink.keys() {
            if !key.ends_with(".jsonl.zst") {
                continue;
            }
            let raw = zstd::stream::decode_all(&sink.get(&key).unwrap()[..]).unwrap();
            for rec in s4logs_core::read::RecordLines::new(&raw) {
                let r = rec.unwrap();
                out.push((r.timestamp, r.stream, r.message));
            }
        }
        out.sort();
        out
    }

    fn multi_stream_events() -> Vec<crate::cw::CwEvent> {
        let mut events = Vec::new();
        for s in 0..7 {
            for i in 0..20i64 {
                events.push(crate::testutil::event_in_stream(
                    DAY0 + (s as i64) * 60_000 + i * 1000,
                    &format!("app/stream-{s}"),
                    &format!("padded line s{s} i{i:03} {}", "z".repeat(30)),
                ));
            }
        }
        events
    }

    #[tokio::test]
    async fn sharded_drain_archives_the_same_record_set_as_unsharded() {
        let events = multi_stream_events();

        let mut o1 = opts(DAY0, DAY0 + HOUR_MS);
        o1.chunk_target_bytes = 2_000; // several rotations
        let (job1, sink1, _m1) = job(
            MockCw {
                events: events.clone(),
                page_size: 7,
                ..MockCw::default()
            },
            o1.clone(),
        );
        let r1 = job1.run().await.unwrap();

        let mut o3 = o1;
        o3.shard_streams = 3;
        let cw = Arc::new(MockCw {
            events,
            page_size: 7,
            ..MockCw::default()
        });
        let sink3 = Arc::new(MemorySink::new("s4logs"));
        let manifests3 = Arc::new(MemoryManifestStore::new());
        let job3 = DrainJob::new(cw.clone(), sink3.clone(), manifests3.clone(), o3);
        let r3 = job3.run().await.unwrap();

        assert_eq!(r1.records, 140);
        assert_eq!(r3.records, 140);
        assert_eq!(r1.raw_bytes, r3.raw_bytes, "same JSONL bytes either way");
        assert_eq!(
            archived_records(&sink1),
            archived_records(&sink3),
            "sharded run must archive exactly the same record set"
        );
        // Streams listed exactly once for the whole run (not per window).
        assert_eq!(
            cw.list_stream_calls.lock().unwrap().len(),
            1,
            "list_log_streams must be called once per run"
        );
        // Manifest record accounting is intact.
        let mkey = manifest_key("s4logs", ACCT, GROUP, DAY0, DAY0 + HOUR_MS);
        let m = Manifest::from_json_bytes(&manifests3.get(&mkey).await.unwrap().unwrap()).unwrap();
        assert_eq!(m.record_count, 140);
    }

    #[tokio::test]
    async fn shard_streams_one_or_single_stream_keeps_default_call_shape() {
        // shard_streams = 1 must not even list streams; > 1 with a single
        // stream must fall back to the unsharded pass.
        let mut o = opts(DAY0, DAY0 + HOUR_MS);
        o.shard_streams = 1;
        let cw = Arc::new(MockCw {
            events: vec![crate::testutil::event(DAY0 + 1, "only")],
            ..MockCw::default()
        });
        let sink = Arc::new(MemorySink::new("s4logs"));
        let manifests = Arc::new(MemoryManifestStore::new());
        DrainJob::new(cw.clone(), sink, manifests, o.clone())
            .run()
            .await
            .unwrap();
        assert!(cw.list_stream_calls.lock().unwrap().is_empty());

        o.shard_streams = 4; // one stream only → must degrade to unsharded
        let cw2 = Arc::new(MockCw {
            events: vec![crate::testutil::event(DAY0 + 1, "only")],
            ..MockCw::default()
        });
        let sink2 = Arc::new(MemorySink::new("s4logs"));
        let manifests2 = Arc::new(MemoryManifestStore::new());
        let report = DrainJob::new(cw2.clone(), sink2, manifests2, o)
            .run()
            .await
            .unwrap();
        assert_eq!(report.records, 1);
        assert_eq!(cw2.list_stream_calls.lock().unwrap().len(), 1);
    }

    // -- progress hooks (wave 4K) --------------------------------------------

    #[tokio::test]
    async fn progress_events_arrive_in_order_for_a_scripted_run() {
        use crate::progress::{Progress, ProgressEvent};
        use std::sync::Mutex;

        let seen: Arc<Mutex<Vec<ProgressEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let tap = seen.clone();
        let mut cw = MockCw {
            page_size: 5,
            ..MockCw::default()
        };
        for i in 0..7i64 {
            cw.events.push(crate::testutil::event(
                DAY0 + i * 1000,
                &format!("line {i}"),
            ));
        }
        let mut o = opts(DAY0, DAY0 + HOUR_MS);
        o.concurrency = 1;
        o.progress = Progress::callback(move |ev| tap.lock().unwrap().push(ev));
        let (job, _sink, _manifests) = job(cw, o);
        job.run().await.unwrap();

        let w = Window {
            start_ms: DAY0,
            end_ms: DAY0 + HOUR_MS,
        };
        let got = seen.lock().unwrap().clone();
        // 7 events, page size 5 → 2 pages; one object flushed at finish.
        assert_eq!(got.len(), 5, "got {got:?}");
        assert_eq!(got[0], ProgressEvent::WindowStarted { window: w });
        assert!(
            matches!(got[1], ProgressEvent::Page { window, events: 5, .. } if window == w),
            "got {:?}",
            got[1]
        );
        assert!(
            matches!(got[2], ProgressEvent::Page { window, events: 2, bytes_so_far } if window == w && bytes_so_far > 0),
            "got {:?}",
            got[2]
        );
        assert!(
            matches!(
                &got[3],
                ProgressEvent::ObjectWritten { key, raw_bytes, compressed_bytes }
                    if key.ends_with(&format!("{DAY0}-000000.jsonl.zst"))
                        && *raw_bytes > 0
                        && *compressed_bytes > 0
            ),
            "got {:?}",
            got[3]
        );
        assert_eq!(
            got[4],
            ProgressEvent::WindowDone {
                window: w,
                records: 7
            }
        );
    }

    #[tokio::test]
    async fn progress_reports_skipped_windows() {
        use crate::progress::{Progress, ProgressEvent};
        use std::sync::Mutex;

        let seen: Arc<Mutex<Vec<ProgressEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let tap = seen.clone();
        let mut o = opts(DAY0, DAY0 + HOUR_MS);
        o.progress = Progress::callback(move |ev| tap.lock().unwrap().push(ev));
        let (job, _sink, manifests) = job(MockCw::default(), o);
        manifests
            .put(
                &manifest_key("s4logs", ACCT, GROUP, DAY0, DAY0 + HOUR_MS),
                Bytes::from_static(b"{}"),
            )
            .await
            .unwrap();
        job.run().await.unwrap();
        let got = seen.lock().unwrap().clone();
        assert_eq!(
            got,
            vec![ProgressEvent::WindowSkipped {
                window: Window {
                    start_ms: DAY0,
                    end_ms: DAY0 + HOUR_MS
                }
            }]
        );
    }

    #[test]
    fn multi_report_aggregate_when_everything_failed() {
        let multi = MultiDrainReport {
            groups: vec![GroupDrainResult {
                log_group: "/g".into(),
                result: Err(DrainError::BadOptions("x".into())),
            }],
        };
        let agg = multi.aggregate();
        assert_eq!(agg.log_group, "0/1 log groups");
        assert!(!agg.dry_run);
        assert_eq!(agg.records, 0);
    }
}
