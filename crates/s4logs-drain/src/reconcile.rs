//! Reconcile mode — repair windows whose manifests were finalized before
//! late-arriving events became visible (wave 4K).
//!
//! # Why
//!
//! The 2026-06-10 real-AWS experiment measured a **3–5.5 minute** lag before
//! backdated events appear in `FilterLogEvents` (agents can deliver much
//! later). A window drained inside that lag gets a manifest, and manifests
//! are skipped on re-runs — so the stragglers never reach the archive. The
//! manual baseline is "delete the manifest and re-drain" (README
//! §Limitations); reconcile automates the repair without deleting anything.
//!
//! # What a reconcile run does
//!
//! [`ReconcileJob`] is a **superset run mode** over the same window grid as
//! [`DrainJob`]:
//!
//! - Window **without** a manifest → drained normally (identical to
//!   `DrainJob`, including the manifest write).
//! - Window **with** a manifest → its data objects are read back via
//!   [`ChunkReader`], every archived record's identity is collected, the
//!   window is re-paged from CloudWatch, and events whose identity is not
//!   in the archive are appended as new objects named
//!   `{window_start_ms}-r{attempt:02}{seq:04}` (sidecars via the normal
//!   `ChunkSink`). The manifest is then updated in place: `objects` +=
//!   appended, `record_count` += n, plus the optional `reconciled_at_ms` /
//!   `reconciled_added` fields. A clean window (nothing missing) is left
//!   **byte-untouched**.
//!
//! `DrainOptions.dry_run` makes reconcile report what *would* be appended
//! without writing anything (it still compresses, for honest byte counts —
//! the same discipline as drain dry-run).
//!
//! # Event identity
//!
//! Identity is the CloudWatch `event_id` when present (always present on
//! FilterLogEvents output, hence on drained data). Records without an
//! `event_id` — gateway-written data — fall back to a hash of
//! `(timestamp, stream, message)`; a CW event is also checked against the
//! fallback identity so drained-over-gateway data does not get duplicated.
//! **Reconcile primarily targets drained data**: for gateway data the
//! fallback cannot distinguish genuinely identical duplicate events (same
//! timestamp, stream and message but different CloudWatch ids), so such
//! duplicates are conservatively treated as already archived.
//!
//! # Memory bound
//!
//! Identities are stored as 128-bit hashes ([`record_identity`]) in a
//! `HashSet<u128>` — ~16 B of payload per archived event, so an extreme
//! 8M-event window costs ≈ 128 MB (plus hash-table slack, ≤ ~2×). Collision
//! risk at 8M events is ~n²/2¹²⁹ ≈ 1×10⁻²⁵ — astronomically below any
//! operational concern (a collision would make one late event be skipped).
//!
//! # Cost honesty
//!
//! Reconcile re-pages **full windows** through FilterLogEvents: a reconcile
//! pass over a range costs the same CloudWatch wall-clock time as draining
//! it (plus the S3 reads of the archived objects). It is a repair tool for
//! suspect ranges, not a cheap verification sweep.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use s4logs_core::layout::manifest_key;
use s4logs_core::read::RecordLines;
use s4logs_core::record::LogRecord;
use s4logs_core::sink::{ChunkSink, MemorySink};
use s4logs_core::store::{ObjectStore, StoreError};
use thiserror::Error;

use crate::cw::CwSource;
use crate::job::{DrainError, DrainJob, DrainOptions, ObjectNaming, WindowWriter, now_ms};
use crate::manifest::{Manifest, ManifestObject, ManifestStore};
use crate::progress::ProgressEvent;
use crate::shard::event_pages;
use crate::window::{Window, windows};

// ---------------------------------------------------------------------------
// ChunkReader — the small read surface reconcile needs (mirrors the
// ManifestStore pattern: trait + ObjectStore adapter + in-memory test impl)
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
#[error("chunk read failed for {key:?}: {message}")]
pub struct ChunkReadError {
    pub key: String,
    pub message: String,
}

/// Whole-object reads of archived data objects. `ObjectStore` is the S3
/// implementation; `MemorySink` doubles as the in-memory one so tests read
/// back exactly what the drain wrote.
#[async_trait]
pub trait ChunkReader: Send + Sync {
    /// Full body of the object at `key`; `Ok(None)` when it does not exist.
    async fn get_object(&self, key: &str) -> Result<Option<Bytes>, ChunkReadError>;
}

/// Thin [`ChunkReader`] adapter over the S3 [`ObjectStore`] (same pattern as
/// `ObjectStoreManifestStore`).
#[derive(Debug, Clone)]
pub struct ObjectStoreChunkReader {
    store: ObjectStore,
}

impl ObjectStoreChunkReader {
    pub fn new(store: ObjectStore) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ChunkReader for ObjectStoreChunkReader {
    async fn get_object(&self, key: &str) -> Result<Option<Bytes>, ChunkReadError> {
        match self.store.get_bytes(key).await {
            Ok(b) => Ok(Some(b)),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(ChunkReadError {
                key: key.to_owned(),
                message: e.to_string(),
            }),
        }
    }
}

/// In-memory impl: lets tests drain into a `MemorySink` and reconcile out of
/// the same store.
#[async_trait]
impl ChunkReader for MemorySink {
    async fn get_object(&self, key: &str) -> Result<Option<Bytes>, ChunkReadError> {
        Ok(self.get(key))
    }
}

// ---------------------------------------------------------------------------
// 128-bit event identity (two independent 64-bit FNV-1a-style lanes; no new
// dependency)
// ---------------------------------------------------------------------------

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
/// Second lane: different seed *and* different odd multiplier (XXH64's
/// PRIME64_2) so the lanes do not collide together.
const LANE2_OFFSET: u64 = FNV_OFFSET ^ 0x9e37_79b9_7f4a_7c15;
const LANE2_PRIME: u64 = 0xc2b2_ae3d_27d4_eb4f;

struct Hash128 {
    a: u64,
    b: u64,
}

impl Hash128 {
    fn new() -> Self {
        Self {
            a: FNV_OFFSET,
            b: LANE2_OFFSET,
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        for &x in bytes {
            self.a = (self.a ^ u64::from(x)).wrapping_mul(FNV_PRIME);
            self.b = (self.b ^ u64::from(x)).wrapping_mul(LANE2_PRIME);
        }
    }

    fn finish(self) -> u128 {
        (u128::from(self.a) << 64) | u128::from(self.b)
    }
}

/// Identity of an event that has a CloudWatch `event_id`.
fn id_identity(event_id: &str) -> u128 {
    let mut h = Hash128::new();
    h.write(&[1u8]); // domain tag: id-based
    h.write(event_id.as_bytes());
    h.finish()
}

/// Identity fallback for records without an `event_id` (gateway-written
/// data). Length-prefixed stream so `("ab", "c")` ≠ `("a", "bc")`.
fn fallback_identity(timestamp: i64, stream: &str, message: &str) -> u128 {
    let mut h = Hash128::new();
    h.write(&[0u8]); // domain tag: content-based
    h.write(&timestamp.to_le_bytes());
    h.write(&(stream.len() as u32).to_le_bytes());
    h.write(stream.as_bytes());
    h.write(message.as_bytes());
    h.finish()
}

/// Identity of one record/event: `event_id` when present, content fallback
/// otherwise. ~16 B per event in the dedup set (see module docs).
pub(crate) fn record_identity(
    event_id: Option<&str>,
    timestamp: i64,
    stream: &str,
    message: &str,
) -> u128 {
    match event_id {
        Some(id) => id_identity(id),
        None => fallback_identity(timestamp, stream, message),
    }
}

// ---------------------------------------------------------------------------
// Errors / report
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReconcileError {
    #[error(transparent)]
    Drain(#[from] DrainError),
    #[error(transparent)]
    Read(#[from] ChunkReadError),
    #[error("manifest references missing data object {key:?} — archive integrity violation")]
    MissingObject { key: String },
    #[error("archived object {key:?} failed to decode: {message}")]
    Corrupt { key: String, message: String },
    #[error(
        "window starting {window_start_ms} already has {max} reconcile attempts (limit 99); \
         delete the manifest and re-drain instead"
    )]
    TooManyAttempts { window_start_ms: i64, max: u32 },
}

/// What happened to one window during a reconcile run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowAction {
    /// No manifest existed — drained normally (superset behavior).
    Drained,
    /// Manifest existed — archive read back, window re-paged, gaps appended.
    Reconciled,
}

/// Per-window reconcile detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowReconcileOutcome {
    pub window: Window,
    pub action: WindowAction,
    /// In-window events CloudWatch returned (for `Drained`: records
    /// archived).
    pub cw_events: u64,
    /// Events already present in the archive (always 0 for `Drained`).
    pub already_archived: u64,
    /// Events appended (or that would be, in dry-run). For `Drained` this
    /// equals `cw_events`.
    pub appended: u64,
    /// Data objects PUT for this window (0 in dry-run).
    pub objects_written: u64,
}

/// Summary of a reconcile run. `windows` is sorted by window start.
#[derive(Debug, Clone, Default)]
pub struct ReconcileReport {
    pub log_group: String,
    pub dry_run: bool,
    pub windows_total: u64,
    /// Windows that had no manifest and were drained normally.
    pub windows_drained: u64,
    /// Windows that had a manifest and were re-paged.
    pub windows_reconciled: u64,
    /// Reconciled windows where at least one late event was found.
    pub windows_repaired: u64,
    pub cw_events: u64,
    pub already_archived: u64,
    pub appended: u64,
    pub objects_written: u64,
    pub windows: Vec<WindowReconcileOutcome>,
}

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

/// Reconcile run over one log group (see module docs). Reuses
/// [`DrainOptions`] wholesale — `dry_run`, `concurrency`, `shard_streams`,
/// `progress` and the chunk knobs all apply.
pub struct ReconcileJob {
    drain: DrainJob,
    reader: Arc<dyn ChunkReader>,
}

impl ReconcileJob {
    pub fn new(
        cw: Arc<dyn CwSource>,
        sink: Arc<dyn ChunkSink>,
        manifests: Arc<dyn ManifestStore>,
        reader: Arc<dyn ChunkReader>,
        opts: DrainOptions,
    ) -> Self {
        Self {
            drain: DrainJob::new(cw, sink, manifests, opts),
            reader,
        }
    }

    pub fn options(&self) -> &DrainOptions {
        self.drain.options()
    }

    /// Run the reconcile. Fails fast on the first window error; windows
    /// already repaired keep their updated manifests, so a re-run resumes
    /// (repaired windows then reconcile clean).
    pub async fn run(&self) -> Result<ReconcileReport, ReconcileError> {
        let opts = self.drain.options();
        let (from, to) = self.drain.resolve_range().await?;
        let shards = self.drain.compute_shards().await?;
        let wins = windows(from, to, opts.window_ms).map_err(DrainError::from)?;
        let mut report = ReconcileReport {
            log_group: opts.log_group.clone(),
            dry_run: opts.dry_run,
            windows_total: wins.len() as u64,
            ..ReconcileReport::default()
        };
        tracing::info!(
            log_group = %opts.log_group,
            from_ms = from,
            to_ms = to,
            windows = wins.len(),
            dry_run = opts.dry_run,
            "reconcile starting"
        );
        let mut stream = futures::stream::iter(
            wins.into_iter()
                .map(|w| self.process_window(w, shards.as_deref())),
        )
        .buffer_unordered(opts.concurrency.max(1));
        let mut outcomes = Vec::new();
        while let Some(res) = stream.next().await {
            outcomes.push(res?);
        }
        drop(stream);
        outcomes.sort_by_key(|o| o.window.start_ms);
        for o in &outcomes {
            match o.action {
                WindowAction::Drained => report.windows_drained += 1,
                WindowAction::Reconciled => {
                    report.windows_reconciled += 1;
                    if o.appended > 0 {
                        report.windows_repaired += 1;
                    }
                }
            }
            report.cw_events += o.cw_events;
            report.already_archived += o.already_archived;
            report.appended += o.appended;
            report.objects_written += o.objects_written;
        }
        report.windows = outcomes;
        Ok(report)
    }

    async fn process_window(
        &self,
        w: Window,
        shards: Option<&[Vec<String>]>,
    ) -> Result<WindowReconcileOutcome, ReconcileError> {
        let opts = self.drain.options();
        let mkey = manifest_key(
            self.drain.sink.key_prefix(),
            &opts.account,
            &opts.log_group,
            w.start_ms,
            w.end_ms,
        );
        let existing = self
            .drain
            .manifests
            .get(&mkey)
            .await
            .map_err(DrainError::from)?;
        let Some(bytes) = existing else {
            // Superset mode: never-drained windows get a normal drain.
            let out = self.drain.process_window(w, shards).await?;
            return Ok(WindowReconcileOutcome {
                window: w,
                action: WindowAction::Drained,
                cw_events: out.records,
                already_archived: 0,
                appended: out.records,
                objects_written: out.objects_written,
            });
        };
        let manifest = Manifest::from_json_bytes(&bytes).map_err(DrainError::from)?;
        self.reconcile_window(w, manifest, &mkey, shards).await
    }

    async fn reconcile_window(
        &self,
        w: Window,
        manifest: Manifest,
        mkey: &str,
        shards: Option<&[Vec<String>]>,
    ) -> Result<WindowReconcileOutcome, ReconcileError> {
        let opts = self.drain.options();
        opts.progress
            .emit(|| ProgressEvent::WindowStarted { window: w });

        // 1. Identity set of everything already archived for this window.
        let mut seen: HashSet<u128> = HashSet::new();
        for obj in &manifest.objects {
            let body = self
                .reader
                .get_object(&obj.data_key)
                .await?
                .ok_or_else(|| ReconcileError::MissingObject {
                    key: obj.data_key.clone(),
                })?;
            let raw = decode_object(obj, &body)?;
            for rec in RecordLines::new(&raw) {
                let rec = rec.map_err(|e| ReconcileError::Corrupt {
                    key: obj.data_key.clone(),
                    message: e.to_string(),
                })?;
                seen.insert(record_identity(
                    rec.event_id.as_deref(),
                    rec.timestamp,
                    &rec.stream,
                    &rec.message,
                ));
            }
        }

        let attempt = next_attempt(&manifest);
        if attempt > 99 {
            return Err(ReconcileError::TooManyAttempts {
                window_start_ms: w.start_ms,
                max: 99,
            });
        }

        // 2. Re-page CloudWatch; append whatever the archive is missing.
        let mut ww = WindowWriter::new(
            &*self.drain.sink,
            opts,
            w,
            ObjectNaming::Reconcile { attempt },
        );
        let mut cw_events = 0u64;
        let mut already = 0u64;
        let mut appended = 0u64;
        {
            let mut pages = event_pages(&*self.drain.cw, &opts.log_group, w, shards);
            while let Some(page) = pages.next().await {
                let events = page.map_err(DrainError::from)?;
                let page_len = events.len() as u64;
                for ev in events {
                    if ev.timestamp < w.start_ms || ev.timestamp >= w.end_ms {
                        // Same defensive drop as drain — an out-of-window
                        // event would corrupt the dt= partition.
                        continue;
                    }
                    cw_events += 1;
                    let id = record_identity(
                        ev.event_id.as_deref(),
                        ev.timestamp,
                        &ev.log_stream_name,
                        &ev.message,
                    );
                    // Second chance for archives written without event_id
                    // (gateway data): match on content identity.
                    if seen.contains(&id)
                        || (ev.event_id.is_some()
                            && seen.contains(&fallback_identity(
                                ev.timestamp,
                                &ev.log_stream_name,
                                &ev.message,
                            )))
                    {
                        already += 1;
                        continue;
                    }
                    seen.insert(id);
                    appended += 1;
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

        // 3. Manifest update — only when something was actually appended
        // (clean windows stay byte-identical; dry-run writes nothing).
        if appended > 0 && !opts.dry_run {
            let mut m = manifest;
            m.record_count += appended;
            m.reconciled_added = Some(m.reconciled_added.unwrap_or(0) + appended);
            m.reconciled_at_ms = Some(now_ms());
            m.objects.append(&mut ww.objects);
            self.drain
                .manifests
                .put(mkey, m.to_json_bytes().map_err(DrainError::from)?)
                .await
                .map_err(DrainError::from)?;
            tracing::info!(
                log_group = %opts.log_group,
                window_start_ms = w.start_ms,
                appended,
                attempt,
                "reconcile appended late events"
            );
        }
        opts.progress.emit(|| ProgressEvent::ReconcileWindowDone {
            window: w,
            cw_events,
            already_archived: already,
            appended,
        });
        Ok(WindowReconcileOutcome {
            window: w,
            action: WindowAction::Reconciled,
            cw_events,
            already_archived: already,
            appended,
            objects_written: ww.objects_written,
        })
    }
}

/// Decompress one archived data object. Objects with a recorded `raw_bytes`
/// go through the size-checked, bomb-capped core decoder; legacy manifest
/// entries (pre-wave-3G, no `raw_bytes`) fall back to plain zstd decoding.
fn decode_object(obj: &ManifestObject, body: &[u8]) -> Result<Vec<u8>, ReconcileError> {
    let corrupt = |e: &dyn std::fmt::Display| ReconcileError::Corrupt {
        key: obj.data_key.clone(),
        message: e.to_string(),
    };
    match obj.raw_bytes {
        Some(raw) => s4logs_core::read::decompress_frames(body, raw).map_err(|e| corrupt(&e)),
        // Legacy manifests (pre-wave-3G) carry no `raw_bytes`. A plain
        // `decode_all` would materialize an unbounded plaintext — a forged or
        // pathological object could OOM the reconcile before it can repair or
        // fail cleanly. Cap the decode at the same per-object ceiling the core
        // bomb guard uses.
        None => {
            use std::io::Read;
            let cap = s4logs_core::read::MAX_DECOMPRESSED_BYTES;
            let mut out = Vec::new();
            let decoder = zstd::stream::read::Decoder::new(body).map_err(|e| corrupt(&e))?;
            let read = decoder
                .take(cap + 1)
                .read_to_end(&mut out)
                .map_err(|e| corrupt(&e))?;
            if read as u64 > cap {
                return Err(ReconcileError::Corrupt {
                    key: obj.data_key.clone(),
                    message: format!("legacy object exceeds {cap}-byte decode cap"),
                });
            }
            Ok(out)
        }
    }
}

/// Next free reconcile attempt for a window: 1 + the highest
/// `-r{attempt:02}{seq:04}` suffix among the manifest's object names (base
/// `{seq:06}` names never match — they have no `r`).
fn next_attempt(m: &Manifest) -> u32 {
    let mut max_attempt = 0u32;
    for obj in &m.objects {
        let Some(base) = obj.data_key.rsplit('/').next() else {
            continue;
        };
        let Some(name) = base.strip_suffix(s4logs_core::layout::DATA_SUFFIX) else {
            continue;
        };
        let Some((_, suffix)) = name.rsplit_once('-') else {
            continue;
        };
        let Some(digits) = suffix.strip_prefix('r') else {
            continue;
        };
        if digits.len() != 6 {
            continue;
        }
        if let Ok(a) = digits[..2].parse::<u32>() {
            max_attempt = max_attempt.max(a);
        }
    }
    max_attempt + 1
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::cw::CwEvent;
    use crate::manifest::{DRAIN_VERSION, MANIFEST_VERSION, MemoryManifestStore};
    use crate::testutil::{MockCw, event, event_in_stream};
    use crate::window::HOUR_MS;
    use s4logs_core::chunk::{ChunkConfig, ChunkWriter};
    use s4logs_core::layout::ChunkLocation;

    const DAY0: i64 = 1_717_545_600_000; // 2024-06-05T00:00:00Z
    const ACCT: &str = "123456789012";
    const GROUP: &str = "/aws/lambda/foo";

    fn opts() -> DrainOptions {
        DrainOptions {
            from_ms: Some(DAY0),
            to_ms: Some(DAY0 + HOUR_MS),
            ..DrainOptions::new(ACCT, GROUP)
        }
    }

    struct Fixture {
        sink: Arc<MemorySink>,
        manifests: Arc<MemoryManifestStore>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                sink: Arc::new(MemorySink::new("s4logs")),
                manifests: Arc::new(MemoryManifestStore::new()),
            }
        }

        async fn drain(&self, events: Vec<CwEvent>, o: DrainOptions) {
            let cw = MockCw {
                events,
                ..MockCw::default()
            };
            DrainJob::new(Arc::new(cw), self.sink.clone(), self.manifests.clone(), o)
                .run()
                .await
                .unwrap();
        }

        fn reconcile_job(&self, events: Vec<CwEvent>, o: DrainOptions) -> ReconcileJob {
            let cw = MockCw {
                events,
                ..MockCw::default()
            };
            ReconcileJob::new(
                Arc::new(cw),
                self.sink.clone(),
                self.manifests.clone(),
                self.sink.clone(),
                o,
            )
        }

        async fn manifest(&self, w: Window) -> Manifest {
            let mkey = manifest_key("s4logs", ACCT, GROUP, w.start_ms, w.end_ms);
            Manifest::from_json_bytes(&self.manifests.get(&mkey).await.unwrap().unwrap()).unwrap()
        }
    }

    fn base_events() -> Vec<CwEvent> {
        (0..3)
            .map(|i| event(DAY0 + i * 1000, &format!("original {i}")))
            .collect()
    }

    fn with_late(mut evs: Vec<CwEvent>, n: i64) -> Vec<CwEvent> {
        for i in 0..n {
            evs.push(event(DAY0 + 500_000 + i * 1000, &format!("late {i}")));
        }
        evs
    }

    const W0: Window = Window {
        start_ms: DAY0,
        end_ms: DAY0 + HOUR_MS,
    };

    #[tokio::test]
    async fn late_events_are_appended_and_manifest_updated() {
        let fx = Fixture::new();
        fx.drain(base_events(), opts()).await;
        let before = fx.manifest(W0).await;
        assert_eq!(before.record_count, 3);
        assert_eq!(before.objects.len(), 1);

        // Two late events became visible after the drain.
        let job = fx.reconcile_job(with_late(base_events(), 2), opts());
        let report = job.run().await.unwrap();
        assert_eq!(report.windows_total, 1);
        assert_eq!(report.windows_reconciled, 1);
        assert_eq!(report.windows_repaired, 1);
        assert_eq!(report.windows_drained, 0);
        assert_eq!(report.cw_events, 5);
        assert_eq!(report.already_archived, 3);
        assert_eq!(report.appended, 2);
        assert_eq!(report.objects_written, 1);

        let after = fx.manifest(W0).await;
        assert_eq!(after.record_count, 5);
        assert_eq!(after.objects.len(), 2);
        assert_eq!(after.reconciled_added, Some(2));
        assert!(after.reconciled_at_ms.is_some());
        // Appended object uses the collision-free reconcile naming scheme.
        let appended_key = &after.objects[1].data_key;
        assert!(
            appended_key.contains(&format!("/{DAY0}-r010000.jsonl.zst")),
            "unexpected appended key: {appended_key}"
        );
        // Data + both sidecars exist; the appended object holds the 2 late
        // events and nothing else.
        assert!(fx.sink.get(appended_key).is_some());
        let raw = zstd::stream::decode_all(&fx.sink.get(appended_key).unwrap()[..]).unwrap();
        let recs: Vec<LogRecord> = RecordLines::new(&raw).collect::<Result<_, _>>().unwrap();
        let mut msgs: Vec<&str> = recs.iter().map(|r| r.message.as_str()).collect();
        msgs.sort_unstable();
        assert_eq!(msgs, vec!["late 0", "late 1"]);
    }

    #[tokio::test]
    async fn clean_window_leaves_manifest_byte_identical() {
        let fx = Fixture::new();
        fx.drain(base_events(), opts()).await;
        let mkey = manifest_key("s4logs", ACCT, GROUP, W0.start_ms, W0.end_ms);
        let before = fx.manifests.get(&mkey).await.unwrap().unwrap();
        let keys_before = fx.sink.keys();

        let report = fx.reconcile_job(base_events(), opts()).run().await.unwrap();
        assert_eq!(report.windows_reconciled, 1);
        assert_eq!(report.windows_repaired, 0);
        assert_eq!(report.appended, 0);
        assert_eq!(report.already_archived, 3);
        assert_eq!(
            fx.manifests.get(&mkey).await.unwrap().unwrap(),
            before,
            "clean reconcile must not rewrite the manifest"
        );
        assert_eq!(fx.sink.keys(), keys_before, "clean reconcile wrote objects");
    }

    #[tokio::test]
    async fn dry_run_counts_but_writes_nothing() {
        let fx = Fixture::new();
        fx.drain(base_events(), opts()).await;
        let mkey = manifest_key("s4logs", ACCT, GROUP, W0.start_ms, W0.end_ms);
        let before = fx.manifests.get(&mkey).await.unwrap().unwrap();
        let keys_before = fx.sink.keys();

        let mut o = opts();
        o.dry_run = true;
        let report = fx
            .reconcile_job(with_late(base_events(), 2), o)
            .run()
            .await
            .unwrap();
        assert!(report.dry_run);
        assert_eq!(report.appended, 2, "dry-run must still count the gap");
        assert_eq!(report.objects_written, 0);
        assert_eq!(fx.manifests.get(&mkey).await.unwrap().unwrap(), before);
        assert_eq!(fx.sink.keys(), keys_before);
    }

    #[tokio::test]
    async fn window_without_manifest_is_drained_normally() {
        let fx = Fixture::new();
        // Drain only window 0; reconcile over [0, 2h) must repair nothing in
        // window 0 and *drain* window 1.
        fx.drain(base_events(), opts()).await;
        let mut o = opts();
        o.to_ms = Some(DAY0 + 2 * HOUR_MS);
        let mut events = base_events();
        events.push(event(DAY0 + HOUR_MS + 1000, "second window event"));
        let report = fx.reconcile_job(events, o).run().await.unwrap();
        assert_eq!(report.windows_total, 2);
        assert_eq!(report.windows_reconciled, 1);
        assert_eq!(report.windows_drained, 1);
        assert_eq!(report.appended, 1);
        // The drained window now has a manifest with base-named objects.
        let w1 = Window {
            start_ms: DAY0 + HOUR_MS,
            end_ms: DAY0 + 2 * HOUR_MS,
        };
        let m1 = fx.manifest(w1).await;
        assert_eq!(m1.record_count, 1);
        assert!(
            m1.objects[0]
                .data_key
                .contains(&format!("/{}-000000.jsonl.zst", w1.start_ms))
        );
        assert_eq!(m1.reconciled_at_ms, None, "fresh drain is not a reconcile");
    }

    #[tokio::test]
    async fn repeated_reconcile_uses_next_attempt_and_accumulates() {
        let fx = Fixture::new();
        fx.drain(base_events(), opts()).await;
        fx.reconcile_job(with_late(base_events(), 2), opts())
            .run()
            .await
            .unwrap();
        // One more straggler shows up even later.
        let report = fx
            .reconcile_job(with_late(base_events(), 3), opts())
            .run()
            .await
            .unwrap();
        assert_eq!(report.already_archived, 5);
        assert_eq!(report.appended, 1);
        let m = fx.manifest(W0).await;
        assert_eq!(m.record_count, 6);
        assert_eq!(m.reconciled_added, Some(3), "2 + 1 accumulates");
        assert!(
            m.objects[2]
                .data_key
                .contains(&format!("/{DAY0}-r020000.jsonl.zst")),
            "second repair must use attempt 02: {}",
            m.objects[2].data_key
        );
        // Idempotent third pass: clean.
        let third = fx
            .reconcile_job(with_late(base_events(), 3), opts())
            .run()
            .await
            .unwrap();
        assert_eq!(third.appended, 0);
        assert_eq!(third.windows_repaired, 0);
    }

    #[tokio::test]
    async fn gateway_records_without_event_id_match_by_content_fallback() {
        // Hand-write an archive object whose records have no event_id (as
        // the Mode B gateway produces), with a manifest, then reconcile
        // against CW events that *do* carry event ids for the same
        // (timestamp, stream, message): nothing must be appended.
        let fx = Fixture::new();
        let mut w = ChunkWriter::new(ChunkConfig::default());
        for i in 0..3i64 {
            w.push(&LogRecord {
                timestamp: DAY0 + i * 1000,
                stream: "app/i-0abc".into(),
                message: format!("original {i}"),
                ingestion_time: None,
                event_id: None, // gateway data
            })
            .unwrap();
        }
        let chunk = w.finish().unwrap().unwrap();
        let loc = ChunkLocation {
            account: ACCT.into(),
            log_group: GROUP.into(),
            date: s4logs_core::layout::date_from_ts_ms(DAY0),
            name: format!("{DAY0}-000000"),
        };
        let receipt = fx.sink.put_chunk(&loc, &chunk).await.unwrap();
        let manifest = Manifest {
            version: MANIFEST_VERSION,
            account: ACCT.into(),
            log_group: GROUP.into(),
            window_start_ms: W0.start_ms,
            window_end_ms: W0.end_ms,
            record_count: 3,
            objects: vec![ManifestObject {
                data_key: receipt.data_key,
                etag: receipt.etag,
                crc32c: receipt.crc32c,
                body_len: receipt.body_len,
                raw_bytes: None, // legacy entry: exercises the decode_all path
                storage_class: None,
                record_count: 3,
                min_ts: DAY0,
                max_ts: DAY0 + 2000,
            }],
            completed_at_ms: 1,
            drain_version: DRAIN_VERSION.into(),
            reconciled_at_ms: None,
            reconciled_added: None,
        };
        let mkey = manifest_key("s4logs", ACCT, GROUP, W0.start_ms, W0.end_ms);
        fx.manifests
            .put(&mkey, manifest.to_json_bytes().unwrap())
            .await
            .unwrap();

        // CW events: the same 3 (with event ids) + 1 genuinely new.
        let report = fx
            .reconcile_job(with_late(base_events(), 1), opts())
            .run()
            .await
            .unwrap();
        assert_eq!(report.already_archived, 3, "content fallback must match");
        assert_eq!(report.appended, 1);
    }

    #[tokio::test]
    async fn missing_archived_object_is_a_typed_error() {
        let fx = Fixture::new();
        fx.drain(base_events(), opts()).await;
        // Corrupt the archive: rebuild a sink without the data object but
        // keep the manifest. Reads come from an empty MemorySink.
        let empty = Arc::new(MemorySink::new("s4logs"));
        let cw = MockCw {
            events: base_events(),
            ..MockCw::default()
        };
        let job = ReconcileJob::new(
            Arc::new(cw),
            fx.sink.clone(),
            fx.manifests.clone(),
            empty,
            opts(),
        );
        let err = job.run().await.unwrap_err();
        assert!(
            matches!(err, ReconcileError::MissingObject { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_pages_with_stream_shards() {
        let fx = Fixture::new();
        let mut events = Vec::new();
        for s in 0..5 {
            for i in 0..4i64 {
                events.push(event_in_stream(
                    DAY0 + (s as i64) * 10_000 + i * 1000,
                    &format!("stream-{s}"),
                    &format!("msg s{s} i{i}"),
                ));
            }
        }
        fx.drain(events.clone(), opts()).await;
        // One late event in one stream; reconcile with sharding enabled.
        events.push(event_in_stream(DAY0 + 700_000, "stream-3", "late one"));
        let mut o = opts();
        o.shard_streams = 3;
        let report = fx.reconcile_job(events, o).run().await.unwrap();
        assert_eq!(report.cw_events, 21);
        assert_eq!(report.already_archived, 20);
        assert_eq!(report.appended, 1);
        let m = fx.manifest(W0).await;
        assert_eq!(m.record_count, 21);
    }

    // -- identity hashing ----------------------------------------------------

    #[test]
    fn identity_is_deterministic_and_domain_separated() {
        let id = record_identity(Some("evt-1"), 1, "s", "m");
        assert_eq!(
            id,
            record_identity(Some("evt-1"), 999, "x", "y"),
            "id-based identity must ignore content"
        );
        assert_ne!(id, record_identity(Some("evt-2"), 1, "s", "m"));
        // An event_id equal to some message must not collide with the
        // content fallback (domain tags differ).
        let by_id = record_identity(Some("x"), 0, "", "");
        let by_content = record_identity(None, 0, "", "x");
        assert_ne!(by_id, by_content);
    }

    #[test]
    fn fallback_identity_is_length_prefixed() {
        // ("ab","c") vs ("a","bc") — same concatenation, different split.
        assert_ne!(
            record_identity(None, 7, "ab", "c"),
            record_identity(None, 7, "a", "bc")
        );
        assert_ne!(
            record_identity(None, 7, "s", "m"),
            record_identity(None, 8, "s", "m")
        );
        assert_eq!(
            record_identity(None, 7, "s", "m"),
            record_identity(None, 7, "s", "m")
        );
    }

    #[test]
    fn next_attempt_scans_reconcile_names_only() {
        let mut m = Manifest {
            version: 1,
            account: "a".into(),
            log_group: "/g".into(),
            window_start_ms: 0,
            window_end_ms: 1,
            objects: vec![],
            record_count: 0,
            completed_at_ms: 0,
            drain_version: "t".into(),
            reconciled_at_ms: None,
            reconciled_added: None,
        };
        let obj = |key: &str| ManifestObject {
            data_key: key.to_owned(),
            etag: None,
            crc32c: 0,
            body_len: 0,
            raw_bytes: None,
            storage_class: None,
            record_count: 0,
            min_ts: 0,
            max_ts: 0,
        };
        assert_eq!(next_attempt(&m), 1, "empty manifest starts at attempt 1");
        m.objects
            .push(obj("p/data/dt=2024-06-05/100-000000.jsonl.zst"));
        m.objects
            .push(obj("p/data/dt=2024-06-05/100-000001.jsonl.zst"));
        assert_eq!(next_attempt(&m), 1, "base names never count");
        m.objects
            .push(obj("p/data/dt=2024-06-05/100-r010000.jsonl.zst"));
        m.objects
            .push(obj("p/data/dt=2024-06-05/100-r010001.jsonl.zst"));
        assert_eq!(next_attempt(&m), 2);
        m.objects
            .push(obj("p/data/dt=2024-06-05/100-r070003.jsonl.zst"));
        assert_eq!(next_attempt(&m), 8);
    }
}
