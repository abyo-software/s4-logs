//! Shared read path for `grep` / `restore` (DESIGN.md §9, §11.3): per-dt
//! prefix listing → S4LT frame pruning → S4IX byte spans → Range GETs →
//! bomb-capped decode → record filtering → **k-way time merge**.
//!
//! Output is strictly timestamp-ascending across chunks/objects, with a
//! stable tie-break on `(timestamp, stream, arrival order)`. Whole objects
//! are downloaded **only** when a sidecar is missing or corrupt (lock-in
//! story: data must stay readable without S4 Logs sidecars), and that
//! fallback is loudly warned about.
//!
//! # Memory model
//!
//! Each chunk overlapping the time range becomes one [`ChunkSource`] that
//! lazily fetches + decodes one *frame cluster* at a time (a cluster is the
//! smallest set of S4LT-overlapping frames that can be decoded independently
//! while keeping the per-chunk stream ascending — exactly one frame for
//! drain-written chunks, whose events arrive chronologically). The merge
//! holds one decoded cluster per open chunk, so peak memory is bounded by
//! `(chunks overlapping the range) × (frame cluster size ≈ frame_target
//! 4 MiB)`. The sidecar-less fallback decodes that whole object in one batch
//! (capped at [`FALLBACK_DECODE_CAP_BYTES`]) — bounded, but per-object large.

use std::collections::VecDeque;
use std::io::Read;

use anyhow::{Context, Result, bail};
use regex::Regex;
use s4logs_core::layout::{ChunkLocation, data_group_prefix, date_from_ts_ms};
use s4logs_core::read::{
    FrameSpan, ReadError, RecordLines, TimeRange, decompress_frames, frames_overlapping,
};
use s4logs_core::record::LogRecord;
use s4logs_core::sink::ChunkSink;
use s4logs_core::store::{ObjectStore, StoreError};

const DAY_MS: i64 = 86_400_000;

/// Output cap for the sidecar-less fallback decode. Drain objects rotate at
/// 256 MiB uncompressed by default; 2 GiB leaves generous headroom while
/// still refusing decompression bombs.
const FALLBACK_DECODE_CAP_BYTES: u64 = 2 << 30;

/// Ranges spanning more days than this fall back to one whole-group LIST
/// (date-filtered afterwards) instead of one LIST per dt= partition — a
/// year-wide `--from` must not issue hundreds of LIST calls, while a
/// few-day grep on a long-history group skips listing years of chunks.
const MAX_PER_DT_LISTS: usize = 32;

#[derive(Debug, Default, Clone, Copy)]
pub struct ScanStats {
    /// Chunk keys returned by the LIST calls (already dt-pruned in per-dt
    /// listing mode; whole group in the wide-range fallback).
    pub chunks_listed: u64,
    /// Chunks inside the dt= date range (sidecars consulted).
    pub chunks_scanned: u64,
    /// Frames fetched via sidecar-pruned Range GETs.
    pub frames_fetched: u64,
    /// Chunks read via full-object GET because a sidecar was missing/corrupt.
    pub fallback_full_objects: u64,
    /// JSONL lines that failed to parse (warned and skipped).
    pub parse_errors: u64,
    /// Records that matched the time range (and pattern, if any).
    pub records_emitted: u64,
}

impl ScanStats {
    fn absorb(&mut self, o: &ScanStats) {
        self.chunks_listed += o.chunks_listed;
        self.chunks_scanned += o.chunks_scanned;
        self.frames_fetched += o.frames_fetched;
        self.fallback_full_objects += o.fallback_full_objects;
        self.parse_errors += o.parse_errors;
        self.records_emitted += o.records_emitted;
    }
}

/// UTC `YYYY-MM-DD` partition labels touched by `[from_ms, to_ms_exclusive)`.
pub fn dates_in_range(from_ms: i64, to_ms_exclusive: i64) -> Vec<String> {
    if from_ms >= to_ms_exclusive {
        return Vec::new();
    }
    let first = from_ms.div_euclid(DAY_MS);
    let last = (to_ms_exclusive - 1).div_euclid(DAY_MS);
    (first..=last)
        .map(|day| date_from_ts_ms(day.saturating_mul(DAY_MS)))
        .collect()
}

/// Pure record filter: event time inside the half-open range, and (for grep)
/// the regex matches the message.
pub fn record_matches(rec: &LogRecord, range: &TimeRange, pattern: Option<&Regex>) -> bool {
    rec.timestamp >= range.from_ms
        && rec.timestamp < range.to_ms_exclusive
        && pattern.is_none_or(|re| re.is_match(&rec.message))
}

/// Decode a whole data object body (concatenated standard zstd frames)
/// without sidecar size hints — the lock-in-avoidance fallback. Output is
/// capped at [`FALLBACK_DECODE_CAP_BYTES`].
pub fn decode_full_object(body: &[u8]) -> Result<Vec<u8>> {
    let decoder =
        zstd::stream::read::Decoder::new(body).context("zstd decoder init (fallback decode)")?;
    let mut limited = decoder.take(FALLBACK_DECODE_CAP_BYTES + 1);
    let mut out = Vec::new();
    limited
        .read_to_end(&mut out)
        .context("zstd decode (fallback decode)")?;
    if out.len() as u64 > FALLBACK_DECODE_CAP_BYTES {
        bail!(
            "sidecar-less decode exceeded the {} byte cap (decompression bomb?)",
            FALLBACK_DECODE_CAP_BYTES
        );
    }
    Ok(out)
}

/// Stream decoded JSONL through the filter into `emit`. Unparseable lines
/// are warned and counted, not fatal — one corrupt line must not kill a
/// whole grep.
fn emit_records<F>(
    jsonl: &[u8],
    range: &TimeRange,
    pattern: Option<&Regex>,
    stats: &mut ScanStats,
    emit: &mut F,
) -> Result<()>
where
    F: FnMut(LogRecord) -> Result<()>,
{
    for item in RecordLines::new(jsonl) {
        match item {
            Ok(rec) => {
                if record_matches(&rec, range, pattern) {
                    stats.records_emitted += 1;
                    emit(rec)?;
                }
            }
            Err(err) => {
                stats.parse_errors += 1;
                tracing::warn!(error = %err, "skipping unparseable JSONL line");
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Frame clustering: per-chunk decode units that keep the stream ascending.
// ---------------------------------------------------------------------------

/// Group time-pruned frames of one chunk into decode units. Input is
/// `(span, min_ts, max_ts)` per frame (S4LT entries); output clusters are
/// sorted by time and **non-overlapping across clusters**, so decoding +
/// sorting one cluster at a time yields an ascending per-chunk stream.
///
/// Drain chunks (FilterLogEvents is chronological) have disjoint ascending
/// frames → every cluster is a single frame. Gateway chunks may interleave
/// arrival order → overlapping frames coalesce into one cluster (worst case
/// the whole chunk, bounded by the gateway flush size).
pub fn cluster_spans(mut spans: Vec<(FrameSpan, i64, i64)>) -> Vec<Vec<FrameSpan>> {
    spans.sort_by_key(|(s, min, _)| (*min, s.frame_idx));
    let mut out: Vec<(i64, Vec<FrameSpan>)> = Vec::new();
    for (span, min, max) in spans {
        match out.last_mut() {
            Some((cluster_max, cluster)) if min <= *cluster_max => {
                cluster.push(span);
                *cluster_max = (*cluster_max).max(max);
            }
            _ => out.push((max, vec![span])),
        }
    }
    out.into_iter().map(|(_, cluster)| cluster).collect()
}

// ---------------------------------------------------------------------------
// K-way merge over abstract batch sources (unit-testable without S3).
// ---------------------------------------------------------------------------

/// One lazily-loaded, time-ordered record source for [`KWayMerge`].
///
/// Contract: every batch is sorted ascending by `(timestamp, stream)`
/// (stable w.r.t. arrival order on full ties), and no record of a later
/// batch is older than any record of an earlier batch from the same source.
#[allow(async_fn_in_trait)] // bin-crate internal; merge is generic, never dyn
pub trait RecordBatchSource {
    async fn next_batch(&mut self) -> Result<Option<Vec<LogRecord>>>;
}

/// Heap entry: the head record of one source. Min-order on
/// `(timestamp, stream, source index)` — source index is chunk arrival
/// order, giving the stable `(timestamp, stream, arrival)` tie-break.
struct Head {
    rec: LogRecord,
    src: usize,
}

impl Head {
    fn key(&self) -> (i64, &str, usize) {
        (self.rec.timestamp, self.rec.stream.as_str(), self.src)
    }
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Head {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

/// Streaming k-way merge: at most one in-flight batch per source plus one
/// heap entry per source — memory is bounded by `sources × batch size`.
pub struct KWayMerge<S> {
    sources: Vec<S>,
    batches: Vec<VecDeque<LogRecord>>,
    heap: std::collections::BinaryHeap<std::cmp::Reverse<Head>>,
    primed: bool,
}

impl<S: RecordBatchSource> KWayMerge<S> {
    pub fn new(sources: Vec<S>) -> Self {
        let batches = sources.iter().map(|_| VecDeque::new()).collect();
        Self {
            sources,
            batches,
            heap: std::collections::BinaryHeap::new(),
            primed: false,
        }
    }

    pub fn sources(&self) -> &[S] {
        &self.sources
    }

    /// Push source `src`'s next record into the heap, pulling batches until
    /// one yields a record or the source is exhausted.
    async fn refill(&mut self, src: usize) -> Result<()> {
        loop {
            if let Some(rec) = self.batches[src].pop_front() {
                self.heap.push(std::cmp::Reverse(Head { rec, src }));
                return Ok(());
            }
            match self.sources[src].next_batch().await? {
                Some(batch) => self.batches[src] = batch.into(),
                None => return Ok(()),
            }
        }
    }

    /// Next record in global `(timestamp, stream, arrival)` order.
    pub async fn next(&mut self) -> Result<Option<LogRecord>> {
        if !self.primed {
            self.primed = true;
            for i in 0..self.sources.len() {
                self.refill(i).await?;
            }
        }
        let Some(std::cmp::Reverse(Head { rec, src })) = self.heap.pop() else {
            return Ok(None);
        };
        self.refill(src).await?;
        Ok(Some(rec))
    }
}

// ---------------------------------------------------------------------------
// S3-backed chunk source.
// ---------------------------------------------------------------------------

enum FetchUnit {
    /// One frame cluster: each span is fetched with its own Range GET.
    Frames(Vec<FrameSpan>),
    /// Sidecar-less fallback: GET + decode the whole object.
    WholeObject,
}

/// Lazily streams one chunk's matching records in ascending batches.
pub struct ChunkSource<'a> {
    store: &'a ObjectStore,
    data_key: String,
    range: TimeRange,
    pattern: Option<&'a Regex>,
    units: VecDeque<FetchUnit>,
    stats: ScanStats,
}

impl RecordBatchSource for ChunkSource<'_> {
    async fn next_batch(&mut self) -> Result<Option<Vec<LogRecord>>> {
        let Some(unit) = self.units.pop_front() else {
            return Ok(None);
        };
        let mut recs: Vec<LogRecord> = Vec::new();
        let mut collect = |jsonl: &[u8], stats: &mut ScanStats| {
            emit_records(jsonl, &self.range, self.pattern, stats, &mut |rec| {
                recs.push(rec);
                Ok(())
            })
        };
        match unit {
            FetchUnit::Frames(spans) => {
                for span in spans {
                    let want = span.byte_end_exclusive - span.byte_start;
                    let bytes = self
                        .store
                        .get_range(&self.data_key, span.byte_start, span.byte_end_exclusive)
                        .await
                        .with_context(|| {
                            format!(
                                "range GET {} bytes {}..{}",
                                self.data_key, span.byte_start, span.byte_end_exclusive
                            )
                        })?;
                    // Early diagnostics (wave-1A note): a short/long range
                    // response must fail before zstd sees the bytes.
                    if bytes.len() as u64 != want {
                        bail!(
                            "range GET for {} frame {} returned {} bytes, \
                             requested {want} (offsets {}..{}) — refusing to decode",
                            self.data_key,
                            span.frame_idx,
                            bytes.len(),
                            span.byte_start,
                            span.byte_end_exclusive
                        );
                    }
                    let jsonl =
                        decompress_frames(&bytes, span.original_size).with_context(|| {
                            format!("decoding frame {} of {}", span.frame_idx, self.data_key)
                        })?;
                    self.stats.frames_fetched += 1;
                    collect(&jsonl, &mut self.stats)?;
                }
            }
            FetchUnit::WholeObject => {
                let body = self
                    .store
                    .get_bytes(&self.data_key)
                    .await
                    .with_context(|| format!("full-object GET {}", self.data_key))?;
                let jsonl = decode_full_object(&body)
                    .with_context(|| format!("decoding whole object {}", self.data_key))?;
                self.stats.fallback_full_objects += 1;
                collect(&jsonl, &mut self.stats)?;
            }
        }
        // Stable sort: arrival order survives full (timestamp, stream) ties.
        recs.sort_by(|a, b| {
            (a.timestamp, a.stream.as_str()).cmp(&(b.timestamp, b.stream.as_str()))
        });
        Ok(Some(recs))
    }
}

// ---------------------------------------------------------------------------
// Scan assembly: listing + pruning + merge.
// ---------------------------------------------------------------------------

/// A merged, timestamp-ascending stream of one log group's matching records.
/// Pull with [`Self::next`]; [`Self::stats`] is final once `next` returns
/// `None`.
pub struct ScanStream<'a> {
    merge: KWayMerge<ChunkSource<'a>>,
    listed: u64,
    scanned: u64,
}

impl ScanStream<'_> {
    pub async fn next(&mut self) -> Result<Option<LogRecord>> {
        self.merge.next().await
    }

    pub fn stats(&self) -> ScanStats {
        let mut s = ScanStats {
            chunks_listed: self.listed,
            chunks_scanned: self.scanned,
            ..ScanStats::default()
        };
        for src in self.merge.sources() {
            s.absorb(&src.stats);
        }
        s
    }
}

/// List the chunks of `log_group` overlapping `range`'s dates: one LIST per
/// dt= partition for narrow ranges (cuts LIST cost on long-history groups),
/// one whole-group LIST date-filtered afterwards for wide ranges. Returns
/// `(locations sorted by (date, name), keys listed)`.
async fn list_range_chunks(
    store: &ObjectStore,
    account: &str,
    log_group: &str,
    range: &TimeRange,
) -> Result<(Vec<ChunkLocation>, u64)> {
    let dates = dates_in_range(range.from_ms, range.to_ms_exclusive);
    let prefix = store.key_prefix();
    let mut locs: Vec<ChunkLocation>;
    let listed: u64;
    if dates.len() <= MAX_PER_DT_LISTS {
        let group_prefix = data_group_prefix(prefix, account, log_group);
        locs = Vec::new();
        let mut count = 0u64;
        for date in &dates {
            let keys = store
                .list_keys(&format!("{group_prefix}dt={date}/"))
                .await
                .with_context(|| format!("listing chunks for {log_group:?} dt={date}"))?;
            count += keys.len() as u64;
            for key in &keys {
                match ChunkLocation::parse_data_key(prefix, key) {
                    Some(loc) => locs.push(loc),
                    None => {
                        tracing::warn!(key = %key, "skipping unparseable key under data prefix");
                    }
                }
            }
        }
        listed = count;
    } else {
        let dateset: std::collections::HashSet<&str> = dates.iter().map(String::as_str).collect();
        let chunks = store
            .list_chunks(account, log_group)
            .await
            .with_context(|| format!("listing chunks for log group {log_group:?}"))?;
        listed = chunks.len() as u64;
        locs = chunks
            .into_iter()
            .filter(|l| dateset.contains(l.date.as_str()))
            .collect();
    }
    locs.sort_by(|a, b| (&a.date, &a.name).cmp(&(&b.date, &b.name)));
    Ok((locs, listed))
}

/// Build the merged scan: list → sidecars → frame pruning/clustering →
/// [`KWayMerge`] over per-chunk lazy sources. Nothing beyond sidecars is
/// fetched until the stream is pulled.
pub async fn open_scan<'a>(
    store: &'a ObjectStore,
    account: &str,
    log_group: &str,
    range: TimeRange,
    pattern: Option<&'a Regex>,
) -> Result<ScanStream<'a>> {
    let (locs, listed) = list_range_chunks(store, account, log_group, &range).await?;
    let prefix = store.key_prefix().to_owned();
    let mut sources: Vec<ChunkSource<'a>> = Vec::new();
    let mut scanned = 0u64;

    for loc in &locs {
        scanned += 1;
        let data_key = loc.data_key(&prefix);

        // Sidecar load. Missing or undecodable sidecars degrade to a
        // full-object GET (data stays readable without S4 Logs tooling);
        // S3-level failures stay fatal.
        let sidecars = match store.load_indexes(loc).await {
            Ok(pair) => Some(pair),
            Err(
                err @ (StoreError::NotFound { .. } | StoreError::Index(_) | StoreError::TsIndex(_)),
            ) => {
                tracing::warn!(
                    key = %data_key,
                    error = %err,
                    "sidecar missing or corrupt; falling back to full-object GET"
                );
                None
            }
            Err(other) => {
                return Err(other).with_context(|| format!("loading sidecars for {data_key}"));
            }
        };

        // Frame pruning + clustering. An S4IX/S4LT entry-count mismatch is
        // also treated as sidecar corruption (typed error from core) →
        // fallback.
        let units: VecDeque<FetchUnit> = match &sidecars {
            Some((frame_index, ts_index)) => {
                match frames_overlapping(frame_index, ts_index, &range) {
                    Ok(spans) => {
                        let with_ts: Vec<(FrameSpan, i64, i64)> = spans
                            .into_iter()
                            .map(|s| {
                                let e = &ts_index.entries[s.frame_idx];
                                (s, e.min_ts, e.max_ts)
                            })
                            .collect();
                        cluster_spans(with_ts)
                            .into_iter()
                            .map(FetchUnit::Frames)
                            .collect()
                    }
                    Err(err @ ReadError::IndexMismatch { .. }) => {
                        tracing::warn!(
                            key = %data_key,
                            error = %err,
                            "sidecars out of sync; falling back to full-object GET"
                        );
                        VecDeque::from([FetchUnit::WholeObject])
                    }
                    Err(other) => {
                        return Err(other).with_context(|| format!("pruning frames of {data_key}"));
                    }
                }
            }
            None => VecDeque::from([FetchUnit::WholeObject]),
        };
        if units.is_empty() {
            continue; // no frame overlaps the time range
        }
        sources.push(ChunkSource {
            store,
            data_key,
            range,
            pattern,
            units,
            stats: ScanStats::default(),
        });
    }

    Ok(ScanStream {
        merge: KWayMerge::new(sources),
        listed,
        scanned,
    })
}

/// Scan every chunk of `log_group` overlapping `range`, calling `emit` for
/// each matching record in **global timestamp-ascending order** (ties:
/// stream, then arrival order — DESIGN.md §11.3).
pub async fn scan_log_group<F>(
    store: &ObjectStore,
    account: &str,
    log_group: &str,
    range: TimeRange,
    pattern: Option<&Regex>,
    mut emit: F,
) -> Result<ScanStats>
where
    F: FnMut(&LogRecord) -> Result<()>,
{
    let mut scan = open_scan(store, account, log_group, range, pattern).await?;
    while let Some(rec) = scan.next().await? {
        emit(&rec)?;
    }
    Ok(scan.stats())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use s4logs_core::chunk::{ChunkConfig, ChunkWriter};

    const DAY0: i64 = 1_717_545_600_000; // 2024-06-05T00:00:00Z

    fn rec(ts: i64, stream: &str, msg: &str) -> LogRecord {
        LogRecord {
            timestamp: ts,
            stream: stream.into(),
            message: msg.into(),
            ingestion_time: None,
            event_id: None,
        }
    }

    #[test]
    fn dates_single_instant_and_day_spans() {
        assert_eq!(dates_in_range(DAY0, DAY0 + 1), vec!["2024-06-05"]);
        assert_eq!(
            dates_in_range(DAY0 + 1000, DAY0 + 2 * DAY_MS),
            vec!["2024-06-05", "2024-06-06"]
        );
        // Exclusive end exactly on midnight does not pull in the next day.
        assert_eq!(dates_in_range(DAY0, DAY0 + DAY_MS), vec!["2024-06-05"]);
        assert!(dates_in_range(DAY0, DAY0).is_empty());
        assert!(dates_in_range(DAY0 + 5, DAY0).is_empty());
    }

    #[test]
    fn dates_pre_epoch_floor() {
        // div_euclid keeps pre-epoch timestamps on the correct calendar day.
        assert_eq!(dates_in_range(-1, 1), vec!["1969-12-31", "1970-01-01"]);
    }

    #[test]
    fn record_matches_range_and_pattern() {
        let range = TimeRange {
            from_ms: 100,
            to_ms_exclusive: 200,
        };
        let re = Regex::new("ERROR").unwrap();
        assert!(record_matches(
            &rec(100, "s", "ERROR boom"),
            &range,
            Some(&re)
        ));
        assert!(record_matches(&rec(199, "s", "x ERROR"), &range, Some(&re)));
        // Half-open: to is exclusive, from inclusive.
        assert!(!record_matches(&rec(200, "s", "ERROR"), &range, Some(&re)));
        assert!(!record_matches(&rec(99, "s", "ERROR"), &range, Some(&re)));
        // Pattern must hit the message.
        assert!(!record_matches(
            &rec(150, "ERROR", "fine"),
            &range,
            Some(&re)
        ));
        // No pattern = time filter only (restore path).
        assert!(record_matches(&rec(150, "s", "anything"), &range, None));
    }

    #[test]
    fn emit_records_filters_counts_and_tolerates_garbage() {
        let mut buf = Vec::new();
        rec(50, "s", "skip too early")
            .append_jsonl(&mut buf)
            .unwrap();
        rec(150, "s", "ERROR one").append_jsonl(&mut buf).unwrap();
        buf.extend_from_slice(b"not json at all\n");
        rec(160, "s", "no match").append_jsonl(&mut buf).unwrap();
        rec(170, "s", "ERROR two").append_jsonl(&mut buf).unwrap();

        let range = TimeRange {
            from_ms: 100,
            to_ms_exclusive: 200,
        };
        let re = Regex::new("ERROR").unwrap();
        let mut got = Vec::new();
        let mut stats = ScanStats::default();
        emit_records(&buf, &range, Some(&re), &mut stats, &mut |r: LogRecord| {
            got.push(r.message);
            Ok(())
        })
        .unwrap();
        assert_eq!(got, vec!["ERROR one", "ERROR two"]);
        assert_eq!(stats.records_emitted, 2);
        assert_eq!(stats.parse_errors, 1);
    }

    #[test]
    fn decode_full_object_roundtrips_multiframe_body() {
        let mut w = ChunkWriter::new(ChunkConfig {
            frame_target_bytes: 200, // force several frames
            zstd_level: 3,
        });
        let mut expect = Vec::new();
        for i in 0..50i64 {
            let r = rec(
                DAY0 + i,
                "s",
                &format!("padded line {i:04} {}", "z".repeat(30)),
            );
            r.append_jsonl(&mut expect).unwrap();
            w.push(&r).unwrap();
        }
        let chunk = w.finish().unwrap().unwrap();
        assert!(
            chunk.frame_index.entries.len() > 1,
            "want a multiframe body"
        );
        let out = decode_full_object(&chunk.body).unwrap();
        assert_eq!(out, expect);
    }

    #[test]
    fn decode_full_object_rejects_garbage() {
        assert!(decode_full_object(b"definitely not zstd").is_err());
    }

    // -- frame clustering ---------------------------------------------------

    fn span(frame_idx: usize) -> FrameSpan {
        FrameSpan {
            frame_idx,
            byte_start: frame_idx as u64 * 100,
            byte_end_exclusive: frame_idx as u64 * 100 + 100,
            original_size: 100,
        }
    }

    #[test]
    fn cluster_disjoint_frames_stay_singletons() {
        // Drain-shaped chunk: ascending, non-overlapping frames.
        let clusters = cluster_spans(vec![(span(0), 0, 9), (span(1), 10, 19), (span(2), 20, 29)]);
        assert_eq!(clusters.len(), 3);
        assert!(clusters.iter().all(|c| c.len() == 1));
        assert_eq!(clusters[0][0].frame_idx, 0);
        assert_eq!(clusters[2][0].frame_idx, 2);
    }

    #[test]
    fn cluster_overlapping_frames_coalesce() {
        // Gateway-shaped chunk: frame 1 overlaps 0, frame 2 is later.
        let clusters = cluster_spans(vec![(span(0), 0, 15), (span(1), 10, 19), (span(2), 30, 40)]);
        assert_eq!(clusters.len(), 2);
        assert_eq!(clusters[0].len(), 2);
        assert_eq!(clusters[1].len(), 1);
    }

    #[test]
    fn cluster_sorts_out_of_order_frames_and_chains_overlap() {
        // Frames written out of time order; 0..=2 chain through overlaps.
        let clusters = cluster_spans(vec![
            (span(0), 20, 29),
            (span(1), 0, 21),
            (span(2), 25, 40),
            (span(3), 100, 110),
        ]);
        assert_eq!(clusters.len(), 2);
        assert_eq!(clusters[0].len(), 3);
        assert_eq!(clusters[1][0].frame_idx, 3);
        assert!(cluster_spans(vec![]).is_empty());
    }

    // -- k-way merge (mock sources, no S3) -----------------------------------

    /// Scripted source: batches handed out in order, pre-sorted by caller.
    struct VecSource {
        batches: VecDeque<Vec<LogRecord>>,
    }

    impl VecSource {
        fn new(batches: Vec<Vec<LogRecord>>) -> Self {
            Self {
                batches: batches.into(),
            }
        }
    }

    impl RecordBatchSource for VecSource {
        async fn next_batch(&mut self) -> Result<Option<Vec<LogRecord>>> {
            Ok(self.batches.pop_front())
        }
    }

    fn drain_merge(sources: Vec<VecSource>) -> Vec<LogRecord> {
        futures::executor::block_on(async {
            let mut merge = KWayMerge::new(sources);
            let mut out = Vec::new();
            while let Some(r) = merge.next().await.unwrap() {
                out.push(r);
            }
            out
        })
    }

    #[test]
    fn merge_orders_across_interleaved_sources() {
        // Chunk A: [1, 4], [7]; chunk B: [2, 3], [8]; chunk C: [5, 6].
        let a = VecSource::new(vec![
            vec![rec(1, "s", "a1"), rec(4, "s", "a4")],
            vec![rec(7, "s", "a7")],
        ]);
        let b = VecSource::new(vec![
            vec![rec(2, "s", "b2"), rec(3, "s", "b3")],
            vec![rec(8, "s", "b8")],
        ]);
        let c = VecSource::new(vec![vec![rec(5, "s", "c5"), rec(6, "s", "c6")]]);
        let out = drain_merge(vec![a, b, c]);
        let ts: Vec<i64> = out.iter().map(|r| r.timestamp).collect();
        assert_eq!(ts, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn merge_tie_breaks_on_stream_then_source_order() {
        // Same timestamp everywhere: stream sorts first, then source index
        // (= chunk arrival order).
        let a = VecSource::new(vec![vec![
            rec(5, "z-stream", "a-z"),
            rec(5, "z-stream", "a-z2"),
        ]]);
        let b = VecSource::new(vec![vec![rec(5, "a-stream", "b-a")]]);
        let c = VecSource::new(vec![vec![rec(5, "z-stream", "c-z")]]);
        let out = drain_merge(vec![a, b, c]);
        let msgs: Vec<&str> = out.iter().map(|r| r.message.as_str()).collect();
        assert_eq!(msgs, vec!["b-a", "a-z", "a-z2", "c-z"]);
    }

    #[test]
    fn merge_handles_empty_batches_and_empty_sources() {
        let a = VecSource::new(vec![vec![], vec![rec(2, "s", "a")], vec![]]);
        let b = VecSource::new(vec![]);
        let c = VecSource::new(vec![vec![rec(1, "s", "c")]]);
        let out = drain_merge(vec![a, b, c]);
        let msgs: Vec<&str> = out.iter().map(|r| r.message.as_str()).collect();
        assert_eq!(msgs, vec!["c", "a"]);
        assert!(drain_merge(vec![]).is_empty());
    }

    proptest! {
        /// Any set of per-source sorted record streams (chopped into
        /// arbitrary batch sizes) merges into the globally sorted multiset
        /// of all records.
        #[test]
        fn merge_is_a_sorted_permutation(
            // Per source: (timestamp increments + stream ids, batch size).
            sources_spec in prop::collection::vec(
                (
                    prop::collection::vec((0i64..50, 0u8..3), 0..20),
                    1usize..5,
                ),
                0..5,
            ),
        ) {
            let mut all: Vec<(i64, String)> = Vec::new();
            let mut sources = Vec::new();
            for (recs_spec, batch_size) in sources_spec {
                let mut ts = 0i64;
                let mut recs: Vec<LogRecord> = recs_spec
                    .into_iter()
                    .map(|(inc, stream_id)| {
                        ts += inc;
                        let stream = format!("s{stream_id}");
                        all.push((ts, stream.clone()));
                        rec(ts, &stream, "m")
                    })
                    .collect();
                // Contract: the source as a whole streams in
                // (timestamp, stream) order, in batches of any size.
                recs.sort_by(|a, b| {
                    (a.timestamp, a.stream.as_str()).cmp(&(b.timestamp, b.stream.as_str()))
                });
                let batches: Vec<Vec<LogRecord>> =
                    recs.chunks(batch_size).map(<[LogRecord]>::to_vec).collect();
                sources.push(VecSource::new(batches));
            }
            let out = drain_merge(sources);
            // Sorted by (timestamp, stream)…
            for w in out.windows(2) {
                prop_assert!(
                    (w[0].timestamp, w[0].stream.as_str())
                        <= (w[1].timestamp, w[1].stream.as_str())
                );
            }
            // …and a permutation of the inputs.
            let mut got: Vec<(i64, String)> =
                out.into_iter().map(|r| (r.timestamp, r.stream)).collect();
            got.sort();
            all.sort();
            prop_assert_eq!(got, all);
        }
    }
}
