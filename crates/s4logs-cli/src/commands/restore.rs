//! `s4logs restore` — DESIGN.md §9. Raw JSONL to stdout/file, or CloudWatch
//! re-ingest via PutLogEvents into stream [`RESTORE_STREAM`].
//!
//! Re-ingest default wraps each message as
//! `{"original_timestamp":..,"original_stream":..,"message":..}` with event
//! timestamp = now (PutLogEvents rejects events older than 14 days).
//! `--raw` sends original timestamps unmodified — CloudWatch WILL reject
//! anything older than 14 days; rejects are reported per batch, not retried.

use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use aws_sdk_cloudwatchlogs::error::{DisplayErrorContext, ProvideErrorMetadata};
use s4logs_core::read::TimeRange;
use s4logs_core::record::LogRecord;
use s4logs_core::store::ObjectStore;

use crate::aws;
use crate::cli::{GlobalArgs, RestoreArgs, UsageError};
use crate::scan::{ScanStats, open_scan, scan_log_group};
use crate::timearg::fmt_ts;

/// All re-ingested events land in this single stream (documented contract).
pub const RESTORE_STREAM: &str = "s4logs-restore";

/// PutLogEvents batch limits (DESIGN.md §9 / AWS API reference).
pub const MAX_BATCH_EVENTS: usize = 10_000;
pub const MAX_BATCH_BYTES: usize = 1_048_576;
pub const EVENT_OVERHEAD_BYTES: usize = 26;
/// A batch may not span more than 24 hours (AWS constraint; only relevant
/// for `--raw` where original timestamps survive).
pub const MAX_BATCH_SPAN_MS: i64 = 24 * 3_600_000;

const PUT_MAX_ATTEMPTS: u32 = 5;
const PUT_BACKOFF_BASE_MS: u64 = 500;
const PUT_BACKOFF_CAP_MS: u64 = 8_000;
const CW_INGEST_MAX_AGE_MS: i64 = 14 * 86_400_000;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub async fn run(global: &GlobalArgs, args: &RestoreArgs) -> Result<()> {
    if args.from >= args.to {
        return Err(UsageError(format!(
            "--from ({}) must be before --to ({})",
            fmt_ts(args.from),
            fmt_ts(args.to)
        ))
        .into());
    }
    if args.raw && args.to_log_group.is_none() {
        // clap also rejects this (requires_if); double-checked here because
        // silently ignoring --raw on a stdout/file restore would be a trap.
        return Err(UsageError("--raw only makes sense with --to-log-group".into()).into());
    }
    let bucket = global.require_bucket()?;
    let account = global.require_account()?;
    let clients = aws::load(global).await;
    let store = ObjectStore::new(clients.s3(), bucket, &global.prefix);
    let range = TimeRange {
        from_ms: args.from,
        to_ms_exclusive: args.to,
    };

    let stats = if args.to_stdout {
        let stdout = std::io::stdout();
        let mut w = std::io::BufWriter::new(stdout.lock());
        let stats = write_jsonl(&store, &account, &args.log_group, range, &mut w).await?;
        w.flush().context("flushing stdout")?;
        stats
    } else if let Some(path) = &args.to_file {
        let file =
            std::fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
        let mut w = std::io::BufWriter::new(file);
        let stats = write_jsonl(&store, &account, &args.log_group, range, &mut w).await?;
        w.flush()
            .with_context(|| format!("flushing {}", path.display()))?;
        stats
    } else if let Some(target_group) = &args.to_log_group {
        restore_to_log_group(
            &clients,
            &store,
            &account,
            &args.log_group,
            target_group,
            range,
            args.raw,
        )
        .await?
    } else {
        // clap's ArgGroup(required) makes this unreachable; keep a typed
        // usage error rather than a panic just in case.
        return Err(UsageError(
            "one of --to-stdout / --to-file / --to-log-group is required".into(),
        )
        .into());
    };

    tracing::info!(
        restored = stats.records_emitted,
        chunks_scanned = stats.chunks_scanned,
        frames_fetched = stats.frames_fetched,
        fallback_full_objects = stats.fallback_full_objects,
        parse_errors = stats.parse_errors,
        "restore complete"
    );
    Ok(())
}

/// Stream raw JSONL records (on-disk schema, time-filtered) to `w`.
async fn write_jsonl(
    store: &ObjectStore,
    account: &str,
    log_group: &str,
    range: TimeRange,
    w: &mut impl Write,
) -> Result<ScanStats> {
    scan_log_group(store, account, log_group, range, None, |rec| {
        serde_json::to_writer(&mut *w, rec)?;
        w.write_all(b"\n")?;
        Ok(())
    })
    .await
}

// ---------------------------------------------------------------------------
// CloudWatch re-ingest: pure batching core + thin SDK shell.
// ---------------------------------------------------------------------------

/// One event headed for PutLogEvents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchEvent {
    pub timestamp: i64,
    pub message: String,
}

/// PutLogEvents accounting cost of one message.
pub fn event_cost(message: &str) -> usize {
    message.len() + EVENT_OVERHEAD_BYTES
}

/// Default (wrapped) re-ingest message. Key order is serde_json's BTreeMap
/// order; the shape, not the order, is the contract.
pub fn wrap_message(rec: &LogRecord) -> String {
    serde_json::json!({
        "original_timestamp": rec.timestamp,
        "original_stream": rec.stream,
        "message": rec.message,
    })
    .to_string()
}

/// Streaming PutLogEvents batcher (DESIGN.md §11.3): feed time-ordered
/// events one at a time; [`Self::push`] hands back a completed batch
/// whenever the incoming event would overflow a limit. Memory is bounded by
/// one in-flight batch (≤ [`MAX_BATCH_EVENTS`] events / ~[`MAX_BATCH_BYTES`]
/// bytes) — no `Vec<all records>`.
///
/// Limits enforced per batch: ≤ [`MAX_BATCH_EVENTS`] events,
/// ≤ [`MAX_BATCH_BYTES`] total cost (`len + 26` per event),
/// ≤ [`MAX_BATCH_SPAN_MS`] timestamp span. Callers must feed events in
/// non-decreasing timestamp order (PutLogEvents requires chronological
/// batches) and filter out single events whose cost exceeds the byte limit.
#[derive(Debug, Default)]
pub struct EventBatcher {
    batch: Vec<BatchEvent>,
    bytes: usize,
}

impl EventBatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one event; returns the previous batch when `ev` did not fit in it.
    pub fn push(&mut self, ev: BatchEvent) -> Option<Vec<BatchEvent>> {
        let cost = event_cost(&ev.message);
        let full = !self.batch.is_empty()
            && (self.batch.len() >= MAX_BATCH_EVENTS
                || self.bytes + cost > MAX_BATCH_BYTES
                || ev.timestamp.saturating_sub(self.batch[0].timestamp) > MAX_BATCH_SPAN_MS);
        let out = if full {
            self.bytes = 0;
            Some(std::mem::take(&mut self.batch))
        } else {
            None
        };
        self.bytes += cost;
        self.batch.push(ev);
        out
    }

    /// The final partial batch, if any.
    pub fn finish(self) -> Option<Vec<BatchEvent>> {
        (!self.batch.is_empty()).then_some(self.batch)
    }
}

/// Reference batching over a materialized slice — kept (test-only) as the
/// executable spec the streaming [`EventBatcher`] is property-tested
/// against. Semantics are identical; see `batcher_matches_split_batches`.
#[cfg(test)]
pub fn split_batches(events: &[BatchEvent]) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut bytes = 0usize;
    for (i, e) in events.iter().enumerate() {
        let cost = event_cost(&e.message);
        let count = i - start;
        let too_many = count >= MAX_BATCH_EVENTS;
        let too_big = bytes + cost > MAX_BATCH_BYTES;
        let too_wide =
            count > 0 && e.timestamp.saturating_sub(events[start].timestamp) > MAX_BATCH_SPAN_MS;
        if count > 0 && (too_many || too_big || too_wide) {
            out.push(start..i);
            start = i;
            bytes = 0;
        }
        bytes += cost;
    }
    if start < events.len() {
        out.push(start..events.len());
    }
    out
}

/// Per-batch rejection counts decoded from `RejectedLogEventsInfo` index
/// semantics: `tooOldLogEventEndIndex` / `expiredLogEventEndIndex` are
/// exclusive end indexes (events `[0, end)` rejected),
/// `tooNewLogEventStartIndex` is an inclusive start (events `[start, len)`
/// rejected).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Rejections {
    pub too_old: usize,
    pub too_new: usize,
    pub expired: usize,
}

impl Rejections {
    pub fn from_indexes(
        too_new_start: Option<i32>,
        too_old_end: Option<i32>,
        expired_end: Option<i32>,
        batch_len: usize,
    ) -> Self {
        let clamp = |v: i32| usize::try_from(v.max(0)).unwrap_or(0).min(batch_len);
        Self {
            too_old: too_old_end.map_or(0, clamp),
            too_new: too_new_start.map_or(0, |s| batch_len - clamp(s)),
            expired: expired_end.map_or(0, clamp),
        }
    }

    pub fn total(&self) -> usize {
        self.too_old + self.too_new + self.expired
    }

    pub fn add(&mut self, o: &Rejections) {
        self.too_old += o.too_old;
        self.too_new += o.too_new;
        self.expired += o.expired;
    }
}

/// Sends completed batches: lazily creates the target group/stream before
/// the first PUT (so an empty restore touches nothing) and accumulates
/// sent/rejection accounting.
struct BatchSender<'a> {
    cw: &'a aws_sdk_cloudwatchlogs::Client,
    target_group: &'a str,
    ensured: bool,
    sent: usize,
    batches: usize,
    rejected: Rejections,
}

impl BatchSender<'_> {
    async fn send(&mut self, batch: &[BatchEvent]) -> Result<()> {
        if !self.ensured {
            ensure_group_and_stream(self.cw, self.target_group).await?;
            self.ensured = true;
        }
        self.batches += 1;
        let mut wire = Vec::with_capacity(batch.len());
        for e in batch {
            wire.push(
                aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                    .timestamp(e.timestamp)
                    .message(e.message.clone())
                    .build()
                    .context("building InputLogEvent")?,
            );
        }
        let r = put_batch_with_backoff(self.cw, self.target_group, &wire)
            .await
            .with_context(|| format!("batch {}", self.batches))?;
        self.sent += batch.len();
        if r.total() > 0 {
            eprintln!(
                "warning: batch {}: CloudWatch rejected {} too-old, \
                 {} too-new, {} expired event(s)",
                self.batches, r.too_old, r.too_new, r.expired
            );
        }
        self.rejected.add(&r);
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn restore_to_log_group(
    clients: &aws::AwsClients,
    store: &ObjectStore,
    account: &str,
    source_group: &str,
    target_group: &str,
    range: TimeRange,
    raw: bool,
) -> Result<ScanStats> {
    let now = now_ms();
    if raw {
        eprintln!(
            "warning: --raw keeps original event timestamps; CloudWatch PutLogEvents \
             rejects events older than 14 days (and more than 2 hours in the future). \
             Rejected events are reported per batch and NOT retried."
        );
    }
    let cutoff = now - CW_INGEST_MAX_AGE_MS;
    let cw = clients.cwl();

    // Streaming re-ingest (DESIGN.md §11.3): the k-way merged scan is
    // already timestamp-ascending (PutLogEvents requires chronological
    // batches), so events flow scan → batcher → PutLogEvents with memory
    // bounded by one in-flight batch + the scan's per-chunk frame buffers —
    // no `Vec<all records>`.
    let mut scan = open_scan(store, account, source_group, range, None).await?;
    let mut batcher = EventBatcher::new();
    let mut sender = BatchSender {
        cw: &cw,
        target_group,
        ensured: false,
        sent: 0,
        batches: 0,
        rejected: Rejections::default(),
    };
    let mut oversized = 0u64;
    let mut raw_too_old = 0u64;
    let mut total = 0u64;
    while let Some(rec) = scan.next().await? {
        let (timestamp, message) = if raw {
            (rec.timestamp, rec.message)
        } else {
            (now, wrap_message(&rec))
        };
        if event_cost(&message) > MAX_BATCH_BYTES {
            oversized += 1;
            tracing::warn!(
                timestamp,
                len = message.len(),
                "skipping event larger than the PutLogEvents batch byte limit"
            );
            continue;
        }
        if raw && timestamp < cutoff {
            raw_too_old += 1;
        }
        total += 1;
        if let Some(batch) = batcher.push(BatchEvent { timestamp, message }) {
            sender.send(&batch).await?;
        }
    }
    if let Some(batch) = batcher.finish() {
        sender.send(&batch).await?;
    }
    let stats = scan.stats();

    if total == 0 {
        println!("restore: no matching events in range; nothing sent");
        return Ok(stats);
    }
    if raw_too_old > 0 {
        eprintln!(
            "warning: {raw_too_old} of {total} events were older than 14 days and \
             rejected by CloudWatch — drop --raw to wrap them with timestamp=now"
        );
    }
    println!(
        "restore: sent {} events to log group {target_group:?} stream \
         {RESTORE_STREAM:?} in {} batch(es)",
        sender.sent, sender.batches
    );
    if sender.rejected.total() > 0 {
        println!(
            "restore: CloudWatch rejected {} event(s): {} too old, {} too new, {} expired",
            sender.rejected.total(),
            sender.rejected.too_old,
            sender.rejected.too_new,
            sender.rejected.expired
        );
    }
    if oversized > 0 {
        println!("restore: skipped {oversized} event(s) above the 1 MiB batch byte limit");
    }
    Ok(stats)
}

/// Create the target log group and the `s4logs-restore` stream;
/// `ResourceAlreadyExistsException` is success.
async fn ensure_group_and_stream(cw: &aws_sdk_cloudwatchlogs::Client, group: &str) -> Result<()> {
    match cw.create_log_group().log_group_name(group).send().await {
        Ok(_) => tracing::info!(group, "created log group"),
        Err(err)
            if err
                .as_service_error()
                .is_some_and(|e| e.is_resource_already_exists_exception()) => {}
        Err(err) => bail!(
            "CreateLogGroup {group:?} failed: {}",
            DisplayErrorContext(&err)
        ),
    }
    match cw
        .create_log_stream()
        .log_group_name(group)
        .log_stream_name(RESTORE_STREAM)
        .send()
        .await
    {
        Ok(_) => tracing::info!(group, stream = RESTORE_STREAM, "created log stream"),
        Err(err)
            if err
                .as_service_error()
                .is_some_and(|e| e.is_resource_already_exists_exception()) => {}
        Err(err) => bail!(
            "CreateLogStream {group:?}/{RESTORE_STREAM:?} failed: {}",
            DisplayErrorContext(&err)
        ),
    }
    Ok(())
}

const THROTTLE_CODES: [&str; 4] = [
    "ThrottlingException",
    "Throttling",
    "TooManyRequestsException",
    "RequestLimitExceeded",
];

/// PutLogEvents with simple exponential backoff on throttling
/// (500 ms · 2ⁿ capped at 8 s, ≤ 5 attempts).
async fn put_batch_with_backoff(
    cw: &aws_sdk_cloudwatchlogs::Client,
    group: &str,
    events: &[aws_sdk_cloudwatchlogs::types::InputLogEvent],
) -> Result<Rejections> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match cw
            .put_log_events()
            .log_group_name(group)
            .log_stream_name(RESTORE_STREAM)
            .set_log_events(Some(events.to_vec()))
            .send()
            .await
        {
            Ok(out) => {
                let info = out.rejected_log_events_info();
                return Ok(Rejections::from_indexes(
                    info.and_then(|i| i.too_new_log_event_start_index()),
                    info.and_then(|i| i.too_old_log_event_end_index()),
                    info.and_then(|i| i.expired_log_event_end_index()),
                    events.len(),
                ));
            }
            Err(err) => {
                let throttled = err
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    .is_some_and(|c| THROTTLE_CODES.contains(&c));
                if throttled && attempt < PUT_MAX_ATTEMPTS {
                    let delay = (PUT_BACKOFF_BASE_MS << (attempt - 1)).min(PUT_BACKOFF_CAP_MS);
                    tracing::warn!(
                        attempt,
                        delay_ms = delay,
                        "PutLogEvents throttled; backing off"
                    );
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                bail!(
                    "PutLogEvents to {group:?} failed (attempt {attempt}): {}",
                    DisplayErrorContext(&err)
                );
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn ev(ts: i64, len: usize) -> BatchEvent {
        BatchEvent {
            timestamp: ts,
            message: "m".repeat(len),
        }
    }

    #[test]
    fn wrap_message_shape() {
        let rec = LogRecord {
            timestamp: 1_717_900_000_123,
            stream: "app/i-0abc".into(),
            message: "hello \"quoted\" world".into(),
            ingestion_time: Some(1),
            event_id: Some("e".into()),
        };
        let wrapped = wrap_message(&rec);
        let v: serde_json::Value = serde_json::from_str(&wrapped).unwrap();
        let obj = v.as_object().unwrap();
        assert_eq!(obj.len(), 3, "exactly the three contract fields: {wrapped}");
        assert_eq!(obj["original_timestamp"], 1_717_900_000_123i64);
        assert_eq!(obj["original_stream"], "app/i-0abc");
        assert_eq!(obj["message"], "hello \"quoted\" world");
    }

    #[test]
    fn split_respects_event_count() {
        let events: Vec<BatchEvent> = (0..(MAX_BATCH_EVENTS as i64 + 1))
            .map(|i| ev(i, 1))
            .collect();
        let batches = split_batches(&events);
        assert_eq!(
            batches,
            vec![0..MAX_BATCH_EVENTS, MAX_BATCH_EVENTS..MAX_BATCH_EVENTS + 1]
        );
    }

    #[test]
    fn split_respects_byte_budget_with_overhead() {
        // cost = len + 26 = 524_288 (half the budget): two fit exactly,
        // the third spills.
        let half = MAX_BATCH_BYTES / 2 - EVENT_OVERHEAD_BYTES;
        let events = vec![ev(0, half), ev(1, half), ev(2, half)];
        assert_eq!(split_batches(&events), vec![0..2, 2..3]);
        // One byte more and only one fits per batch.
        let events = vec![ev(0, half + 1), ev(1, half + 1)];
        assert_eq!(split_batches(&events), vec![0..1, 1..2]);
    }

    #[test]
    fn split_respects_24h_span() {
        let events = vec![
            ev(0, 1),
            ev(MAX_BATCH_SPAN_MS, 1),
            ev(MAX_BATCH_SPAN_MS + 1, 1),
        ];
        // 0 and exactly-24h fit together; +1ms splits.
        assert_eq!(split_batches(&events), vec![0..2, 2..3]);
    }

    #[test]
    fn split_handles_empty_and_single() {
        assert!(split_batches(&[]).is_empty());
        assert_eq!(split_batches(&[ev(5, 10)]), vec![0..1]);
    }

    #[test]
    fn split_covers_all_events_exactly_once() {
        let events: Vec<BatchEvent> = (0..2_500i64).map(|i| ev(i * 60_000, 700)).collect();
        let batches = split_batches(&events);
        let mut covered = 0usize;
        for b in &batches {
            assert_eq!(b.start, covered, "batches must be contiguous");
            assert!(b.end > b.start);
            let bytes: usize = events[b.clone()]
                .iter()
                .map(|e| event_cost(&e.message))
                .sum();
            assert!(bytes <= MAX_BATCH_BYTES);
            assert!(b.len() <= MAX_BATCH_EVENTS);
            covered = b.end;
        }
        assert_eq!(covered, events.len());
    }

    // -- streaming batcher (DESIGN.md §11.3) ---------------------------------

    fn drive_batcher(events: &[BatchEvent]) -> Vec<Vec<BatchEvent>> {
        let mut b = EventBatcher::new();
        let mut out = Vec::new();
        for e in events {
            if let Some(full) = b.push(e.clone()) {
                out.push(full);
            }
        }
        if let Some(rest) = b.finish() {
            out.push(rest);
        }
        out
    }

    #[test]
    fn batcher_respects_limits_and_preserves_order() {
        let events: Vec<BatchEvent> = (0..2_500i64).map(|i| ev(i * 60_000, 700)).collect();
        let batches = drive_batcher(&events);
        assert!(batches.len() > 1, "limits must split this input");
        let mut flat = Vec::new();
        for b in &batches {
            assert!(!b.is_empty());
            assert!(b.len() <= MAX_BATCH_EVENTS);
            let bytes: usize = b.iter().map(|e| event_cost(&e.message)).sum();
            assert!(bytes <= MAX_BATCH_BYTES);
            let span = b.last().unwrap().timestamp - b[0].timestamp;
            assert!(span <= MAX_BATCH_SPAN_MS);
            flat.extend(b.iter().cloned());
        }
        assert_eq!(flat, events, "batching must preserve event order exactly");
    }

    #[test]
    fn batcher_splits_at_event_count_limit() {
        let events: Vec<BatchEvent> = (0..(MAX_BATCH_EVENTS as i64 + 1))
            .map(|i| ev(i, 1))
            .collect();
        let batches = drive_batcher(&events);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), MAX_BATCH_EVENTS);
        assert_eq!(batches[1].len(), 1);
    }

    #[test]
    fn batcher_empty_and_single() {
        assert!(drive_batcher(&[]).is_empty());
        assert_eq!(drive_batcher(&[ev(5, 10)]), vec![vec![ev(5, 10)]]);
    }

    proptest::proptest! {
        /// The streaming batcher produces exactly the batches of the
        /// materialized reference implementation (`split_batches`).
        #[test]
        fn batcher_matches_split_batches(
            spec in proptest::collection::vec(
                (
                    proptest::prop_oneof![
                        proptest::prelude::Just(0i64),
                        0i64..100,
                        proptest::prelude::Just(MAX_BATCH_SPAN_MS),
                        proptest::prelude::Just(MAX_BATCH_SPAN_MS + 1),
                    ],
                    proptest::prop_oneof![
                        proptest::prelude::Just(0usize),
                        0usize..1_000,
                        proptest::prelude::Just(MAX_BATCH_BYTES / 2 - EVENT_OVERHEAD_BYTES),
                        proptest::prelude::Just(MAX_BATCH_BYTES - EVENT_OVERHEAD_BYTES),
                    ],
                ),
                0..50,
            ),
        ) {
            let mut ts = 0i64;
            let events: Vec<BatchEvent> = spec
                .into_iter()
                .map(|(inc, len)| {
                    ts += inc; // chronological, as the merged scan guarantees
                    ev(ts, len)
                })
                .collect();
            let expect: Vec<&[BatchEvent]> = split_batches(&events)
                .into_iter()
                .map(|r| &events[r])
                .collect();
            let got = drive_batcher(&events);
            proptest::prop_assert_eq!(got.len(), expect.len());
            for (g, e) in got.iter().zip(expect) {
                proptest::prop_assert_eq!(g.as_slice(), e);
            }
        }
    }

    #[test]
    fn rejection_index_semantics() {
        // tooOld end-exclusive=3 → 3 rejected; tooNew start-inclusive=8 of
        // 10 → 2 rejected; expired end-exclusive=1 → 1.
        let r = Rejections::from_indexes(Some(8), Some(3), Some(1), 10);
        assert_eq!(
            r,
            Rejections {
                too_old: 3,
                too_new: 2,
                expired: 1
            }
        );
        assert_eq!(r.total(), 6);
        // Absent info → zero rejects; out-of-range indexes clamp.
        assert_eq!(Rejections::from_indexes(None, None, None, 10).total(), 0);
        let clamped = Rejections::from_indexes(Some(-5), Some(99), None, 10);
        assert_eq!(clamped.too_new, 10);
        assert_eq!(clamped.too_old, 10);
    }
}
