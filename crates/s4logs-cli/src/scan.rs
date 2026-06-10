//! Shared read path for `grep` / `restore` (DESIGN.md §9): dt= date pruning
//! → S4LT frame pruning → S4IX byte spans → Range GETs → bomb-capped decode
//! → record filtering. Whole objects are downloaded **only** when a sidecar
//! is missing or corrupt (lock-in story: data must stay readable without
//! S4 Logs sidecars), and that fallback is loudly warned about.

use std::collections::HashSet;
use std::io::Read;

use anyhow::{Context, Result, bail};
use regex::Regex;
use s4logs_core::layout::date_from_ts_ms;
use s4logs_core::read::{ReadError, RecordLines, TimeRange, decompress_frames, frames_overlapping};
use s4logs_core::record::LogRecord;
use s4logs_core::sink::ChunkSink;
use s4logs_core::store::{ObjectStore, StoreError};

const DAY_MS: i64 = 86_400_000;

/// Output cap for the sidecar-less fallback decode. Drain objects rotate at
/// 256 MiB uncompressed by default; 2 GiB leaves generous headroom while
/// still refusing decompression bombs.
const FALLBACK_DECODE_CAP_BYTES: u64 = 2 << 30;

#[derive(Debug, Default, Clone, Copy)]
pub struct ScanStats {
    /// Chunks listed for the log group (all dates).
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
    F: FnMut(&LogRecord) -> Result<()>,
{
    for item in RecordLines::new(jsonl) {
        match item {
            Ok(rec) => {
                if record_matches(&rec, range, pattern) {
                    stats.records_emitted += 1;
                    emit(&rec)?;
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

/// Scan every chunk of `log_group` overlapping `range`, calling `emit` for
/// each matching record. Records are emitted in object order (lexicographic
/// `dt=`/name), record order within each object — not globally
/// time-sorted across objects.
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
    let dates: HashSet<String> = dates_in_range(range.from_ms, range.to_ms_exclusive)
        .into_iter()
        .collect();
    let mut stats = ScanStats::default();
    let chunks = store
        .list_chunks(account, log_group)
        .await
        .with_context(|| format!("listing chunks for log group {log_group:?}"))?;
    stats.chunks_listed = chunks.len() as u64;
    let prefix = store.key_prefix().to_owned();

    for loc in chunks.iter().filter(|l| dates.contains(&l.date)) {
        stats.chunks_scanned += 1;
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

        // Frame pruning. An S4IX/S4LT entry-count mismatch is also treated
        // as sidecar corruption (typed error from core) → fallback.
        let spans = match &sidecars {
            Some((frame_index, ts_index)) => {
                match frames_overlapping(frame_index, ts_index, &range) {
                    Ok(spans) => Some(spans),
                    Err(err @ ReadError::IndexMismatch { .. }) => {
                        tracing::warn!(
                            key = %data_key,
                            error = %err,
                            "sidecars out of sync; falling back to full-object GET"
                        );
                        None
                    }
                    Err(other) => {
                        return Err(other).with_context(|| format!("pruning frames of {data_key}"));
                    }
                }
            }
            None => None,
        };

        match spans {
            Some(spans) => {
                for span in spans {
                    let want = span.byte_end_exclusive - span.byte_start;
                    let bytes = store
                        .get_range(&data_key, span.byte_start, span.byte_end_exclusive)
                        .await
                        .with_context(|| {
                            format!(
                                "range GET {data_key} bytes {}..{}",
                                span.byte_start, span.byte_end_exclusive
                            )
                        })?;
                    // Early diagnostics (wave-1A note): a short/long range
                    // response must fail before zstd sees the bytes.
                    if bytes.len() as u64 != want {
                        bail!(
                            "range GET for {data_key} frame {} returned {} bytes, \
                             requested {want} (offsets {}..{}) — refusing to decode",
                            span.frame_idx,
                            bytes.len(),
                            span.byte_start,
                            span.byte_end_exclusive
                        );
                    }
                    let jsonl =
                        decompress_frames(&bytes, span.original_size).with_context(|| {
                            format!("decoding frame {} of {data_key}", span.frame_idx)
                        })?;
                    stats.frames_fetched += 1;
                    emit_records(&jsonl, &range, pattern, &mut stats, &mut emit)?;
                }
            }
            None => {
                let body = store
                    .get_bytes(&data_key)
                    .await
                    .with_context(|| format!("full-object GET {data_key}"))?;
                let jsonl = decode_full_object(&body)
                    .with_context(|| format!("decoding whole object {data_key}"))?;
                stats.fallback_full_objects += 1;
                emit_records(&jsonl, &range, pattern, &mut stats, &mut emit)?;
            }
        }
    }
    Ok(stats)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
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
        emit_records(&buf, &range, Some(&re), &mut stats, &mut |r: &LogRecord| {
            got.push(r.message.clone());
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
}
