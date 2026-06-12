//! Read path — time-range frame pruning and bomb-capped decompression
//! (DESIGN.md §5, §9).

use std::io::Read;

use s4_codec::index::FrameIndex;
use thiserror::Error;

use crate::record::{LogRecord, RecordError};
use crate::tsindex::TsIndex;

/// Half-open event-time window `[from_ms, to_ms_exclusive)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub from_ms: i64,
    pub to_ms_exclusive: i64,
}

impl TimeRange {
    pub fn overlaps(&self, min_ts: i64, max_ts: i64) -> bool {
        min_ts < self.to_ms_exclusive && max_ts >= self.from_ms
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReadError {
    #[error("S4IX has {frames} entries but S4LT has {ts_frames} — sidecars out of sync")]
    IndexMismatch { frames: usize, ts_frames: usize },
    #[error("decompressed output exceeded cap of {cap} bytes (decompression bomb?)")]
    Bomb { cap: u64 },
    #[error("decompressed {got} bytes, sidecar claims {expected}")]
    SizeMismatch { expected: u64, got: u64 },
    #[error("zstd decode failed")]
    Io(#[from] std::io::Error),
}

/// One frame worth fetching: its index position and byte range in the
/// object body, per the S4IX sidecar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameSpan {
    pub frame_idx: usize,
    pub byte_start: u64,
    pub byte_end_exclusive: u64,
    pub original_size: u64,
}

/// Frames whose S4LT timestamp range overlaps `range`, in object order.
pub fn frames_overlapping(
    frame_index: &FrameIndex,
    ts_index: &TsIndex,
    range: &TimeRange,
) -> Result<Vec<FrameSpan>, ReadError> {
    if frame_index.entries.len() != ts_index.entries.len() {
        return Err(ReadError::IndexMismatch {
            frames: frame_index.entries.len(),
            ts_frames: ts_index.entries.len(),
        });
    }
    Ok(frame_index
        .entries
        .iter()
        .zip(ts_index.entries.iter())
        .enumerate()
        .filter(|(_, (_, ts))| range.overlaps(ts.min_ts, ts.max_ts))
        .map(|(i, (fe, _))| FrameSpan {
            frame_idx: i,
            byte_start: fe.compressed_offset,
            byte_end_exclusive: fe.compressed_end(),
            original_size: fe.original_size,
        })
        .collect())
}

/// Merge spans whose gap is ≤ `max_gap_bytes` into fewer Range GETs.
/// Input must be in object order (as produced by [`frames_overlapping`]).
/// Returns `(byte_start, byte_end_exclusive, total_original_size)` tuples.
pub fn coalesce_spans(spans: &[FrameSpan], max_gap_bytes: u64) -> Vec<(u64, u64, u64)> {
    let mut out: Vec<(u64, u64, u64)> = Vec::new();
    for s in spans {
        match out.last_mut() {
            Some((_, end, orig)) if s.byte_start.saturating_sub(*end) <= max_gap_bytes => {
                *end = s.byte_end_exclusive;
                *orig += s.original_size;
            }
            _ => out.push((s.byte_start, s.byte_end_exclusive, s.original_size)),
        }
    }
    out
}

/// How much slack the bomb cap allows over the sidecar-claimed size
/// (mirrors s4 `cpu_zstd.rs`).
pub const BOMB_SLACK_BYTES: u64 = 1024;
const BOOTSTRAP_CAPACITY: usize = 1 << 20;
/// Hard ceiling on a single object's decompressed size (64 GiB). A claimed
/// `expected_original` above this is treated as a hostile sidecar — real log
/// objects are bounded by the drain `--chunk-target` (default 256 MiB).
pub const MAX_DECOMPRESSED_BYTES: u64 = 64 << 30;

/// Decompress one or more concatenated standard zstd frames, refusing to
/// emit more than `expected_original + BOMB_SLACK_BYTES` bytes.
///
/// NOTE: when called on a *coalesced* span that includes skipped gap bytes,
/// pass the summed `original_size` of the member frames — gap frames decode
/// too (they are whole zstd frames), so prefer exact spans for grep and use
/// coalescing only when the caller filters records afterwards anyway.
pub fn decompress_frames(input: &[u8], expected_original: u64) -> Result<Vec<u8>, ReadError> {
    // A forged sidecar/manifest can claim `expected_original` near u64::MAX;
    // a plain `+` would panic in debug and wrap in release. Reject anything
    // beyond a sane per-object ceiling before constructing the decoder.
    if expected_original > MAX_DECOMPRESSED_BYTES {
        return Err(ReadError::Bomb {
            cap: MAX_DECOMPRESSED_BYTES,
        });
    }
    let cap = expected_original.saturating_add(BOMB_SLACK_BYTES);
    let decoder = zstd::stream::read::Decoder::new(input)?;
    let mut limited = decoder.take(cap);
    let mut out = Vec::with_capacity((expected_original as usize).min(BOOTSTRAP_CAPACITY));
    limited.read_to_end(&mut out)?;
    if out.len() as u64 >= cap {
        return Err(ReadError::Bomb { cap });
    }
    if out.len() as u64 != expected_original {
        return Err(ReadError::SizeMismatch {
            expected: expected_original,
            got: out.len() as u64,
        });
    }
    Ok(out)
}

/// Streaming iterator over decompressed JSONL bytes: one [`LogRecord`] per
/// non-empty line (grep/restore decode frame-by-frame and must not
/// materialize a `Vec<LogRecord>` per multi-MiB frame). The final line may
/// lack a trailing `\n`; empty lines are skipped rather than treated as
/// parse errors so `split`-style padding can't poison a frame.
#[derive(Debug, Clone)]
pub struct RecordLines<'a> {
    rest: &'a [u8],
}

impl<'a> RecordLines<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { rest: bytes }
    }
}

impl Iterator for RecordLines<'_> {
    type Item = Result<LogRecord, RecordError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.rest.is_empty() {
                return None;
            }
            let (line, rest) = match self.rest.iter().position(|&b| b == b'\n') {
                Some(i) => (&self.rest[..i], &self.rest[i + 1..]),
                None => (self.rest, &self.rest[..0]),
            };
            self.rest = rest;
            if !line.is_empty() {
                return Some(LogRecord::from_jsonl(line));
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::chunk::{ChunkConfig, ChunkWriter};
    use crate::record::LogRecord;

    fn chunk_with_frames() -> crate::chunk::EncodedChunk {
        let mut w = ChunkWriter::new(ChunkConfig {
            frame_target_bytes: 200,
            zstd_level: 3,
        });
        for i in 0..100i64 {
            w.push(&LogRecord {
                timestamp: i * 1000,
                stream: "s".into(),
                message: format!("padded message body {i:04}"),
                ingestion_time: None,
                event_id: None,
            })
            .unwrap();
        }
        w.finish().unwrap().unwrap()
    }

    #[test]
    fn prune_then_decompress_selected_frames() {
        let chunk = chunk_with_frames();
        let range = TimeRange {
            from_ms: 30_000,
            to_ms_exclusive: 40_000,
        };
        let spans = frames_overlapping(&chunk.frame_index, &chunk.ts_index, &range).unwrap();
        assert!(!spans.is_empty());
        assert!(spans.len() < chunk.frame_index.entries.len());
        for span in spans {
            let bytes = &chunk.body[span.byte_start as usize..span.byte_end_exclusive as usize];
            let out = decompress_frames(bytes, span.original_size).unwrap();
            assert_eq!(out.len() as u64, span.original_size);
        }
    }

    #[test]
    fn mismatched_sidecars_rejected() {
        let chunk = chunk_with_frames();
        let mut ts = chunk.ts_index.clone();
        ts.entries.pop();
        let range = TimeRange {
            from_ms: 0,
            to_ms_exclusive: i64::MAX,
        };
        assert!(matches!(
            frames_overlapping(&chunk.frame_index, &ts, &range),
            Err(ReadError::IndexMismatch { .. })
        ));
    }

    #[test]
    fn undersized_claim_within_cap_rejected_as_size_mismatch() {
        let chunk = chunk_with_frames();
        let e = &chunk.frame_index.entries[0];
        let bytes = &chunk.body[e.compressed_offset as usize..e.compressed_end() as usize];
        // Claim slightly smaller than reality: output stays under the cap
        // but must still be refused.
        let err = decompress_frames(bytes, e.original_size - 1).unwrap_err();
        assert!(matches!(err, ReadError::SizeMismatch { .. }), "got {err:?}");
    }

    #[test]
    fn huge_expected_original_rejected_not_overflow() {
        // Forged sidecar claiming a near-u64::MAX size must be refused before
        // the `+ BOMB_SLACK_BYTES` (no panic in debug, no wrap in release).
        let err = decompress_frames(&[], u64::MAX).unwrap_err();
        assert!(matches!(err, ReadError::Bomb { .. }), "got {err:?}");
        let err = decompress_frames(&[], MAX_DECOMPRESSED_BYTES + 1).unwrap_err();
        assert!(matches!(err, ReadError::Bomb { .. }), "got {err:?}");
        // Exactly at the ceiling is ALLOWED (the guard is `>`, not `>=`): the
        // empty input then fails as a decode/size error, NOT a Bomb. Pins the
        // boundary operator (mutation testing flagged `>` vs `>=`).
        let err = decompress_frames(&[], MAX_DECOMPRESSED_BYTES).unwrap_err();
        assert!(
            !matches!(err, ReadError::Bomb { .. }),
            "claim == ceiling must pass the bomb guard, got {err:?}"
        );
    }

    #[test]
    fn bomb_cap_stops_decoding_early() {
        let chunk = chunk_with_frames();
        // Whole multi-frame body (>> 8 + 1024 bytes decompressed) against a
        // tiny claimed size: decoding must stop at the cap, not inflate.
        let err = decompress_frames(&chunk.body, 8).unwrap_err();
        assert!(matches!(err, ReadError::Bomb { .. }), "got {err:?}");
    }

    #[test]
    fn record_lines_streams_records_and_skips_blank_lines() {
        let mut buf = Vec::new();
        for i in 0..3i64 {
            LogRecord {
                timestamp: i,
                stream: "s".into(),
                message: format!("m{i}"),
                ingestion_time: None,
                event_id: None,
            }
            .append_jsonl(&mut buf)
            .unwrap();
        }
        buf.extend_from_slice(b"\n\n"); // blank padding must be skipped
        let recs: Vec<LogRecord> = RecordLines::new(&buf).collect::<Result<_, _>>().unwrap();
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[2].message, "m2");
        // Final line without trailing newline still parses.
        let trimmed = buf.trim_ascii_end();
        let recs2: Vec<LogRecord> = RecordLines::new(trimmed).collect::<Result<_, _>>().unwrap();
        assert_eq!(recs, recs2);
        assert!(RecordLines::new(b"").next().is_none());
    }

    #[test]
    fn record_lines_surfaces_parse_errors() {
        let bytes = b"{\"timestamp\":1,\"stream\":\"s\",\"message\":\"m\"}\nnot json\n";
        let items: Vec<_> = RecordLines::new(bytes).collect();
        assert_eq!(items.len(), 2);
        assert!(items[0].is_ok());
        assert!(items[1].is_err());
    }

    #[test]
    fn overlaps_boundary_is_half_open() {
        // Range [100, 200): a frame is in-range iff its span touches
        // [100, 200). These exact-boundary cases pin the `<` / `>=` operators
        // (a time-pruning off-by-one would make grep silently miss or
        // double-count edge records). Found by mutation testing.
        let r = TimeRange {
            from_ms: 100,
            to_ms_exclusive: 200,
        };
        // frame entirely before: max_ts == from_ms-1 → no
        assert!(!r.overlaps(0, 99));
        // frame whose max_ts == from_ms → yes (>= boundary)
        assert!(r.overlaps(50, 100));
        // frame whose min_ts == to_ms_exclusive → no (< boundary, exclusive)
        assert!(!r.overlaps(200, 300));
        // frame whose min_ts == to_ms_exclusive-1 → yes
        assert!(r.overlaps(199, 250));
    }

    #[test]
    fn coalesce_gap_threshold_is_inclusive() {
        // A gap exactly == max_gap_bytes merges; gap+1 does not. Pins the
        // `<=` guard in coalesce_spans (mutation testing flagged it).
        let spans = vec![
            FrameSpan {
                frame_idx: 0,
                byte_start: 0,
                byte_end_exclusive: 10,
                original_size: 5,
            },
            FrameSpan {
                frame_idx: 1,
                byte_start: 18, // gap of 8 from end=10
                byte_end_exclusive: 30,
                original_size: 7,
            },
        ];
        assert_eq!(coalesce_spans(&spans, 8).len(), 1, "gap == max merges");
        assert_eq!(coalesce_spans(&spans, 7).len(), 2, "gap > max stays split");
    }

    #[test]
    fn decompress_cap_boundary_rejects_at_exactly_cap() {
        // When decoding fills the output to exactly `expected_original + slack`
        // (the `take` ceiling), it must be a Bomb — not fall through to
        // SizeMismatch. The whole multi-frame body decodes to ~thousands of
        // bytes; claiming `100` sets cap = 100 + 1024 = 1124 < that, so the
        // decoder truncates at exactly cap and `out.len() == cap` must trip
        // the `>=` Bomb check. Pins `>=` against a `>` mutant, which would
        // instead yield SizeMismatch. (Found by mutation testing.)
        let chunk = chunk_with_frames();
        let claim = 100u64;
        assert!(
            chunk.uncompressed_bytes > claim + BOMB_SLACK_BYTES,
            "test needs the body to exceed the cap"
        );
        let err = decompress_frames(&chunk.body, claim).unwrap_err();
        assert!(matches!(err, ReadError::Bomb { .. }), "got {err:?}");
    }

    #[test]
    fn coalesce_merges_adjacent_frames() {
        let chunk = chunk_with_frames();
        let range = TimeRange {
            from_ms: 0,
            to_ms_exclusive: i64::MAX,
        };
        let spans = frames_overlapping(&chunk.frame_index, &chunk.ts_index, &range).unwrap();
        let merged = coalesce_spans(&spans, 0);
        assert_eq!(merged.len(), 1, "contiguous frames must merge");
        assert_eq!(merged[0].0, 0);
        assert_eq!(merged[0].1, chunk.body.len() as u64);
        assert_eq!(merged[0].2, chunk.uncompressed_bytes);
    }
}
