//! Property tests for the format layer (DESIGN.md §10): chunk encode →
//! body + sidecars → plain-zstd / per-frame decode → records roundtrip,
//! plus the two codec primitives (S4LT sidecar, log-group sanitizer) and
//! the data-key layout parser.
//!
//! Case count: proptest default (256), overridable via `PROPTEST_CASES`.

#![allow(clippy::unwrap_used)]

use proptest::prelude::*;

use s4logs_core::chunk::{ChunkConfig, ChunkWriter};
use s4logs_core::layout::{ChunkLocation, sanitize_log_group, unsanitize_log_group};
use s4logs_core::read::{RecordLines, decompress_frames};
use s4logs_core::record::LogRecord;
use s4logs_core::sink::encode_sidecars;
use s4logs_core::tsindex::{TsEntry, TsIndex, decode_ts_index, encode_ts_index};
use s4logs_core::{FrameIndexEntry, decode_index};

fn arb_record() -> impl Strategy<Value = LogRecord> {
    (
        any::<i64>(),
        ".*",
        ".*",
        proptest::option::of(any::<i64>()),
        proptest::option::of("[0-9a-f]{0,16}"),
    )
        .prop_map(
            |(timestamp, stream, message, ingestion_time, event_id)| LogRecord {
                timestamp,
                stream,
                message,
                ingestion_time,
                event_id,
            },
        )
}

fn arb_ts_entry() -> impl Strategy<Value = TsEntry> {
    (any::<i64>(), any::<i64>()).prop_map(|(a, b)| TsEntry {
        min_ts: a.min(b),
        max_ts: a.max(b),
    })
}

proptest! {
    /// Full pipeline: records → ChunkWriter (random small frame target) →
    /// body + stamped sidecars → decode via plain zstd + decode_index +
    /// decode_ts_index → records. Frame accounting must tile the body and
    /// the per-frame decode must reassemble the whole-stream decode.
    #[test]
    fn chunk_roundtrip(
        records in proptest::collection::vec(arb_record(), 1..32),
        frame_target in 1usize..4096,
    ) {
        let mut w = ChunkWriter::new(ChunkConfig {
            frame_target_bytes: frame_target,
            zstd_level: 3,
        });
        for r in &records {
            w.push(r).unwrap();
        }
        let chunk = w.finish().unwrap().unwrap();
        prop_assert_eq!(chunk.record_count, records.len() as u64);
        prop_assert_eq!(
            chunk.min_timestamp,
            records.iter().map(|r| r.timestamp).min().unwrap()
        );
        prop_assert_eq!(
            chunk.max_timestamp,
            records.iter().map(|r| r.timestamp).max().unwrap()
        );

        // Sidecar roundtrip with the post-PUT stamp applied.
        let (idx_bytes, ts_bytes) = encode_sidecars(&chunk, Some("\"etag\""));
        let idx = decode_index(idx_bytes).unwrap();
        let ts = decode_ts_index(ts_bytes).unwrap();
        prop_assert_eq!(&idx.entries, &chunk.frame_index.entries);
        prop_assert_eq!(idx.source_etag.as_deref(), Some("\"etag\""));
        prop_assert_eq!(idx.source_compressed_size, Some(chunk.body.len() as u64));
        prop_assert_eq!(&ts.entries, &chunk.ts_index.entries);
        prop_assert_eq!(ts.entries.len(), idx.entries.len());

        // Frame byte accounting tiles the body exactly, in order.
        let mut expect_off = 0u64;
        let mut expect_orig_off = 0u64;
        for e in &idx.entries {
            prop_assert_eq!(e.compressed_offset, expect_off);
            prop_assert_eq!(e.original_offset, expect_orig_off);
            expect_off += e.compressed_size;
            expect_orig_off += e.original_size;
        }
        prop_assert_eq!(expect_off, chunk.body.len() as u64);
        prop_assert_eq!(expect_orig_off, chunk.uncompressed_bytes);

        // Whole body decodes as one stream under stock zstd (the
        // lock-in-avoidance claim) and yields the records verbatim.
        let whole = zstd::stream::decode_all(&chunk.body[..]).unwrap();
        prop_assert_eq!(whole.len() as u64, chunk.uncompressed_bytes);
        let decoded: Vec<LogRecord> =
            RecordLines::new(&whole).collect::<Result<_, _>>().unwrap();
        prop_assert_eq!(&decoded, &records);

        // Per-frame bomb-capped decode reassembles the same bytes.
        let mut reassembled = Vec::with_capacity(whole.len());
        for e in &idx.entries {
            let span =
                &chunk.body[e.compressed_offset as usize..e.compressed_end() as usize];
            reassembled.extend_from_slice(
                &decompress_frames(span, e.original_size).unwrap(),
            );
        }
        prop_assert_eq!(reassembled, whole);

        // Every record's timestamp falls inside its frame's S4LT range.
        for (e, t) in idx.entries.iter().zip(ts.entries.iter()) {
            let frame = chunk_slice(&chunk.body, e);
            for r in RecordLines::new(&frame) {
                let r = r.unwrap();
                prop_assert!(t.min_ts <= r.timestamp && r.timestamp <= t.max_ts);
            }
        }
    }

    /// `sanitize_log_group` output stays in the Hive-safe alphabet and is
    /// perfectly reversible for arbitrary unicode input.
    #[test]
    fn sanitize_roundtrip(group in ".*") {
        let enc = sanitize_log_group(&group);
        prop_assert!(enc.bytes().all(|b| matches!(
            b,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'%'
        )));
        prop_assert_eq!(unsanitize_log_group(&enc), Some(group));
    }

    /// S4LT encode/decode is the identity for any valid entry set.
    #[test]
    fn tsindex_roundtrip(entries in proptest::collection::vec(arb_ts_entry(), 0..64)) {
        let idx = TsIndex { entries };
        prop_assert_eq!(decode_ts_index(encode_ts_index(&idx)).unwrap(), idx);
    }

    /// Data-key build → parse is the identity for arbitrary log groups
    /// (incl. unicode), prefixes, and layout-shaped names/dates.
    #[test]
    fn data_key_parse_roundtrip(
        prefix in "[a-z0-9/]{0,12}",
        account in "[0-9]{12}",
        log_group in ".*",
        date in "[0-9]{4}-[0-9]{2}-[0-9]{2}",
        name in "[0-9]{1,13}-[0-9]{6}",
    ) {
        let loc = ChunkLocation { account, log_group, date, name };
        let key = loc.data_key(&prefix);
        prop_assert_eq!(ChunkLocation::parse_data_key(&prefix, &key), Some(loc));
    }
}

/// Decompress the single frame described by `e` out of `body` (helper so the
/// per-frame timestamp check above reads as one expression).
fn chunk_slice(body: &[u8], e: &FrameIndexEntry) -> Vec<u8> {
    let span = &body[e.compressed_offset as usize..e.compressed_end() as usize];
    decompress_frames(span, e.original_size).unwrap()
}
