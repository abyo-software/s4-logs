//! Read-path (grep) E2E: seed + drain, then exercise the library read path
//! exactly as `s4logs grep` does — `list_chunks` → `load_indexes` →
//! `frames_overlapping` → `get_range` → `decompress_frames` →
//! `RecordLines` — asserting time-pruned correctness and that pruning
//! actually skips frames (no full-object downloads).

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::HOUR_MS;
use s4logs_core::chunk::ChunkConfig;
use s4logs_core::read::{RecordLines, TimeRange, decompress_frames, frames_overlapping};
use s4logs_core::store::ObjectStore;
use s4logs_drain::{AwsCwSource, DrainJob, DrainOptions, ObjectStoreManifestStore};

const ACCT: &str = "000000000000";
const PREFIX: &str = "s4logs";

#[tokio::test]
#[ignore = "requires LocalStack (docker compose up -d localstack)"]
async fn grep_read_path_prunes_by_time() {
    let Some(ctx) = common::ctx("grep").await else {
        return;
    };
    let run_id = common::now_ms();
    let group = format!("/s4logs-e2e/grep/{run_id}");
    let stream = "app/i-grep";

    // ---- seed: 240 events, one every 30 s across 2 hours ------------------
    let boundary = common::recent_day_boundary(2 * HOUR_MS + 15 * 60_000);
    let from = boundary - 2 * HOUR_MS;
    let to = boundary; // stay on one side of midnight — pruning is the point here
    common::create_group_and_streams(&ctx.cw, &group, &[stream]).await;
    let mut batch = Vec::new();
    for i in 0..240i64 {
        batch.push((from + i * 30_000, format!("grep event i={i:03} {run_id}")));
    }
    common::put_events(&ctx.cw, &group, stream, &batch).await;

    // ---- drain with small frames so each object holds many frames ----------
    let store = ObjectStore::new(ctx.s3.clone(), &ctx.bucket, PREFIX);
    let manifests = Arc::new(ObjectStoreManifestStore::new(store.clone()));
    let mut opts = DrainOptions::new(ACCT, &group);
    opts.from_ms = Some(from);
    opts.to_ms = Some(to);
    opts.chunk = ChunkConfig {
        frame_target_bytes: 2048,
        zstd_level: 3,
    };
    let report = DrainJob::new(
        Arc::new(AwsCwSource::new(ctx.cw.clone())),
        Arc::new(store.clone()),
        manifests,
        opts,
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.records, 240, "{report:?}");

    // ---- query window: [from+30min, from+45min) -----------------------------
    let q_from = from + 30 * 60_000;
    let q_to = from + 45 * 60_000;
    let expected: BTreeSet<String> = batch
        .iter()
        .filter(|(ts, _)| *ts >= q_from && *ts < q_to)
        .map(|(_, m)| m.clone())
        .collect();
    assert_eq!(expected.len(), 30, "30 events in a 15-minute window");

    let chunks = store.list_chunks(ACCT, &group).await.unwrap();
    assert_eq!(chunks.len(), 2, "one object per hour window");
    let range = TimeRange {
        from_ms: q_from,
        to_ms_exclusive: q_to,
    };
    let mut got: BTreeSet<String> = BTreeSet::new();
    let mut frames_total = 0usize;
    let mut frames_fetched = 0usize;
    let mut bytes_fetched = 0u64;
    let mut bytes_total = 0u64;
    for loc in &chunks {
        let (idx, ts) = store.load_indexes(loc).await.unwrap();
        assert_eq!(idx.entries.len(), ts.entries.len());
        frames_total += idx.entries.len();
        bytes_total += idx.entries.iter().map(|e| e.compressed_size).sum::<u64>();
        let spans = frames_overlapping(&idx, &ts, &range).unwrap();
        frames_fetched += spans.len();
        for span in spans {
            // Range GET only the overlapping frame — never the whole object.
            let bytes = store
                .get_range(
                    &loc.data_key(PREFIX),
                    span.byte_start,
                    span.byte_end_exclusive,
                )
                .await
                .unwrap();
            assert_eq!(
                bytes.len() as u64,
                span.byte_end_exclusive - span.byte_start
            );
            bytes_fetched += bytes.len() as u64;
            let plain = decompress_frames(&bytes, span.original_size).unwrap();
            for rec in RecordLines::new(&plain) {
                let rec = rec.unwrap();
                // Frame granularity: records outside the window may share a
                // frame with in-window ones — the grep layer filters them.
                if rec.timestamp >= q_from && rec.timestamp < q_to {
                    got.insert(rec.message);
                }
            }
        }
    }
    assert!(frames_total > 4, "tiny frames expected, got {frames_total}");
    assert_eq!(
        got, expected,
        "time-pruned read must return exactly the in-window events"
    );
    assert!(
        frames_fetched < frames_total,
        "pruning must skip frames: fetched {frames_fetched} of {frames_total}"
    );
    assert!(
        bytes_fetched < bytes_total,
        "range reads must move fewer bytes than the full objects \
         ({bytes_fetched} vs {bytes_total})"
    );
}
