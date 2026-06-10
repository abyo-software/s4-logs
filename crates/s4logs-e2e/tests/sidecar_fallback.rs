//! Sidecar-loss fallback: delete the `.s4index` sidecar from S3 and prove
//! the data object remains fully readable with the stock `zstd` crate —
//! the lock-in-avoidance property survives sidecar loss (sidecars only
//! accelerate range reads, they are never required for recovery).

#![allow(clippy::unwrap_used)]

mod common;

use s4logs_core::chunk::{ChunkConfig, ChunkWriter};
use s4logs_core::layout::{ChunkLocation, date_from_ts_ms};
use s4logs_core::record::LogRecord;
use s4logs_core::sink::ChunkSink;
use s4logs_core::store::{ObjectStore, StoreError};

const ACCT: &str = "000000000000";
const PREFIX: &str = "s4logs";

#[tokio::test]
#[ignore = "requires LocalStack (docker compose up -d localstack)"]
async fn data_object_survives_sidecar_loss() {
    let Some(ctx) = common::ctx("fallback").await else {
        return;
    };
    let store = ObjectStore::new(ctx.s3.clone(), &ctx.bucket, PREFIX);
    let group = "/s4logs-e2e/fallback";
    let base = common::now_ms() - common::HOUR_MS;

    // Multi-frame chunk so the deleted index is actually load-bearing for
    // range reads.
    let mut w = ChunkWriter::new(ChunkConfig {
        frame_target_bytes: 1024,
        zstd_level: 3,
    });
    for i in 0..50i64 {
        w.push(&LogRecord {
            timestamp: base + i * 1000,
            stream: "app/i-fb".into(),
            message: format!("fallback event i={i:02} {}", "y".repeat(60)),
            ingestion_time: None,
            event_id: None,
        })
        .unwrap();
    }
    let chunk = w.finish().unwrap().unwrap();
    assert!(chunk.frame_index.entries.len() > 1, "want multiple frames");
    let loc = ChunkLocation {
        account: ACCT.into(),
        log_group: group.into(),
        date: date_from_ts_ms(base),
        name: format!("{base}-000000"),
    };
    store.put_chunk(&loc, &chunk).await.unwrap();
    // Sanity: both sidecars are readable before the deletion.
    store.load_indexes(&loc).await.unwrap();

    // ---- delete the .s4index sidecar ----------------------------------------
    ctx.s3
        .delete_object()
        .bucket(&ctx.bucket)
        .key(loc.index_key(PREFIX))
        .send()
        .await
        .unwrap();

    // Indexed read path now fails loudly (typed NotFound, no silent fallback)…
    let err = store.load_indexes(&loc).await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }), "got {err:?}");

    // …but the data object is still completely recoverable with plain zstd:
    // get_bytes + decode_all, zero S4 Logs format knowledge required.
    let body = store.get_bytes(&loc.data_key(PREFIX)).await.unwrap();
    assert_eq!(body, chunk.body);
    let plain = zstd::stream::decode_all(&body[..]).unwrap();
    assert_eq!(plain.len() as u64, chunk.uncompressed_bytes);
    let records: Vec<LogRecord> = s4logs_core::read::RecordLines::new(&plain)
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(records.len(), 50);
    assert_eq!(
        records[49].message,
        format!("fallback event i=49 {}", "y".repeat(60))
    );
}
