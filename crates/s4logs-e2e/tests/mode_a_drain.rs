//! Mode A (drain) E2E against LocalStack: seed CloudWatch Logs, run the real
//! `DrainJob` over `AwsCwSource` + `ObjectStore` + `ObjectStoreManifestStore`,
//! and verify layout, plain-zstd decodability (the lock-in claim), sidecars,
//! manifests, idempotency and the fail-closed retention gate.

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::{DAY_MS, HOUR_MS};
use s4logs_core::layout::{date_from_ts_ms, manifest_group_prefix};
use s4logs_core::store::ObjectStore;
use s4logs_drain::{
    AwsCwSource, DrainJob, DrainOptions, Manifest, ManifestStore, ObjectStoreManifestStore,
    RetentionRequest, enforce_retention,
};

const ACCT: &str = "000000000000"; // LocalStack default account id
const PREFIX: &str = "s4logs";

#[tokio::test]
#[ignore = "requires LocalStack (docker compose up -d localstack)"]
async fn mode_a_drain_end_to_end() {
    let Some(ctx) = common::ctx("mode-a").await else {
        return;
    };
    let run_id = common::now_ms();
    let group = format!("/s4logs-e2e/mode-a/{run_id}");
    let streams = ["app/i-aaa", "app/i-bbb", "web/i-ccc"];

    // ---- seed: 4 hour-windows straddling a UTC day boundary --------------
    // boundary-2h .. boundary+2h, ≤ ~26 h in the past (LocalStack and real
    // CW both accept events up to 14 days old).
    let boundary = common::recent_day_boundary(2 * HOUR_MS + 15 * 60_000);
    let from = boundary - 2 * HOUR_MS;
    let to = boundary + 2 * HOUR_MS;

    common::create_group_and_streams(&ctx.cw, &group, &streams).await;
    let mut seeded: BTreeSet<String> = BTreeSet::new();
    for (s_idx, stream) in streams.iter().enumerate() {
        let mut batch = Vec::new();
        for w in 0..4i64 {
            for i in 0..20i64 {
                let ts = from + w * HOUR_MS + i * 90_000 + (s_idx as i64) * 7_000;
                let msg = format!("mode-a event w={w} stream={stream} i={i:02} {run_id}");
                seeded.insert(msg.clone());
                batch.push((ts, msg));
            }
        }
        batch.sort_by_key(|(ts, _)| *ts); // PutLogEvents requires chronological order
        common::put_events(&ctx.cw, &group, stream, &batch).await;
    }
    assert_eq!(seeded.len(), 240);

    // ---- drain ------------------------------------------------------------
    let store = ObjectStore::new(ctx.s3.clone(), &ctx.bucket, PREFIX);
    let cw_source = Arc::new(AwsCwSource::new(ctx.cw.clone()));
    let manifests = Arc::new(ObjectStoreManifestStore::new(store.clone()));
    let mut opts = DrainOptions::new(ACCT, &group);
    opts.from_ms = Some(from);
    opts.to_ms = Some(to);
    let report = DrainJob::new(
        cw_source.clone(),
        Arc::new(store.clone()),
        manifests.clone(),
        opts.clone(),
    )
    .run()
    .await
    .expect("drain run");

    assert_eq!(report.windows_total, 4, "{report:?}");
    assert_eq!(report.windows_processed, 4);
    assert_eq!(report.windows_skipped, 0);
    assert_eq!(report.records, 240);
    assert_eq!(report.events_outside_window, 0);
    assert_eq!(report.objects_written, 4, "one chunk per hour window");
    assert!(report.compressed_bytes < report.raw_bytes);

    // ---- data objects: layout + plain-zstd decodability (lock-in claim) ---
    let chunks = store.list_chunks(ACCT, &group).await.unwrap();
    assert_eq!(chunks.len(), 4);
    let day0 = date_from_ts_ms(boundary - 1);
    let day1 = date_from_ts_ms(boundary);
    let mut dt_day0 = 0;
    let mut dt_day1 = 0;
    let mut decoded_msgs: BTreeSet<String> = BTreeSet::new();
    for loc in &chunks {
        assert_eq!(loc.account, ACCT);
        assert_eq!(loc.log_group, group);
        // Deterministic drain name {window_start_ms}-{seq:06}.
        let (win_start, seq) = loc.name.rsplit_once('-').unwrap();
        let win_start: i64 = win_start.parse().unwrap();
        assert_eq!(seq, "000000");
        assert_eq!(
            date_from_ts_ms(win_start),
            loc.date,
            "chunk dt= must match its window start"
        );
        match &loc.date {
            d if *d == day0 => dt_day0 += 1,
            d if *d == day1 => dt_day1 += 1,
            other => panic!("unexpected dt partition {other}"),
        }

        let body = store.get_bytes(&loc.data_key(PREFIX)).await.unwrap();
        // THE lock-in property: the object decodes with the stock zstd
        // crate, no S4 Logs tooling involved.
        let plain = zstd::stream::decode_all(&body[..]).expect("plain zstd decode");
        let mut records = 0u64;
        for rec in s4logs_core::read::RecordLines::new(&plain) {
            let rec = rec.unwrap();
            assert!(rec.timestamp >= win_start && rec.timestamp < win_start + HOUR_MS);
            assert_eq!(date_from_ts_ms(rec.timestamp), loc.date);
            assert!(streams.contains(&rec.stream.as_str()), "{}", rec.stream);
            assert!(rec.event_id.is_some(), "FilterLogEvents supplies eventId");
            decoded_msgs.insert(rec.message);
            records += 1;
        }
        assert_eq!(records, 60, "20 events x 3 streams per hour window");

        // ---- sidecars decode and agree with the data object ----------------
        let (idx, ts) = store.load_indexes(loc).await.unwrap();
        assert_eq!(idx.entries.len(), ts.entries.len());
        assert!(!idx.entries.is_empty());
        let total_original: u64 = idx.entries.iter().map(|e| e.original_size).sum();
        assert_eq!(total_original, plain.len() as u64);
        let last = idx.entries.last().unwrap();
        assert_eq!(
            last.compressed_offset + last.compressed_size,
            body.len() as u64,
            "frame byte accounting must tile the object"
        );
        assert_eq!(idx.source_compressed_size, Some(body.len() as u64));
        assert!(
            idx.source_etag.is_some(),
            "sidecar must carry post-PUT etag"
        );
        for e in &ts.entries {
            assert!(e.min_ts >= win_start && e.max_ts < win_start + HOUR_MS);
        }
    }
    assert_eq!((dt_day0, dt_day1), (2, 2), "2 windows per side of midnight");
    assert_eq!(decoded_msgs, seeded, "decoded record set == seeded set");

    // ---- manifests ---------------------------------------------------------
    let mkeys = manifests
        .list(&manifest_group_prefix(PREFIX, ACCT, &group))
        .await
        .unwrap();
    assert_eq!(mkeys.len(), 4);
    let data_keys: BTreeSet<String> = chunks.iter().map(|l| l.data_key(PREFIX)).collect();
    for mkey in &mkeys {
        let m = Manifest::from_json_bytes(&manifests.get(mkey).await.unwrap().unwrap()).unwrap();
        assert_eq!(m.version, 1);
        assert_eq!(m.account, ACCT);
        assert_eq!(m.log_group, group, "manifest carries the raw group name");
        assert_eq!(m.record_count, 60);
        assert_eq!(m.objects.len(), 1);
        let obj = &m.objects[0];
        assert!(data_keys.contains(&obj.data_key), "{}", obj.data_key);
        assert!(obj.etag.is_some());
        let body = store.get_bytes(&obj.data_key).await.unwrap();
        assert_eq!(obj.body_len, body.len() as u64);
        assert!(obj.min_ts >= m.window_start_ms && obj.max_ts < m.window_end_ms);
        assert!(m.completed_at_ms > 0);
    }

    // ---- idempotency: re-run skips every window, writes nothing new --------
    let before = common::list_all_keys(&ctx.s3, &ctx.bucket, "").await;
    let rerun = DrainJob::new(
        cw_source.clone(),
        Arc::new(store.clone()),
        manifests.clone(),
        opts,
    )
    .run()
    .await
    .unwrap();
    assert_eq!(rerun.windows_skipped, 4, "{rerun:?}");
    assert_eq!(rerun.windows_processed, 0);
    assert_eq!(rerun.objects_written, 0);
    assert_eq!(rerun.records, 0);
    let after = common::list_all_keys(&ctx.s3, &ctx.bucket, "").await;
    assert_eq!(before, after, "re-run must not create or change objects");

    // ---- retention gate, report-only ----------------------------------------
    // Full coverage: cutoff lands exactly at the end of the drained range →
    // required windows == the 4 drained windows → allowed (but not applied).
    let req = RetentionRequest {
        account: ACCT.into(),
        log_group: group.clone(),
        retention_days: 1,
        coverage_from_ms: from,
        now_ms: to + DAY_MS,
        window_ms: HOUR_MS,
    };
    let plan = enforce_retention(&*cw_source, &*manifests, PREFIX, &req, false)
        .await
        .unwrap();
    assert_eq!(plan.required_windows.len(), 4);
    assert!(plan.allowed(), "full coverage: {:?}", plan.missing_windows);
    assert!(
        !plan.applied,
        "report-only must never call PutRetentionPolicy"
    );

    // Coverage gap: push the cutoff one window past the drained range →
    // fail-closed refusal even though apply was requested.
    let req_gap = RetentionRequest {
        now_ms: to + DAY_MS + HOUR_MS,
        ..req
    };
    let plan_gap = enforce_retention(&*cw_source, &*manifests, PREFIX, &req_gap, true)
        .await
        .unwrap();
    assert!(!plan_gap.allowed());
    assert!(!plan_gap.applied, "gap must refuse even with apply=true");
    assert_eq!(plan_gap.missing_windows.len(), 1);
}
