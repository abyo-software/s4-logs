//! Mode B (gateway) E2E: a real `Gateway` served on an ephemeral port,
//! driven by a stock `aws_sdk_cloudwatchlogs` client with the endpoint
//! overridden to the gateway — exactly the customer integration path
//! (Fluent Bit / CW Agent endpoint swap). Chunks land in LocalStack S3.

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use aws_config::{BehaviorVersion, Region};
use s4logs_core::layout::{ChunkLocation, date_from_ts_ms};
use s4logs_core::store::ObjectStore;
use s4logs_gateway::{Gateway, GatewayConfig, NoopCwForward, RoutingConfig};

const ACCT: &str = "123456789012";
const PREFIX: &str = "s4logs";
const GROUP: &str = "/e2e/gateway";
const STREAM: &str = "app/i-0gateway";

/// CW Logs client pointed at the gateway (endpoint override — the Mode B
/// customer experience).
async fn gateway_client(addr: &str) -> aws_sdk_cloudwatchlogs::Client {
    let cfg = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(format!("http://{addr}"))
        .region(Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "test",
            "test",
            None,
            None,
            "s4logs-e2e",
        ))
        .load()
        .await;
    aws_sdk_cloudwatchlogs::Client::new(&cfg)
}

#[tokio::test]
#[ignore = "requires LocalStack (docker compose up -d localstack)"]
async fn mode_b_gateway_end_to_end() {
    let Some(ctx) = common::ctx("mode-b").await else {
        return;
    };
    let store = ObjectStore::new(ctx.s3.clone(), &ctx.bucket, PREFIX);

    // Small flush_bytes so one decent batch triggers an inline flush; large
    // flush_interval so the age sweep never interferes with the assertions.
    let gateway = Gateway::new(
        GatewayConfig {
            account: ACCT.to_owned(),
            routing: RoutingConfig::default(), // everything → s3
            flush_bytes: 4 * 1024,
            flush_interval: Duration::from_secs(3600),
            ..GatewayConfig::default()
        },
        Arc::new(store.clone()),
        Arc::new(NoopCwForward),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(gateway.serve_listener(listener, async move {
        let _ = shutdown_rx.await;
    }));

    let cw = gateway_client(&addr).await;

    // ---- control plane: CreateLogGroup / CreateLogStream -------------------
    cw.create_log_group()
        .log_group_name(GROUP)
        .send()
        .await
        .expect("CreateLogGroup via gateway");
    let dup = cw
        .create_log_group()
        .log_group_name(GROUP)
        .send()
        .await
        .expect_err("duplicate group must be rejected");
    assert!(
        dup.into_service_error()
            .is_resource_already_exists_exception(),
        "gateway must speak the CW error wire shape"
    );
    cw.create_log_stream()
        .log_group_name(GROUP)
        .log_stream_name(STREAM)
        .send()
        .await
        .expect("CreateLogStream via gateway");
    let groups = cw
        .describe_log_groups()
        .log_group_name_prefix(GROUP)
        .send()
        .await
        .expect("DescribeLogGroups via gateway");
    assert_eq!(groups.log_groups().len(), 1);
    assert_eq!(groups.log_groups()[0].log_group_name(), Some(GROUP));

    // ---- data plane: PutLogEvents batches -----------------------------------
    // All timestamps land inside one UTC day, away from midnight, so the
    // object-count assertions are deterministic.
    let now = common::now_ms();
    let day = (now / common::DAY_MS) * common::DAY_MS;
    let base = if now - day < 2 * common::HOUR_MS {
        day - 2 * common::HOUR_MS
    } else {
        day + common::HOUR_MS
    };
    let date = date_from_ts_ms(base);

    let mut sent: BTreeSet<String> = BTreeSet::new();
    let mut big_batch = Vec::new();
    for i in 0..60i64 {
        let msg = format!("mode-b batch=0 i={i:03} {}", "x".repeat(80));
        sent.insert(msg.clone());
        big_batch.push(
            aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                .timestamp(base + i)
                .message(msg)
                .build()
                .unwrap(),
        );
    }
    cw.put_log_events()
        .log_group_name(GROUP)
        .log_stream_name(STREAM)
        .set_log_events(Some(big_batch))
        .send()
        .await
        .expect("PutLogEvents via gateway");

    // > flush_bytes ⇒ flushed inline before the response returned.
    let chunks = store.list_chunks(ACCT, GROUP).await.unwrap();
    assert_eq!(chunks.len(), 1, "size-triggered flush: {chunks:?}");
    let loc = &chunks[0];
    assert_eq!(loc.account, ACCT);
    assert_eq!(loc.log_group, GROUP);
    assert_eq!(loc.date, date);
    // Gateway object names: {first_event_ts_ms}-{uuid8}.
    let (first_ts, suffix) = loc.name.rsplit_once('-').unwrap();
    assert_eq!(first_ts.parse::<i64>().unwrap(), base);
    assert_eq!(suffix.len(), 8);
    assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
    // Layout parse round-trip on the raw key.
    let key = loc.data_key(PREFIX);
    assert_eq!(&ChunkLocation::parse_data_key(PREFIX, &key).unwrap(), loc);

    // Contents decode with plain zstd and preserve group/stream/messages.
    let body = store.get_bytes(&key).await.unwrap();
    let plain = zstd::stream::decode_all(&body[..]).unwrap();
    let mut got: BTreeSet<String> = BTreeSet::new();
    for rec in s4logs_core::read::RecordLines::new(&plain) {
        let rec = rec.unwrap();
        assert_eq!(rec.stream, STREAM);
        assert_eq!(date_from_ts_ms(rec.timestamp), date);
        got.insert(rec.message);
    }
    assert_eq!(got, sent);
    // Sidecars accompany the gateway chunk too.
    let (idx, ts) = store.load_indexes(loc).await.unwrap();
    assert_eq!(idx.entries.len(), ts.entries.len());
    assert_eq!(
        idx.entries.iter().map(|e| e.original_size).sum::<u64>(),
        plain.len() as u64
    );

    // ---- a small batch stays buffered until shutdown ------------------------
    let mut small_batch = Vec::new();
    for i in 0..5i64 {
        let msg = format!("mode-b batch=1 i={i}");
        sent.insert(msg.clone());
        small_batch.push(
            aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                .timestamp(base + 1000 + i)
                .message(msg)
                .build()
                .unwrap(),
        );
    }
    cw.put_log_events()
        .log_group_name(GROUP)
        .log_stream_name(STREAM)
        .set_log_events(Some(small_batch))
        .send()
        .await
        .unwrap();
    assert_eq!(
        store.list_chunks(ACCT, GROUP).await.unwrap().len(),
        1,
        "below flush_bytes — must still be buffered"
    );

    // ---- observability ------------------------------------------------------
    let health = common::http_get(&addr, "/health").await;
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
    assert!(health.contains("ok"));
    let ready = common::http_get(&addr, "/ready").await;
    assert!(ready.starts_with("HTTP/1.1 200"), "{ready}");
    let metrics = common::http_get(&addr, "/metrics").await;
    assert!(metrics.starts_with("HTTP/1.1 200"), "{metrics}");
    assert!(
        metrics.contains("s4logs_events_total"),
        "metrics body:\n{metrics}"
    );
    assert!(metrics.contains("s4logs_flush_total"));

    // ---- graceful shutdown flushes the remaining buffer ---------------------
    shutdown_tx.send(()).unwrap();
    server
        .await
        .expect("server task")
        .expect("gateway shutdown flush");
    let chunks = store.list_chunks(ACCT, GROUP).await.unwrap();
    assert_eq!(chunks.len(), 2, "shutdown must flush the buffered batch");
    let mut all: BTreeSet<String> = BTreeSet::new();
    for loc in &chunks {
        let body = store.get_bytes(&loc.data_key(PREFIX)).await.unwrap();
        let plain = zstd::stream::decode_all(&body[..]).unwrap();
        for rec in s4logs_core::read::RecordLines::new(&plain) {
            all.insert(rec.unwrap().message);
        }
    }
    assert_eq!(all, sent, "every accepted event must be durable in S3");
}
