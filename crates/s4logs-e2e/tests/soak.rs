//! Soak test — sustained mixed PutLogEvents load against a real in-process
//! `Gateway` (`serve_listener`) backed by `ObjectStore` on LocalStack S3.
//!
//! Duration / rate are env-tunable so the same binary covers the 30 s local
//! smoke, the nightly 600 s CI soak, and the 24 h Marketplace requirement
//! (`.github/workflows/soak.yml`):
//!
//! - `S4LOGS_SOAK_SECONDS` (default 60) — load duration.
//! - `S4LOGS_SOAK_RPS` (default 50) — PutLogEvents requests/second target.
//!
//! Asserts:
//! 1. **No request failures** (LocalStack and the gateway both up the whole
//!    run; any 5xx surfaces as an SDK service error and fails the run).
//! 2. **Monotonic flush counters** — `/metrics` `s4logs_flush_total` sampled
//!    every ~5 s must never decrease, and must be ≥1 by the end.
//! 3. **Durability reconciliation** — after graceful shutdown (= final
//!    flush), decoding every chunk in S3 yields exactly the per-group record
//!    counts that were acked.
//! 4. **Bounded RSS growth** — process RSS delta between the post-warmup
//!    baseline and the end of the load phase stays under a generous bound.
//!
//! Run: `./scripts/soak.sh [seconds]`, or by hand:
//! `docker compose up -d localstack && S4LOGS_SOAK_SECONDS=30 \
//!  cargo test -p s4logs-e2e --release --test soak -- --ignored --nocapture`

#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_config::{BehaviorVersion, Region};
use s4logs_core::read::RecordLines;
use s4logs_core::store::ObjectStore;
use s4logs_gateway::{Gateway, GatewayConfig, NoopCwForward, RoutingConfig};

const ACCT: &str = "123456789012";
const PREFIX: &str = "s4logs";
const GROUPS: &[&str] = &["/soak/api", "/soak/worker", "/soak/batch"];
const STREAMS: &[&str] = &["app/i-0soak0", "app/i-0soak1"];
const EVENTS_PER_REQ: usize = 10;

/// RSS growth bound between the post-warmup baseline and the end of the
/// load phase. Deliberately generous: it is a *leak* detector, not an
/// allocator-noise detector. Steady state for this process is small — the
/// gateway holds ≤ `flush_bytes` (256 KiB) of pending JSONL per
/// (group, date) buffer (3 groups ⇒ <1 MiB) plus tokio/hyper/SDK pools that
/// plateau after warmup. A real per-event leak trips this bound fast: at the
/// default 50 rps × 10 events even 16 leaked bytes/event is ~28 MiB/hour,
/// and the 24 h Marketplace run would need < ~3 bytes/event to sneak under.
const RSS_BOUND_BYTES: u64 = 256 * 1024 * 1024;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Process RSS in bytes via `/proc/self/status` `VmRSS` (kB — no page-size
/// assumption). Returns 0 where procfs is unavailable (non-Linux dev boxes),
/// which degrades the RSS assertion to a no-op rather than a false failure;
/// CI (Linux) always measures for real.
fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| {
                l.strip_prefix("VmRSS:")?
                    .trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
        })
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

/// Parse the unlabeled `s4logs_flush_total` counter out of a raw `/metrics`
/// HTTP response (Prometheus text format).
fn parse_flush_total(metrics_response: &str) -> Option<u64> {
    metrics_response.lines().find_map(|l| {
        l.strip_prefix("s4logs_flush_total ")
            .and_then(|v| v.trim().parse().ok())
    })
}

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
#[ignore = "soak test — requires LocalStack (./scripts/soak.sh)"]
async fn gateway_soak() {
    // Gateway-side flush failures are tracing::error! (sweep has no client
    // to report to) — a subscriber makes them visible in the soak log.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let soak_secs = env_u64("S4LOGS_SOAK_SECONDS", 60);
    let rps = env_u64("S4LOGS_SOAK_RPS", 50).max(1);
    let Some(ctx) = common::ctx("soak").await else {
        return;
    };
    let store = ObjectStore::new(ctx.s3.clone(), &ctx.bucket, PREFIX);

    // Small flush_bytes + short flush_interval so flushes happen continuously
    // during the soak (the flush path IS the thing being soaked).
    let gateway = Gateway::new(
        GatewayConfig {
            account: ACCT.to_owned(),
            routing: RoutingConfig::default(), // everything → s3
            flush_bytes: 256 * 1024,
            flush_interval: Duration::from_secs(5),
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
    for group in GROUPS {
        cw.create_log_group()
            .log_group_name(*group)
            .send()
            .await
            .expect("CreateLogGroup via gateway");
        for stream in STREAMS {
            cw.create_log_stream()
                .log_group_name(*group)
                .log_stream_name(*stream)
                .send()
                .await
                .expect("CreateLogStream via gateway");
        }
    }

    println!("soak: {soak_secs}s at {rps} req/s × {EVENTS_PER_REQ} events, gateway {addr}");

    // ---- sustained mixed load --------------------------------------------
    let started = Instant::now();
    let deadline = started + Duration::from_secs(soak_secs);
    let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rps as f64));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut sent_per_group = vec![0u64; GROUPS.len()];
    let mut failures: Vec<String> = Vec::new();
    let mut seq = 0u64;

    let mut flush_samples: Vec<u64> = Vec::new();
    let mut next_metrics_at = started + Duration::from_secs(5);
    // RSS baseline after a 5 s warmup (lets tokio/hyper/SDK pools, the SDK
    // connection pool, and the first flushes settle).
    let warmup_until = started + Duration::from_secs(5.min(soak_secs));
    let mut rss_baseline: Option<u64> = None;

    while Instant::now() < deadline {
        tick.tick().await;
        let g = (seq as usize) % GROUPS.len();
        let s = ((seq as usize) / GROUPS.len()) % STREAMS.len();
        let now = common::now_ms();
        let events: Vec<_> = (0..EVENTS_PER_REQ)
            .map(|i| {
                aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                    .timestamp(now)
                    .message(format!(
                        "soak g={g} s={s} seq={seq} i={i} payload={:032x}",
                        seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ i as u64
                    ))
                    .build()
                    .unwrap()
            })
            .collect();
        match cw
            .put_log_events()
            .log_group_name(GROUPS[g])
            .log_stream_name(STREAMS[s])
            .set_log_events(Some(events))
            .send()
            .await
        {
            Ok(_) => sent_per_group[g] += EVENTS_PER_REQ as u64,
            Err(e) => failures.push(format!("seq={seq} group={}: {e}", GROUPS[g])),
        }
        seq += 1;

        let now_inst = Instant::now();
        if rss_baseline.is_none() && now_inst >= warmup_until {
            rss_baseline = Some(rss_bytes());
        }
        if now_inst >= next_metrics_at {
            next_metrics_at = now_inst + Duration::from_secs(5);
            // /ready flips to 503 after a failed flush — catching it here
            // pinpoints *when* a flush failed instead of discovering the
            // loss at final count reconciliation.
            let ready = common::http_get(&addr, "/ready").await;
            assert!(
                ready.starts_with("HTTP/1.1 200"),
                "gateway not ready at +{:?} (failed flush?): {}",
                started.elapsed(),
                ready.lines().next().unwrap_or("")
            );
            let body = common::http_get(&addr, "/metrics").await;
            assert!(body.starts_with("HTTP/1.1 200"), "metrics endpoint: {body}");
            let flush_total = parse_flush_total(&body).unwrap_or(0);
            if let Some(&prev) = flush_samples.last() {
                assert!(
                    flush_total >= prev,
                    "flush counter went backwards: {prev} -> {flush_total}"
                );
            }
            flush_samples.push(flush_total);
        }
    }

    let rss_end = rss_bytes();
    let rss_base = rss_baseline.unwrap_or(rss_end);
    let total_sent: u64 = sent_per_group.iter().sum();
    println!(
        "soak: load done — {seq} requests, {total_sent} events acked, {} failures, \
         {} flush samples (last {:?}), rss {} -> {} bytes",
        failures.len(),
        flush_samples.len(),
        flush_samples.last(),
        rss_base,
        rss_end
    );

    // (1) no failed requests (covers 5xx — the SDK maps them to errors).
    assert!(
        failures.is_empty(),
        "{} failed PutLogEvents (first 5): {:#?}",
        failures.len(),
        &failures[..failures.len().min(5)]
    );
    assert!(total_sent > 0, "soak sent no events — load loop broken");

    // (2) flushes happened and the counter only ever grew (asserted online
    // sample-by-sample above; re-assert the end state here).
    let final_metrics = common::http_get(&addr, "/metrics").await;
    let final_flush = parse_flush_total(&final_metrics).unwrap_or(0);
    assert!(
        final_flush >= flush_samples.last().copied().unwrap_or(0),
        "final flush counter decreased"
    );
    assert!(
        final_flush >= 1,
        "no flush fired during a {soak_secs}s soak with a 5s flush interval"
    );

    // (4) RSS growth bound — see RSS_BOUND_BYTES rationale.
    let rss_delta = rss_end.saturating_sub(rss_base);
    assert!(
        rss_delta < RSS_BOUND_BYTES,
        "RSS grew by {rss_delta} bytes (baseline {rss_base}, end {rss_end}) — \
         exceeds the {RSS_BOUND_BYTES}-byte leak bound"
    );

    // (3) graceful shutdown flushes the tail, then every acked event must be
    // durable in S3: per-group decoded record counts == acked counts.
    //
    // KNOWN FAILURE MODE (2026-06-10, found by this test's first run): the
    // gateway's `serve_listener` calls `sweeper.abort()` before `flush_all`.
    // If the abort lands while `sweep_expired` is mid-flush — after
    // `take_matching` removed buffers from the map, before `put_chunk`
    // finished — those acked events are dropped at the cancelled await
    // point and `flush_all` never sees them (~1000 events/group at 50 rps).
    // Inserting an >flush_interval sleep before `shutdown_tx.send` makes the
    // run pass, which isolates the race to the abort. The fix belongs in
    // s4logs-gateway (cooperatively stop + await the sweeper, or make the
    // sweep's take-then-flush cancel-safe); this test intentionally stays
    // strict so the regression is visible.
    shutdown_tx.send(()).unwrap();
    server
        .await
        .expect("server task")
        .expect("gateway shutdown flush");

    for (g, group) in GROUPS.iter().enumerate() {
        let chunks = store.list_chunks(ACCT, group).await.unwrap();
        let mut durable = 0u64;
        for loc in &chunks {
            let body = store.get_bytes(&loc.data_key(PREFIX)).await.unwrap();
            let plain = zstd::stream::decode_all(&body[..]).unwrap();
            for rec in RecordLines::new(&plain) {
                let rec = rec.unwrap();
                assert!(
                    rec.message.starts_with(&format!("soak g={g} ")),
                    "foreign record in {group}: {}",
                    rec.message
                );
                durable += 1;
            }
            // Sidecars must accompany every soak chunk and stay 1:1.
            let (idx, ts) = store.load_indexes(loc).await.unwrap();
            assert_eq!(idx.entries.len(), ts.entries.len());
        }
        assert_eq!(
            durable,
            sent_per_group[g],
            "group {group}: acked {} events but {} are durable in S3 ({} chunks)",
            sent_per_group[g],
            durable,
            chunks.len()
        );
    }

    println!(
        "soak: PASS — {total_sent} events durable across {} groups, \
         {final_flush} flushes, rss delta {} KiB",
        GROUPS.len(),
        rss_delta / 1024
    );
}
