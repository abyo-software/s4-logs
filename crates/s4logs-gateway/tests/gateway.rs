//! Wave 1C integration tests — MemorySink + mock `CwForward`, requests via
//! `tower::ServiceExt::oneshot` (no socket except the shutdown test, which
//! exercises the real `serve_listener` + graceful flush path).
//!
//! Wave 3F additions: WAL crash/replay, SigV4 verification end-to-end
//! against the official `aws-sigv4` signer, Describe* pagination, and
//! memory-cap backpressure.

#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode, header};
use s4logs_core::chunk::EncodedChunk;
use s4logs_core::layout::ChunkLocation;
use s4logs_core::record::LogRecord;
use s4logs_core::sink::{ChunkSink, MemorySink, PutReceipt, SinkError};
use s4logs_gateway::api::InputLogEvent;
use s4logs_gateway::forward::{CwForward, ForwardError};
use s4logs_gateway::{AuthMode, Gateway, GatewayConfig, GatewaySink, RoutingConfig};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

// ---------------------------------------------------------------- fixtures

/// Recording mock of the CloudWatch passthrough.
#[derive(Default)]
struct MockCw {
    fail: AtomicBool,
    puts: Mutex<Vec<(String, String, Vec<InputLogEvent>)>>,
    groups: Mutex<Vec<String>>,
    streams: Mutex<Vec<(String, String)>>,
}

impl MockCw {
    fn check(&self, op: &'static str) -> Result<(), ForwardError> {
        if self.fail.load(Ordering::Relaxed) {
            Err(ForwardError::Api {
                op,
                message: "injected failure".to_owned(),
            })
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl CwForward for MockCw {
    async fn put_log_events(
        &self,
        log_group: &str,
        log_stream: &str,
        events: &[InputLogEvent],
    ) -> Result<(), ForwardError> {
        self.check("PutLogEvents")?;
        self.puts.lock().unwrap().push((
            log_group.to_owned(),
            log_stream.to_owned(),
            events.to_vec(),
        ));
        Ok(())
    }

    async fn create_log_group(&self, log_group: &str) -> Result<(), ForwardError> {
        self.check("CreateLogGroup")?;
        self.groups.lock().unwrap().push(log_group.to_owned());
        Ok(())
    }

    async fn create_log_stream(
        &self,
        log_group: &str,
        log_stream: &str,
    ) -> Result<(), ForwardError> {
        self.check("CreateLogStream")?;
        self.streams
            .lock()
            .unwrap()
            .push((log_group.to_owned(), log_stream.to_owned()));
        Ok(())
    }
}

fn gw(
    routing: &str,
    tweak: impl FnOnce(&mut GatewayConfig),
) -> (Gateway, Arc<MemorySink>, Arc<MockCw>) {
    let sink = Arc::new(MemorySink::new("s4logs"));
    let gateway_sink: Arc<dyn GatewaySink> = sink.clone();
    let (gateway, cw) = gw_with_sink(routing, gateway_sink, tweak);
    (gateway, sink, cw)
}

fn gw_with_sink(
    routing: &str,
    sink: Arc<dyn GatewaySink>,
    tweak: impl FnOnce(&mut GatewayConfig),
) -> (Gateway, Arc<MockCw>) {
    let mut cfg = GatewayConfig {
        routing: RoutingConfig::from_toml_str(routing).unwrap(),
        ..GatewayConfig::default()
    };
    tweak(&mut cfg);
    let cw = Arc::new(MockCw::default());
    let gateway = Gateway::new(cfg, sink, cw.clone());
    (gateway, cw)
}

/// Sink whose every `put_chunk` fails — keeps data "unflushable" so WAL
/// recovery paths can be exercised.
struct FailSink;

#[async_trait]
impl ChunkSink for FailSink {
    fn key_prefix(&self) -> &str {
        ""
    }

    async fn put_chunk(
        &self,
        _loc: &ChunkLocation,
        _chunk: &EncodedChunk,
    ) -> Result<PutReceipt, SinkError> {
        Err(SinkError::Storage("injected sink failure".to_owned()))
    }
}

#[async_trait]
impl GatewaySink for FailSink {}

fn wal_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|x| x == "wal"))
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

fn amz_req(action: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/")
        .header("x-amz-target", format!("Logs_20140328.{action}"))
        .header(header::CONTENT_TYPE, "application/x-amz-json-1.1")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn body_json(resp: Response<Body>) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn put_body(group: &str, stream: &str, events: &[(i64, &str)]) -> Value {
    json!({
        "logGroupName": group,
        "logStreamName": stream,
        "logEvents": events
            .iter()
            .map(|(ts, msg)| json!({"timestamp": ts, "message": msg}))
            .collect::<Vec<_>>(),
    })
}

fn data_keys(sink: &MemorySink) -> Vec<String> {
    sink.keys()
        .into_iter()
        .filter(|k| k.ends_with(".jsonl.zst"))
        .collect()
}

fn decode_records(sink: &MemorySink, key: &str) -> Vec<LogRecord> {
    let body = sink.get(key).unwrap();
    let raw = zstd::stream::decode_all(&body[..]).unwrap();
    raw.split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| LogRecord::from_jsonl(l).unwrap())
        .collect()
}

// 2026-06-10T00:00:00Z (verified against `date -u`).
const JUN10: i64 = 1_781_049_600_000;

// ------------------------------------------------------------------- tests

#[tokio::test]
async fn put_log_events_buffers_then_flush_writes_layout_keys() {
    let (gateway, sink, cw) = gw("", |_| {});
    let app = gateway.app();

    let resp = app
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body(
                "/aws/lambda/foo",
                "app/i-0abc",
                &[(JUN10 + 123, "hello"), (JUN10 + 456, "world")],
            ),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/x-amz-json-1.1"
    );
    assert_eq!(body_json(resp).await, json!({}));

    assert!(
        sink.keys().is_empty(),
        "below thresholds — flush is deferred"
    );
    gateway.buffers().flush_all().await.unwrap();

    let data = data_keys(&sink);
    assert_eq!(data.len(), 1, "{data:?}");
    let key = &data[0];
    assert!(
        key.starts_with(
            "s4logs/data/account=000000000000/loggroup=%2Faws%2Flambda%2Ffoo/dt=2026-06-10/1781049600123-"
        ),
        "unexpected key {key}"
    );
    // Both sidecars, written under index/ (write-after-data is core's job).
    assert!(
        sink.keys()
            .iter()
            .any(|k| k.ends_with(".jsonl.zst.s4index"))
    );
    assert!(sink.keys().iter().any(|k| k.ends_with(".jsonl.zst.s4lts")));

    let records = decode_records(&sink, key);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].stream, "app/i-0abc");
    assert_eq!(records[0].message, "hello");
    assert_eq!(records[1].timestamp, JUN10 + 456);

    // Default route is s3-only: nothing forwarded.
    assert!(cw.puts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn routing_both_buffers_and_forwards_preserving_stream() {
    let (gateway, sink, cw) = gw(
        "default_action = \"drop\"\n[[rule]]\nlog_group = \"/g\"\naction = \"both\"\n",
        |_| {},
    );
    let resp = gateway
        .app()
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body("/g", "s1", &[(JUN10, "m")]),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let puts = cw.puts.lock().unwrap().clone();
    assert_eq!(puts.len(), 1);
    assert_eq!(puts[0].0, "/g");
    assert_eq!(puts[0].1, "s1", "stream must be preserved on passthrough");
    assert_eq!(
        puts[0].2,
        vec![InputLogEvent {
            timestamp: JUN10,
            message: "m".into()
        }]
    );

    gateway.buffers().flush_all().await.unwrap();
    assert_eq!(data_keys(&sink).len(), 1, "both must also buffer to s3");
}

#[tokio::test]
async fn routing_drop_neither_buffers_nor_forwards() {
    let (gateway, sink, cw) = gw("[[rule]]\nlog_group = \"/g\"\naction = \"drop\"\n", |_| {});
    let resp = gateway
        .app()
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body("/g", "s", &[(JUN10, "m")]),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "drop still ACKs the agent");
    gateway.buffers().flush_all().await.unwrap();
    assert!(data_keys(&sink).is_empty());
    assert!(cw.puts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn flush_bytes_threshold_triggers_inline_flush() {
    let (gateway, sink, _cw) = gw("", |cfg| cfg.flush_bytes = 1024);
    let events: Vec<(i64, String)> = (0..30)
        .map(|i| (JUN10 + i, format!("a padded message body number {i:05}")))
        .collect();
    let events_ref: Vec<(i64, &str)> = events.iter().map(|(t, m)| (*t, m.as_str())).collect();
    let resp = gateway
        .app()
        .oneshot(amz_req("PutLogEvents", &put_body("/g", "s", &events_ref)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        data_keys(&sink).len(),
        1,
        "size threshold must flush without flush_all/sweep"
    );
}

#[tokio::test]
async fn date_split_across_utc_midnight_produces_two_objects() {
    let (gateway, sink, _cw) = gw("", |_| {});
    let resp = gateway
        .app()
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body(
                "/g",
                "s",
                &[(JUN10 - 1, "before midnight"), (JUN10, "after midnight")],
            ),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    gateway.buffers().flush_all().await.unwrap();

    let data = data_keys(&sink);
    assert_eq!(data.len(), 2, "{data:?}");
    let jun09 = data.iter().find(|k| k.contains("/dt=2026-06-09/")).unwrap();
    let jun10 = data.iter().find(|k| k.contains("/dt=2026-06-10/")).unwrap();
    assert_eq!(decode_records(&sink, jun09)[0].message, "before midnight");
    assert_eq!(decode_records(&sink, jun10)[0].message, "after midnight");
}

#[tokio::test]
async fn cw_failure_with_both_never_fails_client() {
    let (gateway, sink, cw) = gw("[[rule]]\nlog_group = \"/g\"\naction = \"both\"\n", |_| {});
    cw.fail.store(true, Ordering::Relaxed);
    let resp = gateway
        .app()
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body("/g", "s", &[(JUN10, "m")]),
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "both: s3 buffer holds the batch — forward failure must not fail the client"
    );
    gateway.buffers().flush_all().await.unwrap();
    assert_eq!(data_keys(&sink).len(), 1);
}

#[tokio::test]
async fn cw_failure_on_pure_passthrough_fails_client_with_500() {
    let (gateway, _sink, cw) = gw(
        "[[rule]]\nlog_group = \"/g\"\naction = \"cloudwatch\"\n",
        |_| {},
    );
    cw.fail.store(true, Ordering::Relaxed);
    let resp = gateway
        .app()
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body("/g", "s", &[(JUN10, "m")]),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = body_json(resp).await;
    assert_eq!(body["__type"], "ServiceUnavailableException");
}

#[tokio::test]
async fn unknown_action_returns_400_invalid_action_shape() {
    let (gateway, _sink, _cw) = gw("", |_| {});
    let resp = gateway
        .app()
        .oneshot(amz_req("GetLogEvents", &json!({})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(body["__type"], "InvalidAction");
    assert!(body["message"].as_str().unwrap().contains("GetLogEvents"));
}

#[tokio::test]
async fn missing_target_and_malformed_body_error_shapes() {
    let (gateway, _sink, _cw) = gw("", |_| {});
    let app = gateway.app();

    let no_target = Request::builder()
        .method("POST")
        .uri("/")
        .body(Body::from("{}"))
        .unwrap();
    let resp = app.clone().oneshot(no_target).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(body_json(resp).await["__type"], "MissingAction");

    let bad = Request::builder()
        .method("POST")
        .uri("/")
        .header("x-amz-target", "Logs_20140328.PutLogEvents")
        .body(Body::from("{not json"))
        .unwrap();
    let resp = app.oneshot(bad).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(body_json(resp).await["__type"], "SerializationException");
}

#[tokio::test]
async fn registry_create_describe_and_duplicate_errors() {
    let (gateway, _sink, cw) = gw(
        "[[rule]]\nlog_group = \"/cw/*\"\naction = \"cloudwatch\"\n",
        |_| {},
    );
    let app = gateway.app();
    let group = json!({"logGroupName": "/cw/api"});

    let resp = app
        .clone()
        .oneshot(amz_req("CreateLogGroup", &group))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await, json!({}));
    // Group routed to cloudwatch → create forwarded.
    assert_eq!(
        cw.groups.lock().unwrap().clone(),
        vec!["/cw/api".to_owned()]
    );

    let resp = app
        .clone()
        .oneshot(amz_req("CreateLogGroup", &group))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_json(resp).await["__type"],
        "ResourceAlreadyExistsException"
    );

    let stream = json!({"logGroupName": "/cw/api", "logStreamName": "s1"});
    let resp = app
        .clone()
        .oneshot(amz_req("CreateLogStream", &stream))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        cw.streams.lock().unwrap().clone(),
        vec![("/cw/api".to_owned(), "s1".to_owned())]
    );
    let resp = app
        .clone()
        .oneshot(amz_req("CreateLogStream", &stream))
        .await
        .unwrap();
    assert_eq!(
        body_json(resp).await["__type"],
        "ResourceAlreadyExistsException"
    );

    // Stream into a group that was never created → ResourceNotFoundException.
    let resp = app
        .clone()
        .oneshot(amz_req(
            "CreateLogStream",
            &json!({"logGroupName": "/missing", "logStreamName": "s"}),
        ))
        .await
        .unwrap();
    assert_eq!(body_json(resp).await["__type"], "ResourceNotFoundException");

    let resp = app
        .clone()
        .oneshot(amz_req(
            "DescribeLogGroups",
            &json!({"logGroupNamePrefix": "/cw/"}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["logGroups"][0]["logGroupName"], "/cw/api");
    assert!(body["logGroups"][0]["creationTime"].is_i64());

    let resp = app
        .clone()
        .oneshot(amz_req(
            "DescribeLogStreams",
            &json!({"logGroupName": "/cw/api"}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["logStreams"][0]["logStreamName"], "s1");

    let resp = app
        .oneshot(amz_req(
            "DescribeLogStreams",
            &json!({"logGroupName": "/missing"}),
        ))
        .await
        .unwrap();
    assert_eq!(body_json(resp).await["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn health_ready_metrics_endpoints() {
    let (gateway, _sink, _cw) = gw("", |_| {});
    let app = gateway.app();
    let get = |path: &str| {
        Request::builder()
            .method("GET")
            .uri(path)
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        app.clone().oneshot(get("/health")).await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone().oneshot(get("/ready")).await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        app.oneshot(get("/metrics")).await.unwrap().status(),
        StatusCode::OK
    );
}

/// Real socket + real `serve_listener`: a buffered batch must be flushed to
/// the sink as part of graceful shutdown (DESIGN.md §8.3 trigger 3).
#[tokio::test]
async fn graceful_shutdown_flushes_buffers() {
    let (gateway, sink, _cw) = gw("", |_| {});
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(gateway.serve_listener(listener, async move {
        rx.await.ok();
    }));

    let body = put_body("/g", "s", &[(JUN10 + 5, "survives shutdown")]).to_string();
    let raw = format!(
        "POST / HTTP/1.1\r\nhost: localhost\r\nx-amz-target: Logs_20140328.PutLogEvents\r\n\
         content-type: application/x-amz-json-1.1\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let mut conn = tokio::net::TcpStream::connect(addr).await.unwrap();
    conn.write_all(raw.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    conn.read_to_end(&mut resp).await.unwrap();
    assert!(
        resp.starts_with(b"HTTP/1.1 200"),
        "unexpected response: {}",
        String::from_utf8_lossy(&resp)
    );
    assert!(sink.keys().is_empty(), "below thresholds — still buffered");

    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let data = data_keys(&sink);
    assert_eq!(data.len(), 1, "graceful shutdown must flush all buffers");
    assert_eq!(
        decode_records(&sink, &data[0])[0].message,
        "survives shutdown"
    );
}

// -------------------------------------------------------------- WAL (§11.1)

#[tokio::test]
async fn wal_unflushed_events_survive_restart_via_replay() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().to_owned();

    // "Crash": gateway dropped with events still buffered (below thresholds).
    {
        let (gateway, sink, _cw) = gw("", |cfg| cfg.wal_dir = Some(wal_dir.clone()));
        let resp = gateway
            .app()
            .oneshot(amz_req(
                "PutLogEvents",
                &put_body("/g", "s", &[(JUN10 + 1, "first"), (JUN10 + 2, "second")]),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(sink.keys().is_empty(), "nothing flushed before the crash");
        assert_eq!(wal_files(&wal_dir).len(), 1, "one segment per buffer key");
    }

    // Restart over the same wal_dir: replay → buffers → flush → sink.
    let (gateway, sink, _cw) = gw("", |cfg| cfg.wal_dir = Some(wal_dir.clone()));
    assert_eq!(gateway.buffers().replay_wal().await.unwrap(), 2);
    gateway.buffers().flush_all().await.unwrap();
    let data = data_keys(&sink);
    assert_eq!(data.len(), 1, "{data:?}");
    let records = decode_records(&sink, &data[0]);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].message, "first");
    assert_eq!(records[1].message, "second");
    assert!(wal_files(&wal_dir).is_empty(), "flush retires the segments");
}

#[tokio::test]
async fn wal_replay_runs_in_serve_listener_before_serving() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().to_owned();
    {
        let (gateway, _sink, _cw) = gw("", |cfg| cfg.wal_dir = Some(wal_dir.clone()));
        let resp = gateway
            .app()
            .oneshot(amz_req(
                "PutLogEvents",
                &put_body("/g", "s", &[(JUN10 + 7, "replayed by serve")]),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    assert_eq!(wal_files(&wal_dir).len(), 1);

    // Second gateway runs the real serve path: replay happens before the
    // listener serves; graceful shutdown then flushes the replayed buffer.
    let (gateway, sink, _cw) = gw("", |cfg| cfg.wal_dir = Some(wal_dir.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(gateway.serve_listener(listener, async move {
        rx.await.ok();
    }));
    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let data = data_keys(&sink);
    assert_eq!(data.len(), 1, "{data:?}");
    assert_eq!(
        decode_records(&sink, &data[0])[0].message,
        "replayed by serve"
    );
    assert!(wal_files(&wal_dir).is_empty());
}

#[tokio::test]
async fn wal_torn_tail_is_skipped_on_replay() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().to_owned();
    {
        let (gateway, _sink, _cw) = gw("", |cfg| cfg.wal_dir = Some(wal_dir.clone()));
        let resp = gateway
            .app()
            .oneshot(amz_req(
                "PutLogEvents",
                &put_body(
                    "/g",
                    "s",
                    &[(JUN10 + 1, "intact-1"), (JUN10 + 2, "intact-2")],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    // Simulate a torn write at the tail of the surviving segment.
    let files = wal_files(&wal_dir);
    assert_eq!(files.len(), 1);
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&files[0])
            .unwrap();
        f.write_all(b"{\"log_group\":\"/g\",\"timestamp\":178")
            .unwrap();
    }

    let (gateway, sink, _cw) = gw("", |cfg| cfg.wal_dir = Some(wal_dir.clone()));
    assert_eq!(
        gateway.buffers().replay_wal().await.unwrap(),
        2,
        "torn tail line skipped, intact lines replayed"
    );
    gateway.buffers().flush_all().await.unwrap();
    let data = data_keys(&sink);
    let records = decode_records(&sink, &data[0]);
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn wal_failed_flush_keeps_segment_for_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().to_owned();
    {
        let (gateway, _cw) = gw_with_sink("", Arc::new(FailSink), |cfg| {
            cfg.wal_dir = Some(wal_dir.clone());
        });
        let resp = gateway
            .app()
            .oneshot(amz_req(
                "PutLogEvents",
                &put_body("/g", "s", &[(JUN10 + 9, "stuck in wal")]),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "accept is pre-flush");
        gateway.buffers().flush_all().await.unwrap_err();
        assert_eq!(
            wal_files(&wal_dir).len(),
            1,
            "failed flush must keep the segment on disk"
        );
    }
    // Restart with a healthy sink: the events the failed flush dropped from
    // memory come back from the WAL.
    let (gateway, sink, _cw) = gw("", |cfg| cfg.wal_dir = Some(wal_dir.clone()));
    assert_eq!(gateway.buffers().replay_wal().await.unwrap(), 1);
    gateway.buffers().flush_all().await.unwrap();
    let data = data_keys(&sink);
    assert_eq!(decode_records(&sink, &data[0])[0].message, "stuck in wal");
    assert!(wal_files(&wal_dir).is_empty());
}

#[tokio::test]
async fn wal_write_failure_fails_request_with_500_internal_failure() {
    // wal_dir pointing at a regular file: segment creation must fail.
    let dir = tempfile::tempdir().unwrap();
    let not_a_dir = dir.path().join("file");
    std::fs::write(&not_a_dir, b"x").unwrap();
    let (gateway, sink, _cw) = gw("", |cfg| cfg.wal_dir = Some(not_a_dir));
    let resp = gateway
        .app()
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body("/g", "s", &[(JUN10, "m")]),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body_json(resp).await["__type"], "InternalFailure");
    gateway.buffers().flush_all().await.unwrap();
    assert!(
        data_keys(&sink).is_empty(),
        "wal-before-memory: nothing may be buffered when the wal append failed"
    );
}

// ------------------------------------------------------------ SigV4 (§11.2)

const AK: &str = "AKIAGATEWAYTEST";
const SK: &str = "gateway-test-secret-key/with+chars";

fn sigv4_gw(routing: &str) -> (Gateway, Arc<MemorySink>, Arc<MockCw>) {
    gw(routing, |cfg| {
        cfg.auth = AuthMode::SigV4 {
            access_key: AK.to_owned(),
            secret_key: SK.to_owned(),
        }
    })
}

/// Sign `body_signed` with the official AWS signer, then send `body_sent`
/// (they differ only in the tamper test).
fn signed_req(
    action: &str,
    body_signed: &str,
    body_sent: &str,
    access_key: &str,
    time: SystemTime,
    unsigned_payload: bool,
) -> Request<Body> {
    use aws_sigv4::http_request::{
        PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings, sign,
    };
    use aws_sigv4::sign::v4;

    let identity =
        aws_credential_types::Credentials::new(access_key, SK, None, None, "test").into();
    let mut settings = SigningSettings::default();
    if unsigned_payload {
        // Make the signer emit (and sign) `x-amz-content-sha256: UNSIGNED-PAYLOAD`.
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    }
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("logs")
        .time(time)
        .settings(settings)
        .build()
        .unwrap()
        .into();
    let target = format!("Logs_20140328.{action}");
    let headers = [
        ("host", "localhost"),
        ("x-amz-target", target.as_str()),
        ("content-type", "application/x-amz-json-1.1"),
    ];
    let signable_body = if unsigned_payload {
        SignableBody::UnsignedPayload
    } else {
        SignableBody::Bytes(body_signed.as_bytes())
    };
    let signable = SignableRequest::new(
        "POST",
        "http://localhost/",
        headers.iter().copied(),
        signable_body,
    )
    .unwrap();
    let (instructions, _sig) = sign(signable, &params).unwrap().into_parts();
    let mut req = Request::builder()
        .method("POST")
        .uri("/")
        .header("host", "localhost")
        .header("x-amz-target", &target)
        .header(header::CONTENT_TYPE, "application/x-amz-json-1.1")
        .body(Body::from(body_sent.to_owned()))
        .unwrap();
    instructions.apply_to_request_http1x(&mut req);
    req
}

#[tokio::test]
async fn sigv4_valid_signature_accepted_and_events_buffered() {
    let (gateway, sink, _cw) = sigv4_gw("");
    let body = put_body("/g", "s", &[(JUN10, "signed hello")]).to_string();
    let resp = gateway
        .app()
        .oneshot(signed_req(
            "PutLogEvents",
            &body,
            &body,
            AK,
            SystemTime::now(),
            false,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await, json!({}));
    gateway.buffers().flush_all().await.unwrap();
    let data = data_keys(&sink);
    assert_eq!(decode_records(&sink, &data[0])[0].message, "signed hello");
}

#[tokio::test]
async fn sigv4_unsigned_payload_accepted() {
    let (gateway, sink, _cw) = sigv4_gw("");
    let body = put_body("/g", "s", &[(JUN10, "unsigned payload")]).to_string();
    let resp = gateway
        .app()
        .oneshot(signed_req(
            "PutLogEvents",
            &body,
            &body,
            AK,
            SystemTime::now(),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    gateway.buffers().flush_all().await.unwrap();
    assert_eq!(data_keys(&sink).len(), 1);
}

#[tokio::test]
async fn sigv4_tampered_body_rejected() {
    let (gateway, sink, _cw) = sigv4_gw("");
    let signed = put_body("/g", "s", &[(JUN10, "original")]).to_string();
    let sent = put_body("/g", "s", &[(JUN10, "tampered")]).to_string();
    let resp = gateway
        .app()
        .oneshot(signed_req(
            "PutLogEvents",
            &signed,
            &sent,
            AK,
            SystemTime::now(),
            false,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(body_json(resp).await["__type"], "InvalidSignatureException");
    gateway.buffers().flush_all().await.unwrap();
    assert!(
        data_keys(&sink).is_empty(),
        "rejected batch must not buffer"
    );
}

#[tokio::test]
async fn sigv4_expired_date_rejected() {
    let (gateway, _sink, _cw) = sigv4_gw("");
    let body = put_body("/g", "s", &[(JUN10, "old")]).to_string();
    let resp = gateway
        .app()
        .oneshot(signed_req(
            "PutLogEvents",
            &body,
            &body,
            AK,
            SystemTime::now() - Duration::from_secs(16 * 60),
            false,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let body = body_json(resp).await;
    assert_eq!(body["__type"], "InvalidSignatureException");
    assert!(body["message"].as_str().unwrap().contains("skew"));
}

#[tokio::test]
async fn sigv4_wrong_access_key_rejected() {
    let (gateway, _sink, _cw) = sigv4_gw("");
    let body = put_body("/g", "s", &[(JUN10, "m")]).to_string();
    let resp = gateway
        .app()
        .oneshot(signed_req(
            "PutLogEvents",
            &body,
            &body,
            "AKIASOMEONEELSE",
            SystemTime::now(),
            false,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(body_json(resp).await["__type"], "InvalidSignatureException");
}

#[tokio::test]
async fn sigv4_missing_authorization_rejected_health_exempt() {
    let (gateway, _sink, _cw) = sigv4_gw("");
    let app = gateway.app();

    let resp = app
        .clone()
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body("/g", "s", &[(JUN10, "m")]),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        body_json(resp).await["__type"],
        "MissingAuthenticationTokenException"
    );

    // /health /ready /metrics stay exempt (DESIGN.md §11.2).
    for path in ["/health", "/ready", "/metrics"] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{path} must skip auth");
    }
}

// ------------------------------------------------------ Describe* pagination

#[tokio::test]
async fn describe_log_groups_paginates_with_limit_and_token() {
    let (gateway, _sink, _cw) = gw("", |_| {});
    let app = gateway.app();
    for name in ["/pg/a", "/pg/b", "/pg/c", "/pg/d", "/pg/e", "/other"] {
        let resp = app
            .clone()
            .oneshot(amz_req("CreateLogGroup", &json!({"logGroupName": name})))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut req = json!({"logGroupNamePrefix": "/pg/", "limit": 2});
        if let Some(t) = &token {
            req["nextToken"] = json!(t);
        }
        let resp = app
            .clone()
            .oneshot(amz_req("DescribeLogGroups", &req))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let groups = body["logGroups"].as_array().unwrap();
        assert!(groups.len() <= 2);
        for g in groups {
            seen.push(g["logGroupName"].as_str().unwrap().to_owned());
        }
        pages += 1;
        match body.get("nextToken").and_then(Value::as_str) {
            Some(t) => token = Some(t.to_owned()),
            None => break,
        }
    }
    assert_eq!(pages, 3, "5 matches at limit=2 → 2+2+1");
    assert_eq!(seen, vec!["/pg/a", "/pg/b", "/pg/c", "/pg/d", "/pg/e"]);

    // Invalid token shape → 400 InvalidParameterException.
    let resp = app
        .clone()
        .oneshot(amz_req(
            "DescribeLogGroups",
            &json!({"nextToken": "not-a-token"}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(body_json(resp).await["__type"], "InvalidParameterException");
}

#[tokio::test]
async fn describe_log_streams_paginates_and_filters() {
    let (gateway, _sink, _cw) = gw("", |_| {});
    let app = gateway.app();
    let resp = app
        .clone()
        .oneshot(amz_req("CreateLogGroup", &json!({"logGroupName": "/pg"})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    for s in ["app-1", "app-2", "app-3", "audit-1"] {
        let resp = app
            .clone()
            .oneshot(amz_req(
                "CreateLogStream",
                &json!({"logGroupName": "/pg", "logStreamName": s}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let resp = app
        .clone()
        .oneshot(amz_req(
            "DescribeLogStreams",
            &json!({"logGroupName": "/pg", "logStreamNamePrefix": "app-", "limit": 2}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let names: Vec<&str> = body["logStreams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["logStreamName"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["app-1", "app-2"]);
    let token = body["nextToken"].as_str().unwrap().to_owned();

    let resp = app
        .clone()
        .oneshot(amz_req(
            "DescribeLogStreams",
            &json!({
                "logGroupName": "/pg",
                "logStreamNamePrefix": "app-",
                "limit": 2,
                "nextToken": token,
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let names: Vec<&str> = body["logStreams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["logStreamName"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["app-3"]);
    assert!(body.get("nextToken").is_none(), "last page has no token");
}

// ------------------------------------------------- memory cap / backpressure

#[tokio::test]
async fn memory_cap_flushes_largest_then_returns_503() {
    let (gateway, sink, _cw) = gw("", |cfg| cfg.max_buffered_bytes = 1000);
    let app = gateway.app();

    // Fits under the cap: buffered, nothing flushed.
    let msg1 = "x".repeat(600);
    let resp = app
        .clone()
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body("/g", "s", &[(JUN10, &msg1)]),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(data_keys(&sink).is_empty());

    // Would exceed the cap: largest (only) buffer is force-flushed, batch
    // accepted.
    let msg2 = "y".repeat(600);
    let resp = app
        .clone()
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body("/g", "s", &[(JUN10 + 1, &msg2)]),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(data_keys(&sink).len(), 1, "largest buffer force-flushed");

    // A batch bigger than the whole cap: still over after flushing → 503.
    let huge = "z".repeat(1200);
    let resp = app
        .clone()
        .oneshot(amz_req(
            "PutLogEvents",
            &put_body("/g", "s", &[(JUN10 + 2, &huge)]),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        body_json(resp).await["__type"],
        "ServiceUnavailableException"
    );
    assert_eq!(
        data_keys(&sink).len(),
        2,
        "the pre-503 emergency flush still ran"
    );
}
