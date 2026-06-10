//! Wave 1C integration tests — MemorySink + mock `CwForward`, requests via
//! `tower::ServiceExt::oneshot` (no socket except the shutdown test, which
//! exercises the real `serve_listener` + graceful flush path).

#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode, header};
use s4logs_core::record::LogRecord;
use s4logs_core::sink::MemorySink;
use s4logs_gateway::api::InputLogEvent;
use s4logs_gateway::forward::{CwForward, ForwardError};
use s4logs_gateway::{Gateway, GatewayConfig, RoutingConfig};
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
    let mut cfg = GatewayConfig {
        routing: RoutingConfig::from_toml_str(routing).unwrap(),
        ..GatewayConfig::default()
    };
    tweak(&mut cfg);
    let sink = Arc::new(MemorySink::new("s4logs"));
    let cw = Arc::new(MockCw::default());
    let gateway = Gateway::new(cfg, sink.clone(), cw.clone());
    (gateway, sink, cw)
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
