//! Shared LocalStack plumbing for the E2E suites.
//!
//! Every suite is `#[ignore]` and requires a reachable LocalStack
//! (`docker compose up -d localstack`, or point `S4LOGS_E2E_ENDPOINT`
//! somewhere else). When LocalStack is unreachable the test prints a SKIP
//! message and returns early instead of failing.

#![allow(dead_code)] // each integration-test binary uses a different subset
#![allow(clippy::unwrap_used)] // test-support code

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, Region, SdkConfig};

pub const DEFAULT_ENDPOINT: &str = "http://localhost:4566";
/// One UTC hour / day in epoch milliseconds.
pub const HOUR_MS: i64 = 3_600_000;
pub const DAY_MS: i64 = 86_400_000;

pub fn endpoint() -> String {
    std::env::var("S4LOGS_E2E_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned())
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// TCP-level reachability probe with a 2 s timeout.
pub async fn reachable(endpoint: &str) -> bool {
    let hostport = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_owned();
    matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(hostport),
        )
        .await,
        Ok(Ok(_))
    )
}

/// SdkConfig pointed at LocalStack with static test credentials.
pub async fn base_config(endpoint: &str) -> SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "test",
            "test",
            None,
            None,
            "s4logs-e2e",
        ))
        .load()
        .await
}

/// S3 client with path-style addressing (LocalStack does not resolve
/// virtual-hosted bucket DNS).
///
/// `ResponseChecksumValidation::WhenRequired`: LocalStack echoes the
/// *full-object* CRC32C checksum header on **ranged** GetObject responses,
/// so the SDK's default validation computes a checksum over the partial
/// body and fails with `ChecksumMismatch`. Real S3 omits the checksum
/// header on ranged GETs, so this is a LocalStack-only accommodation.
pub fn s3_client(cfg: &SdkConfig) -> aws_sdk_s3::Client {
    let conf = aws_sdk_s3::config::Builder::from(cfg)
        .force_path_style(true)
        .response_checksum_validation(aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

/// Per-test-run LocalStack context with a freshly created unique bucket.
pub struct Ctx {
    pub s3: aws_sdk_s3::Client,
    pub cw: aws_sdk_cloudwatchlogs::Client,
    pub bucket: String,
    pub endpoint: String,
}

/// `None` (after printing a SKIP line) when LocalStack is unreachable.
pub async fn ctx(test: &str) -> Option<Ctx> {
    let endpoint = endpoint();
    if !reachable(&endpoint).await {
        eprintln!(
            "SKIP: LocalStack not reachable at {endpoint} — start it with \
             `docker compose up -d localstack` (or set S4LOGS_E2E_ENDPOINT)"
        );
        return None;
    }
    let cfg = base_config(&endpoint).await;
    let s3 = s3_client(&cfg);
    let cw = aws_sdk_cloudwatchlogs::Client::new(&cfg);
    // Unique bucket per test run: bucket names must be lowercase and the
    // suites may run repeatedly against a long-lived LocalStack.
    let bucket = format!("s4logs-e2e-{test}-{}", now_ms());
    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create_bucket against LocalStack");
    Some(Ctx {
        s3,
        cw,
        bucket,
        endpoint,
    })
}

/// Most recent UTC midnight that is at least `margin_ms` in the past — the
/// seeded day boundary. Events seeded ±2 h around it stay "recent-ish"
/// (≤ ~26 h old, well inside the 14-day PutLogEvents acceptance window)
/// while still spanning a real `dt=` boundary.
pub fn recent_day_boundary(margin_ms: i64) -> i64 {
    let now = now_ms();
    let midnight = (now / DAY_MS) * DAY_MS;
    if now - midnight >= margin_ms {
        midnight
    } else {
        midnight - DAY_MS
    }
}

/// Create a log group + streams in LocalStack CloudWatch Logs.
pub async fn create_group_and_streams(
    cw: &aws_sdk_cloudwatchlogs::Client,
    group: &str,
    streams: &[&str],
) {
    cw.create_log_group()
        .log_group_name(group)
        .send()
        .await
        .expect("CreateLogGroup");
    for s in streams {
        cw.create_log_stream()
            .log_group_name(group)
            .log_stream_name(*s)
            .send()
            .await
            .expect("CreateLogStream");
    }
}

/// PutLogEvents one chronologically sorted batch into one stream.
pub async fn put_events(
    cw: &aws_sdk_cloudwatchlogs::Client,
    group: &str,
    stream: &str,
    events: &[(i64, String)],
) {
    let wire: Vec<_> = events
        .iter()
        .map(|(ts, msg)| {
            aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                .timestamp(*ts)
                .message(msg.clone())
                .build()
                .unwrap()
        })
        .collect();
    cw.put_log_events()
        .log_group_name(group)
        .log_stream_name(stream)
        .set_log_events(Some(wire))
        .send()
        .await
        .expect("PutLogEvents");
}

/// Minimal HTTP/1.1 GET over a raw TcpStream (the e2e crate has no HTTP
/// client dependency). Returns the full response (status line + headers +
/// body) as a string.
pub async fn http_get(addr: &str, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect for http_get");
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("write GET");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read response");
    String::from_utf8_lossy(&buf).into_owned()
}

/// All S3 keys in `bucket` under `prefix`, paginated to exhaustion and
/// sorted (used for before/after idempotency snapshots).
pub async fn list_all_keys(s3: &aws_sdk_s3::Client, bucket: &str, prefix: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = s3
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .set_continuation_token(token.take())
            .send()
            .await
            .expect("ListObjectsV2");
        keys.extend(
            page.contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_owned)),
        );
        token = page.next_continuation_token().map(str::to_owned);
        if token.is_none() {
            break;
        }
    }
    keys.sort();
    keys
}
