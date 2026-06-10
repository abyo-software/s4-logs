//! CloudWatch Logs access layer (DESIGN.md §7).
//!
//! All CW reads/writes go through the [`CwSource`] trait so the drain logic
//! is unit-testable with a scripted mock. [`AwsCwSource`] is the real
//! implementation over `aws_sdk_cloudwatchlogs::Client`, with explicit
//! exponential backoff + deterministic jitter on `ThrottlingException`
//! (FilterLogEvents quota is account-wide and the SDK's built-in retry is
//! too shallow for sustained drains).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_cloudwatchlogs::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use thiserror::Error;

/// One CloudWatch log event as returned by `FilterLogEvents`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CwEvent {
    /// Event timestamp, epoch milliseconds.
    pub timestamp: i64,
    pub message: String,
    pub log_stream_name: String,
    pub ingestion_time: Option<i64>,
    pub event_id: Option<String>,
}

/// One page of `FilterLogEvents` output.
#[derive(Debug, Clone, Default)]
pub struct CwEventPage {
    pub events: Vec<CwEvent>,
    /// Opaque continuation token; `None` means the range is exhausted.
    pub next_token: Option<String>,
}

/// Subset of `DescribeLogGroups` output the drain needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogGroupInfo {
    pub retention_days: Option<i32>,
    pub stored_bytes: Option<i64>,
    /// Log group creation time, epoch milliseconds (drain `--from` default).
    pub creation_time_ms: i64,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CwError {
    #[error(
        "CloudWatch {op} for {log_group:?} still throttled after {attempts} attempts: {message}"
    )]
    Throttled {
        op: &'static str,
        log_group: String,
        attempts: u32,
        message: String,
    },
    #[error("CloudWatch {op} failed for {log_group:?}: {message}")]
    Api {
        op: &'static str,
        log_group: String,
        message: String,
    },
    #[error("log group not found: {log_group:?}")]
    GroupNotFound { log_group: String },
    #[error("CloudWatch returned a malformed {what} for {log_group:?}")]
    Malformed {
        what: &'static str,
        log_group: String,
    },
}

/// Abstracts CloudWatch Logs reads + the retention write (DESIGN.md §7:
/// "CW client は trait で抽象化し、unit test は mock").
#[async_trait]
pub trait CwSource: Send + Sync {
    /// One page of events with `start_ms <= timestamp < end_ms_exclusive`,
    /// interleaved across streams. Pass the previous page's `next_token` to
    /// continue; `None` starts from the beginning of the range.
    async fn filter_log_events(
        &self,
        log_group: &str,
        start_ms: i64,
        end_ms_exclusive: i64,
        next_token: Option<&str>,
    ) -> Result<CwEventPage, CwError>;

    async fn describe_log_group(&self, log_group: &str) -> Result<LogGroupInfo, CwError>;

    /// Every log group in the account (DESIGN.md §11.4), optionally narrowed
    /// server-side by a name prefix, paginated to exhaustion. Listing entries
    /// without a name or creation time are skipped with a warning — one
    /// malformed entry must not kill a multi-group drain.
    async fn list_log_groups(
        &self,
        prefix_hint: Option<&str>,
    ) -> Result<Vec<(String, LogGroupInfo)>, CwError>;

    async fn put_retention_policy(
        &self,
        log_group: &str,
        retention_days: i32,
    ) -> Result<(), CwError>;
}

/// Exponential backoff parameters for throttling retries.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// First retry delay scale (milliseconds).
    pub base_ms: u64,
    /// Maximum single delay (milliseconds).
    pub cap_ms: u64,
    /// Total attempts (initial call included) before surfacing
    /// [`CwError::Throttled`].
    pub max_attempts: u32,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base_ms: 200,
            cap_ms: 10_000,
            max_attempts: 8,
        }
    }
}

/// Deterministic jitter without a `rand` dependency: the delay for retry
/// `attempt` lands in `[exp/2, exp]` where `exp = min(base * 2^(attempt-1),
/// cap)`, with the position inside the range derived from a hash of
/// `(key, attempt)`. Different log groups desynchronize; the same call is
/// reproducible.
fn delay_ms(cfg: &BackoffConfig, attempt: u32, key: &str) -> u64 {
    let shift = attempt.saturating_sub(1).min(20);
    let exp = cfg
        .base_ms
        .saturating_mul(1u64 << shift)
        .min(cfg.cap_ms)
        .max(1);
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    attempt.hash(&mut h);
    exp / 2 + h.finish() % (exp / 2 + 1)
}

/// Outcome classification for one SDK call inside the retry loop.
pub(crate) enum CallFailure {
    /// Throttled — eligible for backoff + retry.
    Throttled(String),
    /// Anything else — surface immediately.
    Fatal(String),
}

/// Run `call` with throttling-aware exponential backoff. `call` is invoked
/// at most `cfg.max_attempts` times; only [`CallFailure::Throttled`] results
/// are retried.
pub(crate) async fn with_backoff<T, F, Fut>(
    cfg: &BackoffConfig,
    op: &'static str,
    log_group: &str,
    mut call: F,
) -> Result<T, CwError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, CallFailure>>,
{
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        match call().await {
            Ok(v) => return Ok(v),
            Err(CallFailure::Fatal(message)) => {
                return Err(CwError::Api {
                    op,
                    log_group: log_group.to_owned(),
                    message,
                });
            }
            Err(CallFailure::Throttled(message)) => {
                if attempts >= cfg.max_attempts {
                    return Err(CwError::Throttled {
                        op,
                        log_group: log_group.to_owned(),
                        attempts,
                        message,
                    });
                }
                let d = delay_ms(cfg, attempts, log_group);
                tracing::warn!(
                    op,
                    log_group,
                    attempts,
                    delay_ms = d,
                    "throttled; backing off"
                );
                tokio::time::sleep(Duration::from_millis(d)).await;
            }
        }
    }
}

/// Error codes treated as throttling. `ThrottlingException` is what
/// CloudWatch Logs actually emits; the others are defensive aliases seen
/// across AWS services.
const THROTTLE_CODES: [&str; 4] = [
    "ThrottlingException",
    "Throttling",
    "TooManyRequestsException",
    "RequestLimitExceeded",
];

fn classify<E, R>(err: SdkError<E, R>) -> CallFailure
where
    E: ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    let throttled = err
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .is_some_and(|c| THROTTLE_CODES.contains(&c));
    let message = format!("{}", DisplayErrorContext(&err));
    if throttled {
        CallFailure::Throttled(message)
    } else {
        CallFailure::Fatal(message)
    }
}

/// Real [`CwSource`] over the AWS SDK.
///
/// NOTE: `end_ms_exclusive` is converted to the SDK's **inclusive** `endTime`
/// (`end_ms_exclusive - 1`) — FilterLogEvents returns events with
/// `timestamp == endTime`, and a chunk leaking one millisecond into the next
/// window would break both idempotency and the dt= partition invariant.
#[derive(Debug, Clone)]
pub struct AwsCwSource {
    client: aws_sdk_cloudwatchlogs::Client,
    backoff: BackoffConfig,
}

impl AwsCwSource {
    pub fn new(client: aws_sdk_cloudwatchlogs::Client) -> Self {
        Self::with_backoff(client, BackoffConfig::default())
    }

    pub fn with_backoff(client: aws_sdk_cloudwatchlogs::Client, backoff: BackoffConfig) -> Self {
        Self { client, backoff }
    }
}

#[async_trait]
impl CwSource for AwsCwSource {
    async fn filter_log_events(
        &self,
        log_group: &str,
        start_ms: i64,
        end_ms_exclusive: i64,
        next_token: Option<&str>,
    ) -> Result<CwEventPage, CwError> {
        let out = with_backoff(&self.backoff, "FilterLogEvents", log_group, || {
            let req = self
                .client
                .filter_log_events()
                .log_group_name(log_group)
                .start_time(start_ms)
                .end_time(end_ms_exclusive.saturating_sub(1))
                .set_next_token(next_token.map(str::to_owned));
            async move { req.send().await.map_err(classify) }
        })
        .await?;
        let mut events = Vec::with_capacity(out.events.as_ref().map_or(0, Vec::len));
        for e in out.events.unwrap_or_default() {
            let Some(timestamp) = e.timestamp else {
                tracing::warn!(
                    log_group,
                    "FilterLogEvents returned an event without timestamp; skipping"
                );
                continue;
            };
            events.push(CwEvent {
                timestamp,
                message: e.message.unwrap_or_default(),
                log_stream_name: e.log_stream_name.unwrap_or_default(),
                ingestion_time: e.ingestion_time,
                event_id: e.event_id,
            });
        }
        Ok(CwEventPage {
            events,
            next_token: out.next_token,
        })
    }

    async fn describe_log_group(&self, log_group: &str) -> Result<LogGroupInfo, CwError> {
        // DescribeLogGroups is prefix-matched, so paginate until the exact
        // name shows up (other groups can share the prefix).
        let mut token: Option<String> = None;
        loop {
            let tok = token.clone();
            let out = with_backoff(&self.backoff, "DescribeLogGroups", log_group, || {
                let req = self
                    .client
                    .describe_log_groups()
                    .log_group_name_prefix(log_group)
                    .set_next_token(tok.clone());
                async move { req.send().await.map_err(classify) }
            })
            .await?;
            if let Some(lg) = out
                .log_groups
                .unwrap_or_default()
                .into_iter()
                .find(|g| g.log_group_name.as_deref() == Some(log_group))
            {
                let creation_time_ms = lg.creation_time.ok_or(CwError::Malformed {
                    what: "log group creation_time",
                    log_group: log_group.to_owned(),
                })?;
                return Ok(LogGroupInfo {
                    retention_days: lg.retention_in_days,
                    stored_bytes: lg.stored_bytes,
                    creation_time_ms,
                });
            }
            token = out.next_token;
            if token.is_none() {
                return Err(CwError::GroupNotFound {
                    log_group: log_group.to_owned(),
                });
            }
        }
    }

    async fn list_log_groups(
        &self,
        prefix_hint: Option<&str>,
    ) -> Result<Vec<(String, LogGroupInfo)>, CwError> {
        // Backoff key: the hint when present, "*" for the full enumeration —
        // distinct keys keep the deterministic jitter desynchronized across
        // concurrent listings.
        let label = prefix_hint.unwrap_or("*");
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let tok = token.clone();
            let resp = with_backoff(&self.backoff, "DescribeLogGroups", label, || {
                let req = self
                    .client
                    .describe_log_groups()
                    .set_log_group_name_prefix(prefix_hint.map(str::to_owned))
                    .set_next_token(tok.clone());
                async move { req.send().await.map_err(classify) }
            })
            .await?;
            for lg in resp.log_groups.unwrap_or_default() {
                let Some(name) = lg.log_group_name else {
                    tracing::warn!("DescribeLogGroups returned a group without a name; skipping");
                    continue;
                };
                let Some(creation_time_ms) = lg.creation_time else {
                    tracing::warn!(
                        log_group = %name,
                        "DescribeLogGroups returned a group without creation_time; skipping"
                    );
                    continue;
                };
                out.push((
                    name,
                    LogGroupInfo {
                        retention_days: lg.retention_in_days,
                        stored_bytes: lg.stored_bytes,
                        creation_time_ms,
                    },
                ));
            }
            token = resp.next_token;
            if token.is_none() {
                break;
            }
        }
        Ok(out)
    }

    async fn put_retention_policy(
        &self,
        log_group: &str,
        retention_days: i32,
    ) -> Result<(), CwError> {
        with_backoff(&self.backoff, "PutRetentionPolicy", log_group, || {
            let req = self
                .client
                .put_retention_policy()
                .log_group_name(log_group)
                .retention_in_days(retention_days);
            async move { req.send().await.map(|_| ()).map_err(classify) }
        })
        .await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn fast_cfg(max_attempts: u32) -> BackoffConfig {
        BackoffConfig {
            base_ms: 1,
            cap_ms: 2,
            max_attempts,
        }
    }

    #[tokio::test]
    async fn backoff_retries_throttling_then_succeeds() {
        let calls = AtomicU32::new(0);
        let res = with_backoff(&fast_cfg(5), "Test", "g", || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(CallFailure::Throttled("injected".into()))
                } else {
                    Ok(42u32)
                }
            }
        })
        .await;
        assert_eq!(res.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn backoff_surfaces_typed_error_after_exhaustion() {
        let calls = AtomicU32::new(0);
        let res: Result<(), CwError> = with_backoff(&fast_cfg(3), "Test", "g", || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(CallFailure::Throttled("still throttled".into())) }
        })
        .await;
        match res.unwrap_err() {
            CwError::Throttled { op, attempts, .. } => {
                assert_eq!(op, "Test");
                assert_eq!(attempts, 3);
            }
            other => panic!("expected Throttled, got {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn backoff_does_not_retry_fatal_errors() {
        let calls = AtomicU32::new(0);
        let res: Result<(), CwError> = with_backoff(&fast_cfg(5), "Test", "g", || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(CallFailure::Fatal("bad request".into())) }
        })
        .await;
        assert!(matches!(res.unwrap_err(), CwError::Api { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn list_log_groups_paginates_and_skips_malformed_entries() {
        use aws_sdk_cloudwatchlogs::operation::describe_log_groups::DescribeLogGroupsOutput;
        use aws_sdk_cloudwatchlogs::types::LogGroup;
        use aws_smithy_mocks::{RuleMode, mock, mock_client};

        let page1 = mock!(aws_sdk_cloudwatchlogs::Client::describe_log_groups)
            .match_requests(|inp| {
                inp.log_group_name_prefix() == Some("/aws/") && inp.next_token().is_none()
            })
            .then_output(|| {
                DescribeLogGroupsOutput::builder()
                    .log_groups(
                        LogGroup::builder()
                            .log_group_name("/aws/lambda/a")
                            .creation_time(11)
                            .retention_in_days(30)
                            .stored_bytes(1024)
                            .build(),
                    )
                    // No creation_time — must be skipped, not fatal.
                    .log_groups(LogGroup::builder().log_group_name("/aws/broken").build())
                    .next_token("tok-1")
                    .build()
            });
        let page2 = mock!(aws_sdk_cloudwatchlogs::Client::describe_log_groups)
            .match_requests(|inp| inp.next_token() == Some("tok-1"))
            .then_output(|| {
                DescribeLogGroupsOutput::builder()
                    .log_groups(
                        LogGroup::builder()
                            .log_group_name("/aws/lambda/b")
                            .creation_time(22)
                            .build(),
                    )
                    .build()
            });
        let client = mock_client!(
            aws_sdk_cloudwatchlogs,
            RuleMode::Sequential,
            [&page1, &page2]
        );
        let src = AwsCwSource::new(client);
        let groups = src.list_log_groups(Some("/aws/")).await.unwrap();
        assert_eq!(
            groups,
            vec![
                (
                    "/aws/lambda/a".to_owned(),
                    LogGroupInfo {
                        retention_days: Some(30),
                        stored_bytes: Some(1024),
                        creation_time_ms: 11,
                    }
                ),
                (
                    "/aws/lambda/b".to_owned(),
                    LogGroupInfo {
                        retention_days: None,
                        stored_bytes: None,
                        creation_time_ms: 22,
                    }
                ),
            ]
        );
        assert_eq!(page1.num_calls(), 1);
        assert_eq!(page2.num_calls(), 1);
    }

    #[test]
    fn jitter_is_deterministic_and_bounded() {
        let cfg = BackoffConfig {
            base_ms: 200,
            cap_ms: 10_000,
            max_attempts: 8,
        };
        for attempt in 1..=8u32 {
            let exp = cfg
                .base_ms
                .saturating_mul(1u64 << (attempt - 1).min(20))
                .min(cfg.cap_ms);
            let d1 = delay_ms(&cfg, attempt, "/aws/lambda/foo");
            let d2 = delay_ms(&cfg, attempt, "/aws/lambda/foo");
            assert_eq!(d1, d2, "same inputs must give the same delay");
            assert!(
                d1 >= exp / 2 && d1 <= exp,
                "attempt {attempt}: {d1} not in [{}, {exp}]",
                exp / 2
            );
        }
        // different keys should (very likely) jitter differently somewhere
        let spread: std::collections::HashSet<u64> = (0..16)
            .map(|i| delay_ms(&cfg, 3, &format!("g{i}")))
            .collect();
        assert!(spread.len() > 1, "jitter never varies across keys");
    }
}
