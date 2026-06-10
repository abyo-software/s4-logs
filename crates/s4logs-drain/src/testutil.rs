//! Test-only scripted [`CwSource`] mock: deterministic paging, throttling
//! injection, retention-call recording.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::cw::{CwError, CwEvent, CwEventPage, CwSource, LogGroupInfo};

pub(crate) fn event(timestamp: i64, message: &str) -> CwEvent {
    CwEvent {
        timestamp,
        message: message.to_owned(),
        log_stream_name: "app/i-0abc".to_owned(),
        ingestion_time: Some(timestamp + 250),
        event_id: Some(format!("evt-{timestamp}")),
    }
}

pub(crate) struct MockCw {
    /// Global event pool; `filter_log_events` selects by timestamp range.
    pub events: Vec<CwEvent>,
    /// Events per FilterLogEvents page.
    pub page_size: usize,
    /// While > 0, each `filter_log_events` call decrements and fails with
    /// [`CwError::Throttled`] (post-backoff-exhaustion shape, as the real
    /// `AwsCwSource` would surface it).
    pub throttle_remaining: Arc<AtomicU32>,
    pub filter_calls: Arc<AtomicU32>,
    /// `(log_group, retention_days)` per `put_retention_policy` call.
    pub retention_calls: Mutex<Vec<(String, i32)>>,
    pub info: LogGroupInfo,
    /// Misbehave: ignore the requested range (tests the drain's defensive
    /// out-of-window drop).
    pub ignore_range_filter: bool,
}

impl Default for MockCw {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            page_size: 5,
            throttle_remaining: Arc::new(AtomicU32::new(0)),
            filter_calls: Arc::new(AtomicU32::new(0)),
            retention_calls: Mutex::new(Vec::new()),
            info: LogGroupInfo {
                retention_days: None,
                stored_bytes: None,
                creation_time_ms: 0,
            },
            ignore_range_filter: false,
        }
    }
}

#[async_trait]
impl CwSource for MockCw {
    async fn filter_log_events(
        &self,
        log_group: &str,
        start_ms: i64,
        end_ms_exclusive: i64,
        next_token: Option<&str>,
    ) -> Result<CwEventPage, CwError> {
        self.filter_calls.fetch_add(1, Ordering::SeqCst);
        if self
            .throttle_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1))
            .is_ok()
        {
            return Err(CwError::Throttled {
                op: "FilterLogEvents",
                log_group: log_group.to_owned(),
                attempts: 8,
                message: "injected throttle".to_owned(),
            });
        }
        let in_range: Vec<CwEvent> = self
            .events
            .iter()
            .filter(|e| {
                self.ignore_range_filter
                    || (e.timestamp >= start_ms && e.timestamp < end_ms_exclusive)
            })
            .cloned()
            .collect();
        let offset: usize = next_token.map(|t| t.parse().unwrap_or(0)).unwrap_or(0);
        let page: Vec<CwEvent> = in_range
            .iter()
            .skip(offset)
            .take(self.page_size)
            .cloned()
            .collect();
        let next = offset + self.page_size;
        Ok(CwEventPage {
            events: page,
            next_token: (next < in_range.len()).then(|| next.to_string()),
        })
    }

    async fn describe_log_group(&self, _log_group: &str) -> Result<LogGroupInfo, CwError> {
        Ok(self.info.clone())
    }

    async fn put_retention_policy(
        &self,
        log_group: &str,
        retention_days: i32,
    ) -> Result<(), CwError> {
        self.retention_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((log_group.to_owned(), retention_days));
        Ok(())
    }
}
