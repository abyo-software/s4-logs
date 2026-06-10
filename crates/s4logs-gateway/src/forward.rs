//! CloudWatch passthrough — `CwForward` trait (mockable) plus the real
//! `aws-sdk-cloudwatchlogs` implementation.
//!
//! The gateway forwards the *original* batch (group/stream preserved) for
//! `cloudwatch` / `both` routes. Error policy lives in the HTTP handler
//! (`handlers::put_log_events`), not here.

use async_trait::async_trait;
use aws_sdk_cloudwatchlogs::error::DisplayErrorContext;
use thiserror::Error;

use crate::api::InputLogEvent;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ForwardError {
    #[error("cloudwatch {op} failed: {message}")]
    Api { op: &'static str, message: String },
}

impl ForwardError {
    fn api(op: &'static str, err: impl std::fmt::Display) -> Self {
        Self::Api {
            op,
            message: err.to_string(),
        }
    }
}

/// Forwarding surface of CloudWatch Logs. Mocked in unit tests; implemented
/// over `aws_sdk_cloudwatchlogs::Client` for production.
#[async_trait]
pub trait CwForward: Send + Sync {
    /// Forward one PutLogEvents batch verbatim (stream preserved).
    async fn put_log_events(
        &self,
        log_group: &str,
        log_stream: &str,
        events: &[InputLogEvent],
    ) -> Result<(), ForwardError>;

    /// Idempotent: `ResourceAlreadyExistsException` is success.
    async fn create_log_group(&self, log_group: &str) -> Result<(), ForwardError>;

    /// Idempotent: `ResourceAlreadyExistsException` is success.
    async fn create_log_stream(
        &self,
        log_group: &str,
        log_stream: &str,
    ) -> Result<(), ForwardError>;
}

/// No-op forwarder for S3-only deployments (no `cloudwatch`/`both` routes)
/// and tests. Every call succeeds without touching the network.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopCwForward;

#[async_trait]
impl CwForward for NoopCwForward {
    async fn put_log_events(
        &self,
        _log_group: &str,
        _log_stream: &str,
        _events: &[InputLogEvent],
    ) -> Result<(), ForwardError> {
        Ok(())
    }

    async fn create_log_group(&self, _log_group: &str) -> Result<(), ForwardError> {
        Ok(())
    }

    async fn create_log_stream(
        &self,
        _log_group: &str,
        _log_stream: &str,
    ) -> Result<(), ForwardError> {
        Ok(())
    }
}

/// Real CloudWatch Logs forwarder.
#[derive(Debug, Clone)]
pub struct SdkCwForward {
    client: aws_sdk_cloudwatchlogs::Client,
}

impl SdkCwForward {
    pub fn new(client: aws_sdk_cloudwatchlogs::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl CwForward for SdkCwForward {
    async fn put_log_events(
        &self,
        log_group: &str,
        log_stream: &str,
        events: &[InputLogEvent],
    ) -> Result<(), ForwardError> {
        let mut wire = Vec::with_capacity(events.len());
        for e in events {
            wire.push(
                aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                    .timestamp(e.timestamp)
                    .message(e.message.clone())
                    .build()
                    .map_err(|err| ForwardError::api("PutLogEvents", err))?,
            );
        }
        self.client
            .put_log_events()
            .log_group_name(log_group)
            .log_stream_name(log_stream)
            .set_log_events(Some(wire))
            .send()
            .await
            .map_err(|e| ForwardError::api("PutLogEvents", DisplayErrorContext(&e)))?;
        Ok(())
    }

    async fn create_log_group(&self, log_group: &str) -> Result<(), ForwardError> {
        match self
            .client
            .create_log_group()
            .log_group_name(log_group)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_resource_already_exists_exception() {
                    Ok(())
                } else {
                    Err(ForwardError::api(
                        "CreateLogGroup",
                        DisplayErrorContext(&service_err),
                    ))
                }
            }
        }
    }

    async fn create_log_stream(
        &self,
        log_group: &str,
        log_stream: &str,
    ) -> Result<(), ForwardError> {
        match self
            .client
            .create_log_stream()
            .log_group_name(log_group)
            .log_stream_name(log_stream)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_resource_already_exists_exception() {
                    Ok(())
                } else {
                    Err(ForwardError::api(
                        "CreateLogStream",
                        DisplayErrorContext(&service_err),
                    ))
                }
            }
        }
    }
}
