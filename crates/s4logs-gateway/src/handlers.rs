//! AWS JSON 1.1 dispatch + observability endpoints (DESIGN.md §8.1, §8.4).
//!
//! - `POST /` with `X-Amz-Target: Logs_20140328.<Action>` dispatches the
//!   CW Logs API subset.
//! - **SigV4 is NOT validated in P1** (DESIGN.md §8.1). The `Authorization`
//!   header is ignored entirely; deploy behind TLS + a network boundary.
//!   Static-credential validation is planned for P3.
//! - `GET /health` (unconditional 200), `GET /ready` (sink probe + last
//!   flush outcome), `GET /metrics` (Prometheus).

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use metrics::counter;
use metrics_exporter_prometheus::PrometheusHandle;
use serde::de::DeserializeOwned;
use serde_json::json;

use crate::api::{
    self, ApiError, CreateLogGroupRequest, CreateLogStreamRequest, DescribeLogGroupsRequest,
    DescribeLogGroupsResponse, DescribeLogStreamsRequest, DescribeLogStreamsResponse,
    LogGroupSummary, LogStreamSummary, PutLogEventsRequest, TARGET_PREFIX,
};
use crate::buffer::BufferManager;
use crate::forward::CwForward;
use crate::registry::{Registry, RegistryError};
use crate::routing::RoutingConfig;

/// Shared handler state (cheap to clone).
#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    routing: RoutingConfig,
    buffers: Arc<BufferManager>,
    registry: Registry,
    forward: Arc<dyn CwForward>,
    /// `None` if another metrics recorder was installed first — `/metrics`
    /// then renders empty (the co-resident recorder owns the data).
    metrics: Option<PrometheusHandle>,
}

impl AppState {
    pub fn new(
        routing: RoutingConfig,
        buffers: Arc<BufferManager>,
        forward: Arc<dyn CwForward>,
        metrics: Option<PrometheusHandle>,
    ) -> Self {
        Self(Arc::new(Inner {
            routing,
            buffers,
            registry: Registry::default(),
            forward,
            metrics,
        }))
    }
}

/// Build the axum application (also used directly by tests via
/// `tower::ServiceExt::oneshot`).
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/", post(dispatch))
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_endpoint))
        .with_state(state)
}

async fn ready(State(state): State<AppState>) -> Response {
    if state.0.buffers.ready().await {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
    }
}

async fn metrics_endpoint(State(state): State<AppState>) -> String {
    state
        .0
        .metrics
        .as_ref()
        .map(PrometheusHandle::render)
        .unwrap_or_default()
}

fn parse_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|e| ApiError::serialization(&e))
}

async fn dispatch(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let Some(target) = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
    else {
        return ApiError::missing_action().into_response();
    };
    let Some(action) = target.strip_prefix(TARGET_PREFIX) else {
        tracing::warn!(target = %target, "rejecting non-Logs X-Amz-Target");
        return ApiError::invalid_action(&target).into_response();
    };
    let result = match action {
        "PutLogEvents" => put_log_events(&state, &body).await,
        "CreateLogGroup" => create_log_group(&state, &body).await,
        "CreateLogStream" => create_log_stream(&state, &body).await,
        "DescribeLogGroups" => describe_log_groups(&state, &body),
        "DescribeLogStreams" => describe_log_streams(&state, &body),
        other => {
            tracing::warn!(action = other, "rejecting unsupported action");
            Err(ApiError::invalid_action(&target))
        }
    };
    result.unwrap_or_else(IntoResponse::into_response)
}

/// `PutLogEvents` — route, buffer and/or forward.
///
/// Error contract (DESIGN.md §8.1 + wave 1C decision, mirrored in tests):
/// - `s3` / `both`: a buffer failure returns 500 `ServiceUnavailableException`
///   so the agent retries the batch (in-process buffers are the only copy).
/// - `both`: a **CW forward** failure is logged + counted
///   (`s4logs_cw_passthrough_errors_total`) but NEVER fails the client
///   response — the batch is already durable in the S3 buffer path, and
///   failing would make the agent re-send (duplicating S3 data).
/// - `cloudwatch` (pure passthrough): a forward failure DOES return 500,
///   because the gateway holds no other copy; the agent must retry.
/// - Batch limits (≤10,000 events / ≤1 MiB) are NOT enforced — accept and
///   pass through, compatibility first.
async fn put_log_events(state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let req: PutLogEventsRequest = parse_body(body)?;
    let action = state
        .0
        .routing
        .route(&req.log_group_name, &req.log_stream_name);
    counter!("s4logs_events_total", "action" => action.as_str())
        .increment(req.log_events.len() as u64);
    state
        .0
        .registry
        .touch(&req.log_group_name, &req.log_stream_name);

    if action.to_s3() {
        state
            .0
            .buffers
            .push_events(&req.log_group_name, &req.log_stream_name, &req.log_events)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, log_group = %req.log_group_name, "buffering failed");
                ApiError::service_unavailable("event buffering failed; retry the batch")
            })?;
    }
    if action.to_cloudwatch()
        && let Err(err) = state
            .0
            .forward
            .put_log_events(&req.log_group_name, &req.log_stream_name, &req.log_events)
            .await
    {
        counter!("s4logs_cw_passthrough_errors_total").increment(1);
        tracing::warn!(
            error = %err,
            log_group = %req.log_group_name,
            log_stream = %req.log_stream_name,
            "cloudwatch passthrough failed"
        );
        if !action.to_s3() {
            // Pure passthrough: nothing was persisted anywhere — fail loudly.
            return Err(ApiError::service_unavailable(
                "cloudwatch passthrough failed; retry the batch",
            ));
        }
        // `both`: S3 buffer already holds the events — succeed.
    }
    // Sequence tokens are obsolete — CW itself no longer requires them.
    api::amz_json_ok(&json!({}))
}

/// `CreateLogGroup`. Forwarded to CW when any route of this group may reach
/// CloudWatch; forward errors are logged + counted but do not fail the
/// client (the create is recorded locally and CW creates are retried
/// implicitly by agents).
async fn create_log_group(state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let req: CreateLogGroupRequest = parse_body(body)?;
    match state.0.registry.create_group(&req.log_group_name) {
        Ok(()) => {}
        Err(RegistryError::GroupExists) => return Err(ApiError::already_exists("log group")),
        Err(e) => return Err(ApiError::invalid_parameter(e.to_string())),
    }
    if state
        .0
        .routing
        .group_may_reach_cloudwatch(&req.log_group_name)
        && let Err(err) = state.0.forward.create_log_group(&req.log_group_name).await
    {
        counter!("s4logs_cw_passthrough_errors_total").increment(1);
        tracing::warn!(error = %err, log_group = %req.log_group_name, "CreateLogGroup forward failed");
    }
    api::amz_json_ok(&json!({}))
}

/// `CreateLogStream`. Same forward-error policy as `CreateLogGroup`.
async fn create_log_stream(state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let req: CreateLogStreamRequest = parse_body(body)?;
    match state
        .0
        .registry
        .create_stream(&req.log_group_name, &req.log_stream_name)
    {
        Ok(()) => {}
        Err(RegistryError::StreamExists) => return Err(ApiError::already_exists("log stream")),
        Err(RegistryError::GroupNotFound) => return Err(ApiError::not_found("log group")),
        Err(e) => return Err(ApiError::invalid_parameter(e.to_string())),
    }
    if state
        .0
        .routing
        .route(&req.log_group_name, &req.log_stream_name)
        .to_cloudwatch()
        && let Err(err) = state
            .0
            .forward
            .create_log_stream(&req.log_group_name, &req.log_stream_name)
            .await
    {
        counter!("s4logs_cw_passthrough_errors_total").increment(1);
        tracing::warn!(error = %err, log_group = %req.log_group_name, "CreateLogStream forward failed");
    }
    api::amz_json_ok(&json!({}))
}

fn describe_log_groups(state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    // CW agents may send an empty body for Describe* — treat as no filter.
    let req: DescribeLogGroupsRequest = if body.is_empty() {
        DescribeLogGroupsRequest::default()
    } else {
        parse_body(body)?
    };
    let log_groups = state
        .0
        .registry
        .describe_groups(req.log_group_name_prefix.as_deref())
        .into_iter()
        .map(|(name, creation_time)| LogGroupSummary {
            arn: format!("arn:aws:logs:local:000000000000:log-group:{name}:*"),
            log_group_name: name,
            creation_time,
        })
        .collect();
    api::amz_json_ok(&DescribeLogGroupsResponse { log_groups })
}

fn describe_log_streams(state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let req: DescribeLogStreamsRequest = parse_body(body)?;
    let group = req
        .log_group_name
        .as_deref()
        .or(req.log_group_identifier.as_deref())
        .ok_or_else(|| {
            ApiError::invalid_parameter("logGroupName or logGroupIdentifier is required")
        })?;
    let streams = state
        .0
        .registry
        .describe_streams(group, req.log_stream_name_prefix.as_deref())
        .map_err(|_| ApiError::not_found("log group"))?;
    let log_streams = streams
        .into_iter()
        .map(|(name, creation_time)| LogStreamSummary {
            log_stream_name: name,
            creation_time,
        })
        .collect();
    api::amz_json_ok(&DescribeLogStreamsResponse { log_streams })
}
