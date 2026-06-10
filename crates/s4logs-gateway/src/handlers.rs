//! AWS JSON 1.1 dispatch + observability endpoints (DESIGN.md §8.1, §8.4,
//! §11.2).
//!
//! - `POST /` with `X-Amz-Target: Logs_20140328.<Action>` dispatches the
//!   CW Logs API subset.
//! - **SigV4** (DESIGN.md §11.2): opt-in via `GatewayConfig::auth`. With
//!   `AuthMode::SigV4`, the API route verifies every request against the
//!   static credential before dispatch; `/health`, `/ready` and `/metrics`
//!   stay exempt. With the default `AuthMode::None` the `Authorization`
//!   header is ignored (P1 posture — deploy behind TLS + network boundary).
//! - `GET /health` (unconditional 200), `GET /ready` (sink probe + last
//!   flush outcome), `GET /metrics` (Prometheus).

use std::sync::Arc;
use std::time::SystemTime;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
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
use crate::auth::{self, AuthMode};
use crate::buffer::{BufferError, BufferManager};
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
    auth: AuthMode,
    /// `None` if another metrics recorder was installed first — `/metrics`
    /// then renders empty (the co-resident recorder owns the data).
    metrics: Option<PrometheusHandle>,
}

impl AppState {
    pub fn new(
        routing: RoutingConfig,
        buffers: Arc<BufferManager>,
        forward: Arc<dyn CwForward>,
        auth: AuthMode,
        metrics: Option<PrometheusHandle>,
    ) -> Self {
        Self(Arc::new(Inner {
            routing,
            buffers,
            registry: Registry::default(),
            forward,
            auth,
            metrics,
        }))
    }
}

/// Build the axum application (also used directly by tests via
/// `tower::ServiceExt::oneshot`). The SigV4 layer wraps only the API route
/// added before it — `/health`, `/ready`, `/metrics` are added after and
/// stay exempt (DESIGN.md §11.2).
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/", post(dispatch))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_endpoint))
        .with_state(state)
}

/// SigV4 verification (no-op for `AuthMode::None`). The body must be
/// buffered to compute the payload hash; it is replayed downstream.
async fn auth_middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let AuthMode::SigV4 {
        access_key,
        secret_key,
    } = &state.0.auth
    else {
        return next.run(req).await;
    };
    let (parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return ApiError::internal_failure(format!("request body read failed: {err}"))
                .into_response();
        }
    };
    let verdict = auth::verify(
        access_key,
        secret_key,
        &auth::RequestView {
            method: parts.method.as_str(),
            path: parts.uri.path(),
            query: parts.uri.query(),
            headers: &parts.headers,
            body: &bytes,
        },
        SystemTime::now(),
    );
    match verdict {
        Ok(()) => {
            next.run(Request::from_parts(parts, Body::from(bytes)))
                .await
        }
        Err(auth::AuthError::MissingToken) => {
            tracing::warn!("rejecting request without Authorization header");
            ApiError::missing_auth_token().into_response()
        }
        Err(auth::AuthError::Invalid(reason)) => {
            tracing::warn!(reason, "rejecting request with invalid signature");
            ApiError::invalid_signature(format!(
                "The request signature we calculated does not match: {reason}"
            ))
            .into_response()
        }
    }
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
                match e {
                    // Memory cap: 503, agents back off and retry.
                    BufferError::OverCapacity => ApiError::backpressure(),
                    // WAL failure: the durability promise cannot be kept —
                    // fail loudly (500 InternalFailure) instead of degrading
                    // to memory-only buffering silently.
                    BufferError::Wal(_) => {
                        ApiError::internal_failure("wal append failed; retry the batch")
                    }
                    _ => ApiError::service_unavailable("event buffering failed; retry the batch"),
                }
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
    let all = state
        .0
        .registry
        .describe_groups(req.log_group_name_prefix.as_deref());
    let (page, next_token) = paginate(all, req.limit, req.next_token.as_deref())?;
    let log_groups = page
        .into_iter()
        .map(|(name, creation_time)| LogGroupSummary {
            arn: format!("arn:aws:logs:local:000000000000:log-group:{name}:*"),
            log_group_name: name,
            creation_time,
        })
        .collect();
    api::amz_json_ok(&DescribeLogGroupsResponse {
        log_groups,
        next_token,
    })
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
    let (page, next_token) = paginate(streams, req.limit, req.next_token.as_deref())?;
    let log_streams = page
        .into_iter()
        .map(|(name, creation_time)| LogStreamSummary {
            log_stream_name: name,
            creation_time,
        })
        .collect();
    api::amz_json_ok(&DescribeLogStreamsResponse {
        log_streams,
        next_token,
    })
}

/// `limit` + opaque `nextToken` pagination over a stable (name-ordered)
/// snapshot. The token is the start offset — opaque to clients, validated
/// here. CW caps `limit` at 50 for both Describe* actions.
fn paginate<T>(
    items: Vec<T>,
    limit: Option<i64>,
    next_token: Option<&str>,
) -> Result<(Vec<T>, Option<String>), ApiError> {
    let start = match next_token {
        None => 0usize,
        Some(t) => t
            .parse::<usize>()
            .map_err(|_| ApiError::invalid_parameter("invalid nextToken"))?,
    };
    let limit = usize::try_from(limit.unwrap_or(50).clamp(1, 50))
        .map_err(|_| ApiError::invalid_parameter("invalid limit"))?;
    let total = items.len();
    let page: Vec<T> = items.into_iter().skip(start).take(limit).collect();
    let consumed = start.saturating_add(page.len());
    let next = (consumed < total && !page.is_empty()).then(|| consumed.to_string());
    Ok((page, next))
}
