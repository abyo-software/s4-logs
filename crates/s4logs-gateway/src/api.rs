//! AWS JSON 1.1 wire types for the CloudWatch Logs API subset (DESIGN.md
//! §8.1). Field names are the CW wire contract (camelCase) — do not rename.
//!
//! Error shape: HTTP 400 (or 500 for server-side failures) with body
//! `{"__type":"<ExceptionName>","message":"..."}` and content-type
//! `application/x-amz-json-1.1`, matching what AWS SDKs / agents expect.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

/// `X-Amz-Target` prefix for every CloudWatch Logs action.
pub const TARGET_PREFIX: &str = "Logs_20140328.";
/// Request/response content type of the AWS JSON 1.1 protocol.
pub const AMZ_JSON_CONTENT_TYPE: &str = "application/x-amz-json-1.1";

/// One event of a `PutLogEvents` batch (CW wire shape).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InputLogEvent {
    /// Event time, epoch milliseconds.
    pub timestamp: i64,
    pub message: String,
}

/// `PutLogEvents` request. Unknown fields (entity, kmsKeyId, …) are
/// ignored — validation is deliberately lenient (DESIGN.md §8.1: accept,
/// don't reject; compatibility first).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutLogEventsRequest {
    pub log_group_name: String,
    pub log_stream_name: String,
    #[serde(default)]
    pub log_events: Vec<InputLogEvent>,
    /// Obsolete in current CloudWatch (accepted, never required). Parsed so
    /// older agents that still send it round-trip cleanly; otherwise unused.
    #[serde(default)]
    pub sequence_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateLogGroupRequest {
    pub log_group_name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateLogStreamRequest {
    pub log_group_name: String,
    pub log_stream_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeLogGroupsRequest {
    #[serde(default)]
    pub log_group_name_prefix: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeLogStreamsRequest {
    #[serde(default)]
    pub log_group_name: Option<String>,
    /// Newer agents may send the ARN-or-name identifier instead.
    #[serde(default)]
    pub log_group_identifier: Option<String>,
    #[serde(default)]
    pub log_stream_name_prefix: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogGroupSummary {
    pub log_group_name: String,
    /// Registry creation time, epoch milliseconds.
    pub creation_time: i64,
    pub arn: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeLogGroupsResponse {
    pub log_groups: Vec<LogGroupSummary>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogStreamSummary {
    pub log_stream_name: String,
    pub creation_time: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeLogStreamsResponse {
    pub log_streams: Vec<LogStreamSummary>,
}

/// AWS JSON 1.1 error: `{"__type":"<Name>","message":"..."}`.
#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    #[serde(skip)]
    pub status: StatusCode,
    #[serde(rename = "__type")]
    pub kind: String,
    pub message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, kind: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            kind: kind.to_owned(),
            message: message.into(),
        }
    }

    /// Unsupported / unknown `X-Amz-Target` action (DESIGN.md §8.1: a 400
    /// `InvalidAction`, not `UnrecognizedClientException`).
    pub fn invalid_action(target: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "InvalidAction",
            format!("unsupported action: {target}"),
        )
    }

    pub fn missing_action() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "MissingAction",
            "missing X-Amz-Target header",
        )
    }

    pub fn serialization(err: &serde_json::Error) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "SerializationException",
            format!("invalid request body: {err}"),
        )
    }

    pub fn already_exists(what: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "ResourceAlreadyExistsException",
            format!("The specified {what} already exists"),
        )
    }

    pub fn not_found(what: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("The specified {what} does not exist"),
        )
    }

    pub fn invalid_parameter(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            message,
        )
    }

    /// Server-side failure (buffer flush / pure-passthrough forward). HTTP
    /// 500 so well-behaved CW agents retry the batch.
    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ServiceUnavailableException",
            message,
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::to_string(&self).unwrap_or_else(|_| {
            r#"{"__type":"InternalFailure","message":"error encode failed"}"#.to_owned()
        });
        amz_json_body(self.status, body)
    }
}

/// Build a JSON 1.1 response from an already-serialized body.
pub(crate) fn amz_json_body(status: StatusCode, body: String) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, AMZ_JSON_CONTENT_TYPE)],
        body,
    )
        .into_response()
}

/// 200 response with a JSON 1.1 body.
pub(crate) fn amz_json_ok<T: Serialize>(value: &T) -> Result<Response, ApiError> {
    let body = serde_json::to_string(value).map_err(|e| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            format!("response encode failed: {e}"),
        )
    })?;
    Ok(amz_json_body(StatusCode::OK, body))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn put_request_parses_cw_wire_names() {
        let req: PutLogEventsRequest = serde_json::from_str(
            r#"{"logGroupName":"/g","logStreamName":"s","sequenceToken":"49590",
                "logEvents":[{"timestamp":1717900000123,"message":"hello"}],
                "entity":{"keyAttributes":{}}}"#,
        )
        .unwrap();
        assert_eq!(req.log_group_name, "/g");
        assert_eq!(req.log_stream_name, "s");
        assert_eq!(req.sequence_token.as_deref(), Some("49590"));
        assert_eq!(
            req.log_events,
            vec![InputLogEvent {
                timestamp: 1717900000123,
                message: "hello".into()
            }]
        );
    }

    #[test]
    fn error_body_shape() {
        let err = ApiError::invalid_action("Logs_20140328.GetLogEvents");
        let v: serde_json::Value = serde_json::to_value(&err).unwrap();
        assert_eq!(v["__type"], "InvalidAction");
        assert!(v["message"].as_str().unwrap().contains("GetLogEvents"));
        assert!(v.get("status").is_none());
    }
}
