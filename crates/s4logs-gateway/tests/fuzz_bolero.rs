//! Bolero fuzz targets over the gateway's untrusted-input surfaces: the
//! AWS JSON 1.1 request bodies, the routing TOML, WAL replay lines, and the
//! SigV4 verifier. Every target's contract is "typed error or Ok — never a
//! panic, never unbounded allocation".
//!
//! Plain test mode (CI smoke): `cargo test -p s4logs-gateway --test fuzz_bolero`
//! Coverage-guided:  `cd crates/s4logs-gateway && cargo bolero test
//!                    --engine libfuzzer --toolchain nightly <target>`

#![allow(clippy::unwrap_used)]

use axum::http::HeaderMap;
use bolero::check;
use s4logs_gateway::api::{
    CreateLogGroupRequest, CreateLogStreamRequest, DescribeLogGroupsRequest,
    DescribeLogStreamsRequest, PutLogEventsRequest,
};
use s4logs_gateway::auth::{RequestView, verify};
use s4logs_gateway::routing::RoutingConfig;
use s4logs_gateway::wal::WalEntry;

/// Routing config is operator-supplied but read at startup from a file the
/// operator may template badly — parse must never panic.
#[test]
fn routing_toml_parse_bolero() {
    check!().with_type::<String>().for_each(|s| {
        let _ = RoutingConfig::from_toml_str(s);
    });
}

/// WAL replay reads whatever is on disk after a crash — torn writes, partial
/// JSON, garbage. Line decode must never panic; valid entries re-encode.
#[test]
fn wal_entry_line_decode_bolero() {
    check!().with_type::<Vec<u8>>().for_each(|bytes| {
        if let Ok(entry) = serde_json::from_slice::<WalEntry>(bytes) {
            let _ = serde_json::to_vec(&entry).unwrap();
        }
    });
}

/// The five JSON 1.1 action bodies arrive from the network. serde must
/// reject garbage with a typed error, never a panic.
#[test]
fn api_request_decode_bolero() {
    check!().with_type::<Vec<u8>>().for_each(|bytes| {
        let _ = serde_json::from_slice::<PutLogEventsRequest>(bytes);
        let _ = serde_json::from_slice::<CreateLogGroupRequest>(bytes);
        let _ = serde_json::from_slice::<CreateLogStreamRequest>(bytes);
        let _ = serde_json::from_slice::<DescribeLogGroupsRequest>(bytes);
        let _ = serde_json::from_slice::<DescribeLogStreamsRequest>(bytes);
    });
}

/// SigV4 verification parses an attacker-controlled Authorization header
/// (plus x-amz-date and arbitrary signed-header lists) — the entire parse +
/// canonicalize + HMAC chain must fail closed without panicking.
#[test]
fn sigv4_verify_bolero() {
    check!()
        .with_type::<(
            String,
            String,
            Option<String>,
            Vec<(String, String)>,
            Vec<u8>,
        )>()
        .for_each(|(method, path, query, headers, body)| {
            let mut map = HeaderMap::new();
            for (name, value) in headers {
                let (Ok(n), Ok(v)) = (
                    axum::http::HeaderName::try_from(name.as_str()),
                    axum::http::HeaderValue::try_from(value.as_str()),
                ) else {
                    continue;
                };
                map.append(n, v);
            }
            let req = RequestView {
                method,
                path,
                query: query.as_deref(),
                headers: &map,
                body,
            };
            // Any outcome but a panic is correct; with random headers the
            // overwhelming majority must be Err.
            let _ = verify(
                "AKIDEXAMPLE",
                "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                &req,
                std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_781_000_000),
            );
        });
}
