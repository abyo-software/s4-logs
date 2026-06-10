//! Opt-in SigV4 verification of incoming requests (DESIGN.md §11.2).
//!
//! Single-tenant static credential: the `Authorization: AWS4-HMAC-SHA256`
//! header is decomposed, the canonical request is rebuilt from the request
//! actually received (method, path, query, the headers the client signed,
//! payload hash), the signing key is derived for whatever region/service the
//! client put in its credential scope (scope enforcement is the access-key
//! match — there is exactly one tenant), and the signature is compared in
//! constant time (`hmac::Mac::verify_slice`).
//!
//! Accepted payload hashes: the literal SHA-256 of the body, and
//! `UNSIGNED-PAYLOAD` (`x-amz-content-sha256` header). Clock skew: the
//! `x-amz-date` must be within ±15 minutes of server time.
//!
//! Failure mapping (done by the HTTP layer in `handlers`): missing
//! `Authorization` → 403 `MissingAuthenticationTokenException`; everything
//! else → 403 `InvalidSignatureException`. `/health`, `/ready` and
//! `/metrics` are exempt.
//!
//! Implementation note: HMAC-SHA256/SHA-256 come from the RustCrypto `hmac` /
//! `sha2` crates — deliberately *not* a full SigV4 crate; the official
//! `aws-sigv4` signer appears only as a dev-dependency to generate
//! known-answer requests for the verifier tests.

use std::time::{Duration, SystemTime};

use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// `GatewayConfig::auth` — default [`AuthMode::None`] (P1-compatible).
#[derive(Debug, Clone, Default)]
pub enum AuthMode {
    /// No verification; deploy behind TLS + a network boundary.
    #[default]
    None,
    /// Verify every API request against this static credential pair.
    SigV4 {
        access_key: String,
        secret_key: String,
    },
}

/// Maximum tolerated |server time − x-amz-date|.
pub const MAX_CLOCK_SKEW: Duration = Duration::from_secs(15 * 60);

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// Verification failure, mapped to the wire by `handlers`.
#[derive(Debug, PartialEq, Eq)]
pub enum AuthError {
    /// No `Authorization` header at all.
    MissingToken,
    /// Anything else — the reason goes to the response message and the
    /// access log, never the signature internals.
    Invalid(&'static str),
}

/// The signature-relevant view of one received request. `query` is the raw
/// (still percent-encoded) query string; `body` the full payload.
///
/// `pub` (not `pub(crate)`) so the bolero fuzz target in `tests/` can drive
/// [`verify`] with adversarial requests; not part of the supported API.
#[doc(hidden)]
pub struct RequestView<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: Option<&'a str>,
    pub headers: &'a HeaderMap,
    pub body: &'a [u8],
}

/// Verify one request against the static credential.
///
/// `pub` + `#[doc(hidden)]` for the fuzz target only — see [`RequestView`].
#[doc(hidden)]
pub fn verify(
    access_key: &str,
    secret_key: &str,
    req: &RequestView<'_>,
    now: SystemTime,
) -> Result<(), AuthError> {
    let authorization = req
        .headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(AuthError::MissingToken)?;
    let parsed = parse_authorization(authorization)?;
    if parsed.access_key != access_key {
        return Err(AuthError::Invalid("unknown access key id"));
    }

    let amz_date = req
        .headers
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .ok_or(AuthError::Invalid("missing x-amz-date header"))?;
    let request_time =
        parse_amz_date(amz_date).ok_or(AuthError::Invalid("malformed x-amz-date"))?;
    let skew = match now.duration_since(request_time) {
        Ok(behind) => behind,
        Err(ahead) => ahead.duration(),
    };
    if skew > MAX_CLOCK_SKEW {
        return Err(AuthError::Invalid("request time too skewed (±15 min)"));
    }
    if parsed.scope_date != &amz_date[..8] {
        return Err(AuthError::Invalid("credential scope date mismatch"));
    }

    let canonical = canonical_request(req, &parsed.signed_headers)?;
    let string_to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{}/{}/{}/aws4_request\n{}",
        parsed.scope_date,
        parsed.scope_region,
        parsed.scope_service,
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    // Signing key derived for the region/service the client chose — accepted
    // as-is (single-tenant static key; module docs).
    let mut key = hmac256(
        format!("AWS4{secret_key}").as_bytes(),
        parsed.scope_date.as_bytes(),
    );
    for part in [parsed.scope_region, parsed.scope_service, "aws4_request"] {
        key = hmac256(&key, part.as_bytes());
    }
    let provided =
        hex::decode(parsed.signature).map_err(|_| AuthError::Invalid("malformed signature"))?;
    let mut mac = mac256(&key);
    mac.update(string_to_sign.as_bytes());
    // Constant-time comparison via the `subtle`-backed MAC verifier.
    mac.verify_slice(&provided)
        .map_err(|_| AuthError::Invalid("signature mismatch"))
}

struct ParsedAuthorization<'a> {
    access_key: &'a str,
    scope_date: &'a str,
    scope_region: &'a str,
    scope_service: &'a str,
    /// Lowercased, sorted, deduped — SigV4 requires sorted signed headers,
    /// so re-sorting cannot diverge from a conforming signer.
    signed_headers: Vec<String>,
    signature: &'a str,
}

fn parse_authorization(header: &str) -> Result<ParsedAuthorization<'_>, AuthError> {
    let rest = header
        .strip_prefix(ALGORITHM)
        .ok_or(AuthError::Invalid("unsupported authorization algorithm"))?;
    let (mut credential, mut signed_headers, mut signature) = (None, None, None);
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v);
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v);
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v);
        }
    }
    let credential = credential.ok_or(AuthError::Invalid("authorization missing Credential"))?;
    let signed_headers =
        signed_headers.ok_or(AuthError::Invalid("authorization missing SignedHeaders"))?;
    let signature = signature.ok_or(AuthError::Invalid("authorization missing Signature"))?;

    let scope: Vec<&str> = credential.split('/').collect();
    let [
        access_key,
        scope_date,
        scope_region,
        scope_service,
        terminal,
    ] = scope[..]
    else {
        return Err(AuthError::Invalid("malformed credential scope"));
    };
    if terminal != "aws4_request" {
        return Err(AuthError::Invalid("malformed credential scope"));
    }
    let mut signed_headers: Vec<String> = signed_headers
        .split(';')
        .filter(|h| !h.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    signed_headers.sort();
    signed_headers.dedup();
    if signed_headers.is_empty() {
        return Err(AuthError::Invalid("empty SignedHeaders"));
    }
    Ok(ParsedAuthorization {
        access_key,
        scope_date,
        scope_region,
        scope_service,
        signed_headers,
        signature,
    })
}

/// Rebuild the SigV4 canonical request from what was actually received.
fn canonical_request(
    req: &RequestView<'_>,
    signed_headers: &[String],
) -> Result<String, AuthError> {
    let canonical_uri = if req.path.is_empty() { "/" } else { req.path };

    // Already-encoded pairs as sent, sorted by (name, value) — clients send
    // the encoding they signed with.
    let mut pairs: Vec<(&str, &str)> = req
        .query
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| p.split_once('=').unwrap_or((p, "")))
        .collect();
    pairs.sort_unstable();
    let canonical_query = pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let mut canonical_headers = String::new();
    for name in signed_headers {
        let mut values = req.headers.get_all(name.as_str()).iter().peekable();
        if values.peek().is_none() {
            return Err(AuthError::Invalid("signed header absent from request"));
        }
        let joined = values
            .map(|v| {
                v.to_str()
                    .map(normalize_header_value)
                    .map_err(|_| AuthError::Invalid("signed header is not valid UTF-8"))
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(",");
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(&joined);
        canonical_headers.push('\n');
    }

    let payload_hash = match req
        .headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
    {
        Some(UNSIGNED_PAYLOAD) => UNSIGNED_PAYLOAD.to_owned(),
        // Header absent (CW Logs style) or a literal hash: always hash the
        // body we received, so a tampered payload breaks verification even
        // if the attacker keeps the original header value.
        _ => hex::encode(Sha256::digest(req.body)),
    };

    Ok(format!(
        "{}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{}\n{payload_hash}",
        req.method,
        signed_headers.join(";")
    ))
}

/// Trim and collapse runs of spaces/tabs (SigV4 header canonicalization).
fn normalize_header_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut in_space = false;
    for c in value.trim().chars() {
        if c == ' ' || c == '\t' {
            if !in_space {
                out.push(' ');
            }
            in_space = true;
        } else {
            out.push(c);
            in_space = false;
        }
    }
    out
}

fn mac256(key: &[u8]) -> HmacSha256 {
    // HMAC accepts keys of any length; `InvalidLength` is unreachable.
    #[allow(clippy::expect_used)]
    HmacSha256::new_from_slice(key).expect("HMAC-SHA256 accepts any key length")
}

fn hmac256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = mac256(key);
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Parse `YYYYMMDD'T'HHMMSS'Z'` into a `SystemTime` (no chrono dependency —
/// Howard Hinnant's days-from-civil algorithm).
fn parse_amz_date(s: &str) -> Option<SystemTime> {
    let b = s.as_bytes();
    if b.len() != 16 || b[8] != b'T' || b[15] != b'Z' {
        return None;
    }
    let num = |range: std::ops::Range<usize>| -> Option<i64> { s.get(range)?.parse::<i64>().ok() };
    let (y, m, d) = (num(0..4)?, num(4..6)?, num(6..8)?);
    let (hh, mm, ss) = (num(9..11)?, num(11..13)?, num(13..15)?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    let days = days_from_civil(y, m, d);
    let secs = days.checked_mul(86_400)? + hh * 3_600 + mm * 60 + ss;
    if secs < 0 {
        return None;
    }
    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs as u64))
}

/// Days since 1970-01-01 for a proleptic Gregorian civil date.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    // Official AWS SigV4 documentation example (IAM ListUsers,
    // 2015-08-30T12:36:00Z): independently published known-answer vector.
    const AKID: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const DOC_SIGNATURE: &str = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7";

    fn doc_headers(signature: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            "content-type",
            HeaderValue::from_static("application/x-www-form-urlencoded; charset=utf-8"),
        );
        h.insert("host", HeaderValue::from_static("iam.amazonaws.com"));
        h.insert("x-amz-date", HeaderValue::from_static("20150830T123600Z"));
        h.insert(
            "authorization",
            HeaderValue::from_str(&format!(
                "AWS4-HMAC-SHA256 Credential={AKID}/20150830/us-east-1/iam/aws4_request, \
                 SignedHeaders=content-type;host;x-amz-date, Signature={signature}"
            ))
            .unwrap(),
        );
        h
    }

    fn doc_now() -> SystemTime {
        parse_amz_date("20150830T123600Z").unwrap()
    }

    fn doc_view<'a>(headers: &'a HeaderMap, query: &'a str) -> RequestView<'a> {
        RequestView {
            method: "GET",
            path: "/",
            query: Some(query),
            headers,
            body: b"",
        }
    }

    fn doc_verify(headers: &HeaderMap, now: SystemTime) -> Result<(), AuthError> {
        verify(
            AKID,
            SECRET,
            &doc_view(headers, "Action=ListUsers&Version=2010-05-08"),
            now,
        )
    }

    #[test]
    fn aws_docs_known_answer_vector_verifies() {
        doc_verify(&doc_headers(DOC_SIGNATURE), doc_now()).unwrap();
    }

    #[test]
    fn tampered_signature_rejected() {
        let mut bad = DOC_SIGNATURE.to_owned();
        bad.replace_range(..1, "6");
        assert_eq!(
            doc_verify(&doc_headers(&bad), doc_now()),
            Err(AuthError::Invalid("signature mismatch"))
        );
    }

    #[test]
    fn tampered_query_rejected() {
        let headers = doc_headers(DOC_SIGNATURE);
        let res = verify(
            AKID,
            SECRET,
            &doc_view(&headers, "Action=DeleteUsers&Version=2010-05-08"),
            doc_now(),
        );
        assert_eq!(res, Err(AuthError::Invalid("signature mismatch")));
    }

    #[test]
    fn wrong_access_key_rejected() {
        let headers = doc_headers(DOC_SIGNATURE);
        assert_eq!(
            verify(
                "AKIDOTHER",
                SECRET,
                &doc_view(&headers, "Action=ListUsers&Version=2010-05-08"),
                doc_now(),
            ),
            Err(AuthError::Invalid("unknown access key id"))
        );
    }

    #[test]
    fn skewed_clock_rejected_and_boundary_accepted() {
        let headers = doc_headers(DOC_SIGNATURE);
        let res = doc_verify(&headers, doc_now() + Duration::from_secs(16 * 60));
        assert_eq!(
            res,
            Err(AuthError::Invalid("request time too skewed (±15 min)"))
        );
        let res = doc_verify(&headers, doc_now() - Duration::from_secs(16 * 60));
        assert_eq!(
            res,
            Err(AuthError::Invalid("request time too skewed (±15 min)"))
        );
        // 14 minutes off in either direction is fine.
        doc_verify(&headers, doc_now() + Duration::from_secs(14 * 60)).unwrap();
        doc_verify(&headers, doc_now() - Duration::from_secs(14 * 60)).unwrap();
    }

    #[test]
    fn missing_authorization_is_missing_token() {
        let mut headers = doc_headers(DOC_SIGNATURE);
        headers.remove("authorization");
        assert_eq!(
            doc_verify(&headers, doc_now()),
            Err(AuthError::MissingToken)
        );
    }

    #[test]
    fn amz_date_parses_known_epochs() {
        assert_eq!(
            parse_amz_date("19700101T000000Z").unwrap(),
            SystemTime::UNIX_EPOCH
        );
        assert_eq!(
            parse_amz_date("20150830T123600Z")
                .unwrap()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            1_440_938_160
        );
        assert!(parse_amz_date("2015-08-30T12:36:00Z").is_none());
        assert!(parse_amz_date("20151330T123600Z").is_none());
    }

    #[test]
    fn header_value_normalization_collapses_spaces() {
        assert_eq!(normalize_header_value("  a   b\t c  "), "a b c");
    }
}
