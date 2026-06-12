//! S3 key layout (DESIGN.md §3). Frozen for the 1.x series (DESIGN.md §14) —
//! external Athena/Glue table definitions depend on the partition scheme.
//!
//! ```text
//! {prefix}data/account={acct}/loggroup={g}/dt={YYYY-MM-DD}/{name}.jsonl.zst
//! {prefix}index/...same tail...{name}.jsonl.zst.s4index   (+ .s4lts)
//! {prefix}manifest/account={acct}/loggroup={g}/window={start_ms}-{end_ms}.json
//! ```
//!
//! `data/` and `index/` are split because Athena / Spark read every file
//! under a partition — a binary sidecar sitting next to the JSONL data
//! would break queries.

use chrono::{DateTime, Utc};

pub const DATA_SEG: &str = "data";
pub const INDEX_SEG: &str = "index";
pub const MANIFEST_SEG: &str = "manifest";
pub const DATA_SUFFIX: &str = ".jsonl.zst";
/// S4IX sidecar suffix — single source of truth is s4-codec.
pub use s4_codec::index::SIDECAR_SUFFIX as INDEX_SIDECAR_SUFFIX;
/// S4LT timestamp sidecar suffix (DESIGN.md §5).
pub const TS_SIDECAR_SUFFIX: &str = ".s4lts";

/// Percent-encode every byte outside `[A-Za-z0-9_.-]` as `%XX` (uppercase
/// hex). Reversible, Hive-partition-safe (`=` and `/` are always encoded).
pub fn sanitize_log_group(group: &str) -> String {
    let mut out = String::with_capacity(group.len());
    for &b in group.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' => out.push(b as char),
            _ => {
                out.push('%');
                out.push_str(&format!("{b:02X}"));
            }
        }
    }
    out
}

/// Inverse of [`sanitize_log_group`]. Returns `None` on malformed input
/// (truncated `%XX`, non-hex digits, or invalid UTF-8 after decoding).
pub fn unsanitize_log_group(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let hi = (hex[0] as char).to_digit(16)?;
            let lo = (hex[1] as char).to_digit(16)?;
            out.push((hi * 16 + lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Normalize a key prefix to `""` or `"…/"`.
pub fn norm_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}/")
    }
}

/// UTC calendar date (`YYYY-MM-DD`) of an epoch-milliseconds timestamp.
/// Timestamps outside chrono's representable range clamp to epoch.
pub fn date_from_ts_ms(ts_ms: i64) -> String {
    let dt = DateTime::<Utc>::from_timestamp_millis(ts_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    dt.format("%Y-%m-%d").to_string()
}

/// Identifies one data object (and its sidecars) within the layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkLocation {
    /// 12-digit AWS account id (or any operator-chosen scope label).
    pub account: String,
    /// Raw (unsanitized) log group name.
    pub log_group: String,
    /// UTC date partition `YYYY-MM-DD`.
    pub date: String,
    /// Object basename without `.jsonl.zst` — e.g. `1717900000000-000001`.
    pub name: String,
}

impl ChunkLocation {
    fn tail(&self) -> String {
        format!(
            "account={}/loggroup={}/dt={}/{}{}",
            self.account,
            sanitize_log_group(&self.log_group),
            self.date,
            self.name,
            DATA_SUFFIX
        )
    }

    pub fn data_key(&self, prefix: &str) -> String {
        format!("{}{}/{}", norm_prefix(prefix), DATA_SEG, self.tail())
    }

    pub fn index_key(&self, prefix: &str) -> String {
        format!(
            "{}{}/{}{}",
            norm_prefix(prefix),
            INDEX_SEG,
            self.tail(),
            INDEX_SIDECAR_SUFFIX
        )
    }

    pub fn ts_index_key(&self, prefix: &str) -> String {
        format!(
            "{}{}/{}{}",
            norm_prefix(prefix),
            INDEX_SEG,
            self.tail(),
            TS_SIDECAR_SUFFIX
        )
    }

    /// Parse a full data key back into its components. `prefix` must be the
    /// same value used to build the key. Returns `None` if the key does not
    /// belong to this layout/prefix.
    pub fn parse_data_key(prefix: &str, key: &str) -> Option<ChunkLocation> {
        let rest = key.strip_prefix(&format!("{}{}/", norm_prefix(prefix), DATA_SEG))?;
        let mut parts = rest.split('/');
        let account = parts.next()?.strip_prefix("account=")?.to_string();
        let group_enc = parts.next()?.strip_prefix("loggroup=")?;
        let date = parts.next()?.strip_prefix("dt=")?.to_string();
        let file = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        let name = file.strip_suffix(DATA_SUFFIX)?.to_string();
        Some(ChunkLocation {
            account,
            log_group: unsanitize_log_group(group_enc)?,
            date,
            name,
        })
    }
}

/// Key prefix that lists every data object of one log group:
/// `{prefix}data/account={acct}/loggroup={g}/`.
pub fn data_group_prefix(prefix: &str, account: &str, log_group: &str) -> String {
    format!(
        "{}{}/account={}/loggroup={}/",
        norm_prefix(prefix),
        DATA_SEG,
        account,
        sanitize_log_group(log_group)
    )
}

/// Manifest key for one drain window (DESIGN.md §7).
pub fn manifest_key(
    prefix: &str,
    account: &str,
    log_group: &str,
    window_start_ms: i64,
    window_end_ms: i64,
) -> String {
    format!(
        "{}{}/account={}/loggroup={}/window={}-{}.json",
        norm_prefix(prefix),
        MANIFEST_SEG,
        account,
        sanitize_log_group(log_group),
        window_start_ms,
        window_end_ms
    )
}

/// Manifest prefix for one log group (for listing completed windows).
pub fn manifest_group_prefix(prefix: &str, account: &str, log_group: &str) -> String {
    format!(
        "{}{}/account={}/loggroup={}/",
        norm_prefix(prefix),
        MANIFEST_SEG,
        account,
        sanitize_log_group(log_group)
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_roundtrip_lambda_group() {
        let g = "/aws/lambda/payments-prod";
        let enc = sanitize_log_group(g);
        assert_eq!(enc, "%2Faws%2Flambda%2Fpayments-prod");
        assert_eq!(unsanitize_log_group(&enc).unwrap(), g);
    }

    #[test]
    fn sanitize_encodes_equals_and_percent() {
        let g = "g=1%2";
        let enc = sanitize_log_group(g);
        assert!(!enc.contains('='));
        assert_eq!(unsanitize_log_group(&enc).unwrap(), g);
    }

    #[test]
    fn data_key_layout_and_parse() {
        let loc = ChunkLocation {
            account: "123456789012".into(),
            log_group: "/aws/lambda/foo".into(),
            date: "2026-06-10".into(),
            name: "1717900000000-000001".into(),
        };
        let key = loc.data_key("s4logs");
        assert_eq!(
            key,
            "s4logs/data/account=123456789012/loggroup=%2Faws%2Flambda%2Ffoo/dt=2026-06-10/1717900000000-000001.jsonl.zst"
        );
        assert_eq!(ChunkLocation::parse_data_key("s4logs", &key).unwrap(), loc);
        assert!(loc.index_key("s4logs").starts_with("s4logs/index/"));
        assert!(loc.index_key("s4logs").ends_with(".jsonl.zst.s4index"));
        assert!(loc.ts_index_key("s4logs").ends_with(".jsonl.zst.s4lts"));
    }

    #[test]
    fn date_from_ts() {
        assert_eq!(date_from_ts_ms(0), "1970-01-01");
        assert_eq!(date_from_ts_ms(1765411200000), "2025-12-11");
    }
}
