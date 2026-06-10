//! Hand-rolled argument parsers: timestamps (RFC3339 | epoch-ms), durations
//! ("1h", "15m", "500ms"), and sizes ("256MiB", "8MB"). No extra deps.

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};

/// Parse `--from`/`--to`: either epoch **milliseconds** (all digits, optional
/// leading `-`) or an RFC3339 timestamp (`2026-06-01T00:00:00Z`).
pub fn parse_time_ms(s: &str) -> Result<i64, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty timestamp".into());
    }
    let digits = t.strip_prefix('-').unwrap_or(t);
    if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
        return t
            .parse::<i64>()
            .map_err(|e| format!("epoch-ms timestamp {t:?} out of range: {e}"));
    }
    DateTime::parse_from_rfc3339(t)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| {
            format!(
                "expected RFC3339 (e.g. 2026-06-01T00:00:00Z) or epoch milliseconds, got {t:?}: {e}"
            )
        })
}

/// Epoch-ms → RFC3339 with millisecond precision (UTC, `Z` suffix).
pub fn fmt_ts(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true))
        .unwrap_or_else(|| format!("{ms}ms"))
}

/// Parse a duration with a required unit suffix: `ms`, `s`, `m`, `h`, `d`.
/// Single unit only (`1h`, not `1h30m`).
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let t = s.trim();
    let split = t
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("duration {t:?} needs a unit suffix: ms, s, m, h or d"))?;
    if split == 0 {
        return Err(format!("duration {t:?} must start with digits"));
    }
    let n: u64 = t[..split]
        .parse()
        .map_err(|e| format!("bad duration value in {t:?}: {e}"))?;
    let per_unit: u64 = match t[split..].trim().to_ascii_lowercase().as_str() {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        other => {
            return Err(format!(
                "unknown duration unit {other:?} (use ms, s, m, h, d)"
            ));
        }
    };
    n.checked_mul(per_unit)
        .map(Duration::from_millis)
        .ok_or_else(|| format!("duration {t:?} overflows"))
}

/// [`parse_duration`] as epoch-milliseconds `i64` (drain window math).
pub fn parse_duration_ms(s: &str) -> Result<i64, String> {
    let d = parse_duration(s)?;
    i64::try_from(d.as_millis()).map_err(|_| format!("duration {s:?} overflows i64 ms"))
}

/// Parse a byte size: plain digits = bytes; decimal suffixes `kB/MB/GB/TB`
/// (powers of 1000) and binary `KiB/MiB/GiB/TiB` (powers of 1024),
/// case-insensitive. Integers only.
pub fn parse_size_bytes(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    if split == 0 {
        return Err(format!("size {t:?} must start with digits"));
    }
    let n: u64 = t[..split]
        .parse()
        .map_err(|e| format!("bad size value in {t:?}: {e}"))?;
    let mult: u64 = match t[split..].trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" | "k" => 1_000,
        "mb" | "m" => 1_000_000,
        "gb" | "g" => 1_000_000_000,
        "tb" | "t" => 1_000_000_000_000,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        "tib" => 1 << 40,
        other => {
            return Err(format!(
                "unknown size unit {other:?} (use B, kB/MB/GB/TB or KiB/MiB/GiB/TiB)"
            ));
        }
    };
    n.checked_mul(mult)
        .ok_or_else(|| format!("size {t:?} overflows"))
}

/// Human-readable binary size for reports.
pub fn format_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn time_epoch_ms_passthrough() {
        assert_eq!(parse_time_ms("1717900000123").unwrap(), 1_717_900_000_123);
        assert_eq!(parse_time_ms("0").unwrap(), 0);
        assert_eq!(parse_time_ms("-5").unwrap(), -5);
    }

    #[test]
    fn time_rfc3339_utc_and_offset() {
        // 2024-06-05T00:00:00Z — same constant the drain tests use.
        assert_eq!(
            parse_time_ms("2024-06-05T00:00:00Z").unwrap(),
            1_717_545_600_000
        );
        // +09:00 (JST midnight = 15:00Z previous day).
        assert_eq!(
            parse_time_ms("2024-06-05T09:00:00+09:00").unwrap(),
            1_717_545_600_000
        );
        assert_eq!(
            parse_time_ms("2024-06-05T00:00:00.250Z").unwrap(),
            1_717_545_600_250
        );
    }

    #[test]
    fn time_rejects_garbage() {
        assert!(parse_time_ms("").is_err());
        assert!(parse_time_ms("yesterday").is_err());
        assert!(
            parse_time_ms("2024-06-05").is_err(),
            "date-only is not RFC3339"
        );
        assert!(
            parse_time_ms("99999999999999999999").is_err(),
            "i64 overflow"
        );
    }

    #[test]
    fn fmt_ts_roundtrips() {
        let s = fmt_ts(1_717_545_600_250);
        assert_eq!(s, "2024-06-05T00:00:00.250Z");
        assert_eq!(parse_time_ms(&s).unwrap(), 1_717_545_600_250);
    }

    #[test]
    fn duration_units() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("2d").unwrap(), Duration::from_secs(172_800));
        assert_eq!(parse_duration_ms("1h").unwrap(), 3_600_000);
    }

    #[test]
    fn duration_rejects_bare_numbers_and_unknown_units() {
        assert!(parse_duration("60").is_err(), "unit suffix is required");
        assert!(parse_duration("1w").is_err());
        assert!(parse_duration("h").is_err());
        assert!(parse_duration("").is_err());
        assert!(parse_duration("99999999999999999999d").is_err());
    }

    #[test]
    fn size_units() {
        assert_eq!(parse_size_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_size_bytes("256MiB").unwrap(), 256 << 20);
        assert_eq!(parse_size_bytes("8MB").unwrap(), 8_000_000);
        assert_eq!(parse_size_bytes("1KiB").unwrap(), 1024);
        assert_eq!(parse_size_bytes("2gib").unwrap(), 2 << 30);
        assert_eq!(parse_size_bytes("7B").unwrap(), 7);
        assert_eq!(parse_size_bytes("3k").unwrap(), 3_000);
    }

    #[test]
    fn size_rejects_garbage() {
        assert!(parse_size_bytes("MiB").is_err());
        assert!(parse_size_bytes("1.5GB").is_err(), "integers only");
        assert!(parse_size_bytes("").is_err());
        assert!(parse_size_bytes("8XB").is_err());
        assert!(parse_size_bytes("99999999999999999999").is_err());
    }

    #[test]
    fn format_bytes_binary() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(format_bytes(256 << 20), "256.0 MiB");
        assert_eq!(format_bytes((3 << 30) + (512 << 20)), "3.5 GiB");
    }
}
