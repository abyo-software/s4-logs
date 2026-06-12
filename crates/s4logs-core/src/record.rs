//! On-disk JSONL record schema (DESIGN.md §2). Field names are part of the
//! format — they become Athena column names. Do not rename. Frozen for the
//! 1.x series (DESIGN.md §14); new fields may only be added as optional.

use serde::{Deserialize, Serialize};

/// One CloudWatch Logs event as stored in a data object (one JSONL line).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    /// Event timestamp, epoch milliseconds.
    pub timestamp: i64,
    /// Source log stream name.
    pub stream: String,
    /// Raw log line.
    pub message: String,
    /// CloudWatch ingestion time, epoch milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingestion_time: Option<i64>,
    /// CloudWatch event id (present on FilterLogEvents output).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("jsonl encode failed")]
    Encode(#[source] serde_json::Error),
    #[error("jsonl decode failed")]
    Decode(#[source] serde_json::Error),
}

impl LogRecord {
    /// Append this record as one JSONL line (`{...}\n`) to `out`.
    pub fn append_jsonl(&self, out: &mut Vec<u8>) -> Result<(), RecordError> {
        serde_json::to_writer(&mut *out, self).map_err(RecordError::Encode)?;
        out.push(b'\n');
        Ok(())
    }

    /// Parse one JSONL line (with or without trailing newline).
    pub fn from_jsonl(line: &[u8]) -> Result<Self, RecordError> {
        serde_json::from_slice(line).map_err(RecordError::Decode)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn jsonl_roundtrip_optional_fields_omitted() {
        let rec = LogRecord {
            timestamp: 1717900000123,
            stream: "app/i-0abc".into(),
            message: "hello".into(),
            ingestion_time: None,
            event_id: None,
        };
        let mut buf = Vec::new();
        rec.append_jsonl(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        assert!(!s.contains("ingestion_time"));
        assert!(s.ends_with('\n'));
        assert_eq!(LogRecord::from_jsonl(buf.trim_ascii_end()).unwrap(), rec);
    }
}
