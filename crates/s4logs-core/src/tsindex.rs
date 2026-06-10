//! S4LT v1 — per-frame timestamp-range sidecar (DESIGN.md §5).
//!
//! Companion to the S4IX byte-offset sidecar: entry `i` here describes the
//! same zstd frame as S4IX entry `i`. Layout (all little-endian):
//!
//! ```text
//! magic "S4LT" (4) | version u32 = 1 | frame_count u64
//! per frame: min_ts i64 | max_ts i64          (16 B/frame)
//! ```
//!
//! S4IX itself is reused verbatim from s4-codec; S4LT deliberately lives in
//! a separate file so the S4IX format stays byte-identical to S4 proper.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

pub const TS_INDEX_MAGIC: &[u8; 4] = b"S4LT";
pub const TS_INDEX_VERSION: u32 = 1;
pub const TS_HEADER_BYTES: usize = 4 + 4 + 8;
pub const TS_ENTRY_BYTES: usize = 8 + 8;
/// Same hard cap as `s4_codec::index::MAX_FRAMES` — the two sidecars are
/// 1:1, so a count that S4IX would refuse is equally hostile here.
pub const MAX_FRAMES: u64 = 16 * 1024 * 1024;

/// Timestamp range (epoch ms, inclusive both ends) of one zstd frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TsEntry {
    pub min_ts: i64,
    pub max_ts: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TsIndex {
    pub entries: Vec<TsEntry>,
}

/// `#[non_exhaustive]` for the same reason as s4-codec's `IndexError`:
/// validation guards may grow in minor releases.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TsIndexError {
    #[error("ts index too short: {0} bytes")]
    TooShort(usize),
    #[error("bad ts index magic: {got:?}")]
    BadMagic { got: [u8; 4] },
    #[error("unsupported ts index version {0} (this build supports {TS_INDEX_VERSION})")]
    UnsupportedVersion(u32),
    #[error("entry count {claimed} doesn't match buffer remaining {remaining}")]
    EntryCountMismatch { claimed: u64, remaining: usize },
    #[error("ts index entry count {got} exceeds MAX_FRAMES={MAX_FRAMES}")]
    TooManyFrames { got: u64 },
    #[error("ts index entry {idx} has min_ts {min_ts} > max_ts {max_ts}")]
    InvalidEntry {
        idx: usize,
        min_ts: i64,
        max_ts: i64,
    },
}

pub fn encode_ts_index(idx: &TsIndex) -> Bytes {
    let mut buf = BytesMut::with_capacity(TS_HEADER_BYTES + idx.entries.len() * TS_ENTRY_BYTES);
    buf.put_slice(TS_INDEX_MAGIC);
    buf.put_u32_le(TS_INDEX_VERSION);
    buf.put_u64_le(idx.entries.len() as u64);
    for e in &idx.entries {
        buf.put_i64_le(e.min_ts);
        buf.put_i64_le(e.max_ts);
    }
    buf.freeze()
}

pub fn decode_ts_index(mut input: Bytes) -> Result<TsIndex, TsIndexError> {
    if input.len() < TS_HEADER_BYTES {
        return Err(TsIndexError::TooShort(input.len()));
    }
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&input[..4]);
    if &magic != TS_INDEX_MAGIC {
        return Err(TsIndexError::BadMagic { got: magic });
    }
    input.advance(4);
    let version = input.get_u32_le();
    if version != TS_INDEX_VERSION {
        return Err(TsIndexError::UnsupportedVersion(version));
    }
    let n = input.get_u64_le();
    if n > MAX_FRAMES {
        return Err(TsIndexError::TooManyFrames { got: n });
    }
    let expected_remaining = (n as usize).saturating_mul(TS_ENTRY_BYTES);
    if input.len() != expected_remaining {
        return Err(TsIndexError::EntryCountMismatch {
            claimed: n,
            remaining: input.len(),
        });
    }
    // Same bootstrap-capacity discipline as s4-codec `decode_index`: never
    // pre-allocate more than a sane bound off an attacker-supplied count.
    const BOOTSTRAP_ENTRIES: usize = 4096;
    let mut entries = Vec::with_capacity((n as usize).min(BOOTSTRAP_ENTRIES));
    for idx in 0..n {
        let min_ts = input.get_i64_le();
        let max_ts = input.get_i64_le();
        if min_ts > max_ts {
            return Err(TsIndexError::InvalidEntry {
                idx: idx as usize,
                min_ts,
                max_ts,
            });
        }
        entries.push(TsEntry { min_ts, max_ts });
    }
    Ok(TsIndex { entries })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn sample() -> TsIndex {
        TsIndex {
            entries: vec![
                TsEntry {
                    min_ts: 100,
                    max_ts: 200,
                },
                TsEntry {
                    min_ts: 150,
                    max_ts: 900,
                },
                TsEntry {
                    min_ts: -5,
                    max_ts: -1,
                },
            ],
        }
    }

    #[test]
    fn roundtrip() {
        let idx = sample();
        assert_eq!(decode_ts_index(encode_ts_index(&idx)).unwrap(), idx);
    }

    #[test]
    fn empty_roundtrip() {
        let idx = TsIndex::default();
        assert_eq!(decode_ts_index(encode_ts_index(&idx)).unwrap(), idx);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = encode_ts_index(&sample()).to_vec();
        bytes[0] = b'X';
        assert!(matches!(
            decode_ts_index(Bytes::from(bytes)),
            Err(TsIndexError::BadMagic { .. })
        ));
    }

    #[test]
    fn rejects_count_mismatch() {
        let mut bytes = encode_ts_index(&sample()).to_vec();
        bytes.truncate(bytes.len() - 1);
        assert!(matches!(
            decode_ts_index(Bytes::from(bytes)),
            Err(TsIndexError::EntryCountMismatch { .. })
        ));
    }

    #[test]
    fn rejects_min_gt_max() {
        let mut buf = bytes::BytesMut::new();
        buf.put_slice(TS_INDEX_MAGIC);
        buf.put_u32_le(TS_INDEX_VERSION);
        buf.put_u64_le(1);
        buf.put_i64_le(10);
        buf.put_i64_le(5);
        assert!(matches!(
            decode_ts_index(buf.freeze()),
            Err(TsIndexError::InvalidEntry { .. })
        ));
    }

    #[test]
    fn rejects_huge_count_before_alloc() {
        let mut buf = bytes::BytesMut::new();
        buf.put_slice(TS_INDEX_MAGIC);
        buf.put_u32_le(TS_INDEX_VERSION);
        buf.put_u64_le(u64::MAX);
        assert!(matches!(
            decode_ts_index(buf.freeze()),
            Err(TsIndexError::TooManyFrames { .. })
        ));
    }
}
