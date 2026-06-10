//! Chunk encoder — records in, standard-zstd multiframe body + sidecars out
//! (DESIGN.md §4–5).
//!
//! A "chunk" is one S3 data object: the concatenation of independent
//! RFC 8878 zstd frames, each holding ~`frame_target_bytes` of uncompressed
//! JSONL, cut on record boundaries, with the zstd content checksum (XXH64)
//! enabled. Concatenated standard frames decompress as a single stream
//! under `zstd -dc`, Athena, and every stock zstd binding — that property
//! is the product's lock-in-avoidance claim, so the container must stay
//! plain zstd.

use std::io::Write;

use bytes::Bytes;
use s4_codec::index::{FrameIndex, FrameIndexEntry};

use crate::record::{LogRecord, RecordError};
use crate::tsindex::{TsEntry, TsIndex};

#[derive(Debug, Clone)]
pub struct ChunkConfig {
    /// Cut a zstd frame once this much uncompressed JSONL accumulated.
    pub frame_target_bytes: usize,
    /// zstd compression level (S4 default: 3).
    pub zstd_level: i32,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            frame_target_bytes: 4 << 20,
            zstd_level: 3,
        }
    }
}

/// A fully encoded data object plus everything the sidecars and manifest
/// need to describe it.
#[derive(Debug, Clone)]
pub struct EncodedChunk {
    /// Concatenated standard zstd frames.
    pub body: Bytes,
    /// S4IX entries (byte mapping); `source_etag`/`source_compressed_size`
    /// are stamped by the sink after the PUT (s4-server discipline).
    pub frame_index: FrameIndex,
    /// S4LT entries, 1:1 with `frame_index.entries`.
    pub ts_index: TsIndex,
    pub record_count: u64,
    pub uncompressed_bytes: u64,
    /// Min/max event timestamp across the whole chunk (epoch ms).
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    /// CRC32C over `body` (manifest field; S3 PUT additionally verifies via
    /// `ChecksumAlgorithm::Crc32C`).
    pub crc32c: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum ChunkError {
    #[error(transparent)]
    Record(#[from] RecordError),
    #[error("zstd frame encode failed")]
    Io(#[from] std::io::Error),
}

/// Incremental chunk writer. `push` records (no ordering requirement —
/// S4LT stores min/max per frame), then `finish` to obtain the object.
pub struct ChunkWriter {
    cfg: ChunkConfig,
    /// Pending uncompressed JSONL for the current (uncut) frame.
    pending: Vec<u8>,
    pending_min_ts: i64,
    pending_max_ts: i64,
    pending_records: u64,
    body: Vec<u8>,
    entries: Vec<FrameIndexEntry>,
    ts_entries: Vec<TsEntry>,
    original_off: u64,
    record_count: u64,
    min_ts: i64,
    max_ts: i64,
}

impl ChunkWriter {
    pub fn new(cfg: ChunkConfig) -> Self {
        Self {
            cfg,
            pending: Vec::new(),
            pending_min_ts: i64::MAX,
            pending_max_ts: i64::MIN,
            pending_records: 0,
            body: Vec::new(),
            entries: Vec::new(),
            ts_entries: Vec::new(),
            original_off: 0,
            record_count: 0,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.record_count == 0 && self.pending_records == 0
    }

    /// Total uncompressed bytes accepted so far (flush-threshold signal for
    /// the gateway buffer).
    pub fn uncompressed_len(&self) -> u64 {
        self.original_off + self.pending.len() as u64
    }

    pub fn record_count(&self) -> u64 {
        self.record_count + self.pending_records
    }

    pub fn push(&mut self, rec: &LogRecord) -> Result<(), ChunkError> {
        rec.append_jsonl(&mut self.pending)?;
        self.pending_min_ts = self.pending_min_ts.min(rec.timestamp);
        self.pending_max_ts = self.pending_max_ts.max(rec.timestamp);
        self.pending_records += 1;
        if self.pending.len() >= self.cfg.frame_target_bytes {
            self.cut_frame()?;
        }
        Ok(())
    }

    fn cut_frame(&mut self) -> Result<(), ChunkError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let compressed = compress_frame(&self.pending, self.cfg.zstd_level)?;
        self.entries.push(FrameIndexEntry {
            original_offset: self.original_off,
            original_size: self.pending.len() as u64,
            compressed_offset: self.body.len() as u64,
            compressed_size: compressed.len() as u64,
        });
        self.ts_entries.push(TsEntry {
            min_ts: self.pending_min_ts,
            max_ts: self.pending_max_ts,
        });
        self.original_off += self.pending.len() as u64;
        self.body.extend_from_slice(&compressed);
        self.record_count += self.pending_records;
        self.min_ts = self.min_ts.min(self.pending_min_ts);
        self.max_ts = self.max_ts.max(self.pending_max_ts);
        self.pending.clear();
        self.pending_min_ts = i64::MAX;
        self.pending_max_ts = i64::MIN;
        self.pending_records = 0;
        Ok(())
    }

    /// Finalize. Returns `None` if no record was pushed.
    pub fn finish(mut self) -> Result<Option<EncodedChunk>, ChunkError> {
        self.cut_frame()?;
        if self.entries.is_empty() {
            return Ok(None);
        }
        let body = Bytes::from(self.body);
        let crc = crc32c::crc32c(&body);
        Ok(Some(EncodedChunk {
            frame_index: FrameIndex {
                total_padded_size: body.len() as u64,
                entries: self.entries,
                source_etag: None,
                source_compressed_size: None,
                sse_v3: None,
            },
            ts_index: TsIndex {
                entries: self.ts_entries,
            },
            record_count: self.record_count,
            uncompressed_bytes: self.original_off,
            min_timestamp: self.min_ts,
            max_timestamp: self.max_ts,
            crc32c: crc,
            body,
        }))
    }
}

/// Compress one standalone standard zstd frame with content checksum.
fn compress_frame(input: &[u8], level: i32) -> std::io::Result<Vec<u8>> {
    let mut enc =
        zstd::stream::write::Encoder::new(Vec::with_capacity(input.len() / 4 + 64), level)?;
    enc.include_checksum(true)?;
    enc.set_pledged_src_size(Some(input.len() as u64))?;
    enc.write_all(input)?;
    enc.finish()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn rec(ts: i64, msg: &str) -> LogRecord {
        LogRecord {
            timestamp: ts,
            stream: "s".into(),
            message: msg.into(),
            ingestion_time: None,
            event_id: None,
        }
    }

    #[test]
    fn empty_writer_finishes_to_none() {
        let w = ChunkWriter::new(ChunkConfig::default());
        assert!(w.finish().unwrap().is_none());
    }

    #[test]
    fn single_frame_roundtrips_via_plain_zstd() {
        let mut w = ChunkWriter::new(ChunkConfig::default());
        w.push(&rec(100, "hello")).unwrap();
        w.push(&rec(50, "world")).unwrap();
        let chunk = w.finish().unwrap().unwrap();
        assert_eq!(chunk.record_count, 2);
        assert_eq!(chunk.min_timestamp, 50);
        assert_eq!(chunk.max_timestamp, 100);
        assert_eq!(chunk.frame_index.entries.len(), 1);
        assert_eq!(chunk.ts_index.entries.len(), 1);
        // Plain zstd must decode the body with no S4 tooling.
        let out = zstd::stream::decode_all(&chunk.body[..]).unwrap();
        assert_eq!(out.len() as u64, chunk.uncompressed_bytes);
        let lines: Vec<_> = out
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(LogRecord::from_jsonl(lines[0]).unwrap().message, "hello");
    }

    #[test]
    fn multi_frame_cut_and_concatenated_decode() {
        let mut w = ChunkWriter::new(ChunkConfig {
            frame_target_bytes: 256,
            zstd_level: 3,
        });
        for i in 0..200i64 {
            w.push(&rec(i, &format!("message number {i} with some padding")))
                .unwrap();
        }
        let chunk = w.finish().unwrap().unwrap();
        assert!(
            chunk.frame_index.entries.len() > 1,
            "expected multiple frames"
        );
        assert_eq!(
            chunk.frame_index.entries.len(),
            chunk.ts_index.entries.len()
        );
        // Concatenated standard frames decode as one stream.
        let out = zstd::stream::decode_all(&chunk.body[..]).unwrap();
        assert_eq!(out.len() as u64, chunk.uncompressed_bytes);
        // Frame byte accounting must tile the body exactly.
        let last = chunk.frame_index.entries.last().unwrap();
        assert_eq!(
            last.compressed_offset + last.compressed_size,
            chunk.body.len() as u64
        );
    }
}
