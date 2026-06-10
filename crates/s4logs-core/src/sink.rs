//! `ChunkSink` — where encoded chunks go. `ObjectStore` (S3) is the real
//! one; `MemorySink` backs drain/gateway unit tests.
//!
//! Write ordering contract (DESIGN.md §6): data object first, sidecars only
//! after the data write succeeded. Sidecar `FrameIndex` is stamped with the
//! post-PUT etag + body length before encoding (s4-server discipline).

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use s4_codec::index::encode_index;
use thiserror::Error;

use crate::chunk::EncodedChunk;
use crate::layout::ChunkLocation;
use crate::tsindex::encode_ts_index;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SinkError {
    #[error("chunk storage failed: {0}")]
    Storage(String),
}

/// Receipt for a stored chunk; everything a drain manifest entry needs.
#[derive(Debug, Clone)]
pub struct PutReceipt {
    pub data_key: String,
    pub index_key: String,
    pub ts_index_key: String,
    /// Backend-reported ETag of the data object (None for MemorySink).
    pub etag: Option<String>,
    pub crc32c: u32,
    pub body_len: u64,
}

#[async_trait]
pub trait ChunkSink: Send + Sync {
    /// Layout key prefix this sink writes under.
    fn key_prefix(&self) -> &str;

    /// Store `chunk` (data + both sidecars) at `loc`. Implementations MUST
    /// write the data object before either sidecar.
    async fn put_chunk(
        &self,
        loc: &ChunkLocation,
        chunk: &EncodedChunk,
    ) -> Result<PutReceipt, SinkError>;
}

/// Encode both sidecars with the post-PUT stamp applied. Shared by
/// `MemorySink` and `ObjectStore` so the stamp discipline can't drift.
pub fn encode_sidecars(chunk: &EncodedChunk, etag: Option<&str>) -> (Bytes, Bytes) {
    let mut idx = chunk.frame_index.clone();
    idx.source_etag = etag.map(str::to_owned);
    idx.source_compressed_size = Some(chunk.body.len() as u64);
    (encode_index(&idx), encode_ts_index(&chunk.ts_index))
}

/// In-memory sink for unit tests (key → bytes, BTreeMap for stable listing).
#[derive(Debug, Default)]
pub struct MemorySink {
    prefix: String,
    objects: Mutex<BTreeMap<String, Bytes>>,
}

impl MemorySink {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: crate::layout::norm_prefix(prefix),
            objects: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.objects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .cloned()
    }

    pub fn keys(&self) -> Vec<String> {
        self.objects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    /// Raw insert for tests that need manifests or pre-seeded state.
    pub fn insert(&self, key: String, value: Bytes) {
        self.objects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, value);
    }
}

#[async_trait]
impl ChunkSink for MemorySink {
    fn key_prefix(&self) -> &str {
        &self.prefix
    }

    async fn put_chunk(
        &self,
        loc: &ChunkLocation,
        chunk: &EncodedChunk,
    ) -> Result<PutReceipt, SinkError> {
        let data_key = loc.data_key(&self.prefix);
        let index_key = loc.index_key(&self.prefix);
        let ts_index_key = loc.ts_index_key(&self.prefix);
        let etag = format!("\"mem-{:08x}\"", chunk.crc32c);
        let (idx_bytes, ts_bytes) = encode_sidecars(chunk, Some(&etag));
        {
            let mut map = self.objects.lock().unwrap_or_else(|e| e.into_inner());
            map.insert(data_key.clone(), chunk.body.clone());
            map.insert(index_key.clone(), idx_bytes);
            map.insert(ts_index_key.clone(), ts_bytes);
        }
        Ok(PutReceipt {
            data_key,
            index_key,
            ts_index_key,
            etag: Some(etag),
            crc32c: chunk.crc32c,
            body_len: chunk.body.len() as u64,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::chunk::{ChunkConfig, ChunkWriter};
    use crate::record::LogRecord;
    use s4_codec::index::decode_index;

    #[tokio::test]
    async fn memory_sink_stores_data_and_stamped_sidecars() {
        let sink = MemorySink::new("s4logs");
        let mut w = ChunkWriter::new(ChunkConfig::default());
        w.push(&LogRecord {
            timestamp: 1,
            stream: "s".into(),
            message: "m".into(),
            ingestion_time: None,
            event_id: None,
        })
        .unwrap();
        let chunk = w.finish().unwrap().unwrap();
        let loc = ChunkLocation {
            account: "123456789012".into(),
            log_group: "/g".into(),
            date: "2026-06-10".into(),
            name: "0-000000".into(),
        };
        let receipt = sink.put_chunk(&loc, &chunk).await.unwrap();
        assert_eq!(sink.get(&receipt.data_key).unwrap(), chunk.body);
        let idx = decode_index(sink.get(&receipt.index_key).unwrap()).unwrap();
        assert_eq!(idx.entries, chunk.frame_index.entries);
        assert_eq!(idx.source_compressed_size, Some(chunk.body.len() as u64));
        assert_eq!(idx.source_etag, receipt.etag);
        assert!(sink.get(&receipt.ts_index_key).is_some());
    }
}
