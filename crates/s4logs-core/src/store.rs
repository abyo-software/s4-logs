//! `ObjectStore` — the S3 implementation of [`crate::sink::ChunkSink`] plus
//! generic read/list helpers used by grep/restore and drain manifests.
//!
//! Wave 1A implements every `todo!()` here. Contract (DESIGN.md §6):
//! - `put_bytes` uses `ChecksumAlgorithm::Crc32C` so S3 verifies end-to-end.
//! - `put_chunk` writes data, then `.s4index`, then `.s4lts`
//!   (write-after-data), stamping sidecars via `sink::encode_sidecars`.
//! - `get_range` issues `Range: bytes=start-(end-1)`.
//! - `list_keys` paginates `ListObjectsV2` to exhaustion.

use bytes::Bytes;
use s4_codec::index::{FrameIndex, IndexError, decode_index};

use crate::layout::ChunkLocation;
use crate::sink::{ChunkSink, PutReceipt, SinkError};
use crate::tsindex::{TsIndex, TsIndexError, decode_ts_index};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    #[error("s3 {op} failed for key {key}: {source}")]
    Aws {
        op: &'static str,
        key: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("object not found: {key}")]
    NotFound { key: String },
    #[error("bad S4IX sidecar")]
    Index(#[from] IndexError),
    #[error("bad S4LT sidecar")]
    TsIndex(#[from] TsIndexError),
}

#[derive(Debug, Clone)]
pub struct ObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl ObjectStore {
    pub fn new(client: aws_sdk_s3::Client, bucket: impl Into<String>, prefix: &str) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            prefix: crate::layout::norm_prefix(prefix),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn client(&self) -> &aws_sdk_s3::Client {
        &self.client
    }

    /// PUT with CRC32C checksum; returns the backend ETag.
    pub async fn put_bytes(&self, _key: &str, _body: Bytes) -> Result<Option<String>, StoreError> {
        todo!("wave 1A")
    }

    pub async fn get_bytes(&self, _key: &str) -> Result<Bytes, StoreError> {
        todo!("wave 1A")
    }

    /// Fetch `[start, end_exclusive)` of the object body.
    pub async fn get_range(
        &self,
        _key: &str,
        _start: u64,
        _end_exclusive: u64,
    ) -> Result<Bytes, StoreError> {
        todo!("wave 1A")
    }

    /// All keys under `key_prefix` (paginated to exhaustion, lexicographic).
    pub async fn list_keys(&self, _key_prefix: &str) -> Result<Vec<String>, StoreError> {
        todo!("wave 1A")
    }

    pub async fn exists(&self, _key: &str) -> Result<bool, StoreError> {
        todo!("wave 1A")
    }

    /// Fetch + decode both sidecars for one data object.
    pub async fn load_indexes(
        &self,
        loc: &ChunkLocation,
    ) -> Result<(FrameIndex, TsIndex), StoreError> {
        let idx = decode_index(self.get_bytes(&loc.index_key(&self.prefix)).await?)?;
        let ts = decode_ts_index(self.get_bytes(&loc.ts_index_key(&self.prefix)).await?)?;
        Ok((idx, ts))
    }

    /// Every data object of one log group, parsed back to locations.
    /// Keys that don't parse (foreign objects under the prefix) are skipped
    /// with a `tracing::warn!`.
    pub async fn list_chunks(
        &self,
        account: &str,
        log_group: &str,
    ) -> Result<Vec<ChunkLocation>, StoreError> {
        let prefix = crate::layout::data_group_prefix(&self.prefix, account, log_group);
        let keys = self.list_keys(&prefix).await?;
        Ok(keys
            .iter()
            .filter_map(|k| {
                let parsed = ChunkLocation::parse_data_key(&self.prefix, k);
                if parsed.is_none() {
                    tracing::warn!(key = %k, "skipping unparseable key under data prefix");
                }
                parsed
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl ChunkSink for ObjectStore {
    fn key_prefix(&self) -> &str {
        &self.prefix
    }

    async fn put_chunk(
        &self,
        _loc: &ChunkLocation,
        _chunk: &crate::chunk::EncodedChunk,
    ) -> Result<PutReceipt, SinkError> {
        todo!("wave 1A")
    }
}
