//! `ObjectStore` — the S3 implementation of [`crate::sink::ChunkSink`] plus
//! generic read/list helpers used by grep/restore and drain manifests.
//!
//! Wave 1A implements every `todo!()` here. Contract (DESIGN.md §6):
//! - `put_bytes` uses `ChecksumAlgorithm::Crc32C` so S3 verifies end-to-end.
//! - `put_chunk` writes data, then `.s4index`, then `.s4lts`
//!   (write-after-data), stamping sidecars via `sink::encode_sidecars`.
//! - `get_range` issues `Range: bytes=start-(end-1)`.
//! - `list_keys` paginates `ListObjectsV2` to exhaustion.

use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumAlgorithm;
use bytes::Bytes;
use s4_codec::index::{FrameIndex, IndexError, decode_index};

use crate::layout::ChunkLocation;
use crate::sink::{ChunkSink, PutReceipt, SinkError, encode_sidecars};
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

/// Boxes any SDK error into [`StoreError::Aws`]. `key` is the object key for
/// single-object ops, or the listing prefix for `ListObjectsV2`.
fn aws_err(
    op: &'static str,
    key: &str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> StoreError {
    StoreError::Aws {
        op,
        key: key.to_owned(),
        source: Box::new(source),
    }
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
    pub async fn put_bytes(&self, key: &str, body: Bytes) -> Result<Option<String>, StoreError> {
        let out = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            // DESIGN.md §6: the SDK computes CRC32C over the wire body and
            // S3 verifies it server-side — end-to-end integrity per PUT.
            .checksum_algorithm(ChecksumAlgorithm::Crc32C)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|e| aws_err("PutObject", key, e))?;
        Ok(out.e_tag().map(str::to_owned))
    }

    pub async fn get_bytes(&self, key: &str) -> Result<Bytes, StoreError> {
        let resp = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                if err
                    .as_service_error()
                    .is_some_and(GetObjectError::is_no_such_key)
                {
                    return Err(StoreError::NotFound {
                        key: key.to_owned(),
                    });
                }
                return Err(aws_err("GetObject", key, err));
            }
        };
        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| aws_err("GetObject(body)", key, e))?;
        Ok(data.into_bytes())
    }

    /// Fetch `[start, end_exclusive)` of the object body.
    pub async fn get_range(
        &self,
        key: &str,
        start: u64,
        end_exclusive: u64,
    ) -> Result<Bytes, StoreError> {
        // An empty range cannot be expressed as an HTTP `Range` header
        // (`bytes=N-(N-1)` would be rejected); there is nothing to fetch.
        if start >= end_exclusive {
            return Ok(Bytes::new());
        }
        let resp = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(format!("bytes={start}-{}", end_exclusive - 1))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                if err
                    .as_service_error()
                    .is_some_and(GetObjectError::is_no_such_key)
                {
                    return Err(StoreError::NotFound {
                        key: key.to_owned(),
                    });
                }
                return Err(aws_err("GetObject(range)", key, err));
            }
        };
        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| aws_err("GetObject(range body)", key, e))?;
        Ok(data.into_bytes())
    }

    /// All keys under `key_prefix` (paginated to exhaustion, lexicographic).
    pub async fn list_keys(&self, key_prefix: &str) -> Result<Vec<String>, StoreError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let page = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(key_prefix)
                .set_continuation_token(token.take())
                .send()
                .await
                .map_err(|e| aws_err("ListObjectsV2", key_prefix, e))?;
            keys.extend(
                page.contents()
                    .iter()
                    .filter_map(|obj| obj.key().map(str::to_owned)),
            );
            // Loop on the token, not `is_truncated` — S3-compatible stores
            // (MinIO, LocalStack) are inconsistent about the boolean, but a
            // present token always means another page.
            token = page.next_continuation_token().map(str::to_owned);
            if token.is_none() {
                break;
            }
        }
        Ok(keys)
    }

    pub async fn exists(&self, key: &str) -> Result<bool, StoreError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(HeadObjectError::is_not_found) =>
            {
                Ok(false)
            }
            Err(err) => Err(aws_err("HeadObject", key, err)),
        }
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
        loc: &ChunkLocation,
        chunk: &crate::chunk::EncodedChunk,
    ) -> Result<PutReceipt, SinkError> {
        let data_key = loc.data_key(&self.prefix);
        let index_key = loc.index_key(&self.prefix);
        let ts_index_key = loc.ts_index_key(&self.prefix);
        // Write-after-data (DESIGN.md §6): a crash between PUTs may leave
        // data without sidecars (recoverable, re-derivable) but never a
        // sidecar pointing at data that was never acknowledged.
        let etag = self.put_bytes(&data_key, chunk.body.clone()).await?;
        // Sidecars are stamped with the post-PUT etag + body length before
        // encoding; any sidecar PUT failure surfaces — a half-indexed chunk
        // must fail the drain window, not silently pass.
        let (idx_bytes, ts_bytes) = encode_sidecars(chunk, etag.as_deref());
        self.put_bytes(&index_key, idx_bytes).await?;
        self.put_bytes(&ts_index_key, ts_bytes).await?;
        Ok(PutReceipt {
            data_key,
            index_key,
            ts_index_key,
            etag,
            crc32c: chunk.crc32c,
            body_len: chunk.body.len() as u64,
        })
    }
}

// HTTP-free tests via aws-smithy-mocks (operation-level interception).
// Live-S3 behavior (real checksums, real pagination) is wave 2E's
// LocalStack E2E; these pin our request construction and error mapping.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use aws_sdk_s3::error::ErrorMetadata;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
    use aws_sdk_s3::types::Object;
    use aws_sdk_s3::types::error::{NoSuchKey, NotFound};
    use aws_smithy_mocks::{RuleMode, mock, mock_client};

    use crate::chunk::{ChunkConfig, ChunkWriter};
    use crate::record::LogRecord;

    fn store(client: aws_sdk_s3::Client) -> ObjectStore {
        ObjectStore::new(client, "test-bucket", "s4logs")
    }

    fn sample_chunk() -> crate::chunk::EncodedChunk {
        let mut w = ChunkWriter::new(ChunkConfig::default());
        w.push(&LogRecord {
            timestamp: 1_717_900_000_123,
            stream: "app/i-0abc".into(),
            message: "hello".into(),
            ingestion_time: None,
            event_id: None,
        })
        .unwrap();
        w.finish().unwrap().unwrap()
    }

    fn sample_loc() -> ChunkLocation {
        ChunkLocation {
            account: "123456789012".into(),
            log_group: "/aws/lambda/foo".into(),
            date: "2026-06-10".into(),
            name: "1717900000000-000001".into(),
        }
    }

    #[tokio::test]
    async fn put_bytes_sets_crc32c_and_returns_etag() {
        let rule = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(|inp| {
                inp.bucket() == Some("test-bucket")
                    && inp.key() == Some("k/v")
                    && inp.checksum_algorithm() == Some(&ChecksumAlgorithm::Crc32C)
            })
            .then_output(|| PutObjectOutput::builder().e_tag("\"abc123\"").build());
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let etag = store(client)
            .put_bytes("k/v", Bytes::from_static(b"payload"))
            .await
            .unwrap();
        assert_eq!(etag.as_deref(), Some("\"abc123\""));
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test]
    async fn get_bytes_collects_body() {
        let rule = mock!(aws_sdk_s3::Client::get_object)
            .match_requests(|inp| inp.key() == Some("k/v"))
            .then_output(|| {
                GetObjectOutput::builder()
                    .body(ByteStream::from_static(b"hello world"))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let body = store(client).get_bytes("k/v").await.unwrap();
        assert_eq!(body, Bytes::from_static(b"hello world"));
    }

    #[tokio::test]
    async fn get_bytes_no_such_key_maps_to_not_found() {
        let rule = mock!(aws_sdk_s3::Client::get_object)
            .then_error(|| GetObjectError::NoSuchKey(NoSuchKey::builder().build()));
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let err = store(client).get_bytes("missing").await.unwrap_err();
        assert!(
            matches!(err, StoreError::NotFound { ref key } if key == "missing"),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn get_bytes_other_error_maps_to_aws() {
        let rule = mock!(aws_sdk_s3::Client::get_object).then_error(|| {
            GetObjectError::generic(
                ErrorMetadata::builder()
                    .code("AccessDenied")
                    .message("denied")
                    .build(),
            )
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let err = store(client).get_bytes("k").await.unwrap_err();
        match err {
            StoreError::Aws { op, key, .. } => {
                assert_eq!(op, "GetObject");
                assert_eq!(key, "k");
            }
            other => panic!("expected Aws, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_range_sends_inclusive_range_header() {
        let rule = mock!(aws_sdk_s3::Client::get_object)
            .match_requests(|inp| inp.range() == Some("bytes=10-19"))
            .then_output(|| {
                GetObjectOutput::builder()
                    .body(ByteStream::from_static(b"0123456789"))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let body = store(client).get_range("k", 10, 20).await.unwrap();
        assert_eq!(body.len(), 10);
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test]
    async fn get_range_empty_span_skips_the_request() {
        // Unmatchable rule: any GetObject call would panic the mock harness.
        let rule = mock!(aws_sdk_s3::Client::get_object)
            .match_requests(|_| false)
            .then_output(|| GetObjectOutput::builder().build());
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let s = store(client);
        assert!(s.get_range("k", 5, 5).await.unwrap().is_empty());
        assert!(s.get_range("k", 7, 5).await.unwrap().is_empty());
        assert_eq!(rule.num_calls(), 0);
    }

    #[tokio::test]
    async fn list_keys_paginates_to_exhaustion() {
        let page1 = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|inp| {
                inp.prefix() == Some("s4logs/data/") && inp.continuation_token().is_none()
            })
            .then_output(|| {
                ListObjectsV2Output::builder()
                    .contents(Object::builder().key("s4logs/data/a").build())
                    .contents(Object::builder().key("s4logs/data/b").build())
                    .is_truncated(true)
                    .next_continuation_token("tok-1")
                    .build()
            });
        let page2 = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|inp| inp.continuation_token() == Some("tok-1"))
            .then_output(|| {
                ListObjectsV2Output::builder()
                    .contents(Object::builder().key("s4logs/data/c").build())
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, [&page1, &page2]);
        let keys = store(client).list_keys("s4logs/data/").await.unwrap();
        assert_eq!(
            keys,
            vec!["s4logs/data/a", "s4logs/data/b", "s4logs/data/c"]
        );
    }

    #[tokio::test]
    async fn exists_maps_head_object() {
        let found = mock!(aws_sdk_s3::Client::head_object)
            .match_requests(|inp| inp.key() == Some("present"))
            .then_output(|| HeadObjectOutput::builder().build());
        let missing = mock!(aws_sdk_s3::Client::head_object)
            .match_requests(|inp| inp.key() == Some("absent"))
            .then_error(|| HeadObjectError::NotFound(NotFound::builder().build()));
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&found, &missing]);
        let s = store(client);
        assert!(s.exists("present").await.unwrap());
        assert!(!s.exists("absent").await.unwrap());
    }

    #[tokio::test]
    async fn put_chunk_writes_data_then_index_then_ts_index() {
        let chunk = sample_chunk();
        // RuleMode::Sequential + per-key matchers pins the write order:
        // data → .s4index → .s4lts. A sidecar-first implementation would
        // fail rule 1's matcher.
        let put_data = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(|inp| {
                inp.key()
                    .is_some_and(|k| k.starts_with("s4logs/data/") && k.ends_with(".jsonl.zst"))
                    && inp.checksum_algorithm() == Some(&ChecksumAlgorithm::Crc32C)
            })
            .then_output(|| PutObjectOutput::builder().e_tag("\"data-etag\"").build());
        let put_index = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(|inp| {
                inp.key()
                    .is_some_and(|k| k.starts_with("s4logs/index/") && k.ends_with(".s4index"))
            })
            .then_output(|| PutObjectOutput::builder().build());
        let put_ts = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(|inp| {
                inp.key()
                    .is_some_and(|k| k.starts_with("s4logs/index/") && k.ends_with(".s4lts"))
            })
            .then_output(|| PutObjectOutput::builder().build());
        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            [&put_data, &put_index, &put_ts]
        );
        let receipt = store(client)
            .put_chunk(&sample_loc(), &chunk)
            .await
            .unwrap();
        assert_eq!(put_data.num_calls(), 1);
        assert_eq!(put_index.num_calls(), 1);
        assert_eq!(put_ts.num_calls(), 1);
        assert_eq!(receipt.etag.as_deref(), Some("\"data-etag\""));
        assert_eq!(receipt.crc32c, chunk.crc32c);
        assert_eq!(receipt.body_len, chunk.body.len() as u64);
        assert_eq!(receipt.data_key, sample_loc().data_key("s4logs"));
        assert_eq!(receipt.index_key, sample_loc().index_key("s4logs"));
        assert_eq!(receipt.ts_index_key, sample_loc().ts_index_key("s4logs"));
    }

    #[tokio::test]
    async fn put_chunk_data_failure_skips_sidecars() {
        let chunk = sample_chunk();
        let put_data_fail = mock!(aws_sdk_s3::Client::put_object).then_error(|| {
            // Non-retryable 4xx so the SDK surfaces it on the first attempt.
            PutObjectError::generic(
                ErrorMetadata::builder()
                    .code("AccessDenied")
                    .message("denied")
                    .build(),
            )
        });
        let put_sidecar = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| PutObjectOutput::builder().build());
        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            [&put_data_fail, &put_sidecar]
        );
        let err = store(client)
            .put_chunk(&sample_loc(), &chunk)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SinkError::Store(StoreError::Aws {
                    op: "PutObject",
                    ..
                })
            ),
            "got {err:?}"
        );
        assert_eq!(put_sidecar.num_calls(), 0, "sidecars must not be written");
    }

    #[tokio::test]
    async fn put_chunk_sidecar_failure_surfaces() {
        let chunk = sample_chunk();
        let put_data = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(|inp| inp.key().is_some_and(|k| k.ends_with(".jsonl.zst")))
            .then_output(|| PutObjectOutput::builder().e_tag("\"e\"").build());
        let put_index_fail = mock!(aws_sdk_s3::Client::put_object).then_error(|| {
            PutObjectError::generic(
                ErrorMetadata::builder()
                    .code("AccessDenied")
                    .message("denied")
                    .build(),
            )
        });
        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            [&put_data, &put_index_fail]
        );
        let err = store(client)
            .put_chunk(&sample_loc(), &chunk)
            .await
            .unwrap_err();
        assert!(
            matches!(err, SinkError::Store(StoreError::Aws { .. })),
            "sidecar failure must surface, got {err:?}"
        );
    }

    #[tokio::test]
    async fn list_chunks_skips_foreign_keys() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .contents(
                    Object::builder()
                        .key(
                            "s4logs/data/account=123456789012/loggroup=%2Fg/\
                             dt=2026-06-10/1-000000.jsonl.zst",
                        )
                        .build(),
                )
                .contents(Object::builder().key("s4logs/data/garbage.txt").build())
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&rule]);
        let chunks = store(client)
            .list_chunks("123456789012", "/g")
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].log_group, "/g");
        assert_eq!(chunks[0].name, "1-000000");
    }
}
