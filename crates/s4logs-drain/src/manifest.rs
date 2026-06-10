//! Drain window manifest (DESIGN.md §7) + the small object-store surface the
//! drain needs beyond `ChunkSink`.
//!
//! A manifest proves one `(log_group, window)` is fully archived: its
//! existence is the idempotency check before re-draining, and the retention
//! gate refuses to shrink CW retention unless every window older than the
//! cutoff has one. Empty windows get a manifest too (empty `objects`) so the
//! gate can distinguish "drained, nothing there" from "never drained".
//!
//! Field names and order are part of the on-disk format — golden-tested
//! below; do not rename.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use s4logs_core::store::{ObjectStore, StoreError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::window::Window;

pub const MANIFEST_VERSION: u32 = 1;
/// Stamped into every manifest (`drain_version` field).
pub const DRAIN_VERSION: &str = env!("CARGO_PKG_VERSION");

/// One data object written for the window (DESIGN.md §7 schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestObject {
    /// Full S3 key of the data object (`{prefix}data/...jsonl.zst`).
    pub data_key: String,
    /// Backend ETag of the data object (`None` only for test sinks).
    pub etag: Option<String>,
    /// CRC32C over the object body.
    pub crc32c: u32,
    /// Compressed body length, bytes.
    pub body_len: u64,
    pub record_count: u64,
    /// Min/max event timestamp in the object, epoch ms.
    pub min_ts: i64,
    pub max_ts: i64,
}

/// Manifest JSON, version 1 (DESIGN.md §7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub account: String,
    /// Raw (unsanitized) log group name.
    pub log_group: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub objects: Vec<ManifestObject>,
    /// Total records across `objects`.
    pub record_count: u64,
    /// Wall-clock completion time (the only wall-clock field in the format —
    /// DESIGN.md §10).
    pub completed_at_ms: i64,
    /// s4logs-drain crate version that wrote this manifest.
    pub drain_version: String,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ManifestError {
    #[error("manifest store {op} failed for {key:?}: {message}")]
    Storage {
        op: &'static str,
        key: String,
        message: String,
    },
    #[error("manifest encode failed")]
    Encode(#[source] serde_json::Error),
    #[error("manifest decode failed")]
    Decode(#[source] serde_json::Error),
    #[error("unsupported manifest version {0} (this build supports {MANIFEST_VERSION})")]
    UnsupportedVersion(u32),
}

impl Manifest {
    pub fn to_json_bytes(&self) -> Result<Bytes, ManifestError> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(ManifestError::Encode)
    }

    /// Decode + version check.
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, ManifestError> {
        let m: Manifest = serde_json::from_slice(bytes).map_err(ManifestError::Decode)?;
        if m.version != MANIFEST_VERSION {
            return Err(ManifestError::UnsupportedVersion(m.version));
        }
        Ok(m)
    }
}

/// Parse the window out of a manifest key
/// (`…/window={start_ms}-{end_ms}.json`). Returns `None` for foreign keys.
/// Window timestamps are non-negative by construction ([`crate::window`]).
pub fn parse_manifest_key_window(key: &str) -> Option<Window> {
    let base = key.rsplit('/').next()?;
    let inner = base.strip_prefix("window=")?.strip_suffix(".json")?;
    let (s, e) = inner.split_once('-')?;
    Some(Window {
        start_ms: s.parse().ok()?,
        end_ms: e.parse().ok()?,
    })
}

/// Object-store operations the drain needs beyond `ChunkSink`: manifest
/// get/put/exists/list. Implemented in-memory for tests and as a thin
/// adapter over `s4logs_core::store::ObjectStore` for S3.
#[async_trait]
pub trait ManifestStore: Send + Sync {
    async fn exists(&self, key: &str) -> Result<bool, ManifestError>;
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ManifestError>;
    async fn put(&self, key: &str, body: Bytes) -> Result<(), ManifestError>;
    /// All keys under `prefix`, lexicographic, paginated to exhaustion.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ManifestError>;
}

/// In-memory [`ManifestStore`] for unit tests.
#[derive(Debug, Default)]
pub struct MemoryManifestStore {
    objects: Mutex<BTreeMap<String, Bytes>>,
}

impl MemoryManifestStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn keys(&self) -> Vec<String> {
        self.objects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.objects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }
}

#[async_trait]
impl ManifestStore for MemoryManifestStore {
    async fn exists(&self, key: &str) -> Result<bool, ManifestError> {
        Ok(self
            .objects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(key))
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ManifestError> {
        Ok(self
            .objects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .cloned())
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<(), ManifestError> {
        self.objects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key.to_owned(), body);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ManifestError> {
        Ok(self
            .objects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range(prefix.to_owned()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect())
    }
}

/// Thin [`ManifestStore`] adapter over the S3 `ObjectStore`.
///
/// COMPILE-ONLY in wave 1B: `ObjectStore`'s S3 calls are implemented by
/// wave 1A and integration-tested by wave 2E. Do not add tests here that
/// would hit the underlying client.
#[derive(Debug, Clone)]
pub struct ObjectStoreManifestStore {
    store: ObjectStore,
}

impl ObjectStoreManifestStore {
    pub fn new(store: ObjectStore) -> Self {
        Self { store }
    }
}

fn storage_err(op: &'static str, key: &str, e: StoreError) -> ManifestError {
    ManifestError::Storage {
        op,
        key: key.to_owned(),
        message: e.to_string(),
    }
}

#[async_trait]
impl ManifestStore for ObjectStoreManifestStore {
    async fn exists(&self, key: &str) -> Result<bool, ManifestError> {
        self.store
            .exists(key)
            .await
            .map_err(|e| storage_err("exists", key, e))
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ManifestError> {
        match self.store.get_bytes(key).await {
            Ok(b) => Ok(Some(b)),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(storage_err("get", key, e)),
        }
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<(), ManifestError> {
        self.store
            .put_bytes(key, body)
            .await
            .map(|_etag| ())
            .map_err(|e| storage_err("put", key, e))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ManifestError> {
        self.store
            .list_keys(prefix)
            .await
            .map_err(|e| storage_err("list", prefix, e))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use s4logs_core::layout::manifest_key;

    fn sample() -> Manifest {
        Manifest {
            version: 1,
            account: "123456789012".into(),
            log_group: "/aws/lambda/foo".into(),
            window_start_ms: 1_717_545_600_000,
            window_end_ms: 1_717_549_200_000,
            objects: vec![ManifestObject {
                data_key: "s4logs/data/account=123456789012/loggroup=%2Faws%2Flambda%2Ffoo/dt=2024-06-05/1717545600000-000000.jsonl.zst".into(),
                etag: Some("\"abc\"".into()),
                crc32c: 305_419_896,
                body_len: 1024,
                record_count: 10,
                min_ts: 1_717_545_600_001,
                max_ts: 1_717_549_199_999,
            }],
            record_count: 10,
            completed_at_ms: 1_717_550_000_000,
            drain_version: "test".into(),
        }
    }

    /// Golden test: exact field names and order are the on-disk format.
    #[test]
    fn manifest_json_golden() {
        let json = String::from_utf8(sample().to_json_bytes().unwrap().to_vec()).unwrap();
        assert_eq!(
            json,
            r#"{"version":1,"account":"123456789012","log_group":"/aws/lambda/foo","window_start_ms":1717545600000,"window_end_ms":1717549200000,"objects":[{"data_key":"s4logs/data/account=123456789012/loggroup=%2Faws%2Flambda%2Ffoo/dt=2024-06-05/1717545600000-000000.jsonl.zst","etag":"\"abc\"","crc32c":305419896,"body_len":1024,"record_count":10,"min_ts":1717545600001,"max_ts":1717549199999}],"record_count":10,"completed_at_ms":1717550000000,"drain_version":"test"}"#
        );
    }

    #[test]
    fn manifest_roundtrip() {
        let m = sample();
        let back = Manifest::from_json_bytes(&m.to_json_bytes().unwrap()).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn empty_window_manifest_roundtrip() {
        let m = Manifest {
            objects: vec![],
            record_count: 0,
            ..sample()
        };
        let back = Manifest::from_json_bytes(&m.to_json_bytes().unwrap()).unwrap();
        assert!(back.objects.is_empty());
        assert_eq!(back.record_count, 0);
    }

    #[test]
    fn rejects_future_version() {
        let mut m = sample();
        m.version = 2;
        let bytes = serde_json::to_vec(&m).unwrap();
        assert!(matches!(
            Manifest::from_json_bytes(&bytes),
            Err(ManifestError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn parse_window_from_layout_key() {
        let key = manifest_key("s4logs", "123456789012", "/aws/lambda/foo", 1000, 2000);
        assert_eq!(
            parse_manifest_key_window(&key),
            Some(Window {
                start_ms: 1000,
                end_ms: 2000
            })
        );
        assert_eq!(parse_manifest_key_window("s4logs/manifest/foo.txt"), None);
    }

    #[tokio::test]
    async fn memory_store_list_is_prefix_scoped() {
        let store = MemoryManifestStore::new();
        store.put("a/1", Bytes::from_static(b"x")).await.unwrap();
        store.put("a/2", Bytes::from_static(b"y")).await.unwrap();
        store.put("b/1", Bytes::from_static(b"z")).await.unwrap();
        assert_eq!(store.list("a/").await.unwrap(), vec!["a/1", "a/2"]);
        assert!(store.exists("b/1").await.unwrap());
        assert!(!store.exists("c/1").await.unwrap());
        assert_eq!(
            store.get("a/1").await.unwrap().unwrap(),
            Bytes::from_static(b"x")
        );
        assert!(store.get("nope").await.unwrap().is_none());
    }
}
