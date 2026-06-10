//! s4logs-drain — Mode A (退避). Wave 1B implements this crate.
//!
//! Contract: DESIGN.md §7. Unit of work = (log_group, UTC-aligned window).
//! FilterLogEvents pagination behind a [`CwSource`] trait (mockable),
//! manifest-based idempotency, fail-closed retention gate.
//!
//! # Intended CLI usage (wave 2D)
//!
//! ```ignore
//! use std::sync::Arc;
//! use s4logs_core::store::ObjectStore;
//! use s4logs_drain::{
//!     AwsCwSource, DrainJob, DrainOptions, ObjectStoreManifestStore,
//!     RetentionRequest, enforce_retention,
//! };
//!
//! let store = ObjectStore::new(s3_client, bucket, prefix);
//! let cw = Arc::new(AwsCwSource::new(cwl_client));
//! let sink = Arc::new(store.clone());
//! let manifests = Arc::new(ObjectStoreManifestStore::new(store));
//!
//! let mut opts = DrainOptions::new(account, log_group); // 1h windows,
//! opts.dry_run = cli.dry_run;                           // 256 MiB chunks,
//! opts.concurrency = cli.concurrency;                   // concurrency 2
//! let report = DrainJob::new(cw.clone(), sink, manifests.clone(), opts)
//!     .run()
//!     .await?;
//! println!("saved ~${:.2}/mo", report.estimated_monthly_savings_usd());
//!
//! // Retention shortening: report-only unless --apply-retention.
//! let req = RetentionRequest { account, log_group, retention_days: 7,
//!     coverage_from_ms: creation_time_ms, now_ms, window_ms: opts.window_ms };
//! let plan = enforce_retention(&*cw, &*manifests, key_prefix, &req,
//!     cli.apply_retention).await?;
//! ```

pub mod cw;
pub mod discover;
pub mod job;
pub mod manifest;
pub mod retention;
pub mod window;

#[cfg(test)]
pub(crate) mod testutil;

pub use cw::{AwsCwSource, BackoffConfig, CwError, CwEvent, CwEventPage, CwSource, LogGroupInfo};
pub use discover::{DiscoverError, GroupSelector, discover_log_groups};
pub use job::{
    CW_STORAGE_USD_PER_GIB_MONTH, DrainError, DrainJob, DrainOptions, DrainReport,
    GroupDrainResult, MultiDrainReport, S3_STORAGE_USD_PER_GIB_MONTH, drain_groups,
};
pub use manifest::{
    DRAIN_VERSION, MANIFEST_VERSION, Manifest, ManifestError, ManifestObject, ManifestStore,
    MemoryManifestStore, ObjectStoreManifestStore, manifest_account_prefix,
    parse_manifest_key_log_group, parse_manifest_key_window,
};
pub use retention::{RetentionPlan, RetentionRequest, enforce_retention, plan_retention};
pub use window::{DAY_MS, HOUR_MS, Window, WindowError, windows, windows_covering};
