//! Fail-closed retention gate (DESIGN.md §6 step 4, §7).
//!
//! `PutRetentionPolicy` is only allowed once **every** window that holds (or
//! could hold) events older than the proposed cutoff has a manifest —
//! including the window the cutoff falls inside (see
//! [`crate::window::windows_covering`]). If coverage cannot be proven, the
//! gate does **nothing**. Default is report-only; the actual API call
//! additionally requires `apply = true` (`--apply-retention` in wave 2D).

use s4logs_core::layout::manifest_key;

use crate::cw::CwSource;
use crate::job::DrainError;
use crate::manifest::{ManifestStore, manifest_covers_window};
use crate::window::{DAY_MS, Window, windows_covering};

/// Inputs to the retention gate.
#[derive(Debug, Clone)]
pub struct RetentionRequest {
    pub account: String,
    /// Raw CloudWatch log group name.
    pub log_group: String,
    /// Proposed CW retention, days (must be > 0; AWS validates the exact
    /// allowed set on apply).
    pub retention_days: i32,
    /// Where coverage must start — typically the log group creation time.
    pub coverage_from_ms: i64,
    /// "Now" for the cutoff computation (injectable for tests; CLI passes
    /// wall clock).
    pub now_ms: i64,
    /// Drain window length used for this group (must match what the drain
    /// ran with, or the manifest grid won't line up).
    pub window_ms: i64,
}

/// Outcome of the gate. `allowed()` ⇔ no missing windows; `applied` is true
/// only if `PutRetentionPolicy` was actually called.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPlan {
    pub log_group: String,
    pub retention_days: i32,
    /// Events older than this would be expired by the proposed retention.
    pub cutoff_ms: i64,
    /// Windows that must have manifests (start < cutoff).
    pub required_windows: Vec<Window>,
    /// Required windows with no manifest — must be empty to proceed.
    pub missing_windows: Vec<Window>,
    pub applied: bool,
}

impl RetentionPlan {
    /// Fail-closed verdict: true only when every required window is covered.
    pub fn allowed(&self) -> bool {
        self.missing_windows.is_empty()
    }
}

/// Compute coverage without touching CloudWatch (report-only path).
pub async fn plan_retention(
    manifests: &dyn ManifestStore,
    key_prefix: &str,
    req: &RetentionRequest,
) -> Result<RetentionPlan, DrainError> {
    if req.retention_days <= 0 {
        return Err(DrainError::BadOptions(format!(
            "retention_days must be > 0, got {}",
            req.retention_days
        )));
    }
    let cutoff_ms = req
        .now_ms
        .saturating_sub(i64::from(req.retention_days).saturating_mul(DAY_MS));
    let required_windows = if cutoff_ms <= req.coverage_from_ms {
        // Nothing older than the cutoff can exist — trivially safe.
        Vec::new()
    } else {
        windows_covering(req.coverage_from_ms, cutoff_ms, req.window_ms)?
    };
    // This gate decides whether CloudWatch may DELETE logs, so a key merely
    // *existing* is not proof of coverage: a zero-byte, truncated,
    // wrong-version, or mismatched manifest object at the expected key would
    // otherwise green-light deletion of un-archived data. GET and validate
    // every required window's manifest; anything that does not decode into a
    // matching, complete manifest is treated as missing (fail-closed).
    let mut missing_windows: Vec<Window> = Vec::new();
    for w in &required_windows {
        let key = manifest_key(
            key_prefix,
            &req.account,
            &req.log_group,
            w.start_ms,
            w.end_ms,
        );
        let covered = match manifests.get(&key).await? {
            None => false,
            Some(bytes) => {
                manifest_covers_window(&bytes, &req.account, &req.log_group, w.start_ms, w.end_ms)
            }
        };
        if !covered {
            missing_windows.push(*w);
        }
    }
    Ok(RetentionPlan {
        log_group: req.log_group.clone(),
        retention_days: req.retention_days,
        cutoff_ms,
        required_windows,
        missing_windows,
        applied: false,
    })
}

/// Plan, then — only if coverage is complete **and** `apply` is true — call
/// `PutRetentionPolicy`. With `apply = false` (default) this is report-only.
/// On a coverage gap nothing is called regardless of `apply` (fail-closed).
pub async fn enforce_retention(
    cw: &dyn CwSource,
    manifests: &dyn ManifestStore,
    key_prefix: &str,
    req: &RetentionRequest,
    apply: bool,
) -> Result<RetentionPlan, DrainError> {
    let mut plan = plan_retention(manifests, key_prefix, req).await?;
    if !plan.allowed() {
        tracing::warn!(
            log_group = %req.log_group,
            missing = plan.missing_windows.len(),
            required = plan.required_windows.len(),
            "retention gate refused: coverage gap (fail-closed, nothing changed)"
        );
        return Ok(plan);
    }
    if apply {
        cw.put_retention_policy(&req.log_group, req.retention_days)
            .await?;
        plan.applied = true;
        tracing::info!(
            log_group = %req.log_group,
            retention_days = req.retention_days,
            "retention policy applied"
        );
    }
    Ok(plan)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::manifest::MemoryManifestStore;
    use crate::testutil::MockCw;
    use crate::window::HOUR_MS;
    use bytes::Bytes;
    use s4logs_core::layout::manifest_key;

    const DAY0: i64 = 1_717_545_600_000; // 2024-06-05T00:00:00Z
    const ACCT: &str = "123456789012";
    const GROUP: &str = "/aws/lambda/foo";
    const PREFIX: &str = "s4logs";

    fn req(retention_days: i32, now_ms: i64) -> RetentionRequest {
        RetentionRequest {
            account: ACCT.into(),
            log_group: GROUP.into(),
            retention_days,
            coverage_from_ms: DAY0,
            now_ms,
            window_ms: HOUR_MS,
        }
    }

    fn valid_manifest(w: &Window) -> Bytes {
        crate::manifest::Manifest {
            version: crate::manifest::MANIFEST_VERSION,
            account: ACCT.into(),
            log_group: GROUP.into(),
            window_start_ms: w.start_ms,
            window_end_ms: w.end_ms,
            objects: Vec::new(),
            record_count: 0,
            completed_at_ms: 0,
            drain_version: "test".into(),
            reconciled_at_ms: None,
            reconciled_added: None,
        }
        .to_json_bytes()
        .unwrap()
    }

    async fn seed(store: &MemoryManifestStore, windows: &[Window]) {
        for w in windows {
            store
                .put(
                    &manifest_key(PREFIX, ACCT, GROUP, w.start_ms, w.end_ms),
                    valid_manifest(w),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn full_coverage_allows_and_applies() {
        let store = MemoryManifestStore::new();
        let cw = MockCw::default();
        // now = day0 + 10d, retention 7d → cutoff = day0 + 3d → 72 windows.
        let r = req(7, DAY0 + 10 * DAY_MS);
        let required = windows_covering(DAY0, DAY0 + 3 * DAY_MS, HOUR_MS).unwrap();
        assert_eq!(required.len(), 72);
        seed(&store, &required).await;

        let plan = enforce_retention(&cw, &store, PREFIX, &r, true)
            .await
            .unwrap();
        assert!(plan.allowed());
        assert!(plan.applied);
        assert_eq!(plan.required_windows.len(), 72);
        assert_eq!(
            cw.retention_calls.lock().unwrap().as_slice(),
            &[(GROUP.to_owned(), 7)]
        );
    }

    #[tokio::test]
    async fn gap_refuses_even_with_apply_true() {
        let store = MemoryManifestStore::new();
        let cw = MockCw::default();
        let r = req(7, DAY0 + 10 * DAY_MS);
        let mut required = windows_covering(DAY0, DAY0 + 3 * DAY_MS, HOUR_MS).unwrap();
        let hole = required.remove(40);
        seed(&store, &required).await;

        let plan = enforce_retention(&cw, &store, PREFIX, &r, true)
            .await
            .unwrap();
        assert!(!plan.allowed());
        assert!(!plan.applied);
        assert_eq!(plan.missing_windows, vec![hole]);
        assert!(
            cw.retention_calls.lock().unwrap().is_empty(),
            "fail-closed: must not call AWS"
        );
    }

    #[tokio::test]
    async fn report_only_never_applies() {
        let store = MemoryManifestStore::new();
        let cw = MockCw::default();
        let r = req(7, DAY0 + 10 * DAY_MS);
        seed(
            &store,
            &windows_covering(DAY0, DAY0 + 3 * DAY_MS, HOUR_MS).unwrap(),
        )
        .await;

        let plan = enforce_retention(&cw, &store, PREFIX, &r, false)
            .await
            .unwrap();
        assert!(plan.allowed());
        assert!(!plan.applied);
        assert!(cw.retention_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mid_window_cutoff_requires_containing_window() {
        let store = MemoryManifestStore::new();
        // retention 1d, now = day0 + 1d + 90min → cutoff = day0 + 90min →
        // windows [00,01) AND [01,02) both required.
        let r = req(1, DAY0 + DAY_MS + 90 * 60_000);
        seed(
            &store,
            &[Window {
                start_ms: DAY0,
                end_ms: DAY0 + HOUR_MS,
            }],
        )
        .await;
        let plan = plan_retention(&store, PREFIX, &r).await.unwrap();
        assert_eq!(plan.required_windows.len(), 2);
        assert_eq!(
            plan.missing_windows,
            vec![Window {
                start_ms: DAY0 + HOUR_MS,
                end_ms: DAY0 + 2 * HOUR_MS
            }]
        );
        assert!(!plan.allowed());
    }

    #[tokio::test]
    async fn no_data_older_than_cutoff_is_trivially_allowed() {
        let store = MemoryManifestStore::new();
        // Group created yesterday, retention 7d → cutoff before creation.
        let r = req(7, DAY0 + DAY_MS);
        let plan = plan_retention(&store, PREFIX, &r).await.unwrap();
        assert!(plan.allowed());
        assert!(plan.required_windows.is_empty());
    }

    #[tokio::test]
    async fn rejects_non_positive_retention() {
        let store = MemoryManifestStore::new();
        let r = req(0, DAY0);
        assert!(matches!(
            plan_retention(&store, PREFIX, &r).await.unwrap_err(),
            DrainError::BadOptions(_)
        ));
    }

    #[tokio::test]
    async fn corrupt_or_partial_manifest_does_not_count_as_coverage() {
        // The gate decides whether CloudWatch may DELETE logs. A manifest
        // object that exists at the right key but is empty / truncated /
        // wrong-account must be treated as a gap (fail-closed), or a partial
        // archive could green-light deletion of un-archived data.
        let r = req(7, DAY0 + 10 * DAY_MS);
        let required = windows_covering(DAY0, DAY0 + 3 * DAY_MS, HOUR_MS).unwrap();

        // (a) zero-byte object at every required key
        let store = MemoryManifestStore::new();
        for w in &required {
            store
                .put(
                    &manifest_key(PREFIX, ACCT, GROUP, w.start_ms, w.end_ms),
                    Bytes::from_static(b""),
                )
                .await
                .unwrap();
        }
        assert!(
            !plan_retention(&store, PREFIX, &r).await.unwrap().allowed(),
            "zero-byte manifests must not satisfy coverage"
        );

        // (b) valid JSON but wrong account at one window → that window is a gap
        let store = MemoryManifestStore::new();
        seed(&store, &required).await;
        let bad = &required[5];
        let mut m = crate::manifest::Manifest::from_json_bytes(&valid_manifest(bad)).unwrap();
        m.account = "999999999999".into();
        store
            .put(
                &manifest_key(PREFIX, ACCT, GROUP, bad.start_ms, bad.end_ms),
                m.to_json_bytes().unwrap(),
            )
            .await
            .unwrap();
        let plan = plan_retention(&store, PREFIX, &r).await.unwrap();
        assert!(!plan.allowed(), "account-mismatched manifest is a gap");
        assert_eq!(plan.missing_windows, vec![*bad]);
    }
}
