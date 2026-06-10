//! `s4logs drain` — Mode A wiring (DESIGN.md §7, §9, §11.4): glob/--all
//! group discovery, one independent DrainJob per group (failures skip +
//! aggregate, exit 1), the fail-closed retention gate per successful group,
//! and human-readable reports.

use std::sync::Arc;

use anyhow::{Context, Result};
use s4logs_core::store::ObjectStore;
use s4logs_drain::{
    AwsCwSource, DrainOptions, DrainReport, GroupSelector, LogGroupInfo, MultiDrainReport,
    ObjectStoreManifestStore, RetentionPlan, RetentionRequest, discover_log_groups, drain_groups,
    enforce_retention,
};

use crate::aws;
use crate::cli::{DrainArgs, GlobalArgs, UsageError};
use crate::timearg::{fmt_ts, format_bytes};

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn selector(args: &DrainArgs) -> Result<GroupSelector> {
    if args.all {
        return Ok(GroupSelector::All);
    }
    let Some(pattern) = &args.log_group else {
        // clap's ArgGroup makes this unreachable; typed error just in case.
        return Err(UsageError("one of --log-group / --all is required".into()).into());
    };
    GroupSelector::parse(pattern).map_err(|e| UsageError(format!("{e:#}")).into())
}

pub async fn run(global: &GlobalArgs, args: &DrainArgs) -> Result<()> {
    if let (Some(f), Some(t)) = (args.from, args.to)
        && f >= t
    {
        return Err(UsageError(format!(
            "--from ({}) must be before --to ({})",
            fmt_ts(f),
            fmt_ts(t)
        ))
        .into());
    }
    let selector = selector(args)?;
    let bucket = global.require_bucket()?;
    let account = global.require_account()?;

    let clients = aws::load(global).await;
    let store = ObjectStore::new(clients.s3(), bucket, &global.prefix);
    let cw = Arc::new(AwsCwSource::new(clients.cwl()));
    let sink = Arc::new(store.clone());
    let manifests = Arc::new(ObjectStoreManifestStore::new(store));

    let groups = discover_log_groups(&*cw, &selector)
        .await
        .context("discovering log groups")?;
    if groups.is_empty() {
        return Err(UsageError(format!("no log groups match {}", selector.describe())).into());
    }
    if groups.len() > 1 {
        tracing::info!(
            groups = groups.len(),
            group_concurrency = args.group_concurrency,
            "draining multiple log groups"
        );
    }

    let mut base = DrainOptions::new(account.clone(), String::new());
    base.from_ms = args.from;
    base.to_ms = args.to;
    base.window_ms = args.window;
    base.chunk_target_bytes = args.chunk_target;
    base.concurrency = args.concurrency;
    base.dry_run = args.dry_run;
    let window_ms = base.window_ms;

    let multi = drain_groups(
        cw.clone(),
        sink,
        manifests.clone(),
        &base,
        groups.clone(),
        args.group_concurrency,
    )
    .await;

    for g in &multi.groups {
        match &g.result {
            Ok(report) => {
                for line in report_lines(report) {
                    println!("{line}");
                }
            }
            Err(err) => println!("Drain FAILED for {:?}: {err:#}", g.log_group),
        }
    }
    for line in aggregate_lines(&multi) {
        println!("{line}");
    }

    if let Some(retention_days) = args.retention_days {
        let apply = args.apply_retention && !args.dry_run;
        if args.apply_retention && args.dry_run {
            tracing::warn!("--dry-run: retention stays report-only despite --apply-retention");
        }
        let infos: std::collections::HashMap<&str, &LogGroupInfo> =
            groups.iter().map(|(n, i)| (n.as_str(), i)).collect();
        for g in &multi.groups {
            if g.result.is_err() {
                println!(
                    "Retention gate for {:?}: skipped (drain failed)",
                    g.log_group
                );
                continue;
            }
            // Coverage must start at the log group creation time (DESIGN.md
            // §6 step 4) — anything older than that cannot hold events. The
            // discovery info already carries it; no extra DescribeLogGroups.
            let Some(info) = infos.get(g.log_group.as_str()) else {
                continue; // unreachable: drain_groups preserves the name set
            };
            let req = RetentionRequest {
                account: account.clone(),
                log_group: g.log_group.clone(),
                retention_days,
                coverage_from_ms: info.creation_time_ms,
                now_ms: now_ms(),
                window_ms,
            };
            let plan = enforce_retention(&*cw, &*manifests, &global.prefix, &req, apply)
                .await
                .with_context(|| format!("retention gate for {:?}", g.log_group))?;
            for line in retention_lines(&plan, apply) {
                println!("{line}");
            }
        }
    }

    if multi.any_failed() {
        let failed: Vec<&str> = multi.failures().map(|(g, _)| g).collect();
        anyhow::bail!(
            "{} of {} log group(s) failed: {}",
            failed.len(),
            multi.groups.len(),
            failed.join(", ")
        );
    }
    Ok(())
}

/// Aggregate footer for multi-group drains (pure; empty for single-group
/// runs, whose per-group report already says everything).
fn aggregate_lines(multi: &MultiDrainReport) -> Vec<String> {
    if multi.groups.len() <= 1 {
        return Vec::new();
    }
    let mut lines = report_lines(&multi.aggregate());
    let failed: Vec<&str> = multi.failures().map(|(g, _)| g).collect();
    if !failed.is_empty() {
        lines.push(format!(
            "  FAILED groups ({}): {}",
            failed.len(),
            failed.join(", ")
        ));
    }
    lines
}

/// Pure report rendering (unit-tested without AWS).
fn report_lines(r: &DrainReport) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "Drain report for {:?}{}",
        r.log_group,
        if r.dry_run {
            " (dry-run: nothing written)"
        } else {
            ""
        }
    ));
    lines.push(format!(
        "  windows : {} total = {} processed ({} empty) + {} skipped (manifest exists)",
        r.windows_total, r.windows_processed, r.windows_empty, r.windows_skipped
    ));
    let dropped = if r.events_outside_window > 0 {
        format!(
            " ({} dropped outside their window)",
            r.events_outside_window
        )
    } else {
        String::new()
    };
    lines.push(format!("  records : {}{}", r.records, dropped));
    if r.compressed_bytes > 0 {
        lines.push(format!(
            "  bytes   : {} raw -> {} compressed ({:.1}x, {:.1}% of raw)",
            format_bytes(r.raw_bytes),
            format_bytes(r.compressed_bytes),
            r.raw_bytes as f64 / r.compressed_bytes as f64,
            100.0 * r.compressed_bytes as f64 / r.raw_bytes.max(1) as f64
        ));
    } else {
        lines.push(format!("  bytes   : {} raw", format_bytes(r.raw_bytes)));
    }
    lines.push(format!("  objects : {} written", r.objects_written));
    lines.push(format!(
        "  est. monthly storage: CloudWatch ${:.4} (gzip-billed, ~4x assumed) -> S3 ${:.4} (saves ${:.4}/month)",
        r.cw_monthly_storage_usd(),
        r.s3_monthly_storage_usd(),
        r.estimated_monthly_savings_usd()
    ));
    lines
}

const MISSING_WINDOWS_SHOWN: usize = 20;

/// Pure retention-plan rendering (unit-tested without AWS).
fn retention_lines(plan: &RetentionPlan, apply_requested: bool) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "Retention gate for {:?}: {} days (cutoff {})",
        plan.log_group,
        plan.retention_days,
        fmt_ts(plan.cutoff_ms)
    ));
    lines.push(format!(
        "  required windows: {}, missing manifests: {}",
        plan.required_windows.len(),
        plan.missing_windows.len()
    ));
    if !plan.allowed() {
        lines.push(
            "  REFUSED (fail-closed): nothing changed. Drain these windows first:".to_owned(),
        );
        for w in plan.missing_windows.iter().take(MISSING_WINDOWS_SHOWN) {
            lines.push(format!(
                "    {} .. {}",
                fmt_ts(w.start_ms),
                fmt_ts(w.end_ms)
            ));
        }
        if plan.missing_windows.len() > MISSING_WINDOWS_SHOWN {
            lines.push(format!(
                "    ... and {} more",
                plan.missing_windows.len() - MISSING_WINDOWS_SHOWN
            ));
        }
    } else if plan.applied {
        lines.push(format!(
            "  applied: PutRetentionPolicy({} days) succeeded",
            plan.retention_days
        ));
    } else if apply_requested {
        lines.push("  coverage OK — not applied (--dry-run)".to_owned());
    } else {
        lines.push("  coverage OK — report only (pass --apply-retention to apply)".to_owned());
    }
    lines
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use s4logs_drain::Window;

    const DAY0: i64 = 1_717_545_600_000; // 2024-06-05T00:00:00Z
    const HOUR: i64 = 3_600_000;

    #[test]
    fn report_lines_render_savings_and_ratio() {
        let r = DrainReport {
            log_group: "/aws/lambda/foo".into(),
            dry_run: true,
            windows_total: 4,
            windows_processed: 3,
            windows_skipped: 1,
            windows_empty: 1,
            records: 1000,
            events_outside_window: 2,
            raw_bytes: 10 << 30,
            compressed_bytes: 1 << 30,
            objects_written: 0,
        };
        let lines = report_lines(&r);
        let text = lines.join("\n");
        assert!(text.contains("dry-run"), "{text}");
        assert!(
            text.contains("10.0 GiB raw -> 1.0 GiB compressed (10.0x"),
            "{text}"
        );
        assert!(text.contains("2 dropped outside"), "{text}");
        // 10 GiB raw / 4 gzip-assumed * $0.03 = $0.075 CW vs 1 GiB S3
        // ($0.023) → saves $0.052.
        assert!(text.contains("saves $0.0520/month"), "{text}");
    }

    #[test]
    fn report_lines_handle_zero_compressed() {
        let r = DrainReport::default();
        let text = report_lines(&r).join("\n");
        assert!(text.contains("0 B raw"), "{text}");
        assert!(!text.contains("NaN"), "{text}");
        assert!(!text.contains("inf"), "{text}");
    }

    fn plan(missing: Vec<Window>, applied: bool) -> RetentionPlan {
        RetentionPlan {
            log_group: "/g".into(),
            retention_days: 7,
            cutoff_ms: DAY0,
            required_windows: vec![Window {
                start_ms: DAY0 - HOUR,
                end_ms: DAY0,
            }],
            missing_windows: missing,
            applied,
        }
    }

    #[test]
    fn retention_refusal_lists_missing_windows() {
        let p = plan(
            vec![Window {
                start_ms: DAY0 - HOUR,
                end_ms: DAY0,
            }],
            false,
        );
        let text = retention_lines(&p, true).join("\n");
        assert!(text.contains("REFUSED"), "{text}");
        assert!(
            text.contains("2024-06-04T23:00:00.000Z .. 2024-06-05T00:00:00.000Z"),
            "{text}"
        );
    }

    #[test]
    fn aggregate_lines_only_for_multi_group_runs() {
        use s4logs_drain::{DrainError, GroupDrainResult};
        let ok = |g: &str, records: u64| GroupDrainResult {
            log_group: g.into(),
            result: Ok(DrainReport {
                log_group: g.into(),
                records,
                windows_total: 1,
                windows_processed: 1,
                ..DrainReport::default()
            }),
        };
        let single = MultiDrainReport {
            groups: vec![ok("/g", 5)],
        };
        assert!(
            aggregate_lines(&single).is_empty(),
            "single group needs no aggregate footer"
        );
        let multi = MultiDrainReport {
            groups: vec![
                ok("/a", 5),
                GroupDrainResult {
                    log_group: "/bad".into(),
                    result: Err(DrainError::BadOptions("boom".into())),
                },
                ok("/c", 7),
            ],
        };
        let text = aggregate_lines(&multi).join("\n");
        assert!(text.contains("2/3 log groups"), "{text}");
        assert!(text.contains("records : 12"), "{text}");
        assert!(text.contains("FAILED groups (1): /bad"), "{text}");
    }

    #[test]
    fn retention_report_only_vs_applied() {
        let report_only = retention_lines(&plan(vec![], false), false).join("\n");
        assert!(report_only.contains("--apply-retention"), "{report_only}");
        let applied = retention_lines(&plan(vec![], true), true).join("\n");
        assert!(
            applied.contains("PutRetentionPolicy(7 days) succeeded"),
            "{applied}"
        );
    }
}
