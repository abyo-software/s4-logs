//! `s4logs drain` — Mode A wiring (DESIGN.md §7, §9): DrainJob + the
//! fail-closed retention gate, with a human-readable report.

use std::sync::Arc;

use anyhow::{Context, Result};
use s4logs_core::store::ObjectStore;
use s4logs_drain::{
    AwsCwSource, CwSource, DrainJob, DrainOptions, DrainReport, ObjectStoreManifestStore,
    RetentionPlan, RetentionRequest, enforce_retention,
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
    let bucket = global.require_bucket()?;
    let account = global.require_account()?;

    let clients = aws::load(global).await;
    let store = ObjectStore::new(clients.s3(), bucket, &global.prefix);
    let cw = Arc::new(AwsCwSource::new(clients.cwl()));
    let sink = Arc::new(store.clone());
    let manifests = Arc::new(ObjectStoreManifestStore::new(store));

    let mut opts = DrainOptions::new(account.clone(), args.log_group.clone());
    opts.from_ms = args.from;
    opts.to_ms = args.to;
    opts.window_ms = args.window;
    opts.chunk_target_bytes = args.chunk_target;
    opts.concurrency = args.concurrency;
    opts.dry_run = args.dry_run;
    let window_ms = opts.window_ms;

    let report = DrainJob::new(cw.clone(), sink, manifests.clone(), opts)
        .run()
        .await
        .context("drain failed")?;
    for line in report_lines(&report) {
        println!("{line}");
    }

    if let Some(retention_days) = args.retention_days {
        // Coverage must start at the log group creation time (DESIGN.md §6
        // step 4) — anything older than that cannot hold events.
        let info = cw
            .describe_log_group(&args.log_group)
            .await
            .context("DescribeLogGroups for retention coverage")?;
        let apply = args.apply_retention && !args.dry_run;
        if args.apply_retention && args.dry_run {
            tracing::warn!("--dry-run: retention stays report-only despite --apply-retention");
        }
        let req = RetentionRequest {
            account,
            log_group: args.log_group.clone(),
            retention_days,
            coverage_from_ms: info.creation_time_ms,
            now_ms: now_ms(),
            window_ms,
        };
        let plan = enforce_retention(&*cw, &*manifests, &global.prefix, &req, apply)
            .await
            .context("retention gate")?;
        for line in retention_lines(&plan, apply) {
            println!("{line}");
        }
    }
    Ok(())
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
        "  est. monthly storage: CloudWatch ${:.4} -> S3 ${:.4} (saves ${:.4}/month)",
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
        // 10 GiB CW ($0.30) vs 1 GiB S3 ($0.023) → saves $0.277.
        assert!(text.contains("saves $0.2770/month"), "{text}");
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
