//! `s4logs report` — manifest-only cost/coverage summary (DESIGN.md §11.3).
//!
//! Reads **drain manifests only**: one LIST of the account's manifest
//! prefix + one GET per manifest JSON. Zero CloudWatch API calls, zero S3
//! data-object (or sidecar) reads — safe to run as often as you like.
//!
//! Pre-wave-3G manifests lack per-object `raw_bytes`; their CloudWatch-side
//! cost is unknowable from the manifest alone, so those objects are counted
//! in `objects_missing_raw` and the savings estimate is a lower bound.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use s4logs_core::store::ObjectStore;
use s4logs_drain::{
    CW_STORAGE_USD_PER_GIB_MONTH, GroupSelector, Manifest, ManifestStore, ObjectStoreManifestStore,
    S3_STORAGE_USD_PER_GIB_MONTH, manifest_account_prefix, parse_manifest_key_log_group,
};
use serde::Serialize;

use crate::aws;
use crate::cli::{GlobalArgs, ReportArgs, ReportOutput, UsageError};
use crate::timearg::{fmt_ts, format_bytes};

/// Aggregates for one log group (or the cross-group total).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GroupReport {
    pub log_group: String,
    /// Windows drained (manifest count, empty windows included).
    pub windows: u64,
    pub objects: u64,
    pub records: u64,
    /// Sum of the known per-object uncompressed sizes.
    pub raw_bytes: u64,
    /// Objects whose manifest predates raw-byte accounting — the savings
    /// estimate excludes their CloudWatch side (lower bound).
    pub objects_missing_raw: u64,
    pub compressed_bytes: u64,
    /// raw / compressed, only when every object reports its raw size.
    pub compression_ratio: Option<f64>,
    pub cw_monthly_usd: f64,
    pub s3_monthly_usd: f64,
    pub est_monthly_savings_usd: f64,
    /// First covered window start / last covered window end (epoch ms).
    pub coverage_from_ms: Option<i64>,
    pub coverage_to_ms: Option<i64>,
    /// Contiguous missing regions between covered windows.
    pub coverage_gaps: u64,
}

#[derive(Debug, Serialize)]
pub struct ReportSummary {
    /// Human description of the selector (`--all` or the glob/name).
    pub scope: String,
    pub groups: Vec<GroupReport>,
    /// Cross-group sums; coverage fields are `None` (spans don't add).
    pub total: GroupReport,
}

fn gib(bytes: u64) -> f64 {
    bytes as f64 / (1u64 << 30) as f64
}

/// Pure aggregation over one group's manifests (any order).
fn aggregate_group(log_group: String, manifests: &[Manifest]) -> GroupReport {
    let mut g = GroupReport {
        log_group,
        windows: manifests.len() as u64,
        objects: 0,
        records: 0,
        raw_bytes: 0,
        objects_missing_raw: 0,
        compressed_bytes: 0,
        compression_ratio: None,
        cw_monthly_usd: 0.0,
        s3_monthly_usd: 0.0,
        est_monthly_savings_usd: 0.0,
        coverage_from_ms: None,
        coverage_to_ms: None,
        coverage_gaps: 0,
    };
    let mut windows: Vec<(i64, i64)> = Vec::with_capacity(manifests.len());
    for m in manifests {
        windows.push((m.window_start_ms, m.window_end_ms));
        g.records += m.record_count;
        for o in &m.objects {
            g.objects += 1;
            g.compressed_bytes += o.body_len;
            match o.raw_bytes {
                Some(raw) => g.raw_bytes += raw,
                None => g.objects_missing_raw += 1,
            }
        }
    }
    windows.sort_unstable();
    if let (Some(first), Some(last)) = (windows.first(), windows.last()) {
        g.coverage_from_ms = Some(first.0);
        g.coverage_to_ms = Some(last.1);
    }
    g.coverage_gaps = windows
        .windows(2)
        .filter(|pair| pair[1].0 > pair[0].1)
        .count() as u64;
    finish_pricing(&mut g);
    g
}

/// Derive prices/ratio from the byte sums (shared by groups and the total).
fn finish_pricing(g: &mut GroupReport) {
    g.cw_monthly_usd = gib(g.raw_bytes) * CW_STORAGE_USD_PER_GIB_MONTH;
    g.s3_monthly_usd = gib(g.compressed_bytes) * S3_STORAGE_USD_PER_GIB_MONTH;
    g.est_monthly_savings_usd = g.cw_monthly_usd - g.s3_monthly_usd;
    g.compression_ratio = (g.objects_missing_raw == 0 && g.raw_bytes > 0 && g.compressed_bytes > 0)
        .then(|| g.raw_bytes as f64 / g.compressed_bytes as f64);
}

/// Pure summary over `log_group → manifests` (already selector-filtered).
fn build_summary(scope: String, by_group: BTreeMap<String, Vec<Manifest>>) -> ReportSummary {
    let groups: Vec<GroupReport> = by_group
        .into_iter()
        .map(|(name, manifests)| aggregate_group(name, &manifests))
        .collect();
    let mut total = GroupReport {
        log_group: "TOTAL".to_owned(),
        windows: 0,
        objects: 0,
        records: 0,
        raw_bytes: 0,
        objects_missing_raw: 0,
        compressed_bytes: 0,
        compression_ratio: None,
        cw_monthly_usd: 0.0,
        s3_monthly_usd: 0.0,
        est_monthly_savings_usd: 0.0,
        coverage_from_ms: None,
        coverage_to_ms: None,
        coverage_gaps: 0,
    };
    for g in &groups {
        total.windows += g.windows;
        total.objects += g.objects;
        total.records += g.records;
        total.raw_bytes += g.raw_bytes;
        total.objects_missing_raw += g.objects_missing_raw;
        total.compressed_bytes += g.compressed_bytes;
        total.coverage_gaps += g.coverage_gaps;
    }
    finish_pricing(&mut total);
    ReportSummary {
        scope,
        groups,
        total,
    }
}

/// Human-readable rendering (pure; unit-tested without AWS).
fn render_table(s: &ReportSummary) -> Vec<String> {
    let mut lines = vec![format!(
        "Manifest report for {} — reads manifests only (no CloudWatch calls, no S3 data reads)",
        s.scope
    )];
    if s.groups.is_empty() {
        lines.push("  no manifests found".to_owned());
        return lines;
    }
    for g in &s.groups {
        lines.extend(group_lines(g));
    }
    if s.groups.len() > 1 {
        lines.push(format!("TOTAL ({} log groups)", s.groups.len()));
        lines.extend(group_lines(&s.total).into_iter().skip(1));
    }
    lines
}

fn group_lines(g: &GroupReport) -> Vec<String> {
    let mut lines = vec![format!("{}", g.log_group)];
    let coverage = match (g.coverage_from_ms, g.coverage_to_ms) {
        (Some(f), Some(t)) => format!(
            ", coverage {} .. {} ({} gap{})",
            fmt_ts(f),
            fmt_ts(t),
            g.coverage_gaps,
            if g.coverage_gaps == 1 { "" } else { "s" }
        ),
        _ => String::new(),
    };
    lines.push(format!("  windows : {} drained{coverage}", g.windows));
    lines.push(format!(
        "  records : {} in {} object(s)",
        g.records, g.objects
    ));
    let ratio = match g.compression_ratio {
        Some(r) => format!(" ({r:.1}x)"),
        None if g.objects_missing_raw > 0 => format!(
            " (ratio unknown: {} object(s) predate raw-size accounting)",
            g.objects_missing_raw
        ),
        None => String::new(),
    };
    lines.push(format!(
        "  bytes   : {} raw -> {} compressed{ratio}",
        format_bytes(g.raw_bytes),
        format_bytes(g.compressed_bytes)
    ));
    let floor = if g.objects_missing_raw > 0 {
        " [lower bound]"
    } else {
        ""
    };
    lines.push(format!(
        "  est. monthly storage: CloudWatch ${:.4} -> S3 ${:.4} (saves ${:.4}/month){floor}",
        g.cw_monthly_usd, g.s3_monthly_usd, g.est_monthly_savings_usd
    ));
    lines
}

pub async fn run(global: &GlobalArgs, args: &ReportArgs) -> Result<()> {
    let selector = if args.all {
        GroupSelector::All
    } else {
        let Some(pattern) = &args.log_group else {
            // clap's ArgGroup makes this unreachable; typed error just in case.
            return Err(UsageError("one of --log-group / --all is required".into()).into());
        };
        GroupSelector::parse(pattern).map_err(|e| UsageError(format!("{e:#}")))?
    };
    let bucket = global.require_bucket()?;
    let account = global.require_account()?;

    let clients = aws::load(global).await;
    let store = ObjectStore::new(clients.s3(), bucket, &global.prefix);
    let manifests = ObjectStoreManifestStore::new(store);

    let prefix = manifest_account_prefix(&global.prefix, &account);
    let keys = manifests
        .list(&prefix)
        .await
        .with_context(|| format!("listing manifests under {prefix}"))?;

    let mut by_group: BTreeMap<String, Vec<Manifest>> = BTreeMap::new();
    for key in keys {
        let Some(group) = parse_manifest_key_log_group(&key) else {
            tracing::warn!(key = %key, "skipping foreign key under manifest prefix");
            continue;
        };
        if !selector.matches(&group) {
            continue;
        }
        let Some(bytes) = manifests
            .get(&key)
            .await
            .with_context(|| format!("reading manifest {key}"))?
        else {
            continue; // deleted between LIST and GET
        };
        match Manifest::from_json_bytes(&bytes) {
            Ok(m) => by_group.entry(group).or_default().push(m),
            Err(err) => {
                tracing::warn!(key = %key, error = %err, "skipping undecodable manifest");
            }
        }
    }

    let summary = build_summary(selector.describe(), by_group);
    match args.output {
        ReportOutput::Table => {
            for line in render_table(&summary) {
                println!("{line}");
            }
        }
        ReportOutput::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&summary).context("serializing report")?
            );
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use s4logs_drain::ManifestObject;

    const DAY0: i64 = 1_717_545_600_000; // 2024-06-05T00:00:00Z
    const HOUR: i64 = 3_600_000;

    fn obj(body_len: u64, raw_bytes: Option<u64>, record_count: u64) -> ManifestObject {
        ManifestObject {
            data_key: "k".into(),
            etag: None,
            crc32c: 0,
            body_len,
            raw_bytes,
            record_count,
            min_ts: DAY0,
            max_ts: DAY0 + 1,
        }
    }

    fn manifest(group: &str, start: i64, end: i64, objects: Vec<ManifestObject>) -> Manifest {
        Manifest {
            version: 1,
            account: "123456789012".into(),
            log_group: group.into(),
            window_start_ms: start,
            window_end_ms: end,
            record_count: objects.iter().map(|o| o.record_count).sum(),
            objects,
            completed_at_ms: 0,
            drain_version: "test".into(),
        }
    }

    #[test]
    fn aggregate_math_and_gap_detection() {
        // Windows [0,1h) [1h,2h) [3h,4h): one gap at [2h,3h). Unsorted input.
        let manifests = vec![
            manifest(
                "/g",
                DAY0 + 3 * HOUR,
                DAY0 + 4 * HOUR,
                vec![obj(1 << 30, Some(5 << 30), 100)],
            ),
            manifest(
                "/g",
                DAY0,
                DAY0 + HOUR,
                vec![obj(1 << 30, Some(5 << 30), 200)],
            ),
            manifest("/g", DAY0 + HOUR, DAY0 + 2 * HOUR, vec![]), // empty window
        ];
        let g = aggregate_group("/g".into(), &manifests);
        assert_eq!(g.windows, 3);
        assert_eq!(g.objects, 2);
        assert_eq!(g.records, 300);
        assert_eq!(g.raw_bytes, 10 << 30);
        assert_eq!(g.compressed_bytes, 2 << 30);
        assert_eq!(g.objects_missing_raw, 0);
        assert_eq!(g.compression_ratio, Some(5.0));
        assert_eq!(g.coverage_from_ms, Some(DAY0));
        assert_eq!(g.coverage_to_ms, Some(DAY0 + 4 * HOUR));
        assert_eq!(g.coverage_gaps, 1);
        // 10 GiB CW ($0.30) vs 2 GiB S3 ($0.046) → saves $0.254.
        assert!((g.cw_monthly_usd - 0.30).abs() < 1e-9);
        assert!((g.s3_monthly_usd - 0.046).abs() < 1e-9);
        assert!((g.est_monthly_savings_usd - 0.254).abs() < 1e-9);
    }

    #[test]
    fn contiguous_windows_have_no_gaps() {
        let manifests: Vec<Manifest> = (0..5)
            .map(|i| {
                manifest(
                    "/g",
                    DAY0 + i * HOUR,
                    DAY0 + (i + 1) * HOUR,
                    vec![obj(10, Some(100), 1)],
                )
            })
            .collect();
        let g = aggregate_group("/g".into(), &manifests);
        assert_eq!(g.coverage_gaps, 0);
        assert_eq!(g.coverage_from_ms, Some(DAY0));
        assert_eq!(g.coverage_to_ms, Some(DAY0 + 5 * HOUR));
    }

    #[test]
    fn missing_raw_bytes_disable_ratio_and_mark_lower_bound() {
        let manifests = vec![manifest(
            "/g",
            DAY0,
            DAY0 + HOUR,
            vec![
                obj(100, Some(1000), 1),
                obj(100, None, 1), // pre-wave-3G manifest object
            ],
        )];
        let g = aggregate_group("/g".into(), &manifests);
        assert_eq!(g.objects_missing_raw, 1);
        assert_eq!(g.raw_bytes, 1000, "only known raw bytes are summed");
        assert_eq!(g.compression_ratio, None);
        let text = group_lines(&g).join("\n");
        assert!(text.contains("ratio unknown"), "{text}");
        assert!(text.contains("[lower bound]"), "{text}");
    }

    #[test]
    fn summary_totals_across_groups_and_table_render() {
        let mut by_group = BTreeMap::new();
        by_group.insert(
            "/a".to_owned(),
            vec![manifest(
                "/a",
                DAY0,
                DAY0 + HOUR,
                vec![obj(1 << 30, Some(10 << 30), 10)],
            )],
        );
        by_group.insert(
            "/b".to_owned(),
            vec![
                manifest("/b", DAY0, DAY0 + HOUR, vec![obj(100, Some(500), 5)]),
                manifest("/b", DAY0 + 2 * HOUR, DAY0 + 3 * HOUR, vec![]),
            ],
        );
        let s = build_summary("--all".into(), by_group);
        assert_eq!(s.groups.len(), 2);
        assert_eq!(s.groups[0].log_group, "/a");
        assert_eq!(s.total.windows, 3);
        assert_eq!(s.total.records, 15);
        assert_eq!(s.total.raw_bytes, (10 << 30) + 500);
        assert_eq!(s.total.coverage_gaps, 1, "gap in /b counts in the total");
        assert_eq!(s.total.coverage_from_ms, None, "spans don't add");

        let text = render_table(&s).join("\n");
        assert!(
            text.contains("no CloudWatch calls, no S3 data reads"),
            "{text}"
        );
        assert!(text.contains("TOTAL (2 log groups)"), "{text}");
        assert!(text.contains("/a"), "{text}");
        assert!(text.contains("1 gap"), "{text}");
        assert!(!text.contains("NaN") && !text.contains("inf"), "{text}");
    }

    #[test]
    fn empty_summary_renders_no_manifests() {
        let s = build_summary("glob \"/none/*\"".into(), BTreeMap::new());
        let text = render_table(&s).join("\n");
        assert!(text.contains("no manifests found"), "{text}");
        assert_eq!(s.total.windows, 0);
        assert_eq!(s.total.est_monthly_savings_usd, 0.0);
    }

    #[test]
    fn json_shape_is_stable() {
        let mut by_group = BTreeMap::new();
        by_group.insert(
            "/g".to_owned(),
            vec![manifest(
                "/g",
                DAY0,
                DAY0 + HOUR,
                vec![obj(100, Some(1000), 7)],
            )],
        );
        let s = build_summary("/g".into(), by_group);
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["scope"], "/g");
        assert_eq!(v["groups"][0]["log_group"], "/g");
        assert_eq!(v["groups"][0]["records"], 7);
        assert_eq!(v["groups"][0]["raw_bytes"], 1000);
        assert_eq!(v["groups"][0]["compression_ratio"], 10.0);
        assert_eq!(v["groups"][0]["coverage_from_ms"], DAY0);
        assert_eq!(v["groups"][0]["coverage_gaps"], 0);
        assert_eq!(v["total"]["log_group"], "TOTAL");
    }
}
