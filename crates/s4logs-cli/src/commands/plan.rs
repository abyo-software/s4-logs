//! `s4logs plan` — read-only, zero-write account diagnostic: what each log
//! group costs in CloudWatch today, and what Mode A (drain to S3 zstd) and
//! Mode B (ingest bypass) would save. The GTM entry point: runnable with
//! read-only credentials, no bucket, no account id.
//!
//! Data sources (both read-only):
//! - `DescribeLogGroups` — `storedBytes` per group. This **is** CloudWatch's
//!   storage billing basis (archived storage is billed on gzip-level-6
//!   compressed bytes — AWS pricing footnote), so the "now" storage cost
//!   involves no compression guess at all.
//! - CloudWatch Metrics `GetMetricData` — `AWS/Logs IncomingBytes` (Sum,
//!   daily) per group over `--ingest-days`, scaled to 30 days, for the
//!   ingest line. Batched 500 queries per call (the API maximum).
//!
//! All AWS calls stay thin; the cost math, batching, sorting and rendering
//! are pure functions unit-tested below.

use anyhow::{Context, Result};
use aws_sdk_cloudwatch::primitives::DateTime;
use aws_sdk_cloudwatch::types::{Dimension, Metric, MetricDataQuery, MetricStat};
use s4logs_drain::{
    AwsCwSource, CW_STORAGE_USD_PER_GIB_MONTH, GroupSelector, S3_STORAGE_USD_PER_GIB_MONTH,
    discover_log_groups,
};
use serde::Serialize;

use crate::aws;
use crate::cli::{GlobalArgs, PlanArgs, ReportOutput, UsageError};
use crate::timearg::format_bytes;

/// CloudWatch Logs ingest list price, us-east-1: $0.50 per GiB of **raw**
/// (uncompressed) bytes. AWS additionally bills 26 B per event, which the
/// `IncomingBytes` metric does not expose — the ingest estimate here is a
/// slight underestimate (stated in the footer).
const CW_INGEST_USD_PER_GIB: f64 = 0.50;

/// S3 Glacier Instant Retrieval storage list price, us-east-1 (still
/// millisecond-access — fine for write-once archives).
const S3_GLACIER_IR_USD_PER_GIB_MONTH: f64 = 0.004;

/// gzip→zstd size improvement factor: projected S3 object size =
/// `storedBytes` (already gzip-6 compressed) × 0.65. zstd-3 is typically
/// ~1.5× denser than CW's gzip-6; we measured 6.2× (zstd) vs the assumed 4×
/// (gzip) in the 2026-06-10 real-AWS experiment — 4/6.2 ≈ 0.65.
const ZSTD_VS_GZIP: f64 = 0.65;

/// Mode B assumption: fraction of ingested bytes that can bypass CloudWatch
/// through the gateway (keep alert-critical streams in CW via routing
/// rules; ~90% of typical volume does not need to be there).
const MODE_B_BYPASS_FRACTION: f64 = 0.9;

/// `GetMetricData` accepts at most 500 `MetricDataQuery` entries per call.
const METRIC_QUERIES_PER_CALL: usize = 500;

/// Days used to scale the sampled ingest window to "per month".
const DAYS_PER_MONTH: f64 = 30.0;

fn gib(bytes: f64) -> f64 {
    bytes / (1u64 << 30) as f64
}

/// One log group's current cost and projections (also the JSON row shape).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PlanRow {
    pub log_group: String,
    /// `None` = "Never expire" — flagged in the table: storage grows
    /// without bound.
    pub retention_days: Option<i32>,
    /// `DescribeLogGroups` storedBytes — CloudWatch's (gzip-compressed)
    /// storage billing basis.
    pub stored_bytes: u64,
    /// AWS/Logs IncomingBytes Sum over the sampled `--ingest-days` window.
    pub ingest_bytes_sampled: f64,
    /// The sample scaled to 30 days.
    pub ingest_bytes_monthly: f64,
    pub cw_storage_usd_month: f64,
    pub cw_ingest_usd_month: f64,
    /// Current monthly bill for this group (storage + ingest).
    pub cw_total_usd_month: f64,
    /// Mode A: projected S3 Standard storage (storedBytes × ZSTD_VS_GZIP).
    pub mode_a_s3_standard_usd_month: f64,
    /// Mode A: same bytes on S3 Glacier Instant Retrieval.
    pub mode_a_glacier_ir_usd_month: f64,
    /// Mode B: ingest avoided at MODE_B_BYPASS_FRACTION.
    pub mode_b_avoided_usd_month: f64,
    /// (CW storage − Mode A S3 Standard) + Mode B avoided ingest.
    pub est_savings_usd_month: f64,
}

/// Pure per-group cost math. `ingest_bytes_sampled` is the IncomingBytes
/// Sum over `ingest_days` (0.0 when the group has no metric data).
fn compute_row(
    log_group: String,
    retention_days: Option<i32>,
    stored_bytes: u64,
    ingest_bytes_sampled: f64,
    ingest_days: u32,
) -> PlanRow {
    let ingest_bytes_monthly =
        ingest_bytes_sampled / f64::from(ingest_days.max(1)) * DAYS_PER_MONTH;
    let cw_storage = gib(stored_bytes as f64) * CW_STORAGE_USD_PER_GIB_MONTH;
    let cw_ingest = gib(ingest_bytes_monthly) * CW_INGEST_USD_PER_GIB;
    let s3_std = gib(stored_bytes as f64) * ZSTD_VS_GZIP * S3_STORAGE_USD_PER_GIB_MONTH;
    let glacier = gib(stored_bytes as f64) * ZSTD_VS_GZIP * S3_GLACIER_IR_USD_PER_GIB_MONTH;
    let mode_b = cw_ingest * MODE_B_BYPASS_FRACTION;
    PlanRow {
        log_group,
        retention_days,
        stored_bytes,
        ingest_bytes_sampled,
        ingest_bytes_monthly,
        cw_storage_usd_month: cw_storage,
        cw_ingest_usd_month: cw_ingest,
        cw_total_usd_month: cw_storage + cw_ingest,
        mode_a_s3_standard_usd_month: s3_std,
        mode_a_glacier_ir_usd_month: glacier,
        mode_b_avoided_usd_month: mode_b,
        est_savings_usd_month: (cw_storage - s3_std) + mode_b,
    }
}

/// Assumption constants echoed into the JSON output so a saved dump stays
/// self-describing.
#[derive(Debug, Serialize)]
pub struct PlanAssumptions {
    pub prices: &'static str,
    pub cw_storage_usd_per_gib_month: f64,
    pub cw_ingest_usd_per_gib: f64,
    pub s3_standard_usd_per_gib_month: f64,
    pub s3_glacier_ir_usd_per_gib_month: f64,
    pub zstd_vs_gzip_size_factor: f64,
    pub mode_b_bypass_fraction: f64,
    pub ingest_days_sampled: u32,
    pub note: &'static str,
}

impl PlanAssumptions {
    fn new(ingest_days: u32) -> Self {
        Self {
            prices: "AWS list, us-east-1",
            cw_storage_usd_per_gib_month: CW_STORAGE_USD_PER_GIB_MONTH,
            cw_ingest_usd_per_gib: CW_INGEST_USD_PER_GIB,
            s3_standard_usd_per_gib_month: S3_STORAGE_USD_PER_GIB_MONTH,
            s3_glacier_ir_usd_per_gib_month: S3_GLACIER_IR_USD_PER_GIB_MONTH,
            zstd_vs_gzip_size_factor: ZSTD_VS_GZIP,
            mode_b_bypass_fraction: MODE_B_BYPASS_FRACTION,
            ingest_days_sampled: ingest_days,
            note: "projections are estimates; storedBytes is CloudWatch's actual \
                   (gzip-compressed) storage billing basis, ingest is scaled from \
                   IncomingBytes and excludes the 26 B/event surcharge",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PlanSummary {
    /// Human description of the selector (`--all` or the glob/name).
    pub scope: String,
    /// Table display cutoff (JSON always carries every group).
    pub top: usize,
    /// All groups, sorted by current monthly cost, descending.
    pub groups: Vec<PlanRow>,
    /// Account totals across **all** groups (not just the displayed top).
    pub total: PlanRow,
    pub assumptions: PlanAssumptions,
}

/// Pure: sort rows by current cost desc (name as tie-break) and total them.
fn build_summary(
    scope: String,
    mut rows: Vec<PlanRow>,
    ingest_days: u32,
    top: usize,
) -> PlanSummary {
    rows.sort_by(|a, b| {
        b.cw_total_usd_month
            .total_cmp(&a.cw_total_usd_month)
            .then_with(|| a.log_group.cmp(&b.log_group))
    });
    let mut total = compute_row("TOTAL".to_owned(), None, 0, 0.0, ingest_days);
    let stored: u64 = rows.iter().map(|r| r.stored_bytes).sum();
    let sampled: f64 = rows.iter().map(|r| r.ingest_bytes_sampled).sum();
    total = compute_row(
        "TOTAL".to_owned(),
        total.retention_days,
        stored,
        sampled,
        ingest_days,
    );
    PlanSummary {
        scope,
        top,
        groups: rows,
        total,
        assumptions: PlanAssumptions::new(ingest_days),
    }
}

/// Pure: split `n_groups` into `GetMetricData`-sized index ranges.
fn metric_batches(n_groups: usize, per_call: usize) -> Vec<std::ops::Range<usize>> {
    let per_call = per_call.max(1);
    (0..n_groups)
        .step_by(per_call)
        .map(|start| start..(start + per_call).min(n_groups))
        .collect()
}

/// Query id for group index `i`. `GetMetricData` ids must start with a
/// lowercase letter; the index keys the response back to the group.
fn query_id(i: usize) -> String {
    format!("m{i}")
}

/// Inverse of [`query_id`]; `None` for foreign/malformed ids.
fn parse_query_id(id: &str) -> Option<usize> {
    let digits = id.strip_prefix('m')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn retention_label(retention_days: Option<i32>) -> String {
    match retention_days {
        Some(d) => format!("{d}d"),
        None => "never!".to_owned(),
    }
}

fn money(v: f64) -> String {
    format!("{v:.2}")
}

const TABLE_COLS: [&str; 10] = [
    "RETAIN",
    "STORED",
    "CW-ST$",
    "INGEST/MO",
    "CW-IN$",
    "NOW$/MO",
    "A:S3$",
    "A:IR$",
    "B:SAVED$",
    "EST.SAVE$",
];

fn table_row(name_w: usize, label: &str, retention: &str, r: &PlanRow) -> String {
    format!(
        "{label:<name_w$}  {retention:>6}  {:>10}  {:>8}  {:>10}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>9}",
        format_bytes(r.stored_bytes),
        money(r.cw_storage_usd_month),
        format_bytes(r.ingest_bytes_monthly.max(0.0) as u64),
        money(r.cw_ingest_usd_month),
        money(r.cw_total_usd_month),
        money(r.mode_a_s3_standard_usd_month),
        money(r.mode_a_glacier_ir_usd_month),
        money(r.mode_b_avoided_usd_month),
        money(r.est_savings_usd_month),
    )
}

/// Human-readable rendering (pure; unit-tested without AWS).
fn render_table(p: &PlanSummary) -> Vec<String> {
    let mut lines = vec![
        "s4logs plan — read-only diagnostic — uses DescribeLogGroups + GetMetricData only, \
         no writes, no charges"
            .to_owned(),
        "Projections are estimates; every assumption is stated once in the footer. STORED is \
         CloudWatch's actual (gzip-compressed) storage billing basis — no compression guess on \
         the \"now\" side."
            .to_owned(),
        String::new(),
    ];
    if p.groups.is_empty() {
        lines.push(format!("no log groups match {}", p.scope));
        return lines;
    }
    let total_label = format!("TOTAL ({} group{})", p.groups.len(), plural(p.groups.len()));
    let shown: Vec<&PlanRow> = p.groups.iter().take(p.top).collect();
    let name_w = shown
        .iter()
        .map(|r| r.log_group.len())
        .chain([total_label.len(), "LOG GROUP".len()])
        .max()
        .unwrap_or(9);
    let mut header = format!("{:<name_w$}", "LOG GROUP");
    for (i, col) in TABLE_COLS.iter().enumerate() {
        let w = match i {
            0 => 6,      // RETAIN
            1 | 3 => 10, // STORED / INGEST/MO
            9 => 9,      // EST.SAVE$
            _ => 8,
        };
        header.push_str(&format!("  {col:>w$}"));
    }
    lines.push(header);
    for r in &shown {
        lines.push(table_row(
            name_w,
            &r.log_group,
            &retention_label(r.retention_days),
            r,
        ));
    }
    if p.groups.len() > p.top {
        lines.push(format!(
            "  ... and {} more group{} below the --top {} cutoff (totals include them; \
             --output json lists all)",
            p.groups.len() - p.top,
            plural(p.groups.len() - p.top),
            p.top
        ));
    }
    lines.push(table_row(name_w, &total_label, "", &p.total));
    lines.push(String::new());
    lines.extend(footer_lines(&p.assumptions, p.groups.len()));
    lines
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// Every assumption, printed exactly once.
fn footer_lines(a: &PlanAssumptions, n_groups: usize) -> Vec<String> {
    vec![
        format!("Assumptions (all prices {}):", a.prices),
        format!(
            "  CW-ST$    : storedBytes x ${:.3}/GiB-mo. storedBytes is already gzip-6 \
             compressed — it is exactly what CloudWatch bills for storage",
            a.cw_storage_usd_per_gib_month
        ),
        format!(
            "  CW-IN$    : AWS/Logs IncomingBytes (Sum) over the last {} day(s), scaled to 30 \
             days, x ${:.2}/GiB raw. The 26 B/event billing surcharge is not counted (slight \
             underestimate); the metric can lag recent ingest by minutes",
            a.ingest_days_sampled, a.cw_ingest_usd_per_gib
        ),
        format!(
            "  Mode A    : projected S3 size = storedBytes x {} (ZSTD_VS_GZIP: zstd-3 is \
             typically ~1.5x denser than CW's gzip-6; measured 6.2x vs the assumed 4x in the \
             2026-06-10 experiment), priced at S3 Standard ${:.3}/GiB-mo (A:S3$) and Glacier \
             Instant Retrieval ${:.3}/GiB-mo (A:IR$)",
            a.zstd_vs_gzip_size_factor,
            a.s3_standard_usd_per_gib_month,
            a.s3_glacier_ir_usd_per_gib_month
        ),
        format!(
            "  B:SAVED$  : assumes {:.0}% of ingested bytes can bypass CloudWatch via the \
             gateway (keep alert-critical streams in CW with routing rules) — an assumption, \
             tune it to your stream mix",
            a.mode_b_bypass_fraction * 100.0
        ),
        "  EST.SAVE$ : (CW-ST$ - A:S3$) + B:SAVED$ per month, i.e. Mode A on S3 Standard plus \
         Mode B; Glacier IR widens it further"
            .to_owned(),
        "  RETAIN    : \"never!\" = Never expire — storage grows without bound; the prime \
         drain + retention-shortening candidates"
            .to_owned(),
        format!(
            "  API cost  : DescribeLogGroups is free; GetMetricData lists at $0.01 per 1,000 \
             metrics — this run requested {n_groups} (rounds to $0.00)"
        ),
    ]
}

/// Build the 500-max query slice for one `GetMetricData` call.
fn build_queries(
    groups: &[String],
    range: std::ops::Range<usize>,
    period_s: i32,
) -> Vec<MetricDataQuery> {
    let mut queries = Vec::with_capacity(range.len());
    for i in range {
        let metric = Metric::builder()
            .namespace("AWS/Logs")
            .metric_name("IncomingBytes")
            .dimensions(
                Dimension::builder()
                    .name("LogGroupName")
                    .value(&groups[i])
                    .build(),
            )
            .build();
        let stat = MetricStat::builder()
            .metric(metric)
            .period(period_s)
            .stat("Sum")
            .build();
        queries.push(
            MetricDataQuery::builder()
                .id(query_id(i))
                .metric_stat(stat)
                .return_data(true)
                .build(),
        );
    }
    queries
}

/// Thin SDK wrapper: per-group IncomingBytes sums over `[start_s, end_s)`.
/// Groups with no metric datapoints (nothing ingested in the window) stay
/// at 0.0. Daily period — valid for the full 455-day metric retention.
async fn fetch_ingest_sums(
    metrics: &aws_sdk_cloudwatch::Client,
    groups: &[String],
    start_s: i64,
    end_s: i64,
) -> Result<Vec<f64>> {
    let mut sums = vec![0.0f64; groups.len()];
    for range in metric_batches(groups.len(), METRIC_QUERIES_PER_CALL) {
        let queries = build_queries(groups, range, 86_400);
        let mut token: Option<String> = None;
        loop {
            let resp = metrics
                .get_metric_data()
                .set_metric_data_queries(Some(queries.clone()))
                .start_time(DateTime::from_secs(start_s))
                .end_time(DateTime::from_secs(end_s))
                .set_next_token(token.clone())
                .send()
                .await
                .context("GetMetricData (AWS/Logs IncomingBytes)")?;
            for r in resp.metric_data_results.unwrap_or_default() {
                let Some(idx) = r.id.as_deref().and_then(parse_query_id) else {
                    continue;
                };
                if let Some(sum) = sums.get_mut(idx) {
                    *sum += r.values.unwrap_or_default().iter().sum::<f64>();
                }
            }
            token = resp.next_token;
            if token.is_none() {
                break;
            }
        }
    }
    Ok(sums)
}

fn now_s() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub async fn run(global: &GlobalArgs, args: &PlanArgs) -> Result<()> {
    let selector = if args.all {
        GroupSelector::All
    } else {
        let Some(pattern) = &args.log_group else {
            // clap's ArgGroup makes this unreachable; typed error just in case.
            return Err(UsageError("one of --log-group / --all is required".into()).into());
        };
        GroupSelector::parse(pattern).map_err(|e| UsageError(format!("{e:#}")))?
    };
    // Deliberately NO require_bucket()/require_account(): plan never touches
    // S3 — read-only CloudWatch credentials are the entire requirement.
    let clients = aws::load(global).await;
    let cw = AwsCwSource::new(clients.cwl());
    let groups = discover_log_groups(&cw, &selector)
        .await
        .context("discovering log groups (DescribeLogGroups)")?;

    let names: Vec<String> = groups.iter().map(|(n, _)| n.clone()).collect();
    let end_s = now_s();
    let start_s = end_s - i64::from(args.ingest_days) * 86_400;
    let sums = if names.is_empty() {
        Vec::new()
    } else {
        fetch_ingest_sums(&clients.cw_metrics(), &names, start_s, end_s).await?
    };

    let rows: Vec<PlanRow> = groups
        .iter()
        .zip(&sums)
        .map(|((name, info), &sampled)| {
            compute_row(
                name.clone(),
                info.retention_days,
                info.stored_bytes.map_or(0, |b| b.max(0) as u64),
                sampled,
                args.ingest_days,
            )
        })
        .collect();

    let summary = build_summary(selector.describe(), rows, args.ingest_days, args.top);
    match args.output {
        ReportOutput::Table => {
            for line in render_table(&summary) {
                println!("{line}");
            }
        }
        ReportOutput::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&summary).context("serializing plan")?
            );
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    #[test]
    fn golden_cost_math() {
        // 100 GiB stored (gzip-billed), 60 GiB ingested over a 30-day sample.
        let r = compute_row("/g".into(), Some(90), 100 * GIB, (60 * GIB) as f64, 30);
        // storage: 100 GiB × $0.03 = $3.00
        assert!((r.cw_storage_usd_month - 3.0).abs() < 1e-9, "{r:?}");
        // ingest: 60 GiB / 30 d × 30 d = 60 GiB/mo × $0.50 = $30.00
        assert!((r.ingest_bytes_monthly - (60 * GIB) as f64).abs() < 1e-3);
        assert!((r.cw_ingest_usd_month - 30.0).abs() < 1e-9, "{r:?}");
        assert!((r.cw_total_usd_month - 33.0).abs() < 1e-9);
        // Mode A: 100 GiB × 0.65 = 65 GiB → $1.495 (Standard) / $0.26 (IR)
        assert!(
            (r.mode_a_s3_standard_usd_month - 1.495).abs() < 1e-9,
            "{r:?}"
        );
        assert!((r.mode_a_glacier_ir_usd_month - 0.26).abs() < 1e-9, "{r:?}");
        // Mode B: $30 × 0.9 = $27
        assert!((r.mode_b_avoided_usd_month - 27.0).abs() < 1e-9);
        // savings: (3.0 − 1.495) + 27.0 = 28.505
        assert!((r.est_savings_usd_month - 28.505).abs() < 1e-9, "{r:?}");
    }

    #[test]
    fn ingest_sample_scales_to_thirty_days() {
        // 7-day sample of 7 GiB → 30 GiB/mo → $15 ingest.
        let r = compute_row("/g".into(), None, 0, (7 * GIB) as f64, 7);
        assert!((r.ingest_bytes_monthly - (30 * GIB) as f64).abs() < 1e-3);
        assert!((r.cw_ingest_usd_month - 15.0).abs() < 1e-9);
        assert_eq!(r.cw_storage_usd_month, 0.0);
        assert!((r.est_savings_usd_month - 13.5).abs() < 1e-9, "Mode B only");
    }

    #[test]
    fn zero_group_produces_zero_costs_not_nan() {
        let r = compute_row("/idle".into(), Some(7), 0, 0.0, 30);
        assert_eq!(r.cw_total_usd_month, 0.0);
        assert_eq!(r.est_savings_usd_month, 0.0);
        let text = table_row(10, "/idle", &retention_label(r.retention_days), &r);
        assert!(!text.contains("NaN") && !text.contains("inf"), "{text}");
    }

    #[test]
    fn summary_sorts_by_current_cost_and_totals_all_groups() {
        let rows = vec![
            compute_row("/cheap".into(), Some(7), GIB, 0.0, 30),
            compute_row("/hot".into(), None, 10 * GIB, (100 * GIB) as f64, 30),
            compute_row("/mid".into(), Some(30), 5 * GIB, (10 * GIB) as f64, 30),
        ];
        let s = build_summary("--all".into(), rows, 30, 2);
        assert_eq!(s.groups[0].log_group, "/hot");
        assert_eq!(s.groups[1].log_group, "/mid");
        assert_eq!(s.groups[2].log_group, "/cheap");
        // Totals cover all three, not just --top 2.
        assert_eq!(s.total.stored_bytes, 16 * GIB);
        assert!((s.total.ingest_bytes_sampled - (110 * GIB) as f64).abs() < 1e-3);
        let expected: f64 = s.groups.iter().map(|g| g.cw_total_usd_month).sum();
        assert!((s.total.cw_total_usd_month - expected).abs() < 1e-9);
    }

    #[test]
    fn table_renders_header_flags_and_footer_once() {
        let rows = vec![
            compute_row("/hot".into(), None, 10 * GIB, (100 * GIB) as f64, 30),
            compute_row("/mid".into(), Some(30), 5 * GIB, (10 * GIB) as f64, 30),
            compute_row("/cheap".into(), Some(7), GIB, 0.0, 30),
        ];
        let s = build_summary("--all".into(), rows, 30, 2);
        let text = render_table(&s).join("\n");
        assert!(
            text.contains(
                "read-only diagnostic — uses DescribeLogGroups + GetMetricData only, no writes, no charges"
            ),
            "{text}"
        );
        assert!(text.contains("never!"), "never-expire flag: {text}");
        assert!(text.contains("TOTAL (3 groups)"), "{text}");
        assert!(
            text.contains("... and 1 more group below the --top 2 cutoff"),
            "{text}"
        );
        assert!(!text.contains("/cheap"), "beyond --top: {text}");
        // Each assumption appears exactly once.
        assert_eq!(text.matches("Assumptions").count(), 1, "{text}");
        assert_eq!(text.matches("ZSTD_VS_GZIP").count(), 1, "{text}");
        assert_eq!(text.matches("90% of ingested bytes").count(), 1, "{text}");
        assert_eq!(text.matches("us-east-1").count(), 1, "{text}");
        assert!(!text.contains("NaN") && !text.contains("inf"), "{text}");
    }

    #[test]
    fn table_handles_empty_scope() {
        let s = build_summary("glob \"/none/*\"".into(), Vec::new(), 30, 20);
        let text = render_table(&s).join("\n");
        assert!(
            text.contains("no log groups match glob \"/none/*\""),
            "{text}"
        );
    }

    #[test]
    fn metric_batches_chunk_at_api_limit() {
        assert!(metric_batches(0, 500).is_empty());
        assert_eq!(metric_batches(1, 500), vec![0..1]);
        assert_eq!(metric_batches(500, 500), vec![0..500]);
        assert_eq!(metric_batches(501, 500), vec![0..500, 500..501]);
        assert_eq!(
            metric_batches(1234, 500),
            vec![0..500, 500..1000, 1000..1234]
        );
        // Degenerate per_call is clamped, not an infinite loop.
        assert_eq!(metric_batches(2, 0), vec![0..1, 1..2]);
    }

    #[test]
    fn query_ids_roundtrip_and_reject_foreign() {
        for i in [0usize, 1, 7, 499, 12_345] {
            assert_eq!(parse_query_id(&query_id(i)), Some(i));
        }
        assert_eq!(parse_query_id("m"), None);
        assert_eq!(parse_query_id("x5"), None);
        assert_eq!(parse_query_id("m12x"), None);
        assert_eq!(parse_query_id(""), None);
    }

    #[test]
    fn glob_selector_filters_plan_groups() {
        // Same selector semantics as drain/report (globset: `*` crosses `/`).
        let sel = GroupSelector::parse("/aws/lambda/*").unwrap();
        let names = ["/aws/lambda/a", "/aws/lambda/b/c", "/aws/ecs/x", "/app"];
        let matched: Vec<&str> = names.into_iter().filter(|n| sel.matches(n)).collect();
        assert_eq!(matched, vec!["/aws/lambda/a", "/aws/lambda/b/c"]);
        assert!(GroupSelector::All.matches("/anything"));
    }

    #[test]
    fn json_shape_is_stable() {
        let rows = vec![compute_row(
            "/g".into(),
            None,
            100 * GIB,
            (60 * GIB) as f64,
            30,
        )];
        let s = build_summary("/g".into(), rows, 30, 20);
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["scope"], "/g");
        assert_eq!(v["groups"][0]["log_group"], "/g");
        assert_eq!(v["groups"][0]["retention_days"], serde_json::Value::Null);
        assert_eq!(v["groups"][0]["stored_bytes"], 100 * GIB);
        assert_eq!(v["groups"][0]["cw_storage_usd_month"], 3.0);
        assert_eq!(v["total"]["log_group"], "TOTAL");
        assert_eq!(v["assumptions"]["zstd_vs_gzip_size_factor"], 0.65);
        assert_eq!(v["assumptions"]["mode_b_bypass_fraction"], 0.9);
        assert_eq!(v["assumptions"]["ingest_days_sampled"], 30);
        assert_eq!(v["assumptions"]["prices"], "AWS list, us-east-1");
    }

    #[test]
    fn retention_labels() {
        assert_eq!(retention_label(Some(7)), "7d");
        assert_eq!(retention_label(Some(3653)), "3653d");
        assert_eq!(retention_label(None), "never!");
    }
}
