//! clap surface — DESIGN.md §9. Global flags + drain / grep / restore /
//! serve / version subcommands.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::timearg;

/// Runtime-detected usage problem (missing required global flag, bad regex,
/// inverted time range). `main` maps this to exit code 2; everything else
/// exits 1.
#[derive(Debug)]
pub struct UsageError(pub String);

impl std::fmt::Display for UsageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UsageError {}

#[derive(Debug, Parser)]
#[command(
    name = "s4logs",
    version,
    about = "CloudWatch Logs cost offloader — drain or bypass log groups into zstd-compressed S3",
    propagate_version = true
)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Debug, Args)]
pub struct GlobalArgs {
    /// S3 bucket holding the s4logs layout
    #[arg(long, global = true, env = "S4LOGS_BUCKET")]
    pub bucket: Option<String>,

    /// Key prefix inside the bucket
    #[arg(long, global = true, env = "S4LOGS_PREFIX", default_value = "s4logs")]
    pub prefix: String,

    /// AWS account id used as the account= partition label. P1 keeps this
    /// explicit (no STS lookup): pass --account or set S4LOGS_ACCOUNT
    #[arg(long, global = true, env = "S4LOGS_ACCOUNT")]
    pub account: Option<String>,

    /// AWS region (default: SDK resolution chain — env, profile, IMDS)
    #[arg(long, global = true)]
    pub region: Option<String>,

    /// AWS endpoint override (LocalStack / MinIO); forces S3 path-style
    /// addressing
    #[arg(long, global = true, env = "AWS_ENDPOINT_URL", value_name = "URL")]
    pub endpoint_url: Option<String>,

    /// Log output format (logs always go to stderr; data goes to stdout)
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Pretty)]
    pub log_format: LogFormat,

    /// Log level / tracing filter (e.g. info, debug, s4logs_drain=trace)
    #[arg(long, global = true, default_value = "info")]
    pub log_level: String,
}

impl GlobalArgs {
    pub fn require_bucket(&self) -> anyhow::Result<String> {
        self.bucket.clone().ok_or_else(|| {
            anyhow::Error::new(UsageError(
                "--bucket (or S4LOGS_BUCKET) is required for this command".into(),
            ))
        })
    }

    pub fn require_account(&self) -> anyhow::Result<String> {
        self.account.clone().ok_or_else(|| {
            anyhow::Error::new(UsageError(
                "--account (or S4LOGS_ACCOUNT) is required — P1 does not resolve \
                 the account id via STS"
                    .into(),
            ))
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum GrepOutput {
    /// "RFC3339-timestamp stream message" per record
    Text,
    /// One JSONL record per line (the on-disk schema)
    Jsonl,
}

#[derive(Debug, Subcommand)]
pub enum Cmd {
    /// Read-only account diagnostic: per-log-group CloudWatch cost today and
    /// projected Mode A / Mode B savings. Uses DescribeLogGroups +
    /// GetMetricData only — no writes, no bucket needed, read-only
    /// credentials suffice
    Plan(PlanArgs),
    /// Archive CloudWatch log groups into S3 (Mode A), optionally gating a
    /// retention shortening behind manifest coverage
    Drain(DrainArgs),
    /// Regex-search archived records over a time range (sidecar-pruned
    /// Range GETs — never downloads whole objects unless sidecars are gone).
    /// Output is timestamp-ascending across all objects
    Grep(GrepArgs),
    /// Pull archived records back out: stdout, file, or CloudWatch re-ingest
    Restore(RestoreArgs),
    /// Summarize drain manifests: archived records/bytes, estimated monthly
    /// savings, window coverage. Reads manifests only — zero CloudWatch API
    /// calls, zero S3 data-object reads
    Report(ReportArgs),
    /// Run the PutLogEvents-compatible gateway (Mode B)
    Serve(ServeArgs),
    /// Print the version
    Version,
}

/// `s4logs plan` needs **no bucket and no account id** — it never touches
/// S3. It reads `DescribeLogGroups` (storedBytes = CloudWatch's storage
/// billing basis) and CloudWatch Metrics `GetMetricData`
/// (`AWS/Logs IncomingBytes` per group) and prints cost + savings
/// projections. `GlobalArgs::require_bucket` / `require_account` are never
/// called on this path.
#[derive(Debug, Args)]
#[command(group = clap::ArgGroup::new("groups").required(true).multiple(false))]
pub struct PlanArgs {
    /// Log group to diagnose — exact name or glob (globset syntax, e.g.
    /// "/aws/lambda/*"; same semantics as drain/report)
    #[arg(long, group = "groups")]
    pub log_group: Option<String>,

    /// Diagnose every log group in the account
    #[arg(long, group = "groups")]
    pub all: bool,

    /// Output format
    #[arg(long, value_enum, default_value_t = ReportOutput::Table)]
    pub output: ReportOutput,

    /// Days of IncomingBytes history sampled to project monthly ingest
    /// (CloudWatch retains 1-day datapoints for 455 days)
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u32).range(1..=455))]
    pub ingest_days: u32,

    /// Table rows shown (highest current cost first); totals always cover
    /// every group, and --output json always lists all groups
    #[arg(long, default_value_t = 20)]
    pub top: usize,
}

#[derive(Debug, Args)]
#[command(group = clap::ArgGroup::new("groups").required(true).multiple(false))]
pub struct DrainArgs {
    /// CloudWatch log group to drain — an exact name, or a glob selecting
    /// several groups (globset syntax, e.g. "/aws/lambda/payments-*"; same
    /// semantics as the gateway routing globs)
    #[arg(long, group = "groups")]
    pub log_group: Option<String>,

    /// Drain every log group in the account (DescribeLogGroups enumeration)
    #[arg(long, group = "groups")]
    pub all: bool,

    /// Log groups drained in parallel. Each group still runs --concurrency
    /// windows, so total FilterLogEvents pressure is the product
    #[arg(long, default_value_t = 1)]
    pub group_concurrency: usize,

    /// Range start, inclusive (RFC3339 or epoch ms). Default: log group
    /// creation time
    #[arg(long, value_parser = timearg::parse_time_ms)]
    pub from: Option<i64>,

    /// Range end, exclusive (RFC3339 or epoch ms). Default: now minus a
    /// 15 min ingestion-lag cutoff
    #[arg(long, value_parser = timearg::parse_time_ms)]
    pub to: Option<i64>,

    /// Drain window length, UTC-grid aligned (e.g. 1h, 30m)
    #[arg(long, default_value = "1h", value_parser = timearg::parse_duration_ms)]
    pub window: i64,

    /// Rotate data objects once this much uncompressed JSONL accumulated
    #[arg(long, default_value = "256MiB", value_parser = timearg::parse_size_bytes)]
    pub chunk_target: u64,

    /// Windows processed in parallel (FilterLogEvents quota is account-wide)
    #[arg(long, default_value_t = 2)]
    pub concurrency: usize,

    /// Read CloudWatch, estimate savings, write nothing
    #[arg(long)]
    pub dry_run: bool,

    /// Propose this CloudWatch retention (days); report-only unless
    /// --apply-retention. Fail-closed: refused while any window older than
    /// the cutoff lacks a manifest
    #[arg(long)]
    pub retention_days: Option<i32>,

    /// Actually call PutRetentionPolicy when coverage is proven
    // Plain `requires` is sound here: `retention_days` is not in any
    // ArgGroup (cf. the `RestoreArgs::raw` caveat).
    #[arg(long, requires = "retention_days")]
    pub apply_retention: bool,

    /// S3 storage class for archive data objects (sidecars and manifests
    /// always stay Standard — see README Cost model)
    #[arg(long, value_enum)]
    pub storage_class: Option<StorageClassArg>,

    /// Page each window's streams in N parallel shards (FilterLogEvents
    /// logStreamNames). >1 trades object-content determinism for speed;
    /// manifest idempotency is unaffected
    #[arg(long, default_value_t = 1)]
    pub shard_streams: usize,

    /// Repair mode: re-page manifested windows too, dedup against the
    /// archive by event identity, append only what is missing
    /// (late-arrival recovery — see README Limitations)
    #[arg(long)]
    pub reconcile: bool,

    /// Periodic progress lines on stderr (windows, pages, objects)
    #[arg(long)]
    pub progress: bool,
}

/// CLI face of `aws_sdk_s3::types::StorageClass` (only classes that make
/// sense for log archives; Glacier Deep Archive has minutes-to-hours
/// retrieval, which would break grep's interactive contract).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StorageClassArg {
    Standard,
    StandardIa,
    GlacierIr,
}

impl StorageClassArg {
    pub fn to_sdk(self) -> aws_sdk_s3::types::StorageClass {
        match self {
            StorageClassArg::Standard => aws_sdk_s3::types::StorageClass::Standard,
            StorageClassArg::StandardIa => aws_sdk_s3::types::StorageClass::StandardIa,
            StorageClassArg::GlacierIr => aws_sdk_s3::types::StorageClass::GlacierIr,
        }
    }
}

#[derive(Debug, Args)]
pub struct GrepArgs {
    /// Regex applied to each record's message
    pub pattern: String,

    /// Archived log group to search
    #[arg(long)]
    pub log_group: String,

    /// Range start, inclusive (RFC3339 or epoch ms)
    #[arg(long, value_parser = timearg::parse_time_ms)]
    pub from: i64,

    /// Range end, exclusive (RFC3339 or epoch ms)
    #[arg(long, value_parser = timearg::parse_time_ms)]
    pub to: i64,

    /// Output format
    #[arg(long, value_enum, default_value_t = GrepOutput::Text)]
    pub output: GrepOutput,
}

#[derive(Debug, Args)]
#[command(group = clap::ArgGroup::new("target").required(true).multiple(false))]
pub struct RestoreArgs {
    /// Archived log group to restore from
    #[arg(long)]
    pub log_group: String,

    /// Range start, inclusive (RFC3339 or epoch ms)
    #[arg(long, value_parser = timearg::parse_time_ms)]
    pub from: i64,

    /// Range end, exclusive (RFC3339 or epoch ms)
    #[arg(long, value_parser = timearg::parse_time_ms)]
    pub to: i64,

    /// Write raw JSONL records to stdout
    #[arg(long, group = "target")]
    pub to_stdout: bool,

    /// Write raw JSONL records to a file
    #[arg(long, group = "target", value_name = "PATH")]
    pub to_file: Option<PathBuf>,

    /// Re-ingest into this CloudWatch log group (stream "s4logs-restore").
    /// Default: each message is wrapped as
    /// {"original_timestamp":..,"original_stream":..,"message":..} with
    /// event timestamp = now (PutLogEvents rejects events >14 days old)
    #[arg(long, group = "target", value_name = "GROUP")]
    pub to_log_group: Option<String>,

    /// With --to-log-group: send original timestamps unwrapped. CloudWatch
    /// WILL reject events older than 14 days — rejects are reported, not
    /// retried
    // NOT `requires = "to_log_group"`: when the required arg belongs to an
    // ArgGroup, clap satisfies the requirement through *any* group member
    // (so `--to-stdout --raw` would slip through). Conflicting with the
    // other two targets + the required group yields the intended "only with
    // --to-log-group". `restore::run` double-checks at runtime.
    #[arg(long, conflicts_with_all = ["to_stdout", "to_file"])]
    pub raw: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ReportOutput {
    /// Human-readable per-group summary
    Table,
    /// Machine-readable JSON document
    Json,
}

/// `s4logs report` reads **drain manifests only**: no CloudWatch API calls,
/// no S3 data-object reads — one LIST of the account's manifest prefix plus
/// one GET per manifest JSON.
#[derive(Debug, Args)]
#[command(group = clap::ArgGroup::new("groups").required(true).multiple(false))]
pub struct ReportArgs {
    /// Log group to report on — exact name or glob (globset syntax)
    #[arg(long, group = "groups")]
    pub log_group: Option<String>,

    /// Report on every log group with manifests under the account prefix
    #[arg(long, group = "groups")]
    pub all: bool,

    /// Output format
    #[arg(long, value_enum, default_value_t = ReportOutput::Table)]
    pub output: ReportOutput,
}

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Listen address
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub listen: SocketAddr,

    /// TOML routing config (default: every group/stream → s3)
    #[arg(long, value_name = "FILE")]
    pub routing_config: Option<PathBuf>,

    /// Flush a buffer once it holds this much uncompressed JSONL
    #[arg(long, default_value = "8MiB", value_parser = timearg::parse_size_bytes)]
    pub flush_bytes: u64,

    /// Flush a buffer once its oldest event reaches this age
    #[arg(long, default_value = "60s", value_parser = timearg::parse_duration)]
    pub flush_interval: Duration,

    /// Don't build a CloudWatch client; cloudwatch/both routes become no-ops
    #[arg(long)]
    pub no_cloudwatch: bool,

    /// Write-ahead log directory: events are fsynced here before the
    /// PutLogEvents ack and replayed on restart (DESIGN.md §11.1).
    /// Without it a crash loses buffered events below the flush thresholds.
    #[arg(long, value_name = "DIR")]
    pub wal_dir: Option<PathBuf>,

    /// Require SigV4 on incoming requests, verified against
    /// --auth-access-key / --auth-secret (DESIGN.md §11.2)
    #[arg(long, value_enum, default_value_t = AuthModeArg::None)]
    pub auth_mode: AuthModeArg,

    /// Access key id clients must sign with (sigv4 mode)
    #[arg(long, env = "S4LOGS_AUTH_ACCESS_KEY")]
    pub auth_access_key: Option<String>,

    /// Secret access key for signature verification (sigv4 mode).
    /// Prefer the env var over the flag (process lists leak flag values).
    #[arg(long, env = "S4LOGS_AUTH_SECRET", hide_env_values = true)]
    pub auth_secret: Option<String>,

    /// Total uncompressed buffer cap before backpressure (503) kicks in
    #[arg(long, default_value = "256MiB", value_parser = timearg::parse_size_bytes)]
    pub max_buffered_bytes: u64,

    /// S3 storage class for archive data objects (sidecars stay Standard)
    #[arg(long, value_enum)]
    pub storage_class: Option<StorageClassArg>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AuthModeArg {
    None,
    Sigv4,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use clap::Parser;

    #[test]
    fn command_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn restore_requires_exactly_one_target() {
        let base = [
            "s4logs",
            "restore",
            "--log-group",
            "/g",
            "--from",
            "0",
            "--to",
            "1",
        ];
        assert!(Cli::try_parse_from(base).is_err(), "a target is required");
        let two: Vec<&str> = base
            .iter()
            .copied()
            .chain(["--to-stdout", "--to-file", "f"])
            .collect();
        assert!(Cli::try_parse_from(two).is_err(), "targets are exclusive");
        let one: Vec<&str> = base.iter().copied().chain(["--to-stdout"]).collect();
        assert!(Cli::try_parse_from(one).is_ok());
    }

    #[test]
    fn restore_raw_needs_to_log_group() {
        let bad = [
            "s4logs",
            "restore",
            "--log-group",
            "/g",
            "--from",
            "0",
            "--to",
            "1",
            "--to-stdout",
            "--raw",
        ];
        assert!(
            Cli::try_parse_from(bad).is_err(),
            "--raw without --to-log-group"
        );
        let good = [
            "s4logs",
            "restore",
            "--log-group",
            "/g",
            "--from",
            "0",
            "--to",
            "1",
            "--to-log-group",
            "/restored",
            "--raw",
        ];
        assert!(Cli::try_parse_from(good).is_ok());
    }

    #[test]
    fn drain_apply_retention_needs_retention_days() {
        let bad = ["s4logs", "drain", "--log-group", "/g", "--apply-retention"];
        assert!(Cli::try_parse_from(bad).is_err());
        let good = [
            "s4logs",
            "drain",
            "--log-group",
            "/g",
            "--retention-days",
            "7",
            "--apply-retention",
        ];
        assert!(Cli::try_parse_from(good).is_ok());
    }

    #[test]
    fn drain_requires_exactly_one_of_log_group_or_all() {
        assert!(
            Cli::try_parse_from(["s4logs", "drain"]).is_err(),
            "a group selector is required"
        );
        assert!(
            Cli::try_parse_from(["s4logs", "drain", "--log-group", "/g", "--all"]).is_err(),
            "--log-group and --all are exclusive"
        );
        let glob = Cli::try_parse_from([
            "s4logs",
            "drain",
            "--log-group",
            "/aws/lambda/*",
            "--group-concurrency",
            "3",
        ])
        .unwrap();
        match glob.cmd {
            Cmd::Drain(d) => {
                assert_eq!(d.log_group.as_deref(), Some("/aws/lambda/*"));
                assert!(!d.all);
                assert_eq!(d.group_concurrency, 3);
            }
            other => panic!("expected drain, got {other:?}"),
        }
        let all = Cli::try_parse_from(["s4logs", "drain", "--all"]).unwrap();
        match all.cmd {
            Cmd::Drain(d) => {
                assert!(d.all);
                assert_eq!(d.group_concurrency, 1, "sequential by default");
            }
            other => panic!("expected drain, got {other:?}"),
        }
    }

    #[test]
    fn plan_requires_exactly_one_of_log_group_or_all() {
        assert!(Cli::try_parse_from(["s4logs", "plan"]).is_err());
        assert!(Cli::try_parse_from(["s4logs", "plan", "--log-group", "/g", "--all"]).is_err());
        let cli = Cli::try_parse_from(["s4logs", "plan", "--all"]).unwrap();
        match cli.cmd {
            Cmd::Plan(p) => {
                assert!(p.all);
                assert_eq!(p.ingest_days, 30, "30-day ingest sample by default");
                assert_eq!(p.top, 20, "top 20 by default");
                assert_eq!(p.output, ReportOutput::Table);
            }
            other => panic!("expected plan, got {other:?}"),
        }
    }

    #[test]
    fn plan_needs_no_bucket_and_validates_ingest_days() {
        // The GTM promise: read-only credentials, no --bucket/--account.
        let cli =
            Cli::try_parse_from(["s4logs", "plan", "--all", "--region", "us-east-1"]).unwrap();
        assert!(cli.global.bucket.is_none());
        assert!(cli.global.account.is_none());

        let cli = Cli::try_parse_from([
            "s4logs",
            "plan",
            "--log-group",
            "/aws/lambda/*",
            "--ingest-days",
            "7",
            "--top",
            "5",
            "--output",
            "json",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Plan(p) => {
                assert_eq!(p.log_group.as_deref(), Some("/aws/lambda/*"));
                assert_eq!(p.ingest_days, 7);
                assert_eq!(p.top, 5);
                assert_eq!(p.output, ReportOutput::Json);
            }
            other => panic!("expected plan, got {other:?}"),
        }
        // 1..=455: CloudWatch keeps 1-day datapoints for 455 days.
        assert!(Cli::try_parse_from(["s4logs", "plan", "--all", "--ingest-days", "0"]).is_err());
        assert!(Cli::try_parse_from(["s4logs", "plan", "--all", "--ingest-days", "456"]).is_err());
    }

    #[test]
    fn report_requires_exactly_one_of_log_group_or_all() {
        assert!(Cli::try_parse_from(["s4logs", "report"]).is_err());
        assert!(Cli::try_parse_from(["s4logs", "report", "--log-group", "/g", "--all"]).is_err());
        let cli = Cli::try_parse_from(["s4logs", "report", "--all", "--output", "json"]).unwrap();
        match cli.cmd {
            Cmd::Report(r) => {
                assert!(r.all);
                assert_eq!(r.output, ReportOutput::Json);
            }
            other => panic!("expected report, got {other:?}"),
        }
        let cli = Cli::try_parse_from(["s4logs", "report", "--log-group", "/g/*"]).unwrap();
        match cli.cmd {
            Cmd::Report(r) => {
                assert_eq!(r.log_group.as_deref(), Some("/g/*"));
                assert_eq!(r.output, ReportOutput::Table, "table by default");
            }
            other => panic!("expected report, got {other:?}"),
        }
    }

    #[test]
    fn global_flags_parse_after_subcommand() {
        let cli = Cli::try_parse_from([
            "s4logs",
            "grep",
            "ERR",
            "--log-group",
            "/g",
            "--from",
            "2024-06-05T00:00:00Z",
            "--to",
            "1717549200000",
            "--bucket",
            "b",
            "--account",
            "123456789012",
            "--endpoint-url",
            "http://localhost:4566",
        ])
        .unwrap();
        assert_eq!(cli.global.bucket.as_deref(), Some("b"));
        assert_eq!(cli.global.prefix, "s4logs");
        match cli.cmd {
            Cmd::Grep(g) => {
                assert_eq!(g.from, 1_717_545_600_000);
                assert_eq!(g.to, 1_717_549_200_000);
                assert_eq!(g.pattern, "ERR");
            }
            other => panic!("expected grep, got {other:?}"),
        }
    }
}
