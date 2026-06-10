//! `s4logs grep` — DESIGN.md §9. Sidecar-pruned regex search; matches go to
//! stdout, logs/warnings to stderr.

use std::io::Write;

use anyhow::{Context, Result};
use regex::Regex;
use s4logs_core::read::TimeRange;
use s4logs_core::store::ObjectStore;

use crate::aws;
use crate::cli::{GlobalArgs, GrepArgs, GrepOutput, UsageError};
use crate::scan::scan_log_group;
use crate::timearg::fmt_ts;

pub async fn run(global: &GlobalArgs, args: &GrepArgs) -> Result<()> {
    if args.from >= args.to {
        return Err(UsageError(format!(
            "--from ({}) must be before --to ({})",
            fmt_ts(args.from),
            fmt_ts(args.to)
        ))
        .into());
    }
    let re = Regex::new(&args.pattern)
        .map_err(|e| UsageError(format!("bad regex {:?}: {e}", args.pattern)))?;
    let bucket = global.require_bucket()?;
    let account = global.require_account()?;

    let clients = aws::load(global).await;
    let store = ObjectStore::new(clients.s3(), bucket, &global.prefix);
    let range = TimeRange {
        from_ms: args.from,
        to_ms_exclusive: args.to,
    };

    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    let output = args.output;
    let stats = scan_log_group(&store, &account, &args.log_group, range, Some(&re), |rec| {
        match output {
            GrepOutput::Text => {
                writeln!(
                    out,
                    "{} {} {}",
                    fmt_ts(rec.timestamp),
                    rec.stream,
                    rec.message
                )?;
            }
            GrepOutput::Jsonl => {
                serde_json::to_writer(&mut out, rec)?;
                out.write_all(b"\n")?;
            }
        }
        Ok(())
    })
    .await?;
    out.flush().context("flushing stdout")?;

    tracing::info!(
        matched = stats.records_emitted,
        chunks_listed = stats.chunks_listed,
        chunks_scanned = stats.chunks_scanned,
        frames_fetched = stats.frames_fetched,
        fallback_full_objects = stats.fallback_full_objects,
        parse_errors = stats.parse_errors,
        "grep complete"
    );
    Ok(())
}
