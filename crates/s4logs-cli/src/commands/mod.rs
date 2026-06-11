//! Subcommand implementations. `main.rs` stays thin; each module owns one
//! verb and keeps AWS-touching code minimal around pure, unit-tested cores.

pub mod drain;
pub mod grep;
pub mod plan;
pub mod report;
pub mod restore;
pub mod serve;

use anyhow::{Context, Result};
use s4logs_drain::{AwsCwSource, GroupSelector, discover_log_groups};

use crate::aws::AwsClients;
use crate::cli::UsageError;

/// Resolve a grep/restore `--log-group` glob / `--all` to the concrete set of
/// source log groups, name-sorted.
///
/// FAST PATH (read-only-S3 property): an **exact** name needs no enumeration,
/// so it returns immediately *without* building a CloudWatch client or making
/// any CW call — grep/restore over a single named group stay pure-S3 reads,
/// exactly as before wave 5L. Only a glob or `--all` builds a `CwSource` and
/// calls `DescribeLogGroups` to expand the selector.
pub async fn resolve_source_groups(
    clients: &AwsClients,
    log_group: Option<&str>,
    all: bool,
) -> Result<Vec<String>> {
    let selector = if all {
        GroupSelector::All
    } else {
        let Some(pattern) = log_group else {
            // clap's ArgGroup makes this unreachable; typed error just in case.
            return Err(UsageError("one of --log-group / --all is required".into()).into());
        };
        GroupSelector::parse(pattern).map_err(|e| UsageError(format!("{e:#}")))?
    };

    // Fast path: an exact name skips discovery entirely (no CW client built).
    if let GroupSelector::Exact(name) = &selector {
        return Ok(vec![name.clone()]);
    }

    let cw = AwsCwSource::new(clients.cwl());
    let groups = discover_log_groups(&cw, &selector)
        .await
        .with_context(|| format!("discovering log groups for {}", selector.describe()))?;
    if groups.is_empty() {
        return Err(UsageError(format!("no log groups match {}", selector.describe())).into());
    }
    Ok(groups.into_iter().map(|(name, _)| name).collect())
}
