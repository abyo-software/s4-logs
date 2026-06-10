//! Routing engine — TOML config, first-match-wins (DESIGN.md §8.2).
//!
//! ```toml
//! default_action = "s3"        # s3 | cloudwatch | both | drop
//!
//! [[rule]]
//! log_group = "/aws/lambda/payments-*"   # globset glob
//! stream = "*"                            # optional, default "*"
//! action = "cloudwatch"
//! ```
//!
//! Globs use `globset` defaults (`*` matches `/` too, so `*` alone matches
//! any log group, including `/aws/lambda/foo`).

use globset::{Glob, GlobMatcher};
use serde::Deserialize;
use thiserror::Error;

/// What to do with a (log_group, stream) batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RouteAction {
    /// Buffer into zstd chunks and write to the chunk sink (S3).
    S3,
    /// Forward the original batch to CloudWatch Logs only.
    Cloudwatch,
    /// Buffer to S3 **and** forward to CloudWatch.
    Both,
    /// Discard.
    Drop,
}

impl RouteAction {
    pub fn to_s3(self) -> bool {
        matches!(self, Self::S3 | Self::Both)
    }

    pub fn to_cloudwatch(self) -> bool {
        matches!(self, Self::Cloudwatch | Self::Both)
    }

    /// Stable label value for `s4logs_events_total{action=}`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Cloudwatch => "cloudwatch",
            Self::Both => "both",
            Self::Drop => "drop",
        }
    }
}

#[derive(Debug, Error)]
pub enum RoutingError {
    #[error("routing config parse failed: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("bad glob {glob:?} in rule {index}: {source}")]
    Glob {
        index: usize,
        glob: String,
        #[source]
        source: globset::Error,
    },
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(default)]
    default_action: Option<RouteAction>,
    #[serde(default, rename = "rule")]
    rules: Vec<RawRule>,
}

#[derive(Debug, Deserialize)]
struct RawRule {
    log_group: String,
    #[serde(default)]
    stream: Option<String>,
    action: RouteAction,
}

/// One compiled rule.
#[derive(Debug, Clone)]
pub struct Rule {
    group: GlobMatcher,
    stream: GlobMatcher,
    action: RouteAction,
}

/// Compiled routing table. First matching `[[rule]]` wins; otherwise
/// `default_action` (default `s3`).
#[derive(Debug, Clone)]
pub struct RoutingConfig {
    default_action: RouteAction,
    rules: Vec<Rule>,
}

impl Default for RoutingConfig {
    /// Everything to S3 — the zero-config Mode B posture.
    fn default() -> Self {
        Self {
            default_action: RouteAction::S3,
            rules: Vec::new(),
        }
    }
}

impl RoutingConfig {
    pub fn from_toml_str(s: &str) -> Result<Self, RoutingError> {
        let raw: RawConfig = toml::from_str(s)?;
        let compile = |index: usize, glob: &str| -> Result<GlobMatcher, RoutingError> {
            Ok(Glob::new(glob)
                .map_err(|source| RoutingError::Glob {
                    index,
                    glob: glob.to_owned(),
                    source,
                })?
                .compile_matcher())
        };
        let mut rules = Vec::with_capacity(raw.rules.len());
        for (i, r) in raw.rules.iter().enumerate() {
            rules.push(Rule {
                group: compile(i, &r.log_group)?,
                stream: compile(i, r.stream.as_deref().unwrap_or("*"))?,
                action: r.action,
            });
        }
        Ok(Self {
            default_action: raw.default_action.unwrap_or(RouteAction::S3),
            rules,
        })
    }

    pub fn default_action(&self) -> RouteAction {
        self.default_action
    }

    /// First-match-wins lookup.
    pub fn route(&self, log_group: &str, stream: &str) -> RouteAction {
        self.rules
            .iter()
            .find(|r| r.group.is_match(log_group) && r.stream.is_match(stream))
            .map_or(self.default_action, |r| r.action)
    }

    /// Conservative over-approximation: could *any* stream of `log_group`
    /// route to CloudWatch? Used to decide whether `CreateLogGroup` is
    /// forwarded. Over-forwarding is harmless (CW creates are idempotent
    /// for our purposes — `ResourceAlreadyExistsException` is swallowed),
    /// under-forwarding would break passthrough streams.
    pub fn group_may_reach_cloudwatch(&self, log_group: &str) -> bool {
        self.default_action.to_cloudwatch()
            || self
                .rules
                .iter()
                .any(|r| r.group.is_match(log_group) && r.action.to_cloudwatch())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_defaults_to_s3() {
        let cfg = RoutingConfig::from_toml_str("").unwrap();
        assert_eq!(cfg.route("/aws/lambda/foo", "any"), RouteAction::S3);
        assert!(!cfg.group_may_reach_cloudwatch("/g"));
    }

    #[test]
    fn first_match_wins_precedence() {
        let cfg = RoutingConfig::from_toml_str(
            r#"
default_action = "drop"

[[rule]]
log_group = "/aws/lambda/payments-*"
stream = "audit-*"
action = "both"

[[rule]]
log_group = "/aws/lambda/payments-*"
action = "cloudwatch"

[[rule]]
log_group = "/aws/lambda/*"
action = "s3"
"#,
        )
        .unwrap();
        // Most-specific rule listed first wins over the broader second rule.
        assert_eq!(
            cfg.route("/aws/lambda/payments-prod", "audit-1"),
            RouteAction::Both
        );
        // Second rule catches remaining payments streams.
        assert_eq!(
            cfg.route("/aws/lambda/payments-prod", "app/i-0abc"),
            RouteAction::Cloudwatch
        );
        // Third rule catches other lambda groups (glob `*` crosses `/`… no:
        // here the group itself matches the third glob).
        assert_eq!(cfg.route("/aws/lambda/other", "s"), RouteAction::S3);
        // No rule matches → default.
        assert_eq!(cfg.route("/ecs/api", "s"), RouteAction::Drop);
    }

    #[test]
    fn star_glob_matches_slashes() {
        let cfg = RoutingConfig::from_toml_str(
            r#"
[[rule]]
log_group = "*"
action = "drop"
"#,
        )
        .unwrap();
        assert_eq!(cfg.route("/aws/lambda/foo", "s"), RouteAction::Drop);
    }

    #[test]
    fn stream_defaults_to_star() {
        let cfg = RoutingConfig::from_toml_str(
            r#"
[[rule]]
log_group = "/g"
action = "cloudwatch"
"#,
        )
        .unwrap();
        assert_eq!(cfg.route("/g", "anything/at/all"), RouteAction::Cloudwatch);
    }

    #[test]
    fn group_may_reach_cloudwatch_over_approximates() {
        let cfg = RoutingConfig::from_toml_str(
            r#"
default_action = "s3"

[[rule]]
log_group = "/g"
stream = "audit-*"
action = "cloudwatch"
"#,
        )
        .unwrap();
        assert!(cfg.group_may_reach_cloudwatch("/g"));
        assert!(!cfg.group_may_reach_cloudwatch("/other"));
    }

    #[test]
    fn bad_action_and_bad_glob_are_errors() {
        assert!(matches!(
            RoutingConfig::from_toml_str("default_action = \"tape\""),
            Err(RoutingError::Toml(_))
        ));
        let err = RoutingConfig::from_toml_str("[[rule]]\nlog_group = \"a[\"\naction = \"s3\"\n")
            .unwrap_err();
        assert!(matches!(err, RoutingError::Glob { index: 0, .. }));
    }
}
