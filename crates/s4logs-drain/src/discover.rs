//! Log-group discovery for multi-group drains (DESIGN.md §11.4).
//!
//! `--log-group` accepts an exact name or a globset glob; `--all` enumerates
//! every group via `DescribeLogGroups`. Glob semantics intentionally match
//! the gateway routing globs (`globset` defaults: `*` matches `/` too, so
//! `/aws/lambda/*` and `/aws/*` both select `/aws/lambda/foo`).

use globset::{Glob, GlobMatcher};
use thiserror::Error;

use crate::cw::{CwError, CwSource, LogGroupInfo};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DiscoverError {
    #[error("bad log-group glob {pattern:?}")]
    Glob {
        pattern: String,
        #[source]
        source: globset::Error,
    },
    #[error(transparent)]
    Cw(#[from] CwError),
}

/// Characters that make `--log-group` a glob instead of an exact name.
/// `\` counts too: globset treats it as an escape, so a pattern containing
/// it must go through the glob engine to mean what the operator wrote.
const GLOB_META: &[char] = &['*', '?', '[', ']', '{', '}', '\\'];

/// How `drain` / `report` select log groups.
#[derive(Debug, Clone)]
pub enum GroupSelector {
    /// Exact log group name (no glob metacharacters).
    Exact(String),
    /// Compiled globset pattern, e.g. `/aws/lambda/payments-*`.
    Glob {
        pattern: String,
        matcher: GlobMatcher,
        /// Literal pattern prefix before the first metacharacter — passed to
        /// `DescribeLogGroups` as a server-side prefix filter so account-wide
        /// listings are avoided when the glob anchors a path.
        prefix_hint: String,
    },
    /// Every log group in the account.
    All,
}

impl GroupSelector {
    /// Exact name unless `pattern` contains a glob metacharacter.
    pub fn parse(pattern: &str) -> Result<Self, DiscoverError> {
        let Some(meta_at) = pattern.find(GLOB_META) else {
            return Ok(Self::Exact(pattern.to_owned()));
        };
        let matcher = Glob::new(pattern)
            .map_err(|source| DiscoverError::Glob {
                pattern: pattern.to_owned(),
                source,
            })?
            .compile_matcher();
        Ok(Self::Glob {
            pattern: pattern.to_owned(),
            matcher,
            prefix_hint: pattern[..meta_at].to_owned(),
        })
    }

    /// Whether `name` is selected. Used both for CW discovery filtering and
    /// for `s4logs report`'s manifest-key filtering (zero CW calls there).
    pub fn matches(&self, name: &str) -> bool {
        match self {
            Self::Exact(n) => n == name,
            Self::Glob { matcher, .. } => matcher.is_match(name),
            Self::All => true,
        }
    }

    /// Human label for logs/reports.
    pub fn describe(&self) -> String {
        match self {
            Self::Exact(n) => n.clone(),
            Self::Glob { pattern, .. } => format!("glob {pattern:?}"),
            Self::All => "--all".to_owned(),
        }
    }
}

/// Resolve a selector against CloudWatch (DESIGN.md §11.4): exact names go
/// through `DescribeLogGroups` once (so a typo surfaces as
/// [`CwError::GroupNotFound`]), globs/`--all` enumerate + filter. Output is
/// name-sorted and deduplicated; a glob matching nothing yields an empty
/// vec — the caller decides whether that is an error.
pub async fn discover_log_groups(
    cw: &dyn CwSource,
    selector: &GroupSelector,
) -> Result<Vec<(String, LogGroupInfo)>, DiscoverError> {
    let mut groups = match selector {
        GroupSelector::Exact(name) => {
            vec![(name.clone(), cw.describe_log_group(name).await?)]
        }
        GroupSelector::All => cw.list_log_groups(None).await?,
        GroupSelector::Glob {
            matcher,
            prefix_hint,
            ..
        } => cw
            .list_log_groups((!prefix_hint.is_empty()).then_some(prefix_hint.as_str()))
            .await?
            .into_iter()
            .filter(|(name, _)| matcher.is_match(name))
            .collect(),
    };
    groups.sort_by(|a, b| a.0.cmp(&b.0));
    groups.dedup_by(|a, b| a.0 == b.0);
    Ok(groups)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::testutil::MockCw;

    fn info(creation_time_ms: i64) -> LogGroupInfo {
        LogGroupInfo {
            retention_days: None,
            stored_bytes: None,
            creation_time_ms,
        }
    }

    fn cw_with_groups(names: &[&str]) -> MockCw {
        let mut cw = MockCw::default();
        for (i, n) in names.iter().enumerate() {
            cw.groups.push(((*n).to_owned(), info(i as i64 + 1)));
        }
        cw
    }

    #[test]
    fn parse_classifies_exact_vs_glob() {
        assert!(matches!(
            GroupSelector::parse("/aws/lambda/foo").unwrap(),
            GroupSelector::Exact(n) if n == "/aws/lambda/foo"
        ));
        match GroupSelector::parse("/aws/lambda/*").unwrap() {
            GroupSelector::Glob { prefix_hint, .. } => assert_eq!(prefix_hint, "/aws/lambda/"),
            other => panic!("expected glob, got {other:?}"),
        }
        // Leading metachar → empty hint (full enumeration).
        match GroupSelector::parse("*payments*").unwrap() {
            GroupSelector::Glob { prefix_hint, .. } => assert_eq!(prefix_hint, ""),
            other => panic!("expected glob, got {other:?}"),
        }
        assert!(matches!(
            GroupSelector::parse("/aws/[lambda").unwrap_err(),
            DiscoverError::Glob { .. }
        ));
    }

    #[test]
    fn selector_matching_semantics() {
        let glob = GroupSelector::parse("/aws/lambda/*").unwrap();
        // globset defaults: `*` crosses `/` (same as gateway routing globs).
        assert!(glob.matches("/aws/lambda/foo"));
        assert!(glob.matches("/aws/lambda/team/foo"));
        assert!(!glob.matches("/aws/ecs/foo"));
        assert!(GroupSelector::All.matches("/anything"));
        let exact = GroupSelector::parse("/g").unwrap();
        assert!(exact.matches("/g"));
        assert!(!exact.matches("/g2"));
    }

    #[tokio::test]
    async fn exact_uses_describe_not_listing() {
        let mut cw = cw_with_groups(&["/aws/lambda/foo"]);
        cw.info = info(42); // describe fallback for unknown names
        let sel = GroupSelector::parse("/aws/lambda/foo").unwrap();
        let groups = discover_log_groups(&cw, &sel).await.unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, "/aws/lambda/foo");
        assert_eq!(
            *cw.list_hints.lock().unwrap(),
            Vec::<Option<String>>::new(),
            "exact selector must not enumerate"
        );
    }

    #[tokio::test]
    async fn glob_filters_listing_and_passes_prefix_hint() {
        let cw = cw_with_groups(&[
            "/aws/lambda/b",
            "/aws/lambda/a",
            "/aws/ecs/c",
            "/custom/app",
        ]);
        let sel = GroupSelector::parse("/aws/lambda/*").unwrap();
        let groups = discover_log_groups(&cw, &sel).await.unwrap();
        let names: Vec<&str> = groups.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["/aws/lambda/a", "/aws/lambda/b"], "name-sorted");
        assert_eq!(
            *cw.list_hints.lock().unwrap(),
            vec![Some("/aws/lambda/".to_owned())]
        );
    }

    #[tokio::test]
    async fn all_enumerates_everything_without_hint() {
        let cw = cw_with_groups(&["/b", "/a"]);
        let groups = discover_log_groups(&cw, &GroupSelector::All).await.unwrap();
        let names: Vec<&str> = groups.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["/a", "/b"]);
        assert_eq!(*cw.list_hints.lock().unwrap(), vec![None]);
    }

    #[tokio::test]
    async fn glob_matching_nothing_is_empty_not_error() {
        let cw = cw_with_groups(&["/aws/ecs/c"]);
        let sel = GroupSelector::parse("/aws/lambda/*").unwrap();
        assert!(discover_log_groups(&cw, &sel).await.unwrap().is_empty());
    }
}
