//! In-memory log group / stream registry backing `CreateLogGroup`,
//! `CreateLogStream` and `Describe*` (DESIGN.md §8.1).
//!
//! Purpose is agent compatibility only — CW agent / Fluent Bit create their
//! group/stream on startup and expect a sane answer. State is process-local
//! and lost on restart, which is fine: creates are accepted again and
//! `PutLogEvents` never requires prior registration (lenient by design).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("log group already exists")]
    GroupExists,
    #[error("log stream already exists")]
    StreamExists,
    #[error("log group does not exist")]
    GroupNotFound,
}

#[derive(Debug)]
struct GroupEntry {
    creation_time_ms: i64,
    streams: BTreeMap<String, i64>,
}

/// Process-local registry. BTreeMap so `Describe*` output order is stable
/// (lexicographic, matching CW's default ordering by name).
#[derive(Debug, Default)]
pub struct Registry {
    groups: Mutex<BTreeMap<String, GroupEntry>>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl Registry {
    pub fn create_group(&self, name: &str) -> Result<(), RegistryError> {
        let mut groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        if groups.contains_key(name) {
            return Err(RegistryError::GroupExists);
        }
        groups.insert(
            name.to_owned(),
            GroupEntry {
                creation_time_ms: now_ms(),
                streams: BTreeMap::new(),
            },
        );
        Ok(())
    }

    pub fn create_stream(&self, group: &str, stream: &str) -> Result<(), RegistryError> {
        let mut groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        let entry = groups.get_mut(group).ok_or(RegistryError::GroupNotFound)?;
        if entry.streams.contains_key(stream) {
            return Err(RegistryError::StreamExists);
        }
        entry.streams.insert(stream.to_owned(), now_ms());
        Ok(())
    }

    /// Implicit registration on `PutLogEvents` — never errors, so the
    /// registry reflects traffic even from agents that skip the creates.
    pub fn touch(&self, group: &str, stream: &str) {
        let mut groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        let entry = groups
            .entry(group.to_owned())
            .or_insert_with(|| GroupEntry {
                creation_time_ms: now_ms(),
                streams: BTreeMap::new(),
            });
        entry
            .streams
            .entry(stream.to_owned())
            .or_insert_with(now_ms);
    }

    /// `(name, creation_time_ms)` of groups matching the optional prefix.
    pub fn describe_groups(&self, prefix: Option<&str>) -> Vec<(String, i64)> {
        let groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        groups
            .iter()
            .filter(|(name, _)| prefix.is_none_or(|p| name.starts_with(p)))
            .map(|(name, e)| (name.clone(), e.creation_time_ms))
            .collect()
    }

    /// `(name, creation_time_ms)` of streams of `group` matching the prefix.
    pub fn describe_streams(
        &self,
        group: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, i64)>, RegistryError> {
        let groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        let entry = groups.get(group).ok_or(RegistryError::GroupNotFound)?;
        Ok(entry
            .streams
            .iter()
            .filter(|(name, _)| prefix.is_none_or(|p| name.starts_with(p)))
            .map(|(name, ts)| (name.clone(), *ts))
            .collect())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn create_group_rejects_duplicate() {
        let r = Registry::default();
        r.create_group("/g").unwrap();
        assert_eq!(r.create_group("/g"), Err(RegistryError::GroupExists));
    }

    #[test]
    fn create_stream_requires_group_and_rejects_duplicate() {
        let r = Registry::default();
        assert_eq!(
            r.create_stream("/g", "s"),
            Err(RegistryError::GroupNotFound)
        );
        r.create_group("/g").unwrap();
        r.create_stream("/g", "s").unwrap();
        assert_eq!(r.create_stream("/g", "s"), Err(RegistryError::StreamExists));
    }

    #[test]
    fn touch_is_idempotent_and_describe_filters_by_prefix() {
        let r = Registry::default();
        r.touch("/aws/lambda/a", "s1");
        r.touch("/aws/lambda/a", "s1");
        r.touch("/aws/lambda/b", "s2");
        r.touch("/ecs/api", "s3");
        let groups = r.describe_groups(Some("/aws/"));
        assert_eq!(
            groups.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            vec!["/aws/lambda/a", "/aws/lambda/b"]
        );
        let streams = r.describe_streams("/aws/lambda/a", None).unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].0, "s1");
        assert!(r.describe_streams("/missing", None).is_err());
    }
}
