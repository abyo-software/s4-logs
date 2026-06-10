//! Write-ahead log for buffered events (DESIGN.md §11.1, opt-in via
//! `GatewayConfig::wal_dir`).
//!
//! One append-only segment file per buffer key `(log_group, dt)`: each line
//! is one accepted event as JSONL
//! (`{"log_group":...,"timestamp":...,"stream":...,"message":...}`), written
//! **before** the `PutLogEvents` response is sent for actions that buffer
//! (`s3` / `both`). On a successful chunk flush the segment is deleted; on
//! startup, surviving segments are replayed back into the buffers before the
//! gateway starts processing requests.
//!
//! # Durability semantics (honest contract)
//!
//! - **Group commit**: events of one `PutLogEvents` batch are appended line
//!   by line and fsynced **once per request batch** (`WalSegment::sync`),
//!   not per event. A power loss can therefore lose at most the batches
//!   whose responses were not yet sent — never an acknowledged batch
//!   (the 200 is only sent after the fsync returned).
//! - **At-least-once, not exactly-once**: the segment is deleted only
//!   *after* `put_chunk` returned `Ok`. A crash between the S3 PUT and the
//!   delete replays the whole segment → duplicate events in a second chunk.
//!   Same window exists during replay itself (entries are re-appended to
//!   fresh segments and fsynced before the old file is removed).
//! - **Torn tails**: a torn/corrupt line (typically the last line of a
//!   segment after a crash mid-write) is skipped with a warning and counted
//!   in `s4logs_wal_torn_lines_total`; preceding intact lines still replay.
//! - **The directory itself is not fsynced** after create/delete: on some
//!   filesystems a freshly created segment may be lost on power failure even
//!   though its data blocks were synced. Acceptable for the product's
//!   "survive process crash / OOM kill" goal; full power-loss hardening
//!   would need `fsync(dir)` per rotation.
//!
//! Metrics: `s4logs_wal_appends_total`, `s4logs_wal_replayed_events_total`,
//! `s4logs_wal_torn_lines_total`, `s4logs_wal_fsync_errors_total`.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use metrics::counter;
use s4logs_core::layout::sanitize_log_group;
use serde::{Deserialize, Serialize};

/// Segment file extension.
pub const WAL_SUFFIX: &str = ".wal";

/// One WAL line — the event plus the buffer-key context needed for replay
/// (DESIGN.md §11.1 line shape).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalEntry {
    pub log_group: String,
    /// Event time, epoch milliseconds.
    pub timestamp: i64,
    pub stream: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingestion_time: Option<i64>,
}

/// Append-only segment owned by one in-memory buffer. Dropped without
/// [`WalSegment::delete`] (e.g. flush failure), the file survives on disk
/// and is replayed on the next startup.
#[derive(Debug)]
pub struct WalSegment {
    path: PathBuf,
    file: File,
    /// Appends since the last fsync — `sync` is a no-op when clean.
    dirty: bool,
}

impl WalSegment {
    /// Create a fresh segment for buffer key `(log_group, date)`. The file
    /// name encodes the key via [`sanitize_log_group`] (filesystem-safe) plus
    /// a random suffix so a re-opened buffer for the same key never collides
    /// with a segment still being flushed.
    pub fn create(dir: &Path, log_group: &str, date: &str) -> std::io::Result<Self> {
        fs::create_dir_all(dir)?;
        let name = format!(
            "{}.{}.{}{}",
            sanitize_log_group(log_group),
            date,
            crate::buffer::uuid8(),
            WAL_SUFFIX
        );
        let path = dir.join(name);
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            path,
            file,
            dirty: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one event line (buffered by the OS; durable only after
    /// [`WalSegment::sync`]).
    pub fn append(&mut self, entry: &WalEntry) -> std::io::Result<()> {
        let mut line = serde_json::to_vec(entry).map_err(std::io::Error::other)?;
        line.push(b'\n');
        self.file.write_all(&line)?;
        self.dirty = true;
        counter!("s4logs_wal_appends_total").increment(1);
        Ok(())
    }

    /// Group-commit fsync — called once per accepted request batch, before
    /// the `PutLogEvents` response is sent (durability trade-off in the
    /// module docs). No-op if nothing was appended since the last sync.
    pub fn sync(&mut self) -> std::io::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        if let Err(err) = self.file.sync_data() {
            counter!("s4logs_wal_fsync_errors_total").increment(1);
            return Err(err);
        }
        self.dirty = false;
        Ok(())
    }

    /// Remove the segment after its chunk was durably stored. A failed
    /// delete is logged but not fatal: the orphan replays as duplicates on
    /// the next startup (at-least-once, module docs).
    pub fn delete(self) {
        if let Err(err) = fs::remove_file(&self.path) {
            tracing::warn!(path = %self.path.display(), error = %err, "wal segment delete failed; will replay as duplicates");
        }
    }
}

/// One surviving segment file and its intact entries.
#[derive(Debug)]
pub struct SegmentEntries {
    pub path: PathBuf,
    pub entries: Vec<WalEntry>,
}

/// Scan `dir` for `*.wal` segments and parse every intact line. Torn /
/// corrupt lines are skipped with a warning + `s4logs_wal_torn_lines_total`.
/// A missing directory is an empty WAL, not an error.
pub fn scan_dir(dir: &Path) -> std::io::Result<Vec<SegmentEntries>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files: Vec<PathBuf> = fs::read_dir(dir)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(WAL_SUFFIX))
        })
        .collect();
    files.sort(); // deterministic replay order
    let mut out = Vec::with_capacity(files.len());
    for path in files {
        let bytes = fs::read(&path)?;
        let mut entries = Vec::new();
        for line in bytes.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<WalEntry>(line) {
                Ok(entry) => entries.push(entry),
                Err(err) => {
                    counter!("s4logs_wal_torn_lines_total").increment(1);
                    tracing::warn!(
                        path = %path.display(),
                        error = %err,
                        "skipping torn/corrupt wal line"
                    );
                }
            }
        }
        out.push(SegmentEntries { path, entries });
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn entry(ts: i64, msg: &str) -> WalEntry {
        WalEntry {
            log_group: "/g".to_owned(),
            timestamp: ts,
            stream: "s".to_owned(),
            message: msg.to_owned(),
            ingestion_time: None,
        }
    }

    #[test]
    fn append_sync_scan_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), "/aws/lambda/foo", "2026-06-10").unwrap();
        seg.append(&entry(1, "a")).unwrap();
        seg.append(&entry(2, "b")).unwrap();
        seg.sync().unwrap();
        let name = seg.path().file_name().unwrap().to_str().unwrap().to_owned();
        assert!(name.starts_with("%2Faws%2Flambda%2Ffoo.2026-06-10."));
        assert!(name.ends_with(WAL_SUFFIX));

        let files = scan_dir(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].entries, vec![entry(1, "a"), entry(2, "b")]);
    }

    #[test]
    fn delete_removes_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), "/g", "2026-06-10").unwrap();
        seg.append(&entry(1, "a")).unwrap();
        seg.sync().unwrap();
        let path = seg.path().to_owned();
        assert!(path.exists());
        seg.delete();
        assert!(!path.exists());
        assert!(scan_dir(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn torn_tail_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), "/g", "2026-06-10").unwrap();
        seg.append(&entry(1, "intact")).unwrap();
        seg.sync().unwrap();
        // Simulate a torn write: a partial JSON line without newline.
        {
            use std::io::Write as _;
            let mut f = OpenOptions::new().append(true).open(seg.path()).unwrap();
            f.write_all(b"{\"log_group\":\"/g\",\"timesta").unwrap();
        }
        let files = scan_dir(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].entries, vec![entry(1, "intact")]);
    }

    #[test]
    fn scan_missing_dir_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope");
        assert!(scan_dir(&missing).unwrap().is_empty());
    }

    #[test]
    fn same_key_segments_do_not_collide() {
        let dir = tempfile::tempdir().unwrap();
        let a = WalSegment::create(dir.path(), "/g", "2026-06-10").unwrap();
        let b = WalSegment::create(dir.path(), "/g", "2026-06-10").unwrap();
        assert_ne!(a.path(), b.path());
    }
}
