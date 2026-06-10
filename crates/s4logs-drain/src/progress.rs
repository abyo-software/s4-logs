//! Progress reporting hooks for drain / reconcile (wave 4K).
//!
//! The CLI renders live progress from these events; the library stays
//! render-agnostic. [`Progress`] wraps an optional callback so the hot path
//! pays **zero cost when no observer is installed**: [`Progress::emit`]
//! takes a closure and only constructs the event when a callback exists.
//!
//! Ordering guarantees: within one window the sequence is
//! `WindowStarted → Page* → ObjectWritten* → WindowDone` (drain) or
//! `WindowStarted → Page* → ObjectWritten* → ReconcileWindowDone`
//! (reconcile). With `concurrency > 1` events from *different* windows
//! interleave arbitrarily; renderers must key on `window`.

use std::fmt;
use std::sync::Arc;

use crate::window::Window;

/// One observable step of a drain or reconcile run.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProgressEvent {
    /// A window is about to be paged through CloudWatch.
    WindowStarted { window: Window },
    /// A window was skipped because its manifest already exists.
    WindowSkipped { window: Window },
    /// One `FilterLogEvents` page was consumed. `events` is the page's
    /// event count; `bytes_so_far` is the uncompressed JSONL accepted for
    /// the window so far (drain) or appended so far (reconcile).
    Page {
        window: Window,
        events: u64,
        bytes_so_far: u64,
    },
    /// One data object (plus sidecars) was PUT.
    ObjectWritten {
        key: String,
        raw_bytes: u64,
        compressed_bytes: u64,
    },
    /// A drained window completed (manifest written unless dry-run).
    WindowDone { window: Window, records: u64 },
    /// A reconciled window completed (a window that already had a
    /// manifest and was re-paged for late arrivals).
    ReconcileWindowDone {
        window: Window,
        /// In-window events CloudWatch returned during the re-page.
        cw_events: u64,
        /// Events whose identity was already present in the archive.
        already_archived: u64,
        /// Late events appended (or, in dry-run, that would be appended).
        appended: u64,
    },
}

/// Optional progress callback. `Default`/[`Progress::none`] is a no-op;
/// cloning shares the same callback (it is an `Arc` internally), so one
/// renderer observes every window even at `concurrency > 1`.
#[derive(Clone, Default)]
pub struct Progress(Option<Arc<dyn Fn(ProgressEvent) + Send + Sync>>);

impl Progress {
    /// No observer — every emit is a branch on `None` and nothing else.
    pub fn none() -> Self {
        Self(None)
    }

    /// Install `f` as the observer. `f` is called inline from the drain
    /// task: keep it cheap (push to a channel / update counters) or events
    /// will backpressure paging.
    pub fn callback(f: impl Fn(ProgressEvent) + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(f)))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// Emit lazily: `make` runs only when an observer is installed.
    pub(crate) fn emit(&self, make: impl FnOnce() -> ProgressEvent) {
        if let Some(f) = &self.0 {
            f(make());
        }
    }
}

impl fmt::Debug for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() {
            "Progress(callback)"
        } else {
            "Progress(none)"
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn none_never_constructs_the_event() {
        let p = Progress::none();
        assert!(!p.is_enabled());
        p.emit(|| unreachable!("event must not be constructed without an observer"));
    }

    #[test]
    fn callback_receives_events_and_clones_share_it() {
        let seen: Arc<Mutex<Vec<ProgressEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let p = Progress::callback(move |ev| sink.lock().unwrap().push(ev));
        assert!(p.is_enabled());
        let w = Window {
            start_ms: 0,
            end_ms: 1,
        };
        p.emit(|| ProgressEvent::WindowStarted { window: w });
        let p2 = p.clone();
        p2.emit(|| ProgressEvent::WindowDone {
            window: w,
            records: 3,
        });
        let got = seen.lock().unwrap();
        assert_eq!(
            *got,
            vec![
                ProgressEvent::WindowStarted { window: w },
                ProgressEvent::WindowDone {
                    window: w,
                    records: 3
                },
            ]
        );
    }

    #[test]
    fn debug_does_not_try_to_print_the_closure() {
        assert_eq!(format!("{:?}", Progress::none()), "Progress(none)");
        assert_eq!(
            format!("{:?}", Progress::callback(|_| {})),
            "Progress(callback)"
        );
    }
}
