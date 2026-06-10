//! Drain window iteration (DESIGN.md §7).
//!
//! A window is a half-open UTC time range `[start_ms, end_ms)`. Windows are
//! aligned to a fixed grid anchored at each UTC day start: boundaries are
//! `day_start + k * window_ms` plus the day end itself, so **no window ever
//! spans a `dt=` partition**. The grid is absolute (derived only from the
//! epoch), which makes window identities — and therefore manifest keys —
//! deterministic across runs with different `--from/--to`: that determinism
//! is the basis of drain idempotency.
//!
//! [`windows`] yields only *complete* grid windows that end at or before
//! `to_ms`. A trailing partial window is deliberately excluded: events may
//! still be arriving near `now`, and writing a manifest for a half-window
//! would let the retention gate "prove" coverage that doesn't exist. The
//! next drain run picks the window up once it is complete.

use thiserror::Error;

/// One hour in epoch milliseconds.
pub const HOUR_MS: i64 = 3_600_000;
/// One UTC day in epoch milliseconds (Unix time has no leap seconds, so UTC
/// day boundaries are exact multiples of this).
pub const DAY_MS: i64 = 86_400_000;

/// Half-open drain window `[start_ms, end_ms)`, epoch milliseconds UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Window {
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum WindowError {
    #[error("window length must be positive, got {0} ms")]
    NonPositiveWindow(i64),
    #[error("timestamps must be non-negative (from={from_ms}, to={to_ms})")]
    NegativeRange { from_ms: i64, to_ms: i64 },
}

fn validate(from_ms: i64, to_ms: i64, window_ms: i64) -> Result<(), WindowError> {
    if window_ms <= 0 {
        return Err(WindowError::NonPositiveWindow(window_ms));
    }
    if from_ms < 0 || to_ms < 0 {
        return Err(WindowError::NegativeRange { from_ms, to_ms });
    }
    Ok(())
}

/// Largest grid boundary `<= ts_ms` (grid = day start + multiples of
/// `window_ms` within the day). Requires `ts_ms >= 0`, `window_ms > 0`.
pub fn align_down(ts_ms: i64, window_ms: i64) -> i64 {
    let day = (ts_ms / DAY_MS) * DAY_MS;
    day + ((ts_ms - day) / window_ms) * window_ms
}

/// Next grid boundary strictly after `boundary_ms` (a grid boundary),
/// cutting at the UTC day end.
pub(crate) fn next_boundary(boundary_ms: i64, window_ms: i64) -> i64 {
    let day_end = (boundary_ms / DAY_MS) * DAY_MS + DAY_MS;
    boundary_ms.saturating_add(window_ms).min(day_end)
}

/// Complete grid windows covering `[from_ms, to_ms)`: the first window
/// contains `from_ms` (its start may be earlier), and only windows with
/// `end_ms <= to_ms` are returned (see module docs for why the trailing
/// partial window is excluded).
pub fn windows(from_ms: i64, to_ms: i64, window_ms: i64) -> Result<Vec<Window>, WindowError> {
    validate(from_ms, to_ms, window_ms)?;
    let mut out = Vec::new();
    let mut start = align_down(from_ms, window_ms);
    loop {
        let end = next_boundary(start, window_ms);
        if end > to_ms {
            break;
        }
        out.push(Window {
            start_ms: start,
            end_ms: end,
        });
        start = end;
    }
    Ok(out)
}

/// Grid windows whose `start_ms < until_ms` — i.e. every window that holds
/// (or could hold) events strictly older than `until_ms`, **including** the
/// window that contains `until_ms` itself. The retention gate uses this:
/// if the cutoff falls mid-window, the containing window must also have a
/// manifest before retention may shrink (fail-closed).
pub fn windows_covering(
    from_ms: i64,
    until_ms: i64,
    window_ms: i64,
) -> Result<Vec<Window>, WindowError> {
    validate(from_ms, until_ms, window_ms)?;
    let mut out = Vec::new();
    let mut start = align_down(from_ms, window_ms);
    while start < until_ms {
        let end = next_boundary(start, window_ms);
        out.push(Window {
            start_ms: start,
            end_ms: end,
        });
        start = end;
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // 2024-06-05T00:00:00Z — an exact UTC day boundary.
    const DAY0: i64 = 1_717_545_600_000;

    #[test]
    fn hour_windows_align_down_from_mid_hour() {
        // from 22:30, to 02:00 next day.
        let from = DAY0 + 22 * HOUR_MS + 30 * 60_000;
        let to = DAY0 + DAY_MS + 2 * HOUR_MS;
        let ws = windows(from, to, HOUR_MS).unwrap();
        assert_eq!(
            ws,
            vec![
                Window {
                    start_ms: DAY0 + 22 * HOUR_MS,
                    end_ms: DAY0 + 23 * HOUR_MS
                },
                Window {
                    start_ms: DAY0 + 23 * HOUR_MS,
                    end_ms: DAY0 + DAY_MS
                },
                Window {
                    start_ms: DAY0 + DAY_MS,
                    end_ms: DAY0 + DAY_MS + HOUR_MS
                },
                Window {
                    start_ms: DAY0 + DAY_MS + HOUR_MS,
                    end_ms: DAY0 + DAY_MS + 2 * HOUR_MS
                },
            ]
        );
    }

    #[test]
    fn trailing_partial_window_excluded() {
        let from = DAY0;
        let to = DAY0 + HOUR_MS + 1; // one full hour + 1ms
        let ws = windows(from, to, HOUR_MS).unwrap();
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].end_ms, DAY0 + HOUR_MS);
    }

    #[test]
    fn non_dividing_window_cut_at_day_boundary() {
        // 7h windows: 0,7,14,21 then cut at 24h, grid restarts per day.
        let ws = windows(DAY0, DAY0 + DAY_MS + 7 * HOUR_MS, 7 * HOUR_MS).unwrap();
        let bounds: Vec<(i64, i64)> = ws
            .iter()
            .map(|w| ((w.start_ms - DAY0) / HOUR_MS, (w.end_ms - DAY0) / HOUR_MS))
            .collect();
        assert_eq!(bounds, vec![(0, 7), (7, 14), (14, 21), (21, 24), (24, 31)]);
    }

    #[test]
    fn empty_when_from_at_or_after_to() {
        assert!(windows(DAY0, DAY0, HOUR_MS).unwrap().is_empty());
        assert!(windows(DAY0 + 1, DAY0, HOUR_MS).unwrap().is_empty());
    }

    #[test]
    fn rejects_bad_inputs() {
        assert_eq!(
            windows(0, 10, 0).unwrap_err(),
            WindowError::NonPositiveWindow(0)
        );
        assert!(matches!(
            windows(-1, 10, HOUR_MS).unwrap_err(),
            WindowError::NegativeRange { .. }
        ));
    }

    #[test]
    fn covering_includes_window_containing_cutoff() {
        let cutoff = DAY0 + 90 * 60_000; // 01:30
        let ws = windows_covering(DAY0, cutoff, HOUR_MS).unwrap();
        assert_eq!(
            ws,
            vec![
                Window {
                    start_ms: DAY0,
                    end_ms: DAY0 + HOUR_MS
                },
                Window {
                    start_ms: DAY0 + HOUR_MS,
                    end_ms: DAY0 + 2 * HOUR_MS
                },
            ]
        );
    }

    #[test]
    fn covering_empty_when_cutoff_not_after_from() {
        assert!(windows_covering(DAY0, DAY0, HOUR_MS).unwrap().is_empty());
    }

    proptest! {
        #[test]
        fn windows_tile_contiguously_and_never_span_days(
            from in 0i64..4_000_000_000_000i64,
            len in 1i64..(10 * DAY_MS),
            window_ms in prop_oneof![Just(HOUR_MS), Just(15 * 60_000i64), 1_000i64..(2 * DAY_MS)],
        ) {
            let to = from + len;
            let ws = windows(from, to, window_ms).unwrap();
            let mut prev_end: Option<i64> = None;
            for w in &ws {
                prop_assert!(w.start_ms < w.end_ms);
                prop_assert!(w.end_ms <= to);
                prop_assert!(w.end_ms - w.start_ms <= window_ms);
                // never spans a UTC day partition
                prop_assert_eq!(w.start_ms / DAY_MS, (w.end_ms - 1) / DAY_MS);
                if let Some(p) = prev_end {
                    prop_assert_eq!(p, w.start_ms);
                }
                prev_end = Some(w.end_ms);
            }
            if let Some(first) = ws.first() {
                prop_assert!(first.start_ms <= from);
                prop_assert!(from - first.start_ms < window_ms);
            }
            if let Some(last) = ws.last() {
                // maximal coverage: the uncovered tail is shorter than the
                // next (possibly day-cut) window would be.
                prop_assert!(to - last.end_ms < window_ms);
            }
        }

        #[test]
        fn covering_windows_reach_past_cutoff(
            from in 0i64..4_000_000_000_000i64,
            len in 1i64..(10 * DAY_MS),
            window_ms in prop_oneof![Just(HOUR_MS), 1_000i64..(2 * DAY_MS)],
        ) {
            let until = from + len;
            let ws = windows_covering(from, until, window_ms).unwrap();
            prop_assert!(!ws.is_empty());
            for w in &ws {
                prop_assert!(w.start_ms < until);
            }
            prop_assert!(ws.last().unwrap().end_ms >= until);
        }
    }
}
