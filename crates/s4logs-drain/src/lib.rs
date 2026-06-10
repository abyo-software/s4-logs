//! s4logs-drain — Mode A (退避). Wave 1B implements this crate.
//!
//! Contract: DESIGN.md §7. Unit of work = (log_group, UTC-aligned window).
//! FilterLogEvents pagination behind a `CwSource` trait (mockable),
//! manifest-based idempotency, fail-closed retention gate.
