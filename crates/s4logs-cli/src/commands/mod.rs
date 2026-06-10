//! Subcommand implementations. `main.rs` stays thin; each module owns one
//! verb and keeps AWS-touching code minimal around pure, unit-tested cores.

pub mod drain;
pub mod grep;
pub mod restore;
pub mod serve;
