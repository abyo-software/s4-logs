//! s4logs-core — chunk format & storage layout shared by drain / gateway / cli.
//!
//! Format contract: `DESIGN.md` (repo root). Data objects are **standard
//! RFC 8878 zstd multiframe** — readable by `zstd -dc` and Athena with no
//! S4 Logs tooling. Two sidecars accompany each object: S4IX (byte-offset
//! frame index, reused verbatim from `s4-codec`) and S4LT (per-frame
//! timestamp ranges, defined in [`tsindex`]).

pub mod chunk;
pub mod layout;
pub mod read;
pub mod record;
pub mod sink;
pub mod store;
pub mod tsindex;

pub use s4_codec::index::{FrameIndex, FrameIndexEntry, decode_index, encode_index};
