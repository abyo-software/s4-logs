//! Bolero fuzz targets — coverage-guided fuzzing for the s4logs-core format
//! surface (decoders that eat untrusted bytes from S3, plus a structured
//! encode→decode canary).
//!
//! ## Why bolero on top of `tests/proptest_format.rs`
//!
//! - **proptest**: structural property generator, stable, runs on every CI
//!   push (`PROPTEST_CASES=10000`) and nightly at 1M cases.
//! - **bolero**: the same `check!` API dispatches to multiple fuzz engines.
//!   Under plain `cargo test` it uses the random engine (CI smoke); under
//!   `cargo bolero test --engine libfuzzer` it becomes **coverage-guided**
//!   (nightly fuzz workflow, requires nightly Rust + cargo-bolero).
//!
//! ## Running
//!
//! ```bash
//! # 1. CI / dev smoke (random engine, plain cargo test)
//! cargo test -p s4logs-core --test fuzz_bolero
//!
//! # 2. Coverage-guided fuzz (nightly Rust)
//! cargo install cargo-bolero
//! cd crates/s4logs-core
//! cargo bolero test --engine libfuzzer --profile release ts_index_decode_bolero -- \
//!     -max_total_time=1800
//!
//! # 3. Replay a crash artifact
//! cargo bolero test --engine libfuzzer --profile release <target> -- <crash-file>
//! ```
//!
//! `--profile release` because the workspace root does not define a custom
//! `[profile.fuzz]` (cargo-bolero's default). Corpus accumulates under
//! `crates/s4logs-core/tests/__fuzz__/<target>/corpus/`.

#![allow(clippy::unwrap_used)] // test code

use bytes::Bytes;
use s4logs_core::chunk::{ChunkConfig, ChunkWriter};
use s4logs_core::layout::{ChunkLocation, sanitize_log_group, unsanitize_log_group};
use s4logs_core::read::{RecordLines, decompress_frames};
use s4logs_core::record::LogRecord;
use s4logs_core::tsindex::decode_ts_index;

/// `decode_ts_index` eats an attacker-controllable sidecar object straight
/// from S3: arbitrary bytes must yield a typed `TsIndexError`, never a panic
/// or an attacker-sized allocation.
#[test]
fn ts_index_decode_bolero() {
    bolero::check!()
        .with_type::<Vec<u8>>()
        .for_each(|input: &Vec<u8>| {
            let _ = decode_ts_index(Bytes::from(input.clone()));
        });
}

/// `ChunkLocation::parse_data_key` parses keys returned by ListObjectsV2 —
/// anything can sit in the bucket. No panic on arbitrary (prefix, key), and
/// every successful parse must survive a re-encode → re-parse roundtrip.
#[test]
fn parse_data_key_bolero() {
    bolero::check!().with_type::<(String, String)>().for_each(
        |(prefix, key): &(String, String)| {
            if let Some(loc) = ChunkLocation::parse_data_key(prefix, key) {
                let rebuilt = loc.data_key(prefix);
                assert_eq!(
                    ChunkLocation::parse_data_key(prefix, &rebuilt).as_ref(),
                    Some(&loc),
                    "parse → data_key → parse must be a fixed point"
                );
            }
        },
    );
}

/// `unsanitize_log_group` decodes the `loggroup=` partition value from
/// listed keys: arbitrary input → `Some`/`None`, never a panic. Bonus
/// property: the sanitizer roundtrips any (valid-UTF-8) group name.
#[test]
fn unsanitize_log_group_bolero() {
    bolero::check!()
        .with_type::<String>()
        .for_each(|input: &String| {
            // Arbitrary (possibly malformed) encoded text — must not panic.
            let _ = unsanitize_log_group(input);
            // Encoder output must always decode back to the original.
            assert_eq!(
                unsanitize_log_group(&sanitize_log_group(input)).as_deref(),
                Some(input.as_str()),
                "sanitize → unsanitize must be the identity"
            );
        });
}

/// `LogRecord::from_jsonl` parses lines out of decompressed S3 objects
/// (possibly written by foreign tools): arbitrary bytes → typed error, and
/// anything that parses must re-encode → re-parse to the same record.
#[test]
fn log_record_from_jsonl_bolero() {
    bolero::check!()
        .with_type::<Vec<u8>>()
        .for_each(|input: &Vec<u8>| {
            if let Ok(rec) = LogRecord::from_jsonl(input) {
                let mut buf = Vec::new();
                rec.append_jsonl(&mut buf).unwrap();
                let again = LogRecord::from_jsonl(buf.trim_ascii_end()).unwrap();
                assert_eq!(again, rec, "jsonl re-encode must roundtrip");
            }
        });
}

/// `decompress_frames` decodes untrusted zstd bytes with an untrusted
/// claimed size: never panic, never allocate past `claimed + slack`
/// (decompression-bomb cap), only typed `ReadError`s. On `Ok`, the output
/// length must equal the claim exactly.
#[test]
fn decompress_frames_bolero() {
    bolero::check!()
        .with_generator((
            bolero::generator::produce::<Vec<u8>>(),
            // Small claimed sizes (≤1 MiB): the cap maths and SizeMismatch /
            // Bomb branches all live well below this.
            0u64..=(1 << 20),
        ))
        .for_each(
            |(input, claimed): &(Vec<u8>, u64)| match decompress_frames(input, *claimed) {
                Ok(out) => assert_eq!(out.len() as u64, *claimed),
                Err(_typed) => {}
            },
        );
}

/// Structured encode → decode canary: records through `ChunkWriter` (small
/// frame target so multi-frame paths execute) must come back identical via
/// per-frame `decompress_frames` + `RecordLines`, and the S4IX entries must
/// tile the body exactly.
#[test]
fn chunk_roundtrip_bolero() {
    use bolero::generator::*;
    bolero::check!()
        .with_generator(produce_with::<Vec<(i64, String)>>().len(0usize..64))
        .for_each(|input: &Vec<(i64, String)>| {
            let mut w = ChunkWriter::new(ChunkConfig {
                frame_target_bytes: 512,
                zstd_level: 3,
            });
            for (ts, msg) in input {
                w.push(&LogRecord {
                    timestamp: *ts,
                    stream: "fuzz".into(),
                    message: msg.clone(),
                    ingestion_time: None,
                    event_id: None,
                })
                .unwrap();
            }
            let Some(chunk) = w.finish().unwrap() else {
                assert!(input.is_empty(), "records pushed but chunk came back empty");
                return;
            };
            assert_eq!(chunk.record_count, input.len() as u64);
            assert_eq!(
                chunk.frame_index.entries.len(),
                chunk.ts_index.entries.len()
            );

            // Frame entries must tile the body; each frame must decode to
            // exactly its claimed original size.
            let mut decoded = Vec::with_capacity(chunk.uncompressed_bytes as usize);
            let mut comp_off = 0u64;
            for e in &chunk.frame_index.entries {
                assert_eq!(e.compressed_offset, comp_off, "frames must tile the body");
                let frame = &chunk.body[e.compressed_offset as usize..e.compressed_end() as usize];
                decoded.extend_from_slice(&decompress_frames(frame, e.original_size).unwrap());
                comp_off = e.compressed_end();
            }
            assert_eq!(comp_off, chunk.body.len() as u64);
            assert_eq!(decoded.len() as u64, chunk.uncompressed_bytes);

            // Mutate-free decode equality: same (ts, msg) sequence, in order.
            let got: Vec<(i64, String)> = RecordLines::new(&decoded)
                .map(|r| r.map(|rec| (rec.timestamp, rec.message)))
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(&got, input, "decode must reproduce the pushed records");
        });
}
