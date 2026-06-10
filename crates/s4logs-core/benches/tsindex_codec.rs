//! Criterion bench — S4LT sidecar codec (`encode_ts_index` /
//! `decode_ts_index`), modeled on s4-codec's `index_codec` bench.
//!
//! Every grep/restore fetches and parses the S4LT sidecar before it can
//! prune frames; a parse-time regression multiplies across every object in
//! the queried window. Frame counts cover the production envelope: a 4 MiB
//! frame target × multi-GiB drain objects tops out around 4 K frames.
//!
//! Smoke run: `cargo bench -p s4logs-core --bench tsindex_codec -- --quick`

#![allow(clippy::unwrap_used)] // bench fixtures

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use s4logs_core::tsindex::{TsEntry, TsIndex, decode_ts_index, encode_ts_index};

/// `n_frames` entries of contiguous 30 s windows — the shape drain emits
/// for an ordered FilterLogEvents sweep.
fn build_index(n_frames: usize) -> TsIndex {
    const FRAME_SPAN_MS: i64 = 30_000;
    let base = 1_717_900_000_000i64;
    let entries = (0..n_frames as i64)
        .map(|i| TsEntry {
            min_ts: base + i * FRAME_SPAN_MS,
            max_ts: base + (i + 1) * FRAME_SPAN_MS - 1,
        })
        .collect();
    TsIndex { entries }
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_ts_index");
    for &n_frames in &[128usize, 1024, 4096] {
        let idx = build_index(n_frames);
        let encoded_len = encode_ts_index(&idx).len();
        group.throughput(Throughput::Bytes(encoded_len as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{n_frames}f")),
            &idx,
            |b, idx| {
                b.iter(|| {
                    let out = encode_ts_index(idx);
                    std::hint::black_box(out);
                });
            },
        );
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_ts_index");
    for &n_frames in &[128usize, 1024, 4096] {
        let bytes: Bytes = encode_ts_index(&build_index(n_frames));
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{n_frames}f")),
            &bytes,
            |b, bytes| {
                b.iter(|| {
                    let decoded = decode_ts_index(bytes.clone()).unwrap();
                    std::hint::black_box(decoded);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
