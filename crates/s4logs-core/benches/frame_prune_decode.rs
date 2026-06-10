//! Criterion bench — read path: S4LT frame pruning (`frames_overlapping`)
//! and bomb-capped frame decode (`decompress_frames`).
//!
//! grep/restore do this per object: prune frames by time range, Range GET
//! the survivors, decode each. Pruning must stay O(frames) cheap; decode
//! throughput (reported as **decompressed bytes/s**) bounds how fast a
//! restore can stream.
//!
//! Smoke run: `cargo bench -p s4logs-core --bench frame_prune_decode -- --quick`

#![allow(clippy::unwrap_used)] // bench fixtures

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use s4logs_core::chunk::{ChunkConfig, ChunkWriter, EncodedChunk};
use s4logs_core::read::{TimeRange, decompress_frames, frames_overlapping};
use s4logs_core::record::LogRecord;

/// Multi-frame chunk: ~8 MiB uncompressed at a 64 KiB frame target
/// (≈128 frames), timestamps spread evenly so time-range pruning maps
/// directly onto a frame subset.
fn build_chunk() -> EncodedChunk {
    let mut w = ChunkWriter::new(ChunkConfig {
        frame_target_bytes: 64 << 10,
        zstd_level: 3,
    });
    let base_ts = 1_717_900_000_000i64;
    let mut i = 0u64;
    while w.uncompressed_len() < (8 << 20) {
        w.push(&LogRecord {
            timestamp: base_ts + i as i64 * 10, // 10 ms cadence
            stream: format!("app/i-{:04x}", i % 8),
            message: format!(
                "INFO request_id={:032x} path=/api/v1/items/{} latency_ms={}",
                i.wrapping_mul(0x9E37_79B9_7F4A_7C15),
                i % 100_000,
                (i * 7) % 250,
            ),
            ingestion_time: None,
            event_id: None,
        })
        .unwrap();
        i += 1;
    }
    w.finish().unwrap().unwrap()
}

fn bench_prune(c: &mut Criterion) {
    let chunk = build_chunk();
    let span = chunk.max_timestamp - chunk.min_timestamp;
    let cases: &[(&str, TimeRange)] = &[
        (
            // ~10% of the chunk's time span — the typical grep window.
            "narrow_10pct",
            TimeRange {
                from_ms: chunk.min_timestamp + span / 2,
                to_ms_exclusive: chunk.min_timestamp + span / 2 + span / 10,
            },
        ),
        (
            "full_span",
            TimeRange {
                from_ms: chunk.min_timestamp,
                to_ms_exclusive: chunk.max_timestamp + 1,
            },
        ),
        (
            // Entirely before the chunk: prune-everything fast path.
            "no_overlap",
            TimeRange {
                from_ms: chunk.min_timestamp - 1000,
                to_ms_exclusive: chunk.min_timestamp,
            },
        ),
    ];
    let mut group = c.benchmark_group("frames_overlapping_128f");
    for (label, range) in cases {
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.iter(|| {
                let spans = frames_overlapping(&chunk.frame_index, &chunk.ts_index, range).unwrap();
                std::hint::black_box(spans);
            });
        });
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let chunk = build_chunk();
    let span = chunk.max_timestamp - chunk.min_timestamp;
    let narrow = TimeRange {
        from_ms: chunk.min_timestamp + span / 2,
        to_ms_exclusive: chunk.min_timestamp + span / 2 + span / 10,
    };
    let spans = frames_overlapping(&chunk.frame_index, &chunk.ts_index, &narrow).unwrap();
    assert!(!spans.is_empty() && spans.len() < chunk.frame_index.entries.len());

    let mut group = c.benchmark_group("decompress_frames");

    // One frame — the smallest Range GET unit.
    let first = spans[0];
    let frame = &chunk.body[first.byte_start as usize..first.byte_end_exclusive as usize];
    group.throughput(Throughput::Bytes(first.original_size));
    group.bench_function(BenchmarkId::from_parameter("one_frame_64KiB"), |b| {
        b.iter(|| {
            let out = decompress_frames(frame, first.original_size).unwrap();
            std::hint::black_box(out);
        });
    });

    // Pruned range — decode every surviving frame, as grep does.
    let pruned_orig: u64 = spans.iter().map(|s| s.original_size).sum();
    group.throughput(Throughput::Bytes(pruned_orig));
    group.bench_with_input(
        BenchmarkId::from_parameter("pruned_10pct_range"),
        &spans,
        |b, spans| {
            b.iter(|| {
                for s in spans {
                    let bytes = &chunk.body[s.byte_start as usize..s.byte_end_exclusive as usize];
                    let out = decompress_frames(bytes, s.original_size).unwrap();
                    std::hint::black_box(out);
                }
            });
        },
    );

    // Whole body as one concatenated stream — the sidecar-less fallback.
    group.throughput(Throughput::Bytes(chunk.uncompressed_bytes));
    group.bench_function(BenchmarkId::from_parameter("whole_body_8MiB"), |b| {
        b.iter(|| {
            let out = decompress_frames(&chunk.body, chunk.uncompressed_bytes).unwrap();
            std::hint::black_box(out);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_prune, bench_decode);
criterion_main!(benches);
