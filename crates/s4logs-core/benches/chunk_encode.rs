//! Criterion bench — chunk encode throughput (records → `EncodedChunk`).
//!
//! This is the gateway/drain hot path: every accepted PutLogEvents byte goes
//! through `ChunkWriter::push` + zstd frame cuts. Throughput is reported as
//! **uncompressed input bytes/s** (criterion renders MiB/s), on a ~32 MiB
//! synthetic corpus of realistic JSON-ish app log lines.
//!
//! Smoke run: `cargo bench -p s4logs-core --bench chunk_encode -- --quick`

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use s4logs_core::chunk::{ChunkConfig, ChunkWriter};
use s4logs_core::record::LogRecord;

/// ~`total_bytes` of synthetic log records. Lines mimic production app logs
/// (timestamped request lines with variable ids/latencies) so the zstd ratio
/// is honest — pure repetition would inflate the MiB/s number.
fn synth_records(total_bytes: usize) -> (Vec<LogRecord>, u64) {
    let mut records = Vec::new();
    let mut accumulated = 0u64;
    let mut i = 0u64;
    let base_ts = 1_717_900_000_000i64;
    while (accumulated as usize) < total_bytes {
        let rec = LogRecord {
            timestamp: base_ts + i as i64,
            stream: format!("app/i-{:08x}", i % 16),
            message: format!(
                "INFO request_id={:032x} method=GET path=/api/v1/items/{} status={} \
                 latency_ms={} bytes={} cache={}",
                i.wrapping_mul(0x9E37_79B9_7F4A_7C15),
                i % 100_000,
                if i.is_multiple_of(50) { 500 } else { 200 },
                (i * 7) % 250,
                (i * 131) % 65_536,
                if i.is_multiple_of(3) { "hit" } else { "miss" },
            ),
            ingestion_time: Some(base_ts + i as i64 + 40),
            event_id: None,
        };
        // Account the JSONL line length (what ChunkWriter actually buffers).
        let mut line = Vec::new();
        #[allow(clippy::unwrap_used)] // bench fixture, encode cannot fail
        rec.append_jsonl(&mut line).unwrap();
        accumulated += line.len() as u64;
        records.push(rec);
        i += 1;
    }
    (records, accumulated)
}

fn bench_chunk_encode(c: &mut Criterion) {
    const TOTAL: usize = 32 << 20; // ~32 MiB uncompressed JSONL
    let (records, uncompressed) = synth_records(TOTAL);

    let mut group = c.benchmark_group("chunk_encode");
    // Each iteration encodes 32 MiB (~0.1–0.3 s at zstd-3); 10 samples keeps
    // the full run in seconds while still giving criterion variance data.
    group.sample_size(10);
    group.throughput(Throughput::Bytes(uncompressed));
    group.bench_with_input(
        BenchmarkId::from_parameter("32MiB_default_cfg"),
        &records,
        |b, records| {
            b.iter(|| {
                let mut w = ChunkWriter::new(ChunkConfig::default());
                for rec in records {
                    #[allow(clippy::unwrap_used)] // bench: encode cannot fail
                    w.push(rec).unwrap();
                }
                #[allow(clippy::unwrap_used)]
                let chunk = w.finish().unwrap().expect("non-empty chunk");
                std::hint::black_box(chunk);
            });
        },
    );
    group.finish();
}

criterion_group!(benches, bench_chunk_encode);
criterion_main!(benches);
