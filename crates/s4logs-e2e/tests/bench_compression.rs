//! Bench-style test: ~64 MiB of synthetic-but-realistic log lines in three
//! shapes through `ChunkWriter` (default config: 4 MiB frames, zstd-3,
//! checksum on). Prints a ratio + throughput table for the README.
//!
//! Run with: `cargo test -p s4logs-e2e --release -- --ignored bench --nocapture`
//! (debug-profile numbers are meaningless — serde + zstd C code both build
//! unoptimized).
//!
//! No LocalStack required. Numbers are SYNTHETIC-corpus numbers; the S4
//! family reference for a real corpus is s4's measured 155x on 256 MiB of
//! nginx logs (s4 README, 2026-05-13).

#![allow(clippy::unwrap_used)]

use std::time::Instant;

use s4logs_core::chunk::{ChunkConfig, ChunkWriter};
use s4logs_core::record::LogRecord;

const TARGET_BYTES: u64 = 64 << 20;

/// Tiny deterministic xorshift64* — no rand dependency, reproducible corpus.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len() as u64) as usize]
    }
}

const PATHS: [&str; 8] = [
    "/api/v1/users",
    "/api/v1/orders",
    "/healthz",
    "/static/app.js",
    "/api/v1/payments",
    "/login",
    "/api/v2/search",
    "/favicon.ico",
];
const AGENTS: [&str; 4] = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
    "curl/8.5.0",
    "python-requests/2.32.0",
    "ELB-HealthChecker/2.0",
];
const LEVELS: [&str; 4] = ["INFO", "INFO", "WARN", "ERROR"]; // INFO-heavy
const LOGGERS: [&str; 5] = [
    "com.example.api.OrderService",
    "com.example.api.UserService",
    "com.example.db.ConnectionPool",
    "com.example.cache.RedisClient",
    "com.example.http.RequestFilter",
];
const EXCEPTIONS: [&str; 3] = [
    "java.lang.NullPointerException",
    "java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available",
    "java.util.concurrent.TimeoutException: request timed out after 30000ms",
];

fn nginx_line(rng: &mut Rng, i: u64) -> String {
    let status = [200u64, 200, 200, 200, 304, 404, 500][rng.below(7) as usize];
    format!(
        "10.{}.{}.{} - - [10/Jun/2026:12:{:02}:{:02} +0000] \"GET {}?id={} HTTP/1.1\" {} {} \"-\" \"{}\"",
        rng.below(256),
        rng.below(256),
        rng.below(256),
        (i / 60) % 60,
        i % 60,
        rng.pick(&PATHS),
        rng.below(100_000),
        status,
        rng.below(50_000),
        rng.pick(&AGENTS),
    )
}

fn json_app_line(rng: &mut Rng, ts: i64) -> String {
    serde_json::json!({
        "ts": ts,
        "level": rng.pick(&LEVELS),
        "logger": rng.pick(&LOGGERS),
        "request_id": format!("{:016x}", rng.next()),
        "user_id": rng.below(1_000_000),
        "path": rng.pick(&PATHS),
        "latency_ms": rng.below(500),
        "msg": "request completed",
    })
    .to_string()
}

fn java_line(rng: &mut Rng, ts: i64) -> String {
    if rng.below(100) < 12 {
        // Multi-line stack trace as one CW event message.
        let mut s = format!(
            "2026-06-10 12:00:{:02}.{:03} ERROR [http-nio-8080-exec-{}] {} - request failed\n{}",
            (ts / 1000) % 60,
            ts % 1000,
            rng.below(32),
            rng.pick(&LOGGERS),
            rng.pick(&EXCEPTIONS),
        );
        for _ in 0..(8 + rng.below(10)) {
            s.push_str(&format!(
                "\n\tat {}.{}({}.java:{})",
                rng.pick(&LOGGERS),
                ["process", "handle", "execute", "invoke"][rng.below(4) as usize],
                "Handler",
                rng.below(900),
            ));
        }
        s
    } else {
        format!(
            "2026-06-10 12:00:{:02}.{:03} {} [http-nio-8080-exec-{}] {} - processed request path={} in {}ms",
            (ts / 1000) % 60,
            ts % 1000,
            rng.pick(&LEVELS),
            rng.below(32),
            rng.pick(&LOGGERS),
            rng.pick(&PATHS),
            rng.below(500),
        )
    }
}

/// Generate records up to ~64 MiB of JSONL, then time ChunkWriter
/// (push + finish = serialize + frame + zstd-3 + checksum).
fn bench_shape(name: &str, line: impl Fn(&mut Rng, u64, i64) -> String) -> (String, f64, f64, u64) {
    let mut rng = Rng(0x5EED_0BAD_C0FF_EE00 + name.len() as u64);
    let base_ts = 1_780_000_000_000i64;
    let mut records = Vec::new();
    let mut approx = 0u64;
    let mut i = 0u64;
    while approx < TARGET_BYTES {
        let msg = line(&mut rng, i, base_ts + i as i64);
        approx += msg.len() as u64 + 64; // + JSONL envelope overhead
        records.push(LogRecord {
            timestamp: base_ts + i as i64,
            stream: format!("app/i-{:04x}", i % 8),
            message: msg,
            ingestion_time: Some(base_ts + i as i64 + 1200),
            event_id: None,
        });
        i += 1;
    }

    let start = Instant::now();
    let mut w = ChunkWriter::new(ChunkConfig::default());
    for r in &records {
        w.push(r).unwrap();
    }
    let chunk = w.finish().unwrap().unwrap();
    let elapsed = start.elapsed().as_secs_f64();

    let ratio = chunk.uncompressed_bytes as f64 / chunk.body.len() as f64;
    let mibps = (chunk.uncompressed_bytes as f64 / (1 << 20) as f64) / elapsed;
    (name.to_owned(), ratio, mibps, chunk.uncompressed_bytes)
}

#[test]
#[ignore = "bench — run with --release --nocapture"]
fn bench_chunkwriter_compression() {
    let rows = [
        bench_shape("nginx access log", |rng, i, _| nginx_line(rng, i)),
        bench_shape("JSON app logs", |rng, _, ts| json_app_line(rng, ts)),
        bench_shape("java app + stacktraces", |rng, _, ts| java_line(rng, ts)),
    ];
    println!();
    println!("ChunkWriter (zstd-3, 4 MiB frames, checksum on), synthetic ~64 MiB corpora:");
    println!();
    println!("| Shape | Input | Ratio | Throughput |");
    println!("|---|---:|---:|---:|");
    for (name, ratio, mibps, raw) in &rows {
        println!(
            "| {} | {:.0} MiB | {:.1}x | {:.0} MiB/s |",
            name,
            *raw as f64 / (1 << 20) as f64,
            ratio,
            mibps,
        );
    }
    println!();
    for (name, ratio, _, _) in &rows {
        assert!(*ratio > 2.0, "{name}: implausibly low ratio {ratio}");
    }
}
