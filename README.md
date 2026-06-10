# S4 Logs

[![CI](https://github.com/abyo-software/s4-logs/actions/workflows/ci.yml/badge.svg)](https://github.com/abyo-software/s4-logs/actions/workflows/ci.yml)
[![E2E](https://github.com/abyo-software/s4-logs/actions/workflows/e2e.yml/badge.svg)](https://github.com/abyo-software/s4-logs/actions/workflows/e2e.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.92%2B-orange.svg)](https://www.rust-lang.org)

> **Cut the CloudWatch Logs bill by moving logs to zstd-compressed S3 —
> without changing your applications.**
> CloudWatch charges $0.50/GB to ingest and $0.03/GB-month to store. Most
> logs are written once and almost never read. S4 Logs archives them to S3
> as **standard zstd** (readable by `zstd -dc` and Athena, no S4 tooling
> required) at a storage cost 1–2 orders of magnitude lower.
>
> **Honest framing**: the "70–90% off the bill" number applies to **Mode B**
> (bypassing ingest, the dominant cost for most accounts). **Mode A** alone
> only cuts the *storage* line (by ~90–99%) — the ingest you already paid is
> gone either way. The break-even table below tells you whether either mode
> is worth your time.

S4 Logs is the second product in the [S4 family](https://github.com/abyo-software/s4)
(S4 does the same thing to your **S3** bill; S4 Logs does it to your
**CloudWatch Logs** bill) and reuses s4-codec's S4IX range-read index format.

## How it works

Two independent modes; run either or both.

**Mode A — Drain** (archive what's already in CloudWatch):

```
CloudWatch Logs ──FilterLogEvents──▶ s4logs drain ──zstd──▶ S3 (data + index sidecars)
                                          │
                                          └─▶ PutRetentionPolicy (e.g. 90d → 7d)
                                              only AFTER every affected window has
                                              a verified manifest (fail-closed)
```

- Work unit = (log group, UTC-aligned 1h window). Each window's manifest
  proves it is fully archived; re-runs skip manifested windows (idempotent).
- **Archive first, shrink retention after.** `PutRetentionPolicy` is gated on
  complete manifest coverage of everything older than the proposed cutoff;
  any gap means nothing happens. Default is report-only
  (`--apply-retention` opt-in).

**Mode B — Bypass** (avoid CloudWatch ingest entirely):

```
Fluent Bit / CW Agent / SDK ──PutLogEvents (same wire protocol)──▶ s4logs serve
                                                                      │
                              first-match TOML routing per group/stream:
                                  ├── "s3"          → zstd → S3   ($0 ingest)
                                  ├── "cloudwatch"  → passthrough to real CW
                                  ├── "both"        → both
                                  └── "drop"
```

- The gateway speaks the CloudWatch Logs AWS JSON 1.1 API subset
  (`PutLogEvents`, `CreateLogGroup`, `CreateLogStream`, `DescribeLogGroups`,
  `DescribeLogStreams`) — agents migrate with an **endpoint override only**,
  the same customer experience as S4's S3-compatible endpoint.
- Keep the streams you alert on in CloudWatch via routing rules; everything
  else skips the $0.50/GB toll.

### S3 layout (open, query-engine-friendly)

```
{prefix}data/account={acct}/loggroup={g}/dt=YYYY-MM-DD/{name}.jsonl.zst      ← standard zstd, JSONL inside
{prefix}index/account={acct}/loggroup={g}/dt=YYYY-MM-DD/{name}.jsonl.zst.s4index   ← byte-range index (S4IX)
{prefix}index/...same.../{name}.jsonl.zst.s4lts                              ← per-frame timestamp ranges
{prefix}manifest/account={acct}/loggroup={g}/window={start}-{end}.json       ← drain idempotency + retention gate
```

Index sidecars live under a **separate prefix** so Athena/Spark pointed at
`data/` never see them. Each JSONL line is one event:
`{"timestamp":…,"stream":"…","message":"…","ingestion_time":…,"event_id":"…"}`
(epoch milliseconds; optional fields omitted when absent).

## Quickstart

> The `s4logs` CLI (crate `s4logs-cli`) wraps the library crates below.
> Flags shown are the documented interface (DESIGN.md §9) — run
> `s4logs --help` for the authoritative list.

### Drain a log group (Mode A)

```console
# Report-only first: what would be archived, what would it save?
s4logs drain --log-group /aws/lambda/payments \
  --bucket my-archive-bucket --prefix s4logs \
  --from 2026-01-01T00:00:00Z --to 2026-06-01T00:00:00Z \
  --window 1h --concurrency 2 --dry-run

# Archive for real. Re-running is safe (manifested windows are skipped).
s4logs drain --log-group /aws/lambda/payments \
  --bucket my-archive-bucket --prefix s4logs

# Shrink CW retention — only succeeds if every older window is archived.
s4logs drain --log-group /aws/lambda/payments \
  --bucket my-archive-bucket --retention-days 7 --apply-retention

# Whole-account sweeps: --log-group takes a glob, or use --all. Per-group
# failures are reported and skipped; exit code 1 if any group failed.
s4logs drain --all --bucket my-archive-bucket --group-concurrency 2

# What have I archived so far, and what is it saving? Reads manifests only —
# zero CloudWatch API calls, zero S3 data reads.
s4logs report --all --bucket my-archive-bucket
```

### Run the bypass gateway (Mode B)

```console
s4logs serve --listen 0.0.0.0:8080 \
  --bucket my-archive-bucket --prefix s4logs --account 123456789012 \
  --routing-config routing.toml --flush-bytes 8MiB --flush-interval 60s \
  --wal-dir /var/lib/s4logs/wal
```

`--wal-dir` makes acknowledged events crash-durable (fsync before the ack,
replay on restart — at-least-once). To require signed requests, add
`--auth-mode sigv4` with `S4LOGS_AUTH_ACCESS_KEY` / `S4LOGS_AUTH_SECRET`;
agents and SDKs already sign, so no client change is needed beyond matching
credentials.

`routing.toml` (first match wins):

```toml
default_action = "s3"            # s3 | cloudwatch | both | drop

[[rule]]
log_group = "/aws/lambda/payments-*"   # glob
action = "cloudwatch"                  # keep alert-critical streams in CW
```

Point **Fluent Bit** at it (endpoint override is the entire migration):

```ini
[OUTPUT]
    Name              cloudwatch_logs
    Match             *
    region            us-east-1
    log_group_name    /app/api
    log_stream_prefix node-
    auto_create_group On
    endpoint          http://s4logs-gateway.internal:8080
```

or the **CloudWatch Agent**:

```json
{ "logs": { "endpoint_override": "http://s4logs-gateway.internal:8080" } }
```

### Search and restore

```console
# grep without downloading whole objects: timestamp sidecar prunes frames,
# byte-range sidecar turns the survivors into S3 Range GETs. Output is
# timestamp-ordered across objects (streaming k-way merge, bounded memory).
s4logs grep 'ERROR.*timeout' --log-group /aws/lambda/payments \
  --from 2026-03-01T00:00:00Z --to 2026-03-02T00:00:00Z --output text

# Restore raw JSONL locally (the primary restore path):
s4logs restore --log-group /aws/lambda/payments \
  --from 2026-03-01T00:00:00Z --to 2026-03-02T00:00:00Z --to-file out.jsonl
```

### Local E2E

```console
./scripts/e2e.sh    # LocalStack up → full #[ignore] suite → down
```

## Cost model

Per-GB economics (AWS list prices, us-east-1). One subtlety that most
cost write-ups miss, and that we initially got wrong too: **CloudWatch
bills archived storage on gzip-level-6 compressed bytes**, not raw (AWS
pricing page footnote). Typical text logs gzip ~3–5×, so the honest
comparison is CW-gzip vs S3-zstd:

| | CloudWatch as-is | Mode A (drained) | Mode B (bypassed) |
|---|---|---|---|
| ingest | $0.50/GB (raw bytes + 26 B/event) | $0.50/GB (already paid — not recoverable) | **$0 (avoided)** + negligible S3 PUTs |
| storage | $0.03/GB·mo on gzip-6 bytes ≈ **$0.006–0.010/GB-raw·mo** | S3 $0.023/GB·mo on zstd bytes ≈ **$0.002–0.004/GB-raw·mo** | same |
| reduction | — | **~50–70% of the storage line**, plus retention shortening removes the CW line entirely for drained data | **~90%+ of the whole bill** (ingest dominates) |

Worked example, 1 TiB of raw logs: CloudWatch storage ≈ **$7.7/mo** (at
gzip 4×) → S3 at our measured 6.2× zstd **$3.8/mo**; shrink CW retention
after draining and the CW line goes to ~0 for the drained range. In Mode B
the same TiB skips **$512** of one-time ingest — ingest, not storage, is
where the bill lives.

**Break-even honesty** (same policy as S4's README): if your CloudWatch
Logs bill is **under $500/month, the OSS version is all you need** — and
even that may be more moving parts than your problem deserves. Mode A on
its own is a storage optimization with real but modest margins; the
compelling moves are **retention shortening** (Mode A's gate makes it safe)
and **Mode B ingest avoidance**. Draining itself costs ~$0 in API charges:
FilterLogEvents has no per-call price (verified on a real 5 GiB drain —
see below), only time and account quota.

### Measured compression (synthetic corpora)

Single-threaded `ChunkWriter` (zstd-3, 4 MiB frames, content checksum on),
~64–78 MiB **synthetic** corpora, AMD Ryzen 9 9950X, 2026-06-10
(`cargo test -p s4logs-e2e --release -- --ignored bench --nocapture` to
reproduce):

| Shape | Input | Ratio | Throughput |
|---|---:|---:|---:|
| nginx access log | 75 MiB | 8.3× | 546 MiB/s |
| JSON app logs | 78 MiB | 9.1× | 604 MiB/s |
| java app + stacktraces | 71 MiB | 11.0× | 742 MiB/s |

Synthetic generators underestimate real-log redundancy (real fleets repeat
themselves far more): the S4 family reference on a **real** corpus is
[s4's measured **155×** on 256 MiB of nginx logs at 3.7 GB/s](https://github.com/abyo-software/s4#headline-numbers)
(cpu-zstd-3, 2026-05-13). Treat 8–11× as the floor and 155× as what
repetitive access logs actually do.

### Verified against real AWS (controlled experiment, 2026-06-10)

We ran the full Mode A pipeline against a real us-east-1 account —
**controlled and synthetic** (we seeded the data ourselves; labeled as
such, not passed off as an organic workload):

| Step | Measured |
|---|---|
| Seed: PutLogEvents, 16 streams | 5.00 GiB message bytes, 33,163,647 events, 0 rejections, 592 s (**8.7 MiB/s** aggregate) |
| Backdated-event visibility | events ingested with past timestamps took **3–5.5 min** to appear in FilterLogEvents (see Limitations — this interacts with drain manifests) |
| Drain: 5 windows, `--concurrency 4` | **94.6 min wall**, **0 ThrottlingExceptions**, $0 API charges (FilterLogEvents is unmetered; per-page latency is the bottleneck) |
| Archive | 9.7 GiB JSONL → **1.6 GiB zstd (6.2×)**, 41 objects |
| Fidelity | spot 60 s slice: CW 160,000 = archive 160,000; Athena full count = drain count = **33,163,613** (34 events = 0.0001% vs the seeder's own count remain unattributed — see Limitations) |
| `FilterLogEvents` semantics | `endTime` verified **inclusive** with a live probe (the drain's window math depends on it) |
| 14-day PutLogEvents rejection | confirmed live (`tooOldLogEventEndIndex`) — the restore design constraint is real |
| Retention gate | `PutRetentionPolicy(1 day)` applied through the coverage gate on the real API |
| Athena | DDL + `count(*)` + `LIKE` query ran against the real archive; partition pruning scanned 1.68 GB / 78.6 MB respectively |

Total experiment cost: ~$2.60 (5 GiB × $0.50 ingest + cents of S3/Athena).

## No lock-in: your data is plain zstd

Data objects are concatenated **standard RFC 8878 zstd frames** — not a
custom container. If S4 Logs disappears tomorrow:

```console
aws s3 cp s3://bucket/s4logs/data/.../1781042400000-000000.jsonl.zst - | zstd -dc | head
```

works, today (this exact property is asserted in the E2E suite, including
after deliberately deleting the index sidecar — sidecars only make reads
*fast*, they are never required). This is one notch stronger than S4
proper, whose S4F2 container needs the ~1k-LOC Apache-2.0 `s4-codec`
decoder; S4 Logs objects need nothing at all.

Query the archive in place with Athena (Hive-style partitions, zstd
detected by the `.zst` extension):

```sql
CREATE EXTERNAL TABLE s4logs_archive (
  `timestamp` bigint,
  stream string,
  message string,
  ingestion_time bigint,
  event_id string
)
PARTITIONED BY (account string, loggroup string, dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://YOUR_BUCKET/s4logs/data/';

MSCK REPAIR TABLE s4logs_archive;

SELECT from_unixtime(`timestamp` / 1000) AS t, stream, message
FROM s4logs_archive
WHERE dt = '2026-06-09' AND message LIKE '%ERROR%'
LIMIT 100;
```

**Verified on real Athena** (2026-06-10): a per-log-group variant of this
DDL (table `LOCATION` at the `loggroup=` level, `ADD PARTITION` for the
`dt=` directory) ran against a real 41-object / 1.6 GiB archive —
`count(*)` returned exactly the drained record count, and a `LIKE` query
with partition pruning scanned only 78.6 MB. Note for the `MSCK REPAIR`
form above: log group names are percent-encoded in the `loggroup=`
partition values (`%2Faws%2Flambda%2Ffoo`), so prefer explicit
`ADD PARTITION` or partition projection if your tooling mangles `%`.

## Restore and the 14-day PutLogEvents constraint

The CloudWatch `PutLogEvents` API **rejects events older than 14 days**
(or older than the group's retention). Restoring 90-day-old logs to
CloudWatch *with their original timestamps* is therefore impossible — for
anyone, not just us. S4 Logs handles this honestly:

- **Primary restore path is local**: `s4logs restore --to-stdout / --to-file`
  emits raw JSONL. Combined with `s4logs grep`, this covers most
  investigations.
- `--to-log-group` is provided but ingests events at the **current time**,
  wrapping each message as
  `{"original_timestamp":…,"original_stream":"…","message":"…"}` so Logs
  Insights can still filter on `original_timestamp`. `--raw` disables the
  wrap and then only events newer than 14 days will be accepted.

## Limitations (read before deploying)

- **SigV4 verification is opt-in and single-key.** `--auth-mode sigv4`
  verifies incoming signatures against one static key pair — enough for
  agents/SDKs, but there is no IAM integration, no session tokens, no
  presigned URLs. Default remains no verification: then run it behind TLS
  and a network boundary (security group / private subnet / mTLS mesh).
- **Durability is opt-in via `--wal-dir`.** With it, events are fsynced
  before the ack and replayed on restart (at-least-once: duplicates are
  possible after a crash, never silent loss). Without it, a crash loses up
  to one flush window per (group, day) buffer. `both` routing keeps a CW
  copy if you need belt and braces.
- **Late-arriving events can be missed by a too-eager drain.** CloudWatch
  indexes backdated events with a lag we measured at **3–5.5 minutes** (and
  agents may deliver much later). A window drained before its stragglers
  arrive gets a manifest, and manifests are skipped on re-runs — so those
  events never reach the archive. Mitigations: drain data that is at least
  hours old (the normal archival pattern satisfies this trivially); to
  repair a suspect window, **delete its manifest and re-drain** — object
  names are deterministic, so this is safe and was verified live. An
  `event_id`-based reconcile mode is on the roadmap.
- **Drain speed is latency-bound, not quota-bound.** A real 5 GiB drain
  took 94.6 min at `--concurrency 4` with zero throttling and $0 in API
  charges; FilterLogEvents page latency is the bottleneck, so TB-scale
  initial drains are a *time* budget (raise `--concurrency`), not a money
  one.
- **One unattributed 0.0001% count gap.** In the controlled experiment the
  archive matched CloudWatch exactly on every cross-check we ran
  (slice-level FilterLogEvents counts, Athena vs drain totals), but 34 of
  33,163,647 seeder-counted events (1×10⁻⁶) were never observed in
  CloudWatch reads. We could not attribute them (seeder accounting vs CW
  ingestion); recorded here rather than rounded away.
- **Single account per deployment (P1).** AWS Organizations multi-account
  drain is part of the planned commercial tier, not the OSS core.
- **Compression numbers above are synthetic** except where explicitly
  labeled (s4's real nginx corpus; the real-AWS experiment used synthetic
  data too and is labeled as such).
- **Cost Explorer confirmation pending.** The experiment's usage-side
  numbers are in; the matching AWS bill line items materialize with ~24 h
  lag and will be attached when available.
- **Restore to CloudWatch cannot reproduce original timestamps** older than
  14 days (AWS API constraint — see previous section).

## IAM

Minimal-permission policy documents ship in [`docs/`](docs/), one per role
(replace `YOUR_BUCKET` / `YOUR_ACCOUNT_ID`, and scope `log-group:*` down to
the groups you actually drain):

| File | Grants | Used by |
|---|---|---|
| [`docs/iam-policy-drain.json`](docs/iam-policy-drain.json) | `logs:FilterLogEvents`, `logs:DescribeLogGroups`, `logs:PutRetentionPolicy`; S3 Put/Get + prefix-scoped List | `s4logs drain` |
| [`docs/iam-policy-gateway.json`](docs/iam-policy-gateway.json) | S3 Put/Get + prefix-scoped List; **optional** `logs:PutLogEvents` / `logs:CreateLogGroup` / `logs:CreateLogStream` (delete that statement if no route targets CloudWatch) | `s4logs serve` |
| [`docs/iam-policy-restore.json`](docs/iam-policy-restore.json) | S3 Get + prefix-scoped List; `logs:PutLogEvents` / `logs:Create*` for `--to-log-group` | `s4logs restore` |

## Observability

The gateway serves `/health` (unconditional 200), `/ready` (503 until the
sink probe — a cached `ListObjectsV2 max-keys=1` — succeeds and the last
flush did) and Prometheus `/metrics`: `s4logs_events_total{action=}`,
`s4logs_flush_total`, `s4logs_flush_bytes_total{kind=raw|compressed}`,
`s4logs_cw_passthrough_errors_total`, `s4logs_backpressure_total`, and the
WAL family (`s4logs_wal_appends_total`, `s4logs_wal_replayed_events_total`,
`s4logs_wal_torn_lines_total`, `s4logs_wal_fsync_errors_total`). Logs via
`tracing` (`--log-format json|pretty`).

## Development

```console
cargo test --workspace          # unit + proptest (no network)
./scripts/e2e.sh                # LocalStack E2E (docker compose)
./scripts/soak.sh               # sustained-load soak (S4LOGS_SOAK_SECONDS, default 60)
cargo test -p s4logs-e2e --release -- --ignored bench --nocapture   # bench table
cargo test -p s4logs-core --test fuzz_bolero   # fuzz targets, test mode
cargo bench -p s4logs-core -- --quick          # criterion benches
docker build -t s4logs .                        # 176 MB runtime image
```

Workspace crates: `s4logs-core` (format + S3 layout + read path),
`s4logs-drain` (Mode A), `s4logs-gateway` (Mode B), `s4logs-cli` (binary),
`s4logs-e2e` (LocalStack suites). Format and behavior contract:
[`DESIGN.md`](DESIGN.md).

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).

Amazon CloudWatch, Amazon S3 and AWS are trademarks of Amazon.com, Inc. or
its affiliates. S4 Logs is not affiliated with, endorsed by, or sponsored
by Amazon; CloudWatch is referenced solely to describe interoperability.
