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
```

### Run the bypass gateway (Mode B)

```console
s4logs serve --listen 0.0.0.0:8080 \
  --bucket my-archive-bucket --prefix s4logs --account 123456789012 \
  --routing-config routing.toml --flush-bytes 8388608 --flush-interval 60s
```

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
# byte-range sidecar turns the survivors into S3 Range GETs.
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

Per-GB economics (AWS list prices, us-east-1):

| | CloudWatch as-is | Mode A (drained) | Mode B (bypassed) |
|---|---|---|---|
| ingest | $0.50/GB | $0.50/GB (already paid — not recoverable) | **$0 (avoided)** + negligible S3 PUTs |
| storage | $0.03/GB·mo × retention | S3 $0.023/GB·mo ÷ compression (10–155×) ≈ **$0.0002–0.002/GB·mo** | same |
| reduction | — | ~90–99% of the **storage** line | **~90%+ of the whole bill** (ingest dominates) |

Worked example, 1 TiB of logs: CloudWatch storage **$30.72/mo** → S3 at a
conservative 10× compression **$2.36/mo**, at s4's measured 155× (nginx)
**$0.15/mo**. In Mode B the same TiB also skips **$512** of one-time ingest.

**Break-even honesty** (same policy as S4's README): if your CloudWatch
Logs bill is **under $500/month, the OSS version is all you need** — and
even that may be more moving parts than your problem deserves. Mode A pays
off for long-retention / high-volume accounts; Mode B pays off when ingest
dominates (that is most accounts). Also budget for the drain itself: the
FilterLogEvents read path is an API cost we have **not yet measured at
scale** (see Limitations).

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

**Honesty note**: this DDL is syntactically standard Hive/Athena and the
underlying objects verifiably decode as plain zstd JSONL (proven against
LocalStack + the `zstd` CLI in CI), but it has **not yet been executed
against a real Athena deployment**. If you run it for real before we do,
an issue report is welcome.

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

- **No SigV4 validation on the gateway (P1).** The `Authorization` header is
  ignored. Run it behind TLS and a network boundary (security group /
  private subnet / mTLS mesh). Static-credential validation is planned
  (roadmap, P3).
- **In-memory buffering in the gateway.** Events routed to S3 sit in
  process memory until a flush (8 MiB / 60 s / shutdown, configurable). A
  crash loses up to one flush window per (group, day) buffer. A WAL is on
  the roadmap. `both` routing keeps a CW copy if you need belt and braces.
- **Drain API cost is not yet measured at scale.** FilterLogEvents is a
  metered, account-quota'd API; TB-scale initial drains will take time and
  money we have not yet quantified (planned before any commercial claims —
  see proposal §10). `--dry-run` reads the same API, so it estimates
  savings but not drain cost.
- **E2E coverage is LocalStack-only so far.** Both modes, the read path,
  idempotent re-drain, the retention gate and sidecar-loss recovery run
  green against LocalStack (S3 + CloudWatch Logs) in CI; none of it has
  been pointed at a real AWS account yet (roadmap).
- **Single account per deployment (P1).** AWS Organizations multi-account
  drain is part of the planned commercial tier, not the OSS core.
- **Compression numbers above are synthetic** except where explicitly
  labeled as s4's measured corpus.
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
sink is reachable and the last flush succeeded) and Prometheus `/metrics`:
`s4logs_events_total{action=}`, `s4logs_flush_total`,
`s4logs_flush_bytes_total{kind=raw|compressed}`,
`s4logs_cw_passthrough_errors_total`. Logs via `tracing`
(`--log-format json|pretty`).

## Development

```console
cargo test --workspace          # unit + proptest (no network)
./scripts/e2e.sh                # LocalStack E2E (docker compose)
cargo test -p s4logs-e2e --release -- --ignored bench --nocapture   # bench table
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
