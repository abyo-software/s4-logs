# Changelog

All notable changes to S4 Logs will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] — 2026-06-13

First stable release. **The on-disk formats are now frozen for the 1.x
series** (DESIGN.md §14): the JSONL record schema, the S3 key layout, the
`S4LT` timestamp sidecar, the manifest JSON, and the reused `s4-codec` S4IX
index. Any 1.x release reads what any other 1.x release wrote; new fields
are added as optional only, and breaking changes wait for a 2.0 that keeps a
1.x read path. The CLI subcommand set is the SemVer-stable command surface;
output text and metric names remain implementation detail.

No functional change from 0.4.3 — 1.0.0 is the stability commitment plus the
release-readiness work below.

### Added

- **Format-freeze contract** ([DESIGN.md §14](DESIGN.md)), surfaced in the
  README and in code-doc markers on the format modules.
- **Governance docs**: SECURITY.md, CONTRIBUTING.md, CODE_OF_CONDUCT.md, and
  this changelog.
- **Supply-chain gate**: `deny.toml` + a `cargo deny check` CI job. The
  rustls-webpki 0.101.7 advisories from the AWS SDK's legacy connector are
  documented-and-ignored with justification (the active HTTPS client uses
  the patched rustls 0.23 / webpki 0.103.13; the legacy connector is
  compiled but never constructed).

### Quality / verification

- **Mutation testing** (cargo-mutants) pass; closed the surfaced test gaps
  in `TimeRange::overlaps`, `coalesce_spans`, the decompression bomb-cap
  boundary, and the `ChunkWriter` accessors.
- **Mode B (gateway) + restore validated against real AWS** (2026-06-12),
  previously LocalStack-only: real-S3 buffering, `both`/`s3` routing
  isolation, CloudWatch passthrough, real-S3 grep range reads, and
  `restore --to-log-group` with the 14-day wrap. (Mode A was validated in
  the 0.x experiment, 2026-06-10.)
- **2-hour sustained soak**: 715,817 requests / 7,158,170 events acked / 0
  failures, all events durable, RSS delta 2.3 MiB (no leak). The 24 h
  Marketplace-gate soak uses the same harness on a self-hosted runner.

## [0.4.3] — 2026-06-12

Review convergence. A `gateway flush-failure recovery` fix plus the
remaining findings from review rounds 2–5 (over the v0.4.2 fix wave).

### Fixed

- Gateway flush-failure recovery: a failed S3 flush no longer drops the
  buffered events on the floor — the buffer is retained so the next flush
  (or graceful-shutdown drain) retries them, instead of acknowledging
  events that never reached S3.
- Remaining review-round 2–5 findings resolved to convergence over the
  v0.4.2 fix wave.

## [0.4.2] — 2026-06-12

Codex CLI review pass. Six findings the prior self-review missed, fixed in
one wave.

### Fixed

- Six findings from an external Codex CLI review of the v0.4.x surface
  (the project's own self-review had missed all six). Bug fixes only — no
  format change, no new flags.

## [0.4.1] — 2026-06-11

Packaging fix.

### Fixed

- aarch64 release packaging: strip the binary at link time rather than via
  a host `strip`, which cannot strip a cross-compiled aarch64 ELF on an
  x86_64 builder (the prior release shipped an unstripped aarch64 binary).

## [0.4.0] — 2026-06-11

`grep`/`restore` reach feature parity with `drain`/`report` (glob + `--all`),
`report` prices each object by its recorded storage class, and the
sidecar-missing read path is now streaming.

### Added

- **`grep` / `restore` accept a glob or `--all`**, symmetric with
  `drain`/`report`. An exact `--log-group` reads S3 only (no CloudWatch
  call), preserving the read-only-S3 property for the common single-group
  case; a glob or `--all` first enumerates groups via `DescribeLogGroups`.
  Results stay globally timestamp-ordered across groups via one shared
  k-way merge. `restore --to-log-group` funnels every source group into the
  single target and records `original_log_group` in the wrap JSON.
- **Per-class `report` pricing.** `ManifestObject` gains an optional
  `storage_class` field (canonical S3 class label, omitted for
  Standard/unset). `s4logs report` prices each object by its recorded class
  (STANDARD $0.023, STANDARD_IA $0.0125, GLACIER_IR $0.004 per GiB·mo,
  us-east-1 list, storage only) and shows a per-class breakdown when a group
  mixes classes. Objects from pre-storage-class manifests are billed at
  Standard with the count noted. Additive: older manifests read as `None`.
- README installer/docs: the one-line `curl | sh` installer (sha256-verified
  static-musl tarballs), and the Athena partition-projection DDL (the
  `injected` projection type for the percent-encoded `loggroup` partition).

### Changed

- **Streaming sidecar-missing fallback.** When a sidecar is absent, the read
  path no longer decodes the whole object into one `Vec`; it streams
  (`zstd` `read::Decoder` + line-buffered reader), so peak working set is
  one line + the decoder window regardless of object size. The
  decompression-bomb cap is kept as a running ceiling.

## [0.3.0] — 2026-06-10

`s4logs plan`, Glacier IR, reconcile, sharded drain, progress — the
day-to-day operating commands.

### Added

- **`s4logs plan` / `--all`** — read-only savings diagnostic. Uses
  `DescribeLogGroups` `storedBytes` (CloudWatch's gzip-billed storage bytes)
  plus the `IncomingBytes` metric for ingest projection; needs no bucket or
  account argument and writes nothing. Prints current monthly cost per log
  group with projected Mode A (S3 Standard + Glacier IR) and Mode B savings,
  every assumption stated in the footer. Adds `aws-sdk-cloudwatch` for the
  `IncomingBytes` metrics.
- **Storage class for data objects** (`--storage-class standard |
  standard-ia | glacier-ir`, default `standard`). Applied to **data objects
  only** — the `.s4index`/`.s4lts` sidecars and window manifests stay S3
  Standard (Glacier IR's 128 KiB minimum-billable size + its retrieval
  pricing on the hot query-planning path would cost more there).
- **`s4logs drain --reconcile`** — late-arrival repair. Re-pages manifested
  windows, dedups against the archive by event identity, and appends only
  the missing events. Reconcile-added objects are named
  `{window_start_ms}-r{attempt:02}{seq:04}` (no collision with the base
  `{seq:06}`), and the manifest gains optional `reconciled_at_ms` /
  `reconciled_added` fields (byte-compat; a clean reconcile rewrites
  nothing).
- **Sharded drain** (`--shard-streams N`) — pages a window's streams in
  parallel shards for big backlogs. Object names stay deterministic; with
  `N > 1` the object *content* is no longer byte-deterministic (shard pages
  interleave on completion order) but the record set and manifest-skip
  idempotency are unchanged. `--progress` for a live progress display.

### Changed

- **Cost model corrected**: CloudWatch bills archived storage on
  **gzip-level-6** compressed bytes, not raw — the estimator and the README
  economics were fixed to compare CW-gzip against S3-zstd honestly.

## [0.2.0] — 2026-06-10

Productization wave: durability, optional auth, sorted/searchable reads,
the `report` command, and multi-group drain.

### Added

- **Gateway WAL** (`--wal-dir DIR`, opt-in) — append-only per-(group, day)
  segment files; events are fsynced before the `PutLogEvents` ack and
  replayed on restart (at-least-once: duplicates possible after a crash,
  never silent loss). Torn tail lines are warned-and-skipped on replay.
- **Optional SigV4 verification** (`--auth-mode none|sigv4`, default
  `none`) — verifies incoming signatures against one static key pair
  (canonical-request reconstruction, ±15 min clock skew, UNSIGNED-PAYLOAD +
  `x-amz-content-sha256`). Failures return 403
  `InvalidSignatureException`; `/health` `/ready` `/metrics` are exempt.
- **Real `/ready`** (cached `ListObjectsV2 max-keys=1` sink probe + last
  flush), paginated `DescribeLogGroups` / `DescribeLogStreams`, and
  gateway backpressure.
- **Time-ordered `grep`** — k-way merge across chunks for ascending
  timestamp output (stable on `(stream, input order)` for equal
  timestamps), with per-dt prefix listing to narrow the LIST range.
- **Streaming `restore --to-log-group`** — streams chunks in time order
  while building batches (bounded memory) instead of sorting everything in
  memory first.
- **`s4logs report`** (`--log-group X | --all`) — aggregates manifests
  (archived records / raw / compressed bytes / estimated monthly savings /
  window coverage). Reads manifests only — zero CloudWatch calls, zero S3
  data reads.
- **Multi-group drain** — `--log-group` accepts a globset glob and `--all`
  enumerates the account via `DescribeLogGroups`; each group is an
  independent job (failed groups are skipped and reported, exit code 1).
- Infra: `cargo-bolero` fuzz targets over the untrusted-input surfaces, a
  `[profile.fuzz]` for libfuzzer runs, criterion benches, a soak harness, a
  Dockerfile, and release CI.

### Fixed

- Gateway shutdown data-loss race found by the soak harness: the
  cooperative flush sweeper is now stopped before the final drain so a
  SIGTERM cannot lose a buffer that was mid-sweep.

### Changed

- Pin `s4-codec` to the public git tag `v1.0.0` — the S4IX index format is
  frozen on the s4 1.x line, so S4 Logs depends on a tagged release rather
  than a moving branch.

## [0.1.0] — 2026-06-09

Initial P1 OSS core. The two modes, the open S3 layout, the read path, and
the LocalStack E2E suite — built to the [DESIGN.md](DESIGN.md) contract.

### Added

- **Workspace + DESIGN contract** — five crates (`s4logs-core`,
  `s4logs-drain`, `s4logs-gateway`, `s4logs-cli`, `s4logs-e2e`) with the
  on-disk format / API / crate boundaries fixed in `DESIGN.md`.
- **`s4logs-core`** — the open S3 layout (separate `data/` / `index/` /
  `manifest/` prefixes), records as JSONL inside concatenated standard
  RFC 8878 zstd frames (zstd-3, ~4 MiB frames, XXH64 content checksum), the
  `.s4index` byte-range index (reused unchanged from `s4-codec`), the new
  `S4LT` (`.s4lts`) per-frame timestamp-range sidecar, and `ObjectStore`
  (CRC32C PUT, range GET, paginated list). Format-boundary proptests.
- **Mode A — `s4logs-drain`** — windowed `FilterLogEvents` drain (work unit
  = (log group, UTC-aligned 1h window)), manifest-driven idempotency
  (manifested windows are skipped on re-run), and a **fail-closed**
  retention gate: `PutRetentionPolicy` is issued only when every older
  window has a verified manifest, gated behind `--apply-retention`
  (report-only by default). `--dry-run` does API reads only.
- **Mode B — `s4logs-gateway`** — a `PutLogEvents`-compatible AWS-JSON 1.1
  endpoint (`PutLogEvents`, `CreateLogGroup`/`CreateLogStream`,
  `DescribeLogGroups`/`DescribeLogStreams`), first-match TOML routing
  (`s3` / `cloudwatch` / `both` / `drop`), buffered zstd flush, CloudWatch
  passthrough, and `/health` `/ready` `/metrics`. SigV4 not verified in
  this cut (network-boundary deployment assumed).
- **`s4logs-cli`** — the `drain` / `grep` / `restore` / `serve` subcommands.
- **LocalStack E2E** (`s4logs-e2e`) — Mode A (inject → drain → verify S3 →
  grep) and Mode B (gateway PutLogEvents via the AWS SDK → flush → verify
  S3) suites, plus the measured cost tables and the minimal-permission IAM
  policy documents.
