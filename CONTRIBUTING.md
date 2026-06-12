# Contributing to S4 Logs

Thanks for considering a contribution! S4 Logs is a young project and we
welcome issues, bug reports, code, docs, and ideas.

## Code of Conduct

By participating, you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree your contributions will be licensed under
**Apache License 2.0** (the same license as the project). No separate CLA
required — the [Apache 2.0 License in `LICENSE`](LICENSE) is sufficient
under the [Inbound = Outbound](https://opensource.guide/legal/#which-open-source-license-is-appropriate-for-my-project)
convention.

## The DESIGN.md contract

[`DESIGN.md`](DESIGN.md) is the binding contract for S4 Logs: the on-disk
formats (the JSONL record schema, the S3 layout, the `S4LT` timestamp
sidecar, the manifest JSON shape), the crate boundaries, and the
write-order / verification discipline. **Format or crate-boundary changes
require a DESIGN.md amendment in the same PR** — append a dated amendment
section (see the existing Wave 3/4/5 amendments) rather than editing earlier
sections in place. Additive manifest fields must keep the byte-compat rule:
older manifests must still deserialize, reading the new field as `None`.

## Crate layout

The workspace is five crates, with strict boundaries (do not cross them):

| crate | responsibility |
|---|---|
| `s4logs-core` | record schema, zstd-multiframe chunk encode/decode, `S4LT` sidecar, S3 layout, `ObjectStore`, read path. Depends on `s4-codec` for the S4IX index **only**. |
| `s4logs-drain` | Mode A — windowed `FilterLogEvents` drain, manifest idempotency, fail-closed retention gate. |
| `s4logs-gateway` | Mode B — `PutLogEvents`-compatible HTTP API, routing, buffer/flush, CloudWatch passthrough, WAL, optional SigV4, observability. |
| `s4logs-cli` | the `s4logs` binary: `drain` / `grep` / `restore` / `serve` / `report` / `plan`. |
| `s4logs-e2e` | LocalStack E2E suites + the bench table. |

### The `s4-codec` git dependency

`s4logs-core` reuses the **S4IX** index format (`.s4index` sidecars) from
the [s4](https://github.com/abyo-software/s4) project via a git dependency
**pinned to a tag**:

```toml
s4-codec = { git = "https://github.com/abyo-software/s4", tag = "v1.0.0" }
```

S4 Logs uses S4IX **only** — it does **not** use s4's `S4F2` framed
container or multipart formats (S4 Logs data objects are plain concatenated
zstd frames, readable by `zstd -dc` with no S4 tooling). Bump the pinned
tag in a deliberate commit; don't point it at a moving branch.

## Development setup

```bash
git clone https://github.com/abyo-software/s4-logs
cd s4-logs
cargo build --workspace
cargo test --workspace          # unit + proptest, no network
```

Docker-backed E2E (needs a running Docker daemon — uses LocalStack for
S3 + CloudWatch Logs):

```bash
./scripts/e2e.sh                # LocalStack up → #[ignore] suite → down
./scripts/soak.sh               # sustained-load soak (S4LOGS_SOAK_SECONDS, default 60)
cargo test -p s4logs-e2e --release -- --ignored bench --nocapture   # bench table
```

Fuzzing — the bolero targets run two ways:

```bash
# as ordinary tests (what CI runs)
cargo test -p s4logs-core    --test fuzz_bolero
cargo test -p s4logs-gateway --test fuzz_bolero

# coverage-guided under cargo-bolero (uses the [profile.fuzz] in Cargo.toml)
cargo bolero test --engine libfuzzer -p s4logs-core    <target>
cargo bolero test --engine libfuzzer -p s4logs-gateway <target>
```

Targets cover the untrusted-input surfaces: the zstd/sidecar read path
(`s4logs-core`) and the AWS-JSON `PutLogEvents` request decoder
(`s4logs-gateway`).

## Coding conventions

- Format with `cargo fmt --all` (rustfmt, default settings).
- Lint with `cargo clippy --workspace --all-targets` — must be clean.
- The workspace lints `unsafe_code = "deny"` and
  `clippy::unwrap_used = "deny"`. There is no `unsafe` in S4 Logs; in tests,
  opt out of `unwrap_used` with an explicit per-module
  `#[allow(clippy::unwrap_used)]` (not on production code).
- Adding a parser / decoder / format boundary? Add a `proptest` roundtrip
  (records → chunk → body + sidecars → decode → records), and add a bolero
  target if it parses untrusted bytes.
- Don't bake wall-clock values (`Date.now`-style) into any format — only the
  manifest `completed_at_ms` may carry a wall clock.
- Comments in Japanese or English are both fine. README and public-facing
  docs should be English.

## Commit messages

Conventional-style prefixes encouraged but not required:

- `feat:` new features · `fix:` bug fixes · `test:` test-only ·
  `docs:` documentation · `refactor:` no-behavior-change restructuring ·
  `chore:` tooling/build/deps

One concise sentence summarizing the *why*; longer body for context if
useful.

## Pull request process

1. Fork → branch → push → PR against `main`.
2. CI must pass: `cargo fmt --all --check`, `cargo clippy` clean,
   `cargo test --workspace`, and the fuzz-as-test targets.
3. Adding a feature? Add a test. Touching a format or a crate boundary? The
   PR must include the matching DESIGN.md amendment.
4. We may suggest changes; large or contentious changes are best discussed
   in an issue first.

## What we like

- Bug reports with a minimal reproduction.
- Fuzz-target additions (bolero or proptest).
- Performance benchmarks (criterion preferred — see the bench in
  `s4logs-e2e`).
- Documentation improvements, especially the README and the Athena docs.
- Real-world deployment write-ups — great as blog posts to link from the
  README.

## What we'll push back on

- Changes to the on-disk format we emit (the zstd layout, the `S4LT`
  sidecar, the manifest schema, the S3 path layout) without thorough fuzz
  coverage and a DESIGN.md amendment with a migration plan.
- New runtime dependencies without strong justification (we keep
  `Cargo.lock` small).
- Anything that makes the archive un-readable without S4 tooling — data
  objects must stay plain `zstd -dc`-decodable; sidecars must stay
  optional (a read works without them, just slower).
- Features that couple S4 Logs to anything other than the documented AWS
  CloudWatch Logs / S3 surface.
