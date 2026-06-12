# Security Policy

## Reporting a vulnerability

S4 Logs runs in front of paid AWS accounts: the Mode B gateway accepts
untrusted `PutLogEvents` request bodies over the network, and both modes
parse untrusted byte streams (zstd frame bodies, `.s4index` / `.s4lts`
sidecars, window manifests) read back from S3. More importantly, Mode A
can **delete** CloudWatch Logs data (via `PutRetentionPolicy`) after it
archives. We take security seriously and welcome coordinated disclosure.

**Please do not open a public GitHub issue for vulnerabilities.**

Report privately via either channel:

- **GitHub Security Advisories** — "Report a vulnerability" on the
  [Security tab](https://github.com/abyo-software/s4-logs/security/advisories/new)
  (preferred — keeps the report and fix coordinated in one place).
- **Email** `security@abyo.net` — include a description, steps to
  reproduce (or a minimal proof-of-concept), affected versions/commits,
  and any suggested mitigation.

We aim to acknowledge within **3 business days** and provide an initial
assessment within **7 days**. Critical issues will be fixed and disclosed
within **30 days**; lower-severity issues within **90 days**.

We follow a coordinated disclosure model and credit reporters in the
security advisory unless requested otherwise.

## Supported versions

S4 Logs is **pre-1.0** (currently v0.4.x). While we are pre-1.0, security
fixes land on the **latest released minor only** — the `main` branch always
carries the most recent fixes, and we expect you to track the latest
release. There is no backport window before v1.0.

Once v1.0 ships (the on-disk format + crate API freeze; see the
[Unreleased] section of [CHANGELOG.md](CHANGELOG.md)), this policy moves to
a rolling window of two minors on the v1.x line — security fixes for the
latest minor plus the previous minor, with patch releases cut from the
affected minor.

## Threat model

S4 Logs is a cost tool, not a security boundary. It assumes:

- The **archive S3 bucket** is trusted (you own it / IAM-controlled), and
  the AWS credentials it runs under are scoped to the minimal IAM policies
  shipped in [`docs/`](docs/).
- The **CloudWatch Logs API** it talks to is the genuine AWS endpoint.
- The **`.s4index` / `.s4lts` sidecars** and the **zstd frame bodies** read
  back from S3 may have been tampered with by anyone with write access to
  the backend bucket — the read path must still fail safely (no OOM, no
  out-of-bounds read, no silent corruption). Sidecars are never *required*
  to read an object: they only make reads fast, and a missing/garbage
  sidecar degrades to a full streaming decode, never to wrong bytes.
- The **window manifests** that gate retention shortening may be tampered
  with — same constraint.

### Deletion safety (the load-bearing property)

Mode A can shrink CloudWatch retention, which **permanently deletes** log
data older than the new retention. The design makes that fail-closed:

- **Archive first, shrink retention after.** `PutRetentionPolicy` is gated
  on complete, verified manifest coverage of every window older than the
  proposed cutoff. **Any gap means nothing happens** — the retention call
  is not issued.
- **Verified before counted.** A window's data object is PUT with a
  `CRC32C` checksum (SDK-enforced end-to-end), and the manifest that proves
  the window is archived is written only after the data and its sidecars
  land. The retention gate trusts manifests, and manifests only exist for
  verified writes.
- **Opt-in.** Retention shortening requires `--apply-retention`; the
  default is report-only (it prints the proposal and exits without
  touching anything).

### Gateway authentication

The Mode B gateway's SigV4 verification is **opt-in and default-off**
(`--auth-mode sigv4`, one static key pair). The default is no signature
verification, which is safe **only** behind a network boundary (TLS +
security group / private subnet / mTLS mesh) — this is stated in the
README Limitations and is the operator's responsibility. There is no IAM
integration, no session tokens, and no presigned-URL support; multi-tenant
auth is out of scope for the OSS core.

S4 Logs does **not** provide:

- End-to-end encryption of the archive (use `SSE-S3` / `SSE-KMS` on the
  backend bucket).
- Authentication beyond the optional single-key SigV4 check above.
- Multi-account / AWS Organizations isolation (planned commercial tier).

## Fuzz & audit posture

Untrusted-input parsers carry property/fuzz coverage:

- **`proptest` roundtrips** on every format boundary in `s4logs-core`
  (records → chunk → body + sidecars → decode → records), run on every
  `cargo test --workspace`.
- **`cargo-bolero` libfuzzer targets** over the untrusted-input surfaces —
  the zstd/sidecar read path (`crates/s4logs-core/tests/fuzz_bolero.rs`)
  and the gateway's AWS-JSON request decoder
  (`crates/s4logs-gateway/tests/fuzz_bolero.rs`). They run as ordinary
  tests in CI (`cargo test --test fuzz_bolero`) and coverage-guided under
  `cargo-bolero` with the dedicated `[profile.fuzz]`.
- **Decompression-bomb cap.** The read path caps zstd output at the
  sidecar's `original_size + 1024` bytes, so a tampered sidecar or frame
  cannot drive unbounded allocation (the same discipline as s4's
  `cpu_zstd`).
- **Workspace lints:** `unsafe_code = "deny"` and
  `clippy::unwrap_used = "deny"` across the workspace (tests opt out per
  module with an explicit `#[allow]`).

## See also

- [LICENSE](LICENSE) — Apache-2.0
- [NOTICE](NOTICE) — third-party attributions
- [CONTRIBUTING.md](CONTRIBUTING.md) — development setup
