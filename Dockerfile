# S4 Logs — multi-stage Dockerfile (same shape as s4's CPU image).
#
# Build:
#   docker build -t s4logs .
# Run the Mode B gateway (see also scripts/run-gateway-docker.sh):
#   docker run --rm -p 8080:8080 \
#     -e S4LOGS_BUCKET=my-bucket -e S4LOGS_ACCOUNT=123456789012 \
#     -e AWS_REGION=us-east-1 \
#     -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
#     s4logs serve --listen 0.0.0.0:8080
# One-shot commands (drain / grep / restore / report) work the same way:
#   docker run --rm -e S4LOGS_BUCKET=... -e S4LOGS_ACCOUNT=... s4logs \
#     grep 'ERROR' --log-group /aws/lambda/foo --from 2026-06-01T00:00:00Z --to 2026-06-02T00:00:00Z

# ---- builder ----
FROM rust:1-slim-bookworm AS builder
WORKDIR /usr/src/s4logs

# git: the s4-codec dependency is a git dep (tag v1.0.0) — cargo shells out
# to git to fetch it. pkg-config/build-essential: zstd-sys + aws-lc-sys.
RUN apt-get update && apt-get install -y --no-install-recommends \
    git pkg-config build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p s4logs-cli --bin s4logs

# ---- runtime ----
FROM debian:bookworm-slim

# OCI labels (release/inspection metadata).
LABEL org.opencontainers.image.title="s4logs" \
      org.opencontainers.image.description="S4 Logs — CloudWatch Logs cost offloader: drain or bypass log groups into zstd-compressed S3." \
      org.opencontainers.image.source="https://github.com/abyo-software/s4-logs" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="abyo software"

# `wget` is consumed by the HEALTHCHECK (debian-slim ships neither wget nor
# curl — without it the probe exits 127 and compose marks the container
# unhealthy even when /health is fine). `ca-certificates` is required by the
# rustls HTTPS path to real S3/CloudWatch endpoints.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/s4logs/target/release/s4logs /usr/local/bin/s4logs
COPY LICENSE NOTICE /usr/share/doc/s4logs/

# Run as non-root
RUN useradd -r -u 10001 s4logs
USER s4logs

EXPOSE 8080
# Only meaningful for `serve` (the default CMD); one-shot commands override
# CMD and exit before the first probe fires.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1

ENTRYPOINT ["/usr/local/bin/s4logs"]
# Default: Mode B gateway. --bucket/--account come from S4LOGS_BUCKET /
# S4LOGS_ACCOUNT env vars (clap env fallback), credentials/region from the
# standard AWS env chain.
CMD ["serve", "--listen", "0.0.0.0:8080", "--log-format", "json"]
