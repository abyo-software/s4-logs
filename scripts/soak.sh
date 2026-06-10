#!/usr/bin/env bash
# Soak run wrapper: LocalStack up → soak test (release build) → LocalStack
# down. Duration in seconds as $1 (default 60); request rate via
# S4LOGS_SOAK_RPS (default 50, see crates/s4logs-e2e/tests/soak.rs).
#
#   ./scripts/soak.sh           # 60 s smoke
#   ./scripts/soak.sh 600       # nightly-CI-sized soak
#   ./scripts/soak.sh 86400     # 24 h Marketplace soak
set -euo pipefail
cd "$(dirname "$0")/.."

SOAK_SECONDS="${1:-${S4LOGS_SOAK_SECONDS:-60}}"

docker compose up -d localstack
trap 'docker compose down -v' EXIT

echo "waiting for LocalStack health..."
for _ in $(seq 1 60); do
  if curl -fsS http://localhost:4566/_localstack/health >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS http://localhost:4566/_localstack/health >/dev/null \
  || { echo "LocalStack did not become healthy in 60s" >&2; exit 1; }

# --release: long soaks should measure the shipped code path, and debug-build
# zstd would bottleneck the load generator before the gateway.
S4LOGS_SOAK_SECONDS="$SOAK_SECONDS" \
  cargo test -p s4logs-e2e --release --test soak -- --ignored --nocapture
