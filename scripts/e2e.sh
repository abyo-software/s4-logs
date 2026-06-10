#!/usr/bin/env bash
# Run the full LocalStack E2E suite: bring LocalStack up, wait for health,
# run every #[ignore] test in s4logs-e2e, tear LocalStack down.
set -euo pipefail
cd "$(dirname "$0")/.."

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

# --test-threads 1: suites create/seed CloudWatch + S3 state and the gateway
# test installs the process-global Prometheus recorder; serial keeps the
# output readable and avoids LocalStack contention on small machines.
cargo test -p s4logs-e2e -- --ignored --test-threads 1
