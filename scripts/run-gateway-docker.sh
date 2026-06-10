#!/usr/bin/env bash
# Example: run the Mode B gateway from the Docker image.
#
#   docker build -t s4logs .
#   S4LOGS_BUCKET=my-bucket S4LOGS_ACCOUNT=123456789012 ./scripts/run-gateway-docker.sh
#
# Then point any CloudWatch Logs client at it (endpoint override):
#   aws logs create-log-group  --endpoint-url http://localhost:8080 --log-group-name /demo
#   aws logs create-log-stream --endpoint-url http://localhost:8080 --log-group-name /demo --log-stream-name s1
#   aws logs put-log-events    --endpoint-url http://localhost:8080 --log-group-name /demo \
#     --log-stream-name s1 --log-events timestamp=$(date +%s000),message=hello
#
# AWS credentials/region come from the standard env chain; for LocalStack add
#   -e AWS_ENDPOINT_URL=http://host.docker.internal:4566
set -euo pipefail

: "${S4LOGS_BUCKET:?set S4LOGS_BUCKET (S3 bucket for the s4logs layout)}"
: "${S4LOGS_ACCOUNT:?set S4LOGS_ACCOUNT (account partition label)}"

exec docker run --rm -p 8080:8080 \
  -e S4LOGS_BUCKET \
  -e S4LOGS_ACCOUNT \
  -e S4LOGS_PREFIX \
  -e AWS_REGION \
  -e AWS_ENDPOINT_URL \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  s4logs serve --listen 0.0.0.0:8080 --log-format json "$@"
