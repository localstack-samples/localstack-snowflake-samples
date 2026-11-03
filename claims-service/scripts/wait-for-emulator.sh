#!/usr/bin/env bash
# Wait until the Snowflake-Next emulator answers a SQL statement.
#
# Copy this file into a sample as scripts/wait-for-emulator.sh (samples stay
# self-contained). Usage: scripts/wait-for-emulator.sh [timeout-seconds]
# Reads SNOWFLAKE_PORT (default 4566). Readiness is checked from the host.
set -euo pipefail

port="${SNOWFLAKE_PORT:-4566}"
timeout="${1:-120}"
url="https://snowflake.localhost.localstack.cloud:${port}"
deadline=$((SECONDS + timeout))

until curl -fsS --compressed -o /dev/null -X POST "${url}/api/v2/statements" \
    -H 'Authorization: Bearer local' -H 'Content-Type: application/json' \
    -H 'Accept: application/json' -d '{"statement":"SELECT 1"}' 2>/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "Emulator at ${url} did not become ready within ${timeout}s." >&2
    echo "Check 'docker compose logs snowflake' (is LOCALSTACK_AUTH_TOKEN valid? is port ${port} free?)." >&2
    exit 1
  fi
  sleep 2
done
echo "Emulator ready at ${url}"
