#!/bin/bash
# Load the bundled transactions into Snowflake with 00_load_tables.py, in the
# sample's Docker image, and check the three tables it creates.
set -euo pipefail

[ -f .env.localstack ] || cp .env.sample .env.localstack
make load-tables DOCKER_RUN_FLAGS=

count() {
  snow sql -c local --format json -q "SELECT COUNT(*) AS N FROM test.public.$1" | jq -r '.. | objects | .N? // empty' | head -1
}

# the CSV holds 10 transactions from 9 customers on 10 terminals
for expected in CUSTOMER_TRANSACTIONS_FRAUD:10 CUSTOMERS:9 TERMINALS:10; do
  table=${expected%:*}
  rows=$(count "$table")
  if [ "$rows" != "${expected#*:}" ]; then
    echo "Test failed: $table has ${rows:-no} rows, expected ${expected#*:}"
    exit 1
  fi
  echo "PASS: $table has $rows rows"
done

echo "Test passed: the transactions were loaded into Snowflake"
