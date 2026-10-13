#!/bin/bash
# Seed test_table with two rows and run the Soda scan. checks.yml is written so
# that the row count check fails on purpose ("fewer than 2 rows") and the NULL
# check passes, so the scan must report exactly that.
set -euo pipefail

make init

set +e
output=$(make scan 2>&1)
set -e
echo "$output"

if [[ "$output" != *"Dataset is unreasonably small [FAILED]"* ]]; then
  echo "Test failed: the row count check did not fail on the 2-row table"
  exit 1
fi
echo "PASS: the row count check failed as written"

if [[ "$output" != *"No NULL values [PASSED]"* ]]; then
  echo "Test failed: the NULL check did not pass"
  exit 1
fi
echo "PASS: the NULL check passed"

echo "Test passed: Soda scanned the local Snowflake table"
