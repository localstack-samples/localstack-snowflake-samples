#!/bin/bash
# Run the Glue job against the Snowflake emulator, then check that it read the
# src_glue rows created by init.sf.sql and wrote the active ones to tgt_glue.
set -euo pipefail

SNOWFLAKE_PORT=${SNOWFLAKE_PORT:-4567}

job_logs() {
  local run_id
  run_id=$(cat .last-job-run-id 2>/dev/null || true)
  [ -n "$run_id" ] || return 0
  for group in /aws-glue/jobs/logs-v2 /aws-glue/jobs/output /aws-glue/jobs/error; do
    awslocal logs get-log-events --log-group-name "$group" --log-stream-name "$run_id" \
      --query 'events[*].message' --output text 2>/dev/null | tr '\t' '\n' || true
  done
}

rm -f .last-job-run-id
if ! ./deploy.sh; then
  echo "Test failed: the Glue job did not succeed"
  if [ -s .last-job-run-id ]; then
    awslocal glue get-job-run --job-name glue-job --run-id "$(cat .last-job-run-id)" \
      --query JobRun.ErrorMessage --output text || true
  fi
  job_logs | tail -150
  docker logs --tail 80 glue-localstack-aws 2>&1 || true
  exit 1
fi

logs=""
for attempt in $(seq 1 30); do
  logs=$(job_logs)
  [[ "$logs" == *"Wrote "*" rows to tgt_glue"* ]] && break
  sleep 5
done

session_line=$(echo "$logs" | grep -m1 "Opening session with server" || true)
echo "$session_line"
if [[ "$session_line" != *"http://snowflake:4566"* ]]; then
  echo "Test failed: the Glue job logs do not show a session with the Snowflake emulator"
  echo "$logs" | tail -150
  exit 1
fi
echo "PASS: the Glue job opened a session with the Snowflake emulator"

for line in "Read 5 rows from src_glue" "Wrote 3 rows to tgt_glue"; do
  if [[ "$logs" != *"$line"* ]]; then
    echo "Test failed: the Glue job logs do not contain '$line'"
    echo "$logs" | tail -150
    exit 1
  fi
  echo "PASS: $line"
done

names=$(SNOWFLAKE_PASSWORD=test snow sql --temporary-connection --account test --user test \
  --host snowflake.localhost.localstack.cloud --port "$SNOWFLAKE_PORT" --format json \
  -q "SELECT name FROM test.public.tgt_glue ORDER BY id" 2>/dev/null | jq -r '[.[].NAME] | join(",")')
if [ "$names" != "Alice,Charlie,Eve" ]; then
  echo "Test failed: tgt_glue holds '$names', expected 'Alice,Charlie,Eve'"
  exit 1
fi
echo "PASS: tgt_glue holds the active rows ($names)"

echo "Test passed: the Glue job read from Snowflake, filtered the rows and wrote them back"
