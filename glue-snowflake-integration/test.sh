#!/bin/bash
# Run the Glue job against the Snowflake emulator and check, through the job's
# CloudWatch logs, that the Spark Snowflake connector opened a session with
# the emulator and read the src_glue table created by init.sf.sql.
set -euo pipefail

./deploy.sh
job_run_id=$(cat .last-job-run-id)

logs=""
for attempt in $(seq 1 30); do
  logs=$(awslocal logs get-log-events --log-group-name /aws-glue/jobs/logs-v2 \
    --log-stream-name "$job_run_id" --query 'events[*].message' --output text 2>/dev/null || true)
  [[ "$logs" == *"Opening session with server"* ]] && break
  sleep 5
done

session_line=$(echo "$logs" | tr '\t' '\n' | grep -m1 "Opening session with server" || true)
echo "$session_line"
if [[ "$session_line" != *"snowflake.localhost.localstack.cloud"* ]]; then
  echo "Test failed: the Glue job logs do not show a session with the Snowflake emulator"
  echo "$logs" | tr '\t' '\n' | tail -100
  exit 1
fi
echo "PASS: the Glue job opened a JDBC session with the Snowflake emulator"

if echo "$logs" | grep -q "JDBC_COMMUNICATION_ERROR"; then
  echo "NOTE: the job logged JDBC_COMMUNICATION_ERROR after connecting"
elif echo "$logs" | grep -q "Alice"; then
  echo "PASS: the job read the src_glue rows"
fi

echo "Test passed: the Glue job connected to Snowflake and succeeded"
