#!/bin/bash
# Package and deploy the Lambda function, invoke it, and check that it
# connected to the local Snowflake emulator, inserted the three rows and read
# them back.
set -euo pipefail

FUNCTION_NAME=localstack-snowflake-lambda-example

make zip create-function
make local-invoke
cat output.txt; echo

if ! jq -e '.statusCode == 200 and .body == "Successfully connected to Snowflake and inserted rows!"' output.txt > /dev/null; then
  echo "Test failed: unexpected function response"
  exit 1
fi
echo "PASS: the function returned 200"

logs=""
for attempt in $(seq 1 30); do
  logs=$(awslocal logs filter-log-events --log-group-name "/aws/lambda/$FUNCTION_NAME" \
    --query 'events[].message' --output text 2>/dev/null || true)
  [[ "$logs" == *"Total # of rows: 3"* ]] && break
  sleep 2
done
echo "$logs"

for expected in "Total # of rows: 3" "Row-1 => ('John', 'SQL')" "Row-2 => ('Alex', 'Java')" "Current timestamp from Snowflake:"; do
  if [[ "$logs" != *"$expected"* ]]; then
    echo "Test failed: the function logs do not contain \"$expected\""
    exit 1
  fi
done
echo "PASS: the function read the three inserted rows and the current timestamp from Snowflake"

# read the table the function wrote back from the host, on the port the
# Snowflake emulator is published on, to show the rows landed there
rows=$(SNOWFLAKE_PASSWORD=test snow sql -q "select name, skill from test.test_schema.ability order by name" \
  --format json --temporary-connection --account test --user test \
  --host snowflake.localhost.localstack.cloud --port 4567 2>/dev/null)
echo "$rows"
if ! echo "$rows" | jq -e '[.[] | "\(.NAME)=\(.SKILL)"] == ["Alex=Java", "John=SQL", "Pete=Snowflake"]' > /dev/null; then
  echo "Test failed: the ability table in the Snowflake emulator does not hold the three rows"
  exit 1
fi
echo "PASS: the Snowflake emulator holds the three rows the function inserted"

echo "Test passed: Snowpark connector works from Lambda"
