#!/bin/bash
# Check the two-container setup the README describes:
# 1. the AWS container reaches the Snowflake emulator by container name
# 2. Snowflake loads a CSV from an S3 bucket in the AWS container through a stage
set -euo pipefail

# the image has no curl, so open the session with Python
response=$(docker exec localstack-aws python3 -c "import urllib.request; print(urllib.request.urlopen(urllib.request.Request('http://localstack-snowflake:4567/session', data=b'{}')).read().decode())")
echo "Snowflake session response from the AWS container: $response"
if ! echo "$response" | jq -e '.success == true' > /dev/null; then
  echo "Test failed: the AWS container could not open a Snowflake session"
  exit 1
fi
echo "PASS: the AWS container reached the Snowflake emulator"

BUCKET="multi-container-$(date +%s)"
aws --endpoint-url http://localhost:4566 s3 mb "s3://$BUCKET"
printf 'id,name\n1,Alice\n2,Bob\n3,Charlie\n' > /tmp/people.csv
aws --endpoint-url http://localhost:4566 s3 cp /tmp/people.csv "s3://$BUCKET/people.csv"

snow connection add --connection-name multi-container --user test --password test --account test \
  --host snowflake.localhost.localstack.cloud --port 4567 --no-interactive > /dev/null 2>&1 || true
sql() { snow sql -c multi-container --format json -q "$1"; }

sql "CREATE OR REPLACE TABLE people (id INT, name STRING)"
sql "CREATE OR REPLACE STAGE people_stage URL='s3://$BUCKET' CREDENTIALS=(AWS_KEY_ID='test' AWS_SECRET_KEY='test')"
sql "COPY INTO people FROM @people_stage FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1)"
names=$(sql "SELECT name FROM people ORDER BY id" | jq -r '.. | objects | .NAME? // empty' | paste -sd, -)
echo "Loaded names: $names"
if [ "$names" != "Alice,Bob,Charlie" ]; then
  echo "Test failed: expected Alice,Bob,Charlie loaded from S3"
  exit 1
fi
echo "PASS: Snowflake loaded the CSV from the S3 bucket in the AWS container"

echo "Test passed: the two containers work together"
