#!/bin/bash
# Seed the Citibike trips and weather data from the public S3 bucket, start the
# web server and check that each chart endpoint returns data from Snowflake.
set -euo pipefail

PORT=${PORT:-3000}

make seed

count() { snow sql -c local --format json -q "SELECT COUNT(*) AS N FROM $1" | jq -r '.. | objects | .N? // empty' | head -1; }
trips=$(count trips)
weather=$(count weather)
echo "Loaded $trips trips and $weather weather rows"
if [ "${trips:-0}" -le 0 ] || [ "${weather:-0}" -le 0 ]; then
  echo "Test failed: the seed did not load trips and weather data"
  exit 1
fi
echo "PASS: trips and weather tables are populated"

PORT=$PORT node app.js > web.log 2>&1 &
WEB_PID=$!
trap 'kill $WEB_PID 2>/dev/null || true; cat web.log' EXIT

for attempt in $(seq 1 30); do
  curl -sf "localhost:$PORT/" > /dev/null && break
  sleep 1
done
if ! curl -sf "localhost:$PORT/" | grep -qi '<html'; then
  echo "Test failed: the web page is not served"
  exit 1
fi
echo "PASS: the web page is served"

for endpoint in monthly day_of_week temperature; do
  body=$(curl -s -m 60 -w '\n%{http_code}' "localhost:$PORT/trips/$endpoint")
  status=${body##*$'\n'}
  body=${body%$'\n'*}
  echo "/trips/$endpoint: HTTP $status ${body:0:300}"
  if [ "$status" != "200" ] || ! echo "$body" | jq -e 'type == "array" and length > 0' > /dev/null; then
    echo "Test failed: /trips/$endpoint did not return chart data"
    exit 1
  fi
done
echo "PASS: all chart endpoints return data"

echo "Test passed: the data app serves Citibike data from Snowflake"
