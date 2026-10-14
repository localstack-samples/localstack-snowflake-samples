#!/bin/bash
# Load the demo data, start the Streamlit app and run the app's filter logic
# against Snowflake. The app needs Python 3.8, so both run in a python:3.8
# container on the host network.
set -euo pipefail

CONTAINER=streamlit-snowpark-demo
PORT=${PORT:-8501}

make init

docker rm -f $CONTAINER > /dev/null 2>&1 || true
docker run -d --name $CONTAINER --network host -v "$PWD:/app" -w /app python:3.8-slim-bullseye \
  bash -c "pip install -q -r requirements.txt && streamlit run main.py --server.headless true --server.port $PORT" > /dev/null
trap 'docker logs --tail 50 $CONTAINER; docker rm -f $CONTAINER > /dev/null' EXIT

for attempt in $(seq 1 60); do
  curl -sf "localhost:$PORT/healthz" > /dev/null && break
  sleep 5
done
if ! curl -sf "localhost:$PORT/healthz" | grep -q ok; then
  echo "Test failed: the Streamlit app did not start"
  exit 1
fi
echo "PASS: the Streamlit app is up"

docker exec -e PYTHONPATH=/app $CONTAINER python tests/smoke_test.py
