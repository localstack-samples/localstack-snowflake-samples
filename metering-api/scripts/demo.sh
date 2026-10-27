#!/usr/bin/env bash
# make demo: run the demo client against the API at HTTP_ADDR. If no API is
# listening there, build one, start it in the background and stop it afterwards.
set -euo pipefail
cd "$(dirname "$0")/.."

addr="${HTTP_ADDR:-127.0.0.1:8080}"
url="http://${addr}"
healthy() { curl -fsS -o /dev/null "${url}/healthz" 2>/dev/null; }

if ! healthy; then
  mkdir -p bin
  go build -o bin/metering-api ./cmd/api
  HTTP_ADDR="$addr" LOG_LEVEL=WARN bin/metering-api &
  api_pid=$!
  trap 'kill "$api_pid" 2>/dev/null; wait "$api_pid" 2>/dev/null || true' EXIT
  deadline=$((SECONDS + 60))
  until healthy; do
    if (( SECONDS >= deadline )) || ! kill -0 "$api_pid" 2>/dev/null; then
      echo "The API did not become healthy at ${url} (is the emulator up? run 'make up')." >&2
      exit 1
    fi
    sleep 1
  done
fi

go run ./cmd/demo -addr "$url"
