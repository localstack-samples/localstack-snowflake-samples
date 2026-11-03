#!/usr/bin/env bash
# `make run`: the API on :API_PORT (default 8000) and the dashboard's Vite dev
# server on :WEB_PORT (default 5173), which proxies /api to the API.
# Ctrl-C (or either process exiting) stops both.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

api_port="${API_PORT:-8000}"
web_port="${WEB_PORT:-5173}"
pids=()

cleanup() {
  trap - EXIT INT TERM
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

uv run --frozen uvicorn app.main:app --host 127.0.0.1 --port "$api_port" &
pids+=($!)

deadline=$((SECONDS + 60))
until curl -fsS -o /dev/null "http://127.0.0.1:${api_port}/api/health" 2>/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "API did not answer /api/health within 60s (is the emulator up? run 'make up' first)." >&2
    exit 1
  fi
  sleep 1
done

(cd web && exec npx vite --port "$web_port") &
pids+=($!)

echo
echo "  API        http://localhost:${api_port}/docs"
echo "  Dashboard  http://localhost:${web_port}"
echo

wait -n
