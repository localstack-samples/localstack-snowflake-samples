#!/usr/bin/env bash
# A walkthrough of the service against the emulator started by `make up`:
# bulk backfill, a live POST, then the analytics endpoints and two error paths.
# Starts the API in the background and stops it on exit.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

export DOTNET_ENVIRONMENT=Development ASPNETCORE_ENVIRONMENT=Development
export SNOWFLAKE_PRIVATE_KEY_PATH="${SNOWFLAKE_PRIVATE_KEY_PATH:-$PWD/.secrets/rsa_key.p8}"
api="http://localhost:${DEMO_PORT:-5080}"
data=.cache/telemetry
ingest=(dotnet run --project src/FleetTelemetry.Ingest --)

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
pretty() {
  if command -v jq >/dev/null; then jq .; elif command -v python3 >/dev/null; then python3 -m json.tool; else cat; echo; fi
}
call() { # METHOD PATH [curl args...]
  local method="$1" path="$2"; shift 2
  printf '$ curl -X %s %s\n' "$method" "$path"
  local body status
  body="$(curl -sS -X "$method" "${api}${path}" -w '\n%{http_code}' "$@")"
  status="${body##*$'\n'}"
  printf 'HTTP %s\n' "$status"
  printf '%s' "${body%$'\n'*}" | pretty
}

step "Build"
for project in src/FleetTelemetry.Api src/FleetTelemetry.Ingest; do dotnet build --nologo -v quiet "$project" >/dev/null; done

step "Backfill: PUT the NDJSON exports to @TELEMETRY_STAGE, COPY INTO TELEMETRY_RAW (re-runs load nothing twice)"
[[ -d "$data" ]] || "${ingest[@]}" generate "$data"
"${ingest[@]}" backfill "$data" 2>&1 | grep -v '^info:'

step "Start the API on ${api}"
dotnet run --no-build --project src/FleetTelemetry.Api --urls "$api" >.cache/demo-api.log 2>&1 &
api_pid=$!
trap 'kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true' EXIT
deadline=$((SECONDS + 90))
until curl -fsS "${api}/health" >/dev/null 2>&1; do
  if (( SECONDS >= deadline )) || ! kill -0 "$api_pid" 2>/dev/null; then
    echo "The API did not start; see .cache/demo-api.log" >&2
    exit 1
  fi
  sleep 1
done
curl -fsS "${api}/health"; echo

live="$(find "$data/live" -maxdepth 1 -name '*.json' | sort | head -n 1)"
vehicle="$(basename "$live" | cut -d- -f1-2)"
day="$(basename "$live" .json | cut -d- -f3-)"

step "Live ingestion: POST one trip of ${vehicle} (a JSON array of pings) to /api/telemetry"
call POST /api/telemetry -H 'Content-Type: application/json' --data-binary "@${live}"

step "Trips of ${vehicle} on ${day}: window functions split the pings at gaps over 10 minutes"
call GET "/api/vehicles/${vehicle}/trips?date=${day}"

step "Trips of TRK-203 on 2026-09-08, from the backfill"
call GET "/api/vehicles/TRK-203/trips?date=2026-09-08"

step "Harsh braking: LAG over speed, at least 2.5 m/s² between consecutive pings"
call GET "/api/vehicles/TRK-203/events?type=harsh_braking"

step "Idling: five minutes or more standing still with the engine running"
call GET "/api/vehicles/TRK-203/events?type=idling"

step "Fault codes: LATERAL FLATTEN over each ping's engine.dtc array"
call GET "/api/vehicles/TRK-203/faults"

step "Daily fleet KPIs from the FLEET_DAILY_KPIS dynamic table"
call GET "/api/fleet/daily?from=2026-09-07&to=2026-09-09"

step "Errors are problem details: an unknown vehicle, a malformed batch"
call GET "/api/vehicles/TRK-999/trips?date=2026-09-08"
call POST /api/telemetry -H 'Content-Type: application/json' --data '[{"message_id": "TRK-101-1", "vehicle_id": '

step "Done. The API log is in .cache/demo-api.log"
