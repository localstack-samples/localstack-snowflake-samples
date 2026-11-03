#!/usr/bin/env bash
# The demo moment: from a clean build, `./mvnw verify` runs the whole integration
# suite against a fresh Snowflake-Next emulator. No Snowflake account, no key pair to
# request, no MFA prompt: only Docker, JDK 21 and a LocalStack token.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ -z "${LOCALSTACK_AUTH_TOKEN:-}" ]]; then
  echo "LOCALSTACK_AUTH_TOKEN is not set (run through 'make demo', which reads ~/.localstack/default.env)." >&2
  exit 1
fi

echo "==> ./mvnw clean verify  (Testcontainers starts ${SNOWFLAKE_IMAGE:-localstack/snowflake-next:latest})"
start=$SECONDS
status=0
./mvnw -B -q clean verify || status=$?
elapsed=$((SECONDS - start))

echo
echo "==> Test results"
# One line per test case.
for report in claims-api/target/surefire-reports/TEST-*.xml; do
  [[ -f "$report" ]] || continue
  awk '
    function attr(line, name,   m) { if (match(line, name "=\"[^\"]*\"")) { m = substr(line, RSTART + length(name) + 2, RLENGTH - length(name) - 3); return m } return "" }
    /<testcase / { if (open) print_case(); open = 1; name = attr($0, "name"); sub(/\((String|int)[^)]*\)/, "", name); cls = attr($0, "classname"); t = attr($0, "time"); st = "PASS"
                   if ($0 ~ /\/>[[:space:]]*$/) { print_case() } next }
    open && /<skipped/ { st = "SKIP" }
    open && /<(failure|error)/ { st = "FAIL" }
    open && /<\/testcase>/ { print_case() }
    function print_case() { n = split(cls, parts, "."); printf "  %-4s %6.2fs  %s.%s\n", st, t, parts[n], name; open = 0 }
  ' "$report"
done

echo
scripts/test-summary.sh
if (( status == 0 )); then
  echo "==> PASS in $((elapsed / 60))m $((elapsed % 60))s"
else
  echo "==> FAILED after $((elapsed / 60))m $((elapsed % 60))s (see the Maven output above)"
fi
exit "$status"
