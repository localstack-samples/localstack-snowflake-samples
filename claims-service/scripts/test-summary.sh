#!/usr/bin/env bash
# One Surefire-style totals line over every module's XML reports, for `make test`
# (./mvnw -q prints nothing on success).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
shopt -s nullglob
reports=(claims-api/target/surefire-reports/TEST-*.xml)
if (( ${#reports[@]} == 0 )); then
  echo "No test reports found; run ./mvnw verify first." >&2
  exit 1
fi
awk '/<testsuite / {
           for (i = 1; i <= NF; i++) {
             split($i, kv, "=")
             gsub(/"/, "", kv[2])
             if (kv[1] == "tests") t += kv[2]
             else if (kv[1] == "failures") f += kv[2]
             else if (kv[1] == "errors") e += kv[2]
             else if (kv[1] == "skipped") s += kv[2]
           }
         }
         END { printf "Tests run: %d, Failures: %d, Errors: %d, Skipped: %d\n", t, f, e, s }' "${reports[@]}"
