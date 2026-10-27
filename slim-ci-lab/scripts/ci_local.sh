#!/usr/bin/env bash
# Slim CI for dbt, the same steps GitHub Actions runs (.github-example/workflows/dbt-ci.yml):
#
#   1. load raw data      PUT + COPY INTO (idempotent: re-runs load 0 files)
#   2. production state   dbt build --target prod, save manifest.json
#   3. PR schema          drop pr_<n>, zero-copy clone modified incremental models from prod
#   4. PR build           dbt build --select state:modified+ --defer --state <prod manifest>
#   5. summary            nodes built, wall-clock, credits
#
# Environment (all optional):
#   PROD_PROJECT_DIR  dbt project for production state (default: dbt/, i.e. main)
#   PR_PROJECT_DIR    dbt project of the pull request. Default: a temporary copy
#                     of dbt/ with CI_PR_PATCH applied.
#   CI_PR_PATCH       patch that simulates the PR locally (default: demos/example-pr.patch)
#   PROD_STATE        build (default) or reuse: reuse an existing $STATE_DIR/manifest.json
#   STATE_DIR         where the production manifest is kept (default: prod-artifacts)
#   PROD_TARGET / PR_TARGET   dbt targets (default prod / pr; both "cloud" for real Snowflake)
#   DBT_PR_NUMBER     pull request number; models build into schema pr_<n> (default: local)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
start=$SECONDS

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$root/dbt}"
export SNOWFLAKE_PRIVATE_KEY_PATH="${SNOWFLAKE_PRIVATE_KEY_PATH:-$root/.secrets/rsa_key.p8}"
export DBT_PR_NUMBER="${DBT_PR_NUMBER:-local}"
PROD_PROJECT_DIR="${PROD_PROJECT_DIR:-$root/dbt}"
PR_PROJECT_DIR="${PR_PROJECT_DIR:-}"
CI_PR_PATCH="$(realpath "${CI_PR_PATCH:-$root/demos/example-pr.patch}")"
PROD_STATE="${PROD_STATE:-build}"
STATE_DIR="$(mkdir -p "${STATE_DIR:-$root/prod-artifacts}" && cd "${STATE_DIR:-$root/prod-artifacts}" && pwd)"
PROD_TARGET="${PROD_TARGET:-prod}"
PR_TARGET="${PR_TARGET:-pr}"
RUN="${RUN:-uv run --frozen}"

step() { printf '\n==> %s\n' "$*"; }
dbt() { $RUN dbt --no-use-colors "$@"; }

if [[ -z "$PR_PROJECT_DIR" ]]; then
  scratch="$(mktemp -d)"
  trap 'rm -rf "$scratch"' EXIT
  PR_PROJECT_DIR="$scratch/dbt"
  cp -R "$root/dbt" "$PR_PROJECT_DIR"
  rm -rf "$PR_PROJECT_DIR/target" "$PR_PROJECT_DIR/logs"
  patch --batch --forward --silent -p1 -d "$PR_PROJECT_DIR" -i "$CI_PR_PATCH"
  echo "Simulating a pull request: $(sed -n 's/^Subject: //p' "$CI_PR_PATCH")"
fi

step "1/5 Load raw data (PUT + COPY INTO)"
$RUN slim-ci-load

if [[ "$PROD_STATE" == "reuse" && -f "$STATE_DIR/manifest.json" ]]; then
  step "2/5 Production state: reusing $STATE_DIR/manifest.json"
else
  step "2/5 Build production state (target: $PROD_TARGET)"
  (unset DBT_PR_NUMBER; dbt build --target "$PROD_TARGET" --project-dir "$PROD_PROJECT_DIR")
  cp "$PROD_PROJECT_DIR/target/manifest.json" "$STATE_DIR/manifest.json"
fi

step "3/5 Fresh PR schema; clone modified incremental models from production"
dbt run-operation drop_pr_schema --target "$PR_TARGET" --project-dir "$PR_PROJECT_DIR"
# A modified incremental model would otherwise be built from scratch in the
# PR schema (CREATE TABLE AS) and never exercise its MERGE. Cloning the
# production table first makes the PR build run the same MERGE production's
# next run will, so a bad unique_key fails here instead of after merge.
dbt clone --target "$PR_TARGET" --project-dir "$PR_PROJECT_DIR" --state "$STATE_DIR" \
  --select "state:modified+,config.materialized:incremental,state:old"

step "4/5 PR build: dbt build --select state:modified+ --defer"
status=0
dbt build --target "$PR_TARGET" --project-dir "$PR_PROJECT_DIR" \
  --select state:modified+ --defer --state "$STATE_DIR" || status=$?

step "5/5 Summary"
$RUN slim-ci-summary --run-results "$PR_PROJECT_DIR/target/run_results.json" \
  --manifest "$PR_PROJECT_DIR/target/manifest.json" --seconds $((SECONDS - start))
exit "$status"
