#!/bin/bash
# Create the MWAA environment, deploy the dbt DAG, trigger a run through the
# Airflow CLI in the environment's container and wait for it to succeed. The
# last task of the DAG selects from the PREPPED_DATA view built by dbt.
set -euo pipefail

ENV_NAME=my-mwaa-env
DAG_ID=dbt_snowpark
RUN_ID="ci_$(date +%s)"

make init deploy

# poll <description> <attempts> <seconds> <command...>: retry until the command succeeds
poll() {
  local description=$1 attempts=$2 delay=$3
  shift 3
  for attempt in $(seq 1 "$attempts"); do
    "$@" && return 0
    echo "Waiting for $description ($attempt/$attempts)"
    sleep "$delay"
  done
  echo "Test failed: timed out waiting for $description"
  return 1
}

env_available() {
  [ "$(awslocal mwaa get-environment --name $ENV_NAME --query Environment.Status --output text)" = "AVAILABLE" ]
}
poll "the MWAA environment" 90 10 env_available
echo "PASS: MWAA environment is available"

CONTAINER=$(docker ps --filter "name=mwaa" --format '{{.Names}}' | head -1)
echo "Airflow container: $CONTAINER"
airflow() { docker exec "$CONTAINER" airflow "$@"; }

dag_loaded() { airflow dags list -o json 2>/dev/null | jq -e --arg d $DAG_ID 'any(.[]; .dag_id == $d)' > /dev/null; }
if ! poll "the DAG to be loaded" 60 10 dag_loaded; then
  airflow dags list-import-errors || true
  exit 1
fi
echo "PASS: the $DAG_ID DAG is loaded"

airflow dags unpause $DAG_ID
airflow dags trigger -r "$RUN_ID" $DAG_ID

run_state() { airflow dags list-runs -d $DAG_ID -o json 2>/dev/null | jq -r --arg r "$RUN_ID" '.[] | select(.run_id == $r) | .state'; }
run_finished() { local s; s=$(run_state); [ "$s" = "success" ] || [ "$s" = "failed" ]; }
poll "the DAG run" 120 10 run_finished
state=$(run_state)
airflow tasks states-for-dag-run $DAG_ID "$RUN_ID" || true
if [ "$state" != "success" ]; then
  echo "Test failed: DAG run $RUN_ID ended in state $state"
  exit 1
fi
echo "PASS: the dbt transformation and the PREPPED_DATA query succeeded"

echo "Test passed: Airflow ran the dbt pipeline against Snowflake"
