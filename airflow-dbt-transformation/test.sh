#!/bin/bash
# Create the MWAA environment, deploy the dbt DAG, wait until LocalStack has
# synced it and installed the requirements, restart Airflow, then trigger a run
# through the Airflow CLI and wait for it to succeed. The last task of the DAG
# selects from the PREPPED_DATA view built by dbt.
set -euo pipefail

ENV_NAME=my-mwaa-env
DAG_ID=dbt_snowpark
RUN_ID="ci_$(date +%s)"
CONTAINER=localstack-mwaa-000000000000-us-east-1-my-mwaa-env

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
airflow() { docker exec "$CONTAINER" airflow "$@" 2>/dev/null; }
diagnostics() {
  docker exec "$CONTAINER" bash -c 'ls -la /opt/airflow/dags; pip show astronomer-cosmos dbt-snowflake 2>&1 | grep -E "^(Name|Version)"' || true
  docker exec "$CONTAINER" airflow dags list-import-errors || true
  docker exec "$CONTAINER" bash -c 'tail -n 40 $(find /opt/airflow/logs -path "*query_result_data*" -name "*.log" | head -1)' || true
  docker logs --tail 80 "$CONTAINER" 2>&1 | grep -v SyntaxWarning || true
}
trap 'if [ $? -ne 0 ]; then diagnostics; fi' EXIT

make init

env_available() {
  [ "$(awslocal mwaa get-environment --name $ENV_NAME --query Environment.Status --output text)" = "AVAILABLE" ]
}
poll "the MWAA environment" 90 10 env_available
echo "PASS: MWAA environment is available"

make deploy
dag_synced() { docker exec "$CONTAINER" test -f /opt/airflow/dags/airflow_dag.py -a -d /opt/airflow/dags/macros; }
poll "the DAG files to be synced from S3" 30 10 dag_synced
requirements_installed() { docker exec "$CONTAINER" bash -c "pip show astronomer-cosmos dbt-snowflake" > /dev/null 2>&1; }
poll "the requirements to be installed" 60 10 requirements_installed
echo "PASS: DAG files synced and requirements installed"

make restart
dag_loaded() { airflow dags list -o json | jq -e --arg d $DAG_ID 'any(.[]; .dag_id == $d)' > /dev/null 2>&1; }
poll "the DAG to be loaded" 30 10 dag_loaded
echo "PASS: the $DAG_ID DAG is loaded"

airflow dags unpause $DAG_ID > /dev/null
airflow dags trigger -r "$RUN_ID" $DAG_ID > /dev/null

# Airflow 3 takes the DAG id as an argument, Airflow 2 as -d
list_runs() { airflow dags list-runs $DAG_ID -o json || airflow dags list-runs -d $DAG_ID -o json; }
run_state() { list_runs | jq -r --arg r "$RUN_ID" '.[] | select(.run_id == $r) | .state'; }
run_finished() { local s; s=$(run_state); [ "$s" = "success" ] || [ "$s" = "failed" ]; }
poll "the DAG run" 90 10 run_finished
state=$(run_state)
docker exec "$CONTAINER" airflow tasks states-for-dag-run $DAG_ID "$RUN_ID" 2>/dev/null || true
if [ "$state" != "success" ]; then
  echo "Test failed: DAG run $RUN_ID ended in state $state"
  exit 1
fi
echo "PASS: the dbt seeds, models and the PREPPED_DATA query succeeded"

rows=$(snow sql -c local --format json -q "SELECT COUNT(*) AS N FROM test.transform.PREPPED_DATA" | jq -r '.. | objects | .N? // empty' | head -1)
if [ "${rows:-0}" -le 0 ]; then
  echo "Test failed: PREPPED_DATA is empty"
  exit 1
fi
echo "PASS: PREPPED_DATA holds $rows rows"

echo "Test passed: Airflow ran the dbt pipeline against Snowflake"
