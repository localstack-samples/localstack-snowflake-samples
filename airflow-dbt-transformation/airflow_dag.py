import json
import os
import shutil
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # Airflow 2
    from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles.snowflake.user_pass import SnowflakeUserPasswordProfileMapping
from snowflake import connector

dbt_project_path = Path("/opt/airflow/dags")

# patch Cosmos Snowflake Airflow connector, which currently doesn't support custom host yet :/
# see https://github.com/astronomer/astronomer-cosmos/blob/9420404ad9b9ad0bb4a4ffb73b50a67e4e1d077c/cosmos/profiles/snowflake/user_pass.py#L35

SnowflakeUserPasswordProfileMapping.airflow_param_mapping["host"] = "extra.host"
SnowflakeUserPasswordProfileMapping.airflow_param_mapping["port"] = "extra.port"
SnowflakeUserPasswordProfileMapping.airflow_param_mapping["protocol"] = "extra.protocol"

# The Snowflake emulator runs in its own container (see docker-compose.yml). The
# Airflow container shares its network and reaches it by the service name. Use
# plain HTTP, since the TLS certificate is for snowflake.localhost.localstack.cloud.
snowflake_connection_params = {
    "user": "test",
    "password": "test",
    "host": "snowflake",
    "port": 4566,
    "protocol": "http",
    "account": "test",
    "database": "test",
    "schema": "public",
}


# Airflow reads connections from AIRFLOW_CONN_<CONN_ID> environment variables.
# Airflow 3 does not allow DAG files to write connections to the metadata database.
os.environ["AIRFLOW_CONN_SNOWFLAKE_LOCAL"] = json.dumps({
    "conn_type": "snowflake",
    "login": "test",
    "password": "test",
    "extra": {key: value for key, value in snowflake_connection_params.items() if key not in ("user", "password")},
})

credentials = SnowflakeUserPasswordProfileMapping(
    conn_id="snowflake_local",
    profile_args={"database": "test", "schema": "public"})

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=credentials)

# use a dedicated dbt virtualenv if the environment has one, otherwise the dbt
# installed from requirements.txt
dbt_venv_executable = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
dbt_executable = dbt_venv_executable if os.path.exists(dbt_venv_executable) else shutil.which("dbt")


@dag(schedule="@hourly",
    start_date=datetime(2024, 6, 10),
    catchup=False,
    dag_id="dbt_snowpark",
    # run one task at a time, to avoid concurrent CREATE TABLE statements
    max_active_tasks=1,
)
def dbt_snowpark_dag():
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(dbt_project_path),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable),
        operator_args={"install_deps": True},
    )

    intermediate = EmptyOperator(task_id='intermediate')

    @task
    def query_result_data():
        connection = connector.connect(**snowflake_connection_params)
        # select rows from the `PREPPED_DATA` view created by the dbt transformation
        result = connection.cursor().execute("SELECT * FROM transform.PREPPED_DATA")
        result = list(result)
        print("-----")
        print(f"Query result ({len(result)} rows):")
        for row in result:
            print(row)
        print("-----")
        result = str(result)
        return result

    query_result = query_result_data()
    transform_data >> intermediate >> query_result


dbt_snowpark_dag = dbt_snowpark_dag()
