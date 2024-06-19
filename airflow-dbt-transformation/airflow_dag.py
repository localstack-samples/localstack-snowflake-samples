import os
from datetime import datetime
from pathlib import Path

from airflow import settings
from airflow.models import Connection
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles.snowflake.user_pass import SnowflakeUserPasswordProfileMapping

dbt_project_path = Path("/opt/airflow/dags")

# patch Cosmos Snowflake Airflow connector, which currently doesn't support custom host yet :/
# see https://github.com/astronomer/astronomer-cosmos/blob/9420404ad9b9ad0bb4a4ffb73b50a67e4e1d077c/cosmos/profiles/snowflake/user_pass.py#L35

SnowflakeUserPasswordProfileMapping.airflow_param_mapping["host"] = "extra.host"
SnowflakeUserPasswordProfileMapping.airflow_param_mapping["port"] = "extra.port"


def create_snowflake_connection():
    conn = Connection(
        conn_id="snowflake_local",
        conn_type="snowflake",
        login="test",
        password="test",
        description="LocalStack Snowflake",
        extra={
            "host": "snowflake.localhost.localstack.cloud",
            "port": 4566,
            "account": "test",
            "database": "test",
            "schema": "public",
        }
    )
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if str(conn_name) == str(conn.conn_id):
        return None

    session.add(conn)
    session.commit()
    return conn


create_snowflake_connection()
credentials = SnowflakeUserPasswordProfileMapping(
    conn_id="snowflake_local",
    profile_args={"database": "test", "schema": "public"})

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=credentials)

dbt_executable = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(dbt_project_path,),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=dbt_executable,),
    schedule_interval="@hourly",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_snowflake_dag"
)
