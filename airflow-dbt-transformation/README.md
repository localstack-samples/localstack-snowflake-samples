# LocalStack Demo: Data Engineering with Apache Airflow, Snowflake, Snowpark, dbt & Cosmos

This project illustrates how to use the LocalStack Snowflake+MWAA to run a data transformation pipeline entirely on your local machine.

The code is based on the Snowflake Guide for [Data Engineering with Apache Airflow, Snowflake, Snowpark, dbt & Cosmos](https://quickstarts.snowflake.com/guide/data_engineering_with_apache_airflow).

## Prerequisites
- A valid [LocalStack for Snowflake license](https://www.localstack.cloud/pricing). Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [`lstk`](https://docs.localstack.cloud/snowflake/developer-tools/lstk/) with [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/) environment variable set
- [`awslocal` CLI](https://docs.localstack.cloud/user-guide/integrations/aws-cli/#localstack-aws-cli-awslocal)
- [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)

## Instructions

## Start LocalStack

Start LocalStack for Snowflake:

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
make start
```

This runs `lstk start --type snowflake`. The Airflow container that MWAA starts uses LocalStack's DNS server, so it reaches `snowflake.localhost.localstack.cloud` without a custom Docker network.

## Deploy the app

The sample application provides Makefile targets to simplify the setup process.

Run the following command to initialize the Airflow environment in LocalStack (this may take a couple of seconds). It uploads `requirements.txt` first, so the environment installs `astronomer-cosmos`, `dbt-snowflake` and the Snowflake provider:
```
make init
```

After deploying the Airflow environment, you should be able to request its details, and extract the webserver URL:
```
awslocal mwaa get-environment --name my-mwaa-env
...
    "Status": "AVAILABLE",
    "WebserverUrl": "http://localhost.localstack.cloud:4510"
...
```

Now use the following command to deploy the Airflow DAG with our dbt transformation logic locally:
```
make deploy
```

LocalStack syncs the files from S3 into the Airflow container every 30 seconds. Once `airflow_dag.py` shows up in the container, restart the Airflow services so they load the DAG with the installed dependencies:
```
docker exec localstack-mwaa-000000000000-us-east-1-my-mwaa-env ls /opt/airflow/dags
make restart
```

To run the whole flow and check the result in one go, use `make test`. It creates the environment, deploys the DAG, triggers a run through the Airflow CLI, waits for it to succeed and checks that the `transform.PREPPED_DATA` view has rows.

## Use the Airflow UI to trigger a DAG run

Once the Airflow environment has spun up, and the DAG has been successfully deployed, you should be able to access the Airflow UI under http://localhost.localstack.cloud:4510/home
(Note that the port number may be different - make sure to copy the `WebserverUrl` from the output further above.)
You can use `localstack`/`localstack` as the username/password to log into the Airflow UI.

You can now trigger a DAG run from the UI. If all goes well, the DAG execution result should look something similar to this:

<image src="etc/airflow-screenshot.png" ></image>

## License

The code in this project is licensed under the Apache 2.0 License.
