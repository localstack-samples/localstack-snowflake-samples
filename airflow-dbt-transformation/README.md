# LocalStack Demo: Data Engineering with Apache Airflow, Snowflake, Snowpark, dbt & Cosmos

This project illustrates how to use the LocalStack Snowflake+MWAA to run a data transformation pipeline entirely on your local machine.

The code is based on the Snowflake Guide for [Data Engineering with Apache Airflow, Snowflake, Snowpark, dbt & Cosmos](https://quickstarts.snowflake.com/guide/data_engineering_with_apache_airflow).

## Prerequisites
- A valid [LocalStack for Snowflake license](https://snowflake.localstack.cloud/). Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [`localstack` CLI](https://docs.localstack.cloud/getting-started/installation/#localstack-cli) with [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/) environment variable set
- [`awslocal` CLI](https://docs.localstack.cloud/user-guide/integrations/aws-cli/#localstack-aws-cli-awslocal)
- [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)

## Instructions

## Start LocalStack

Start LocalStack:

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
localstack auth set-token $LOCALSTACK_AUTH_TOKEN
localstack start -s snowflake -d
localstack wait -t 30
```

If you want to run this sample with the custom Snowflake/Airflow networking flags, use:

```bash
docker network create --attachable --subnet 172.20.0.0/24 localstack
DOCKER_FLAGS='-e SF_LOG=trace --network localstack --name=localhost.localstack.cloud --network-alias=snowflake.localhost.localstack.cloud' \
  IMAGE_NAME=localstack/snowflake \
  DEBUG=1 \
  localstack start -s snowflake -d
```

## Deploy the app

The sample application provides Makefile targets to simplify the setup process.

Run the following command to initialize the Airflow environment in LocalStack (this may take a couple of seconds):
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

## Use the Airflow UI to trigger a DAG run

Once the Airflow environment has spun up, and the DAG has been successfully deployed, you should be able to access the Airflow UI under http://localhost.localstack.cloud:4510/home
(Note that the port number may be different - make sure to copy the `WebserverUrl` from the output further above.)
You can use `localstack`/`localstack` as the username/password to log into the Airflow UI.

You can now trigger a DAG run from the UI. If all goes well, the DAG execution result should look something similar to this:

<image src="etc/airflow-screenshot.png" ></image>

## License

The code in this project is licensed under the Apache 2.0 License.
