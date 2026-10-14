# Connecting Snowpark to AWS Lambda locally using LocalStack

This example demonstrates how to connect Snowpark to AWS Lambda locally using LocalStack. Snowpark allows you to query, process, and transform data in a variety of ways using Snowpark Python. In this example, we create a Lambda function that uses Snowpark to:

- Establish a connection to a local Snowflake database provided by LocalStack.
- Create a cursor object and execute a simple query to create a table.
- Insert rows and execute an insert query with `executemany` and a query to select data from the table.
- Fetch the results and execute a query to get the current timestamp.

## Prerequisites
- A valid [LocalStack for Snowflake license](https://www.localstack.cloud/pricing) with access to the Snowflake Next preview. Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [Docker](https://docs.docker.com/get-docker/) with Docker Compose
- Python 3.12 and `pip`
- [`awslocal`](https://docs.localstack.cloud/user-guide/integrations/aws-cli/) and `jq`
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation) (`snow`), used by `make test`

## How it works

The sample runs two containers from [`docker-compose.yml`](docker-compose.yml):

- `localstack`: LocalStack for AWS (`localstack/localstack-pro`) on port 4566. It runs the Lambda function.
- `snowflake`: the Snowflake emulator ([`localstack/snowflake-next`](https://hub.docker.com/r/localstack/snowflake-next)) on host port 4567. It is Snowflake only, so it runs next to LocalStack for AWS. At startup it runs [`init.sf.sql`](init.sf.sql), which creates the `test` database and the `test_schema` schema the function connects to.

Both containers share the `lambda-snowpark` network, and `LAMBDA_DOCKER_NETWORK` puts the Lambda container on it too. The function reads `SNOWFLAKE_HOST`, `SNOWFLAKE_PORT` and `SNOWFLAKE_PROTOCOL` from its environment, which `make create-function` sets to `snowflake`, `4566` and `http`: the service name, over plain HTTP, since the emulator's TLS certificate is for `snowflake.localhost.localstack.cloud`. `SF_HOSTNAMES` tells the emulator to answer to that service name. Without these variables the function uses the usual `snowflake.localhost.localstack.cloud` over HTTPS.

## Instructions

## Install the dependencies

Run the following command to install the dependencies:

```bash
make install
```

## Start the containers

Start LocalStack for AWS and the Snowflake emulator:

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
make start-localstack
```

This runs `docker compose up -d --wait`, which returns once both containers are healthy. `make logs` shows the logs of both, and `make stop` removes them.

## Create the Lambda function

You can create the Lambda function using the following command:

```bash
make zip
make create-function
```

## Invoke the Lambda function

Invoke the Lambda function using the following command:

```bash
make local-invoke
```

Open the `output.txt` file to see the results of the Lambda function.

```text
{"statusCode": 200, "body": "Successfully connected to Snowflake and inserted rows!"}
```

The function logs the rows it read and the current timestamp from Snowflake:

```bash
awslocal logs filter-log-events --log-group-name /aws/lambda/localstack-snowflake-lambda-example \
  --query 'events[].message' --output text
```

```text
Total # of rows: 3
Row-1 => ('John', 'SQL')
Row-2 => ('Alex', 'Java')
Current timestamp from Snowflake: 2026-10-14 09:30:12.345678-07:00
```

You can read the table from the host as well, on port 4567:

```bash
snow sql -q "select * from test.test_schema.ability" --temporary-connection \
  --account test --user test --password test \
  --host snowflake.localhost.localstack.cloud --port 4567
```

## Test

`make test` packages and deploys the function, invokes it, checks its response and logs, and reads the three rows back from the Snowflake emulator:

```bash
make test
```

## License

This code is licensed under the Apache 2.0 License.
