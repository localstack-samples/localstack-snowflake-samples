# Demo: Soda data quality checks with LocalStack Snowflake

This project illustrates how to use the [Soda framework](https://www.soda.io/) to run data quality checks against Snowflake tables, entirely on your local machine.

The code is based on the Snowflake Quickstart Guide on [Data Quality Testing with Soda](https://quickstarts.snowflake.com/guide/soda).

## Prerequisites
- A valid [LocalStack for Snowflake license](https://www.localstack.cloud/pricing). Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [`lstk`](https://docs.localstack.cloud/snowflake/developer-tools/lstk/) with [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/) environment variable set
- [LocalStack Snowflake Next emulator](https://hub.docker.com/r/localstack/snowflake-next) (`localstack/snowflake-next`), which needs an auth token with the Snowflake preview entitlement
- [`snow` CLI](https://snowflake.localstack.cloud/user-guide/integrations/snow-cli/) with `localstack` connection profile pointing to LocalStack Snowflake

To install the `snow` CLI with a local connection profile, follow the instructions [here](https://snowflake.localstack.cloud/user-guide/integrations/snow-cli/#create-a-connection-profile).
It basically boils down to running this command:
```
snow connection add \
    --connection-name localstack \
    --user test \
    --password test \
    --account test \
    --host snowflake.localhost.localstack.cloud
```

## Instructions

## Install dependencies

Run the following command to install dependencies for the project (into a local Python virtual environment):
```
make install
```

## Start LocalStack

To start the Snowflake Next emulator, use:

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
make start
```

This runs `lstk start --type snowflake --image localstack/snowflake-next:latest` (`lstk` 1.3.0 or later). Without `lstk`, start the image with Docker:

```bash
docker run --rm -it -p 127.0.0.1:4566:4566 -p 127.0.0.1:443:443 \
    -e LOCALSTACK_AUTH_TOKEN=${LOCALSTACK_AUTH_TOKEN:?} localstack/snowflake-next
```

## Initialize the data tables

The sample application provides Makefile targets to simplify the setup process.

Run the following command to create the `test` database and seed the `test_table` table in local Snowflake:

```bash
make init
```

## Use Soda to run data quality checks

Once the test data has been set up, we can run the Soda data quality checks via this command:

```bash
make scan
```

The row count check in `checks.yml` is written to fail on the 2-row table, while the NULL and schema checks pass. `make test` runs `make init` and `make scan` and checks for exactly that result.

## License

The code in this project is licensed under the Apache 2.0 License.
