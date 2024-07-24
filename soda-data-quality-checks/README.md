# Demo: Soda data quality checks with LocalStack Snowflake 

This project illustrates how to use the [Soda framework](https://www.soda.io/) to run data quality checks against Snowflake tables, entirely on your local machine.

The code is based on the Snowflake Quickstart Guide on [Data Quality Testing with Soda](https://quickstarts.snowflake.com/guide/soda).

## Prerequisites

- [`localstack` CLI](https://docs.localstack.cloud/getting-started/installation/#localstack-cli) with [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/) environment variable set
- [`awslocal` CLI](https://docs.localstack.cloud/user-guide/integrations/aws-cli/#localstack-aws-cli-awslocal) 
- [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)
- [`snow` CLI](https://snowflake.localstack.cloud/user-guide/integrations/snow-cli/) with `local` connection profile pointing to LocalStack Snowflake

## Instructions

### Start LocalStack

Start the LocalStack Snowflake emulator using the following command:

```bash
DOCKER_FLAGS='-e SF_LOG=trace' \
  IMAGE_NAME=localstack/snowflake \
  DEBUG=1 \
  localstack start
```

### Initialize the data tables

The sample application provides Makefile targets to simplify the setup process. 

Run the following command to initialize the environment and seed test data into local Snowflake:
```
make init  
```

### Use Soda to run data quality checks

Once the test data has been set up, we can run the Soda data quality checks via this command:
```
make scan
```

## License

The code in this project is licensed under the Apache 2.0 License.
