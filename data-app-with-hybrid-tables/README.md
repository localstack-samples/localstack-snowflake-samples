# Data application with Hybrid Tables in LocalStack Snowflake

Note: This sample application has been copied and adapted from its original version here: https://github.com/Snowflake-Labs/sfguide-build-data-application-with-hybrid-tables

## Overview

This quickstart will take you through building a data application that runs on Snowflake Hybrid Tables, deployed fully locally using [LocalStack for Snowflake](https://www.localstack.cloud/localstack-for-snowflake).
[Hybrid Tables](https://docs.snowflake.com/en/user-guide/tables-hybrid?_fsi=siV2rnOG) is a Snowflake table type that has been designed for transactional and operational work together with analytical workloads.
Hybrid Tables typically offers lower latency and higher throughput on row level and point reads and writes, making them a good choice for a backing source for an application that requires faster operations on individual rows and point lookup, especially when compared to standard Snowflake tables that are optimized for analytical operations.

This sample is based on the Snowflake [QuickStart Guide](https://quickstarts.snowflake.com/guide/build_a_data_application_with_hybrid_tables) for hybrid tables.

## Prerequisites

- [`localstack` CLI](https://docs.localstack.cloud/getting-started/installation/#localstack-cli) with [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/) environment variable set
- [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)
- [Snowpark for Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)

## Installing and Initializing

First, install the dependencies for the project:
```
make install
```

We then seed the test data into the LocalStack Snowflake instance:
```
make seed
```

Finally, run this command to start up the Flask application:
```
make start
```

## License

The code in this project is licensed under the Apache 2.0 license.
