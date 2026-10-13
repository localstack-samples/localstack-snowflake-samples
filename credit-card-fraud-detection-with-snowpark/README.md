# Credit Card Fraud detection with Snowflake & LocalStack

## Overview

This example is based on the Machine Learning for Credit Card Fraud detection
and has been forked from a [public Snowflake
sample](https://github.com/Snowflake-Labs/snowpark-python-demos/tree/main/Credit%20Card%20Fraud%20Detection).
This sample shows how to use LocalStack's Snowflake emulator to perform feature
engineering with Snowpark, preparing data for training a Machine Learning model
and finally how to deploy and use a trained model in Snowflake using Python UDF
— all on your local machine!

## Prerequisites

- A valid [LocalStack for Snowflake license](https://www.localstack.cloud/pricing). Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [Docker](https://docs.docker.com/get-docker/)
- [`lstk`](https://docs.localstack.cloud/snowflake/developer-tools/lstk/)
- [`awslocal` CLI](https://docs.localstack.cloud/user-guide/integrations/aws-cli/)
- [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)

## Start LocalStack

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
make start
```

This runs `lstk start --type snowflake`, which starts the Snowflake emulator and waits until it is ready.

## Getting Started

This guide assumes you have already cloned the GitHub repository, and have a
terminal context within this directory.

1.  Check that dependencies exist:

    ```bash
    make check
    ```

1.  Copy the connection settings for LocalStack:

    ```bash
    cp .env.sample .env.localstack
    ```

1.  To load tables into LocalStack for Snowflake, run:

    ```bash
    make load-tables
    ```

    `make test` runs this step and checks the three tables it creates.

1.  Then, to run feature engineering via LocalStack for Snowflake, run:

    ```bash
    make feature-engineering
    ```

1.  Then, to train the ML model via LocalStack for Snowflake, run:

    ```bash
    make train-model
    ```

    Feature engineering and training on LocalStack read data checkpoints from
    `tempdata/`. Only the Snowflake Cloud steps below write them, and they are
    not part of the repository, so these two steps need one run against
    Snowflake Cloud first.

In order to run the sample application against Snowflake Cloud:

1.  To run feature engineering against Snowflake Cloud, run:

    ```bash
    make sf-feature-engineering
    ```

1.  To train the ML model against Snowflake Cloud, run:

    ```bash
    make sf-train-model
    ```

## Updates

To update a Python dependency, make the required change in `requirements.in`,
then run:

```bash
make update-deps
```

To autoformat the directory, run:

```bash
make autoformat
```
