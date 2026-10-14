# Credit Card Fraud detection with Snowflake & LocalStack

## Overview

This example is based on the Machine Learning for Credit Card Fraud detection
and has been forked from a [public Snowflake
sample](https://github.com/Snowflake-Labs/snowpark-python-demos/tree/main/Credit%20Card%20Fraud%20Detection).
This sample shows how to use LocalStack's Snowflake emulator to perform feature
engineering with Snowpark, preparing data for training a Machine Learning model
and finally how to deploy and use a trained model in Snowflake using Python UDF,
all on your local machine.

The same three scripts run against the emulator and against Snowflake Cloud;
only the connection settings differ.

## Prerequisites

- A valid [LocalStack for Snowflake license](https://www.localstack.cloud/pricing) with access to the `localstack/snowflake-next` preview. Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [Docker](https://docs.docker.com/get-docker/)
- [`lstk`](https://docs.localstack.cloud/snowflake/developer-tools/lstk/) 1.3.0 or later
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation) (`snow`) with a `local` connection, see the [repository README](../README.md#configuration)

## Start LocalStack

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
make start
```

This runs `lstk start --type snowflake --image localstack/snowflake-next:latest`,
which starts the [Snowflake Next emulator](https://hub.docker.com/r/localstack/snowflake-next)
on port 4566 and waits until it is ready.

## Getting Started

This guide assumes you have already cloned the GitHub repository, and have a
terminal context within this directory. Every step runs in the sample's Docker
image.

1.  Check that dependencies exist:

    ```bash
    make check
    ```

1.  Copy the connection settings for LocalStack, and create the `test`
    database they use:

    ```bash
    cp .env.sample .env.localstack
    snow sql -c local --port 4566 -q "CREATE DATABASE IF NOT EXISTS test"
    ```

1.  To load the transactions from `data/fraud_transactions.min.csv.gz` into
    LocalStack for Snowflake, run:

    ```bash
    make load-tables
    ```

1.  Then, to build the customer and terminal features with Snowpark, run:

    ```bash
    make feature-engineering
    ```

    This writes the `CUSTOMER_TRX_FRAUD_FEATURES` table.

1.  Then, to train the ML model and deploy it as a Python UDF, run:

    ```bash
    make train-model
    ```

    This step does not complete on the emulator yet: Snowpark validates the UDF
    packages against `INFORMATION_SCHEMA.PACKAGES`, which the emulator does not
    fill with the packages it can install.

`make test` runs the first two steps and checks the tables they create.

In order to run the sample application against Snowflake Cloud, copy
`.env.sample` to `.env.snowflake`, fill in your account settings, leave `HOST`
and `PORT` empty, and run:

```bash
make sf-load-tables
make sf-feature-engineering
make sf-train-model
```

## Data

`data/fraud_transactions.min.csv.gz` holds 11033 simulated transactions from 50
customers on 50 terminals between 2019-04-01 and 2019-07-31, 590 of them frauds.
`data/generate_transactions.py` creates it with the transaction generator from
the [fraud detection handbook](https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html),
scaled down so every step runs in a few minutes.

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
