# Credit Card Fraud detection with Snowflake & LocalStack

## Overview

This example is based on the Machine Learning for Credit Card Fraud detection
and has been forked from a [public Snowflake
sample](https://github.com/Snowflake-Labs/snowpark-python-demos/tree/main/Credit%20Card%20Fraud%20Detection).
This sample shows how to use LocalStack's Snowflake emulator to perform feature
engineering with Snowpark, preparing data for training a Machine Learning model
and finally how to deploy and use a trained model in Snowflake using Python UDF
— all on your local machine!

## Getting Started

This guide assumes you have already cloned the GitHub repository, and have a
terminal context within this directory.

-  Docker with access to Docker Hub
-  Python 3.8 installed locally
-  [`localstack`
   CLI](https://docs.localstack.cloud/getting-started/installation/) with
   `LOCALSTACK_AUTH_TOKEN` environment variable set
-  [LocalStack Snowflake
   emulator](https://snowflake.localstack.cloud/getting-started/installation/)

1.  Check that dependencies exist:

    ```bash
    make check
    ```

1.  To load tables into LocalStack for Snowflake, run:

    ```bash
    make load-tables
    ```

1.  Then, to run feature engineering via LocalStack for Snowflake, run:

    ```bash
    make feature-engineering
    ```

1.  Then, to train the ML model via LocalStack for Snowflake, run:

    ```bash
    make train-model
    ```

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
