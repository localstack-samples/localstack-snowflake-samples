# Credit Card Fraud detection with Snowflake & LocalStack 

## Overview

This example is based on the Machine Learning for Credit Card Fraud detection and has been forked from a [public Snowflake sample](https://github.com/Snowflake-Labs/snowpark-python-demos/tree/main/Credit%20Card%20Fraud%20Detection). This sample shows how to use LocalStack's Snowflake emulator to perform feature engineering with Snowpark, preparing data for training a Machine Learning model and finally how to deploy and use a trained model in Snowflake using Python UDF — all on your local machine!

## Getting Started

This guide assumes you have already cloned the GitHub repository, and have a
terminal context within this directory.

This guide assumes you have a version of [Docker](https://www.docker.com/)
installed, with the ability to fetch [the Python Docker
image](https://hub.docker.com/_/python/) and associated dependencies.

1.  Check that dependencies exist:

    ```bash
    make check
    ```

1.  Load tables:

    ```bash
    make load-tables
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
