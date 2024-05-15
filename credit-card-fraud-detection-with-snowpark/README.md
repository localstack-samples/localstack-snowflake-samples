# Credit Card Fraud detection with Snowflake & LocalStack 

## Overview

Version of [the credit card fraud detection Snowpark Python
demo](https://github.com/Snowflake-Labs/snowpark-python-demos/tree/main/Credit%20Card%20Fraud%20Detection)
that is compatible with the LocalStack Snowflake emulator.

## Getting Started

This guide assumes you have already downloaded the GitHub repository, and have a
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
