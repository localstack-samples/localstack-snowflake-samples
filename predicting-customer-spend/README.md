# Predicting customer spend with Snowpark & LocalStack

An E-Commerce company aims to apply machine learning for understanding customer engagement with its digital platforms (website and app). The goal is to determine whether to prioritize improving the mobile app or website. Using a Linear Regression model, we will assess the impact of user activity on the likelihood of increased spending.

In this sample notebook, we will explore how you can use LocalStack's Snowflake emulator to develop and test your Snowflake data pipelines entirely on your local machine! In this notebook, we will showcase how you can use LocalStack's Snowflake with Snowpark for Python and your favorite Python libraries for data analysis and machine learning.

## Prerequisites
- A valid [LocalStack for Snowflake license](https://www.localstack.cloud/pricing). Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [`lstk`](https://docs.localstack.cloud/snowflake/developer-tools/lstk/) 1.3.0 or later, with [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/) environment variable set
-   The [Snowflake Next emulator](https://hub.docker.com/r/localstack/snowflake-next) (`localstack/snowflake-next`). Your license needs the Snowflake preview entitlement.
-   [Snowpark for Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)
-   [Jupyter Notebook](https://jupyter.org/)

## Start LocalStack

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
make start
```

This runs `lstk start --type snowflake --image localstack/snowflake-next:latest`, which pulls the Snowflake Next emulator, starts it and waits until it is ready. Run `make stop` to stop it.

Without `lstk`, start it with Docker:

```bash
docker run --rm -it -p 127.0.0.1:4566:4566 -p 127.0.0.1:443:443 \
  -e LOCALSTACK_AUTH_TOKEN=${LOCALSTACK_AUTH_TOKEN:?} localstack/snowflake-next:latest
```

## Running the notebook

Start a Jupyter Notebook server and open the notebook in your browser.

```bash
jupyter notebook
```

Open the `linear-regression-model.ipynb` notebook and run the cells to see the results. Follow the instructions in the notebook to install the required packages and connect to the Snowflake emulator.

## Testing

```bash
make install
make start
make test
```

`make test` runs every cell of the notebook with `nbconvert` and checks the results in Snowflake.

## License

This code is available under the Apache 2.0 license.
