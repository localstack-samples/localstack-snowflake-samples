# Credit Scoring with Snowpark & LocalStack

This notebook demonstrates how to use Snowpark for Python to perform exploratory data analysis (EDA) on a credit scoring dataset. This notebook uses LocalStack's Snowflake emulator to run Snowflake data pipelines locally.

## Prerequisites

- [`localstack` CLI](https://docs.localstack.cloud/getting-started/installation/#localstack-cli) with [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/) environment variable set
- [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)
- [Snowpark for Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)
- [Jupyter Notebook](https://jupyter.org/)

## Running the notebook

Start the LocalStack container using your preferred method. Ensure that you have access to LocalStack's Snowflake emulator. Start a Jupyter Notebook server and open the notebook in your browser.

```bash
jupyter notebook
```

Open the `credit-scoring-eda.ipynb` notebook and run the cells to see the results. Follow the instructions in the notebook to install the required packages and set up the Snowflake emulator.

## License

This code is available under the Apache 2.0 license.
