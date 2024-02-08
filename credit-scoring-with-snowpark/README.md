# Credit Scoring with Snowpark & LocalStack

This notebook demonstrates how to use Snowpark for Python to perform exploratory data analysis (EDA) on a credit scoring dataset. This notebook uses LocalStack's Snowflake extension to emulate Snowflake APIs locally.

## Prerequisites

- [LocalStack](https://localstack.cloud/) with `LOCALSTACK_AUTH_TOKEN` environment variable set
- [LocalStack Snowflake extension](https://discuss.localstack.cloud/t/introducing-the-localstack-snowflake-extension-experimental/665)
- [Snowpark for Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)
- [Jupyter Notebook](https://jupyter.org/)

## Running the notebook

Start the LocalStack container using your preferred method. Ensure that you have access to LocalStack's Snowflake extension. Start a Jupyter Notebook server and open the notebook in your browser.

```bash
jupyter notebook
```

Open the `credit-scoring-eda.ipynb` notebook and run the cells to see the results. Follow the instructions in the notebook to install the required packages and set up the Snowflake extension.

## License

This code is available under the Apache 2.0 license.
