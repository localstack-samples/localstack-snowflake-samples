# Predicting customer spend with Snowpark & LocalStack

An E-Commerce company aims to apply machine learning for understanding customer engagement with its digital platforms (website and app). The goal is to determine whether to prioritize improving the mobile app or website. Using a Linear Regression model, we will assess the impact of user activity on the likelihood of increased spending.

In this sample notebook, we will explore how you can use LocalStack's Snowflake emulator extension, to develop and test your Snowflake data pipelines entirely on your local machine! In this notebook, we will showcase how you can use LocalStack's Snowflake with Snowpark for Python and your favorite Python libraries for data analysis and machine learning.

## Prerequisites

-   [LocalStack](https://localstack.cloud/)  with  `LOCALSTACK_AUTH_TOKEN`  environment variable set
-   [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)
-   [Snowpark for Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)
-   [Jupyter Notebook](https://jupyter.org/)

## Running the notebook

Start the [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/) using your preferred method. Ensure that you have access to LocalStack's Snowflake emulator. Start a Jupyter Notebook server and open the notebook in your browser.

```bash
jupyter notebook
```

Open the `linear-regression-model.ipynb` notebook and run the cells to see the results. Follow the instructions in the notebook to install the required packages and set up the Snowflake extension.

## License

This code is available under the Apache 2.0 license.
