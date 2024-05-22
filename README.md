# Examples & use-cases around LocalStack for Snowflake

This repository contains sample projects that can be deployed on LocalStack for Snowflake to demonstrate how to use the Snowflake emulator to develop and test data applications locally.

## Pre-requisites

Each samples project has its own pre-requisites. Please refer to the `README.md` file in each project for more information. In general, you will need the following:

* [Docker](https://docs.docker.com/get-docker/) installed 
* [LocalStack CLI](https://docs.localstack.cloud/getting-started/installation/#localstack-cli) with [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/) environment variable set
- [LocalStack for Snowflake](https://snowflake.localstack.cloud/getting-started/installation/)

## Outline

| Project Name                                                                                          | Description                                                                                               |
|-------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| [citi-bike-data-app](./citi-bike-data-app)                                                            | A sample application that demonstrates how to seed Citibike data into Snowflake and query the data using a web server. |
| [credit-card-fraud-detection-with-snowpark](./credit-card-fraud-detection-with-snowpark)              | A sample application that demonstrates how to use Snowpark to query credit card fraud data in Snowflake.   |
| [credit-scoring-with-snowpark](./credit-scoring-with-snowpark)                                        | A sample application that demonstrates how to use Snowpark to query credit scoring data in Snowflake.      |
| [lambda-snowpark-connector](./lambda-snowpark-connector)                                                  | A sample application that demonstrates how to use Snowpark in an AWS Lambda function to query data in Snowflake. |
| [predicting-customer-spend](./predicting-customer-spend)                                              | A sample application that demonstrates how to use Snowpark to query customer spend data in Snowflake.      |
| [streamlit-snowpark-dynamic-filters](./streamlit-snowpark-dynamic-filters)                            | A sample application that demonstrates how to use Snowpark to query data in Snowflake and visualize the data using Streamlit. |

## Checking out a single sample

To check out a single sample, you can use the following commands:

```bash
mkdir localstack-snowflake-samples && cd localstack-snowflake-samples
git init
git remote add origin -f git@github.com:localstack/localstack-pro-samples.git
git config core.sparseCheckout true
echo <LOCALSTACK_SAMPLE_DIRECTORY_NAME> >> .git/info/sparse-checkout
git pull origin master
```

The above commands use `sparse-checkout` to only pull the sample you are interested in. You can find the name of the sample directory in the table above.
