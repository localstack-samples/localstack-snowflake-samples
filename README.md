# LocalStack for Snowflake Samples

This repository contains sample projects that can be deployed on LocalStack for Snowflake to demonstrate local development and testing workflows for Snowflake data applications.

## Prerequisites

- A valid [LocalStack for Snowflake license](https://localstack.cloud/pricing). Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [Docker](https://docs.docker.com/get-docker/)
- [`localstack` CLI](https://docs.localstack.cloud/getting-started/installation/#localstack-cli)
- [`awslocal` CLI](https://docs.localstack.cloud/user-guide/integrations/aws-cli/)
- [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)

## Configuration

Set your auth token before running any sample:

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
```

Alternatively, use the LocalStack CLI:

```bash
localstack auth set-token <your-auth-token>
```

## Outline

| Project Name | Description |
|---|---|
| [airflow-dbt-transformation](./airflow-dbt-transformation) | Run local data transformation pipelines with Airflow, dbt, and Snowpark. |
| [citi-bike-data-app](./citi-bike-data-app) | Seed Citibike data into Snowflake and query it from a web application. |
| [credit-card-fraud-detection-with-snowpark](./credit-card-fraud-detection-with-snowpark) | Feature engineering and ML-oriented fraud workflows with Snowpark. |
| [credit-scoring-with-snowpark](./credit-scoring-with-snowpark) | Exploratory data analysis for credit scoring using Snowpark. |
| [glue-snowflake-integration](./glue-snowflake-integration) | Integrate AWS Glue ETL jobs with Snowflake in a local environment. |
| [lambda-snowpark-connector](./lambda-snowpark-connector) | Use Snowpark from an AWS Lambda function running on LocalStack. |
| [multi-container](./multi-container) | Run LocalStack for Snowflake and LocalStack AWS emulation in separate containers. |
| [predicting-customer-spend](./predicting-customer-spend) | Predict customer spend using Snowpark and Python ML tooling. |
| [soda-data-quality-checks](./soda-data-quality-checks) | Execute Soda data quality checks against local Snowflake tables. |
| [streamlit-snowpark-dynamic-filters](./streamlit-snowpark-dynamic-filters) | Streamlit application with dynamic Snowpark-powered filters. |

## Checking Out A Single Sample

To check out only one sample directory:

```bash
mkdir localstack-snowflake-samples && cd localstack-snowflake-samples
git init
git remote add origin -f git@github.com:localstack-samples/localstack-snowflake-samples.git
git config core.sparseCheckout true
echo <LOCALSTACK_SAMPLE_DIRECTORY_NAME> >> .git/info/sparse-checkout
git pull origin master
```

The commands above use sparse checkout to pull only the sample you need.
