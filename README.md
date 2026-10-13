# LocalStack for Snowflake Samples

This repository contains sample projects that can be deployed on LocalStack for Snowflake to demonstrate local development and testing workflows for Snowflake data applications.

## Prerequisites

- A valid [LocalStack for Snowflake license](https://localstack.cloud/pricing). Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [Docker](https://docs.docker.com/get-docker/)
- [`lstk`](https://docs.localstack.cloud/snowflake/developer-tools/lstk/), which starts the Snowflake emulator with `lstk start --type snowflake`
- [`awslocal` CLI](https://docs.localstack.cloud/user-guide/integrations/aws-cli/) for the samples that also use AWS services
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation) (`snow`) with a `local` and a `localstack` connection, see below

## Configuration

Set your auth token before running any sample:

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
```

Alternatively, log in with `lstk login`.

Several samples use the Snowflake CLI with a connection named `local` or `localstack`. Create both, pointing at the emulator:

```bash
for name in local localstack; do
  snow connection add --connection-name $name --user test --password test \
    --account test --host snowflake.localhost.localstack.cloud --no-interactive
done
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

## Testing

Every sample has `make start` (or `make start-localstack` where `make start` runs the app) to start LocalStack, and `make test` to check that the sample works. The [CI workflow](.github/workflows/main.yml) runs `make install`, the start target and `make test` for each sample on every pull request.

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
