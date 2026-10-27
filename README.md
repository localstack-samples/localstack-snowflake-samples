# LocalStack for Snowflake Samples

This repository contains sample projects that can be deployed on LocalStack for Snowflake to demonstrate local development and testing workflows for Snowflake data applications.

The samples run on the next-generation Snowflake emulator, [`localstack/snowflake-next`](https://hub.docker.com/r/localstack/snowflake-next). It emulates Snowflake only, so the samples that also use AWS services run LocalStack AWS next to it with Docker Compose.

## Prerequisites

- A valid [LocalStack for Snowflake license](https://localstack.cloud/pricing) with access to the Snowflake Next preview. Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [Docker](https://docs.docker.com/get-docker/)
- [`lstk`](https://docs.localstack.cloud/snowflake/developer-tools/lstk/) 1.3.0 or later, which starts the emulator with `lstk start --type snowflake --image localstack/snowflake-next:latest`
- [Docker Compose](https://docs.docker.com/compose/) and the [`awslocal` CLI](https://docs.localstack.cloud/user-guide/integrations/aws-cli/) for the samples that also use AWS services: `airflow-dbt-transformation`, `glue-snowflake-integration`, `lambda-snowpark-connector` and `multi-container`. Each of them has a `docker-compose.yml` that starts LocalStack AWS on port 4566 and Snowflake Next on a second port.
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation) (`snow`) with a `local` and a `localstack` connection, see below
- For the application samples (`claims-service`, `fleet-telemetry`, `metering-api`, `migrate-and-swap`, `order-to-cash-cdc`, `phi-guardrails`, `slim-ci-lab` and `tenantboard`), Docker Compose v2 and the sample's own toolchain: [`uv`](https://docs.astral.sh/uv/) for the Python samples, Node.js 22 for the Tenantboard UI, JDK 21 for Claims Service, Go for Metering API and the .NET 10 SDK for Fleet Telemetry. They start the emulator themselves and do not need `lstk` or `snow`. On Windows, run them inside WSL 2.

## Configuration

Set your auth token before running any sample:

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
```

Alternatively, log in with `lstk login`. The application samples read the token from the environment or from `~/.localstack/default.env`.

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
| [claims-service](./claims-service) | A Java 21 Spring Boot service with stored procedures, a Java UDF and integration tests on Testcontainers. |
| [credit-card-fraud-detection-with-snowpark](./credit-card-fraud-detection-with-snowpark) | Feature engineering and ML-oriented fraud workflows with Snowpark. |
| [credit-scoring-with-snowpark](./credit-scoring-with-snowpark) | Exploratory data analysis for credit scoring using Snowpark. |
| [fleet-telemetry](./fleet-telemetry) | An ASP.NET Core service that ingests vehicle telemetry as JSON and analyzes it with FLATTEN, window functions and a dynamic table. |
| [glue-snowflake-integration](./glue-snowflake-integration) | Integrate AWS Glue ETL jobs with Snowflake in a local environment. |
| [lambda-snowpark-connector](./lambda-snowpark-connector) | Use Snowpark from an AWS Lambda function running on LocalStack. |
| [metering-api](./metering-api) | A Go usage-metering and invoicing service with idempotent ingestion, transactional invoice close and async rollups. |
| [migrate-and-swap](./migrate-and-swap) | Zero-downtime schema releases with schemachange: migrate a clone, validate it, swap it in, roll back in one step. |
| [multi-container](./multi-container) | Run LocalStack for Snowflake and LocalStack AWS emulation in separate containers. |
| [order-to-cash-cdc](./order-to-cash-cdc) | Kafka change events processed into current-state and history tables with streams, tasks and dynamic tables. |
| [phi-guardrails](./phi-guardrails) | Governance as code: tag-based masking, row access policies and a tested access matrix. |
| [predicting-customer-spend](./predicting-customer-spend) | Predict customer spend using Snowpark and Python ML tooling. |
| [slim-ci-lab](./slim-ci-lab) | A dbt Core project with a Slim CI pull-request pipeline: state selection, defer and per-PR schemas. |
| [soda-data-quality-checks](./soda-data-quality-checks) | Execute Soda data quality checks against local Snowflake tables. |
| [streamlit-snowpark-dynamic-filters](./streamlit-snowpark-dynamic-filters) | Streamlit application with dynamic Snowpark-powered filters. |
| [tenantboard](./tenantboard) | A multi-tenant FastAPI and React analytics app with tenant isolation and PII masking enforced by Snowflake policies. |

## Testing

A fresh Snowflake Next instance only has the `SNOWFLAKE` database, as in a new Snowflake account, so each sample creates the database it uses before loading data.

Every sample has `make start` (or `make start-localstack` where `make start` runs the app) to start LocalStack, and `make test` to check that the sample works. The [CI workflow](.github/workflows/main.yml) runs `make install`, the start target and `make test` for each sample on every pull request, and every night against the newest `localstack/snowflake-next` image to catch regressions.

The application samples document `make up`, `make seed`, `make demo`, `make test` and `make down` in their READMEs, with `make start` and `make stop` as aliases for `make up` and `make down`. They bind the emulator to `127.0.0.1:4566`; set `SNOWFLAKE_PORT` to use another port. Their test suites also run against a real Snowflake account with `make test-cloud`, using the `SNOWFLAKE_*` settings in each sample's `.env.example`.

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
