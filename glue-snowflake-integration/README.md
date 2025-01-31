# AWS Glue - Snowflake Integration with LocalStack

This project demonstrates how to run AWS Glue ETL jobs that interact with Snowflake using LocalStack. It showcases a local development environment where both AWS Glue and Snowflake emulator run in the same container, making it perfect for development and testing.

## Project Structure

```
glue-snowflake-integration/
├── script/         # Contains Glue ETL job scripts
├── jars/          # Directory for Snowflake JDBC and Spark connector JARs
├── tf/            # Terraform configurations for AWS resources
├── init.sf.sql    # Snowflake initialization script
├── deploy.sh      # Deployment script
└── Makefile       # Commands for managing the environment
```

## Prerequisites

- [`localstack` CLI](https://docs.localstack.cloud/getting-started/installation/#localstack-cli) with [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/) environment variable set
- [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)
- Docker
- Make

## Getting Started

1. **Start LocalStack with Snowflake Emulator**
   ```bash
   make start
   ```
   This command starts LocalStack with the Snowflake emulator enabled. The `init.sf.sql` script is automatically mounted and executed to create the necessary Snowflake objects.

2. **Deploy and Run the Glue Job**
   ```bash
   make deploy
   ```
   This command:
   - Initializes Terraform infrastructure
   - Downloads required Snowflake JARs
   - Uploads JARs and Glue script to S3
   - Starts the Glue job and monitors its execution

3. **Stop the Environment**
   ```bash
   make stop
   ```

## Snowflake Initialization

The `init.sf.sql` script automatically creates and populates a sample table in the Snowflake emulator when LocalStack starts. This table is then used by the Glue job for demonstration purposes. The script:
- Creates a table named `src_glue`
- Populates it with sample data
- Sets up the necessary structure for the Glue job to interact with

## Integration Details

- The project uses LocalStack's Snowflake emulator, which runs alongside AWS services in the same container
- Glue jobs can connect to the Snowflake instance using JDBC
- Required Snowflake dependencies (JDBC driver and Spark connector) are automatically downloaded and made available to the Glue job

## Available Make Commands

- `make start` - Start LocalStack with Snowflake emulator
- `make stop` - Stop LocalStack
- `make deploy` - Deploy and test the Glue job
- `make usage` - Display available make commands

## Notes

- The environment uses LocalStack's Snowflake emulator, which provides a subset of Snowflake functionality suitable for testing
- All AWS resources are created locally through Terraform
- The deployment script (`deploy.sh`) handles the entire workflow from infrastructure setup to job execution 