# AWS Glue - Snowflake Integration with LocalStack

This project runs an AWS Glue ETL job that reads from and writes to Snowflake, fully on your machine. AWS Glue, S3 and IAM run in LocalStack AWS, and Snowflake runs in the [Snowflake emulator](https://hub.docker.com/r/localstack/snowflake-next) (`localstack/snowflake-next`). The two emulators run as separate containers on one Docker network.

The Glue job:

1. reads the `src_glue` table, which `init.sf.sql` creates when the Snowflake emulator starts
2. keeps the rows where `status` is `TRUE`
3. writes them to a new `tgt_glue` table

## Project Structure

```
glue-snowflake-integration/
├── script/              # Glue ETL job script
├── tf/                  # Terraform configuration for the AWS resources
├── docker-compose.yml   # LocalStack AWS and the Snowflake emulator
├── init.sf.sql          # Snowflake initialization script
├── deploy.sh            # Deploys and runs the Glue job
├── test.sh              # Runs the job and checks the result in Snowflake
└── Makefile             # Commands for managing the environment
```

## Prerequisites

- A valid [LocalStack for Snowflake license](https://www.localstack.cloud/pricing) with access to the Snowflake emulator preview. Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- Docker with Docker Compose
- Terraform, [`tflocal`](https://github.com/localstack/terraform-local) and [`awslocal`](https://github.com/localstack/awscli-local)
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation) (`snow`) and `jq`, for `make test`
- Make and `wget`

Run `make install` to install `tflocal`, `awslocal` and the Snowflake CLI with pip.

## Getting Started

1. **Start LocalStack AWS and the Snowflake emulator**

   ```bash
   export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
   make start
   ```

   This runs `docker compose up -d --wait`, which starts:

   - `glue-localstack-aws`: LocalStack AWS (`localstack/localstack-pro`) on `localhost:4566`
   - `glue-snowflake`: the Snowflake emulator on `localhost:4567`, with `init.sf.sql` mounted as an init script

2. **Deploy and run the Glue job**

   ```bash
   make deploy
   ```

   This command:

   - applies the Terraform configuration (S3 bucket, IAM role and Glue job)
   - downloads the Snowflake JDBC driver and the Spark connector for Spark 3.3 (Glue 4.0)
   - uploads the JARs and the job script to S3
   - starts the Glue job and waits until it finishes

3. **Check the result in Snowflake**

   ```bash
   snow sql --temporary-connection --account test --user test --password test \
     --host snowflake.localhost.localstack.cloud --port 4567 \
     -q "SELECT * FROM test.public.tgt_glue"
   ```

4. **Stop the environment**

   ```bash
   make stop
   ```

## How the Containers Connect

- LocalStack AWS starts the Glue job in its own container. `MAIN_DOCKER_NETWORK=glue-snowflake` puts that container on the compose network, so it reaches the Snowflake emulator by its service name.
- The job connects with `sfURL` set to `snowflake:4566` and `sfSSL` set to `off`. The emulator serves plain HTTP and HTTPS on every port, and its TLS certificate only covers `*.localhost.localstack.cloud`, so plain HTTP is used inside the network.
- `SF_HOSTNAMES` lists the hostnames the emulator answers to: `snowflake` for the Glue job, and `snowflake.localhost.localstack.cloud` and `localhost` for clients on your machine.
- `SNOWFLAKE_INIT_ACCOUNT=TEST` makes `init.sf.sql` seed account `TEST`, the account the job and the Snowflake CLI connect to.

## Available Make Commands

- `make install` - Install `tflocal`, `awslocal` and the Snowflake CLI
- `make start` - Start LocalStack AWS and the Snowflake emulator
- `make deploy` - Deploy and run the Glue job
- `make test` - Run the Glue job, then check its logs and the rows in `tgt_glue`
- `make logs` - Show the logs of both containers
- `make stop` - Stop and remove both containers
- `make usage` - Display available make commands
