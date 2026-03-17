# LocalStack Multi-Container Setup

This sample demonstrates how to run LocalStack for Snowflake and LocalStack for AWS in separate Docker containers with proper networking configuration.

## Prerequisites

- A valid [LocalStack for Snowflake license](https://snowflake.localstack.cloud/). Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [Docker](https://docs.docker.com/get-docker/)
- [`localstack` CLI](https://docs.localstack.cloud/getting-started/installation/#localstack-cli)
- [`awslocal` CLI](https://docs.localstack.cloud/user-guide/integrations/aws-cli/)
- [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)

## Start LocalStack

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
localstack auth set-token $LOCALSTACK_AUTH_TOKEN
localstack start -d
localstack wait -t 30
```

## Architecture

This setup uses Docker Compose to run two containers:
- `localstack-snowflake`: LocalStack for Snowflake emulator (exposed on port 4567)
- `localstack-aws`: LocalStack AWS emulator (exposed on port 4566)

Both containers are connected through a Docker bridge network named `localstack`.

## Important Configuration

## SF_HOSTNAME_REGEX

The `SF_HOSTNAME_REGEX` environment variable is critical for proper Snowflake connectivity within the Docker network:

```yaml
SF_HOSTNAME_REGEX=localstack-snowflake.*
```

The pattern `localstack-snowflake.*` matches:
- `localstack-snowflake` (exact container name)
- Any hostname starting with `localstack-snowflake` (e.g., with domain suffixes)

**Alternative Configuration:** You can also use just `.*` in `SF_HOSTNAME_REGEX` to match any hostname string, which provides maximum flexibility:

```yaml
SF_HOSTNAME_REGEX=.*
```

This will accept connections from any hostname, which can be useful for development environments or when you need to support multiple container names or DNS configurations.

## Getting Started

1. Set your LocalStack Auth Token:
   ```bash
   export LOCALSTACK_AUTH_TOKEN=your-token-here
   ```

2. Start the containers:
   ```bash
   docker-compose up -d
   ```

3. Verify containers are running:
   ```bash
   docker-compose ps
   ```

## Testing Connectivity

To verify that the LocalStack for AWS container can successfully communicate with the LocalStack for Snowflake container, run the following command:

```bash
docker exec localstack-aws curl -d '{}' localstack-snowflake:4567/session
```

## Expected Result

```json
{"success": true}
```

This confirms that:
- The Docker network is properly configured
- The `SF_HOSTNAME_REGEX` is correctly matching the container name
- The Snowflake emulator is accepting connections

## Troubleshooting

If you don't see `{"success": true}`, check:

1. Both containers are running:
   ```bash
   docker-compose ps
   ```

2. The containers are on the same network:
   ```bash
   docker network inspect localstack
   ```

3. The SF_HOSTNAME_REGEX is set correctly in docker-compose.yml

4. Check container logs:
   ```bash
   docker-compose logs localstack-snowflake
   docker-compose logs localstack
   ```

## Cross-Container Integration: Loading Data from S3 to Snowflake

While this basic setup demonstrates container networking, loading data from an S3 bucket in `localstack-aws` to a database in `localstack-snowflake` requires additional configuration. Specifically, you'll need to configure the S3 endpoint settings so that Snowflake knows how to reach the AWS emulator:

## Required Additional Configuration

Add these environment variables to the `localstack-snowflake` service in `docker-compose.yml`:

```yaml
environment:
  - SF_S3_ENDPOINT=http://localstack-aws:4566
  - SF_S3_ENDPOINT_EXTERNAL=http://localhost:4566
```

- `SF_S3_ENDPOINT`: Internal endpoint for Snowflake container to access S3 within the Docker network
- `SF_S3_ENDPOINT_EXTERNAL`: External endpoint for accessing S3 from your host machine

## Example Workflow

Once configured, you can:

1. Create an S3 bucket and upload data in LocalStack AWS
2. Create a Snowflake stage pointing to the S3 bucket
3. Use `COPY INTO` commands to load data from S3 into Snowflake tables

For detailed instructions on S3 integration and other cross-service capabilities, see the references below.

## Clean Up

Stop and remove containers:
```bash
docker-compose down
```

Remove volumes:
```bash
docker-compose down -v
rm -rf ./volume
```

## References

- [LocalStack for Snowflake Configuration](https://docs.localstack.cloud/snowflake/capabilities/configuration/) - Comprehensive configuration options including S3 endpoints and network settings
- [LocalStack Multi-Service Networking](https://docs.localstack.cloud/references/network-troubleshooting/) - Network troubleshooting and configuration for multi-container setups
