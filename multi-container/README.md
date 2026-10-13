# LocalStack Multi-Container Setup

This sample demonstrates how to run LocalStack for Snowflake and LocalStack for AWS in separate Docker containers with proper networking configuration.

## Prerequisites

- A valid [LocalStack for Snowflake license](https://www.localstack.cloud/pricing). Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [Docker](https://docs.docker.com/get-docker/)
- [`localstack` CLI](https://docs.localstack.cloud/getting-started/installation/#localstack-cli)
- [`awslocal` CLI](https://docs.localstack.cloud/user-guide/integrations/aws-cli/)
- [LocalStack Snowflake emulator](https://snowflake.localstack.cloud/getting-started/installation/)

## Architecture

This setup uses Docker Compose to run two containers:
- `localstack-snowflake`: LocalStack for Snowflake emulator (exposed on port 4567)
- `localstack-aws`: LocalStack AWS emulator (exposed on port 4566)

Both containers are connected through a Docker bridge network named `localstack`.

## Important Configuration

## SF_HOSTNAMES

The `SF_HOSTNAMES` environment variable lists the hostnames the Snowflake emulator answers to:

```yaml
SF_HOSTNAMES=localstack-snowflake,snowflake.localhost.localstack.cloud
```

- `localstack-snowflake` is the container name, used by the AWS container inside the Docker network.
- `snowflake.localhost.localstack.cloud` is the default hostname, used by clients on your machine.

When `SF_HOSTNAMES` is not set, the emulator only answers to hostnames containing `snowflake.`, which does not match the container name. Older versions used `SF_HOSTNAME_REGEX` for this.

## Getting Started

1. Set your LocalStack Auth Token:
   ```bash
   export LOCALSTACK_AUTH_TOKEN=your-token-here
   ```

2. Start the containers:
   ```bash
   docker compose up -d --wait
   ```

3. Verify containers are running:
   ```bash
   docker compose ps
   ```

## Testing Connectivity

To verify that the LocalStack for AWS container can successfully communicate with the LocalStack for Snowflake container, run the following command:

```bash
docker exec localstack-aws python3 -c "import urllib.request; print(urllib.request.urlopen(urllib.request.Request('http://localstack-snowflake:4567/session', data=b'{}')).read().decode())"
```

The `localstack/localstack` image does not include `curl`, so the request is sent with Python.

## Expected Result

```json
{"success": true}
```

This confirms that:
- The Docker network is properly configured
- `SF_HOSTNAMES` routes the container name to the Snowflake emulator
- The Snowflake emulator is accepting connections

## Troubleshooting

If you don't see `{"success": true}`, check:

1. Both containers are running:
   ```bash
   docker compose ps
   ```

2. The containers are on the same network:
   ```bash
   docker network inspect localstack
   ```

3. `SF_HOSTNAMES` is set correctly in `docker-compose.yml`

4. Check container logs:
   ```bash
   docker compose logs localstack-snowflake
   docker compose logs localstack-aws
   ```

## Cross-Container Integration: Loading Data from S3 to Snowflake

Loading data from an S3 bucket in `localstack-aws` into a table in `localstack-snowflake` needs the Snowflake emulator to know how to reach the AWS emulator. `docker-compose.yml` sets this up:

```yaml
environment:
  - SF_S3_ENDPOINT=http://localstack-aws:4566
  - SF_S3_ENDPOINT_EXTERNAL=http://localhost:4566
```

- `SF_S3_ENDPOINT`: Internal endpoint for Snowflake container to access S3 within the Docker network
- `SF_S3_ENDPOINT_EXTERNAL`: External endpoint for accessing S3 from your host machine

## Example Workflow

With this configuration, you can:

1. Create an S3 bucket and upload data in LocalStack AWS
2. Create a Snowflake stage pointing to the S3 bucket
3. Use `COPY INTO` commands to load data from S3 into Snowflake tables

`make test` runs both checks: the session request from the AWS container, and a CSV loaded from an S3 bucket into a Snowflake table through a stage.

For detailed instructions on S3 integration and other cross-service capabilities, see the references below.

## Clean Up

Stop and remove containers:
```bash
docker compose down
```

Remove volumes:
```bash
docker compose down -v
rm -rf ./volume
```

## References

- [LocalStack for Snowflake Configuration](https://docs.localstack.cloud/snowflake/capabilities/configuration/) - Comprehensive configuration options including S3 endpoints and network settings
- [LocalStack Multi-Service Networking](https://docs.localstack.cloud/references/network-troubleshooting/) - Network troubleshooting and configuration for multi-container setups
