# LocalStack Multi-Container Setup

This sample demonstrates how to run LocalStack for Snowflake and LocalStack for AWS in separate Docker containers with proper networking configuration.

## Prerequisites

- A valid [LocalStack for Snowflake license](https://www.localstack.cloud/pricing) with access to the Snowflake Next preview. Your license provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/).
- [Docker](https://docs.docker.com/get-docker/) with Docker Compose
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html), [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation) (`snow`) and `jq`, to run `make test`

## Architecture

`docker-compose.yml` runs two containers on a Docker bridge network named `localstack`:

- `localstack-snowflake`: the Snowflake emulator ([`localstack/snowflake-next`](https://hub.docker.com/r/localstack/snowflake-next)), on port 4567
- `localstack-aws`: the AWS emulator (`localstack/localstack`), on port 4566

Both ports are published on `127.0.0.1` only.

## Important Configuration

### GATEWAY_LISTEN

The Snowflake emulator listens on 4566 and 443 by default. `GATEWAY_LISTEN=0.0.0.0:4567` moves it to 4567, so it does not clash with the AWS container. Every port serves both HTTPS and plain HTTP.

### SF_HOSTNAMES

`SF_HOSTNAMES` lists the hostnames the Snowflake emulator answers to:

```yaml
SF_HOSTNAMES=snowflake.localhost.localstack.cloud,localstack-snowflake
```

- `snowflake.localhost.localstack.cloud` is the default hostname, used by clients on your machine. As the first entry it is also the API host the emulator advertises.
- `localstack-snowflake` is the container name, used by the AWS container inside the Docker network.

The list replaces the built-in one. A request whose `Host` matches no entry gets a 404, so add `localhost` if your clients use it.

### Health check

The image has `bash` but no `curl` or `python3`, so the health check reads `/_localstack/health` over `bash`'s `/dev/tcp`.

## Getting Started

1. Set your LocalStack auth token:
   ```bash
   export LOCALSTACK_AUTH_TOKEN=your-token-here
   ```

2. Start the containers and wait until both are healthy:
   ```bash
   make start
   ```

3. Run the test:
   ```bash
   make test
   ```

## Testing Connectivity

To check that the AWS container reaches the Snowflake emulator by container name, log in from inside it. The `localstack/localstack` image has no `curl`, so the request is sent with Python:

```bash
docker exec localstack-aws python3 -c "
import json, urllib.request
body = json.dumps({'data': {'LOGIN_NAME': 'test', 'PASSWORD': 'test', 'ACCOUNT_NAME': 'test'}}).encode()
req = urllib.request.Request('http://localstack-snowflake:4567/session/v1/login-request', data=body,
                             headers={'Content-Type': 'application/json'})
print(json.load(urllib.request.urlopen(req))['success'])
"
```

It prints `True`. This confirms that:

- The Docker network is configured
- `SF_HOSTNAMES` routes the container name to the Snowflake emulator
- The Snowflake emulator accepts logins

## Troubleshooting

If the login fails, check:

1. Both containers are running and healthy:
   ```bash
   docker compose ps
   ```

2. The containers are on the same network:
   ```bash
   docker network inspect localstack
   ```

3. `SF_HOSTNAMES` includes the container name

4. The container logs:
   ```bash
   make logs
   ```

## Cross-Container Integration: Loading Data from S3 to Snowflake

Loading data from an S3 bucket in `localstack-aws` into a table in `localstack-snowflake` needs the Snowflake emulator to know how to reach the AWS emulator. `docker-compose.yml` sets this up:

```yaml
environment:
  - SF_S3_ENDPOINT=http://localstack-aws:4566
```

`SF_S3_ENDPOINT` is the S3 endpoint the Snowflake emulator calls to read external `s3://` stages (`COPY INTO`, `LIST`, Snowpipe auto-ingest). It is never advertised to clients.

Internal stages (`PUT`/`GET`) are served by the Snowflake emulator itself, on the port the client connected on, so they need no setting here.

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
make stop
```

Remove volumes:
```bash
docker compose down -v
rm -rf ./volume
```

## References

- [`localstack/snowflake-next` on Docker Hub](https://hub.docker.com/r/localstack/snowflake-next): configuration options of the Snowflake Next emulator, including `SF_HOSTNAMES`, `SF_S3_ENDPOINT` and `GATEWAY_LISTEN`
- [LocalStack for Snowflake Configuration](https://docs.localstack.cloud/snowflake/capabilities/configuration/): configuration of the generally available emulator
- [LocalStack Multi-Service Networking](https://docs.localstack.cloud/references/network-troubleshooting/): network troubleshooting and configuration for multi-container setups
