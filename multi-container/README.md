# LocalStack Multi-Container Setup

This sample demonstrates how to run LocalStack Snowflake and LocalStack AWS in separate Docker containers with proper networking configuration.

## Architecture

This setup uses Docker Compose to run two containers:
- `localstack-snowflake`: LocalStack Snowflake emulator (exposed on port 4567)
- `localstack-aws`: LocalStack AWS emulator (exposed on port 4566)

Both containers are connected through a Docker bridge network named `localstack`.

## Important Configuration

### SF_HOSTNAME_REGEX

The `SF_HOSTNAME_REGEX` environment variable is critical for proper Snowflake connectivity within the Docker network:

```yaml
SF_HOSTNAME_REGEX=localstack-snowflake.*
```

This regex pattern tells LocalStack Snowflake to accept connections using the container name `localstack-snowflake` as the hostname. Without this configuration, the Snowflake connector would only accept connections to localhost or 127.0.0.1, which wouldn't work for inter-container communication.

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

To verify that the LocalStack AWS container can successfully communicate with the LocalStack Snowflake container, run the following command:

```bash
docker exec localstack-aws curl -d '{}' localstack-snowflake:4566/session
```

### Expected Result

```json
{"success": true}
```

This confirms that:
- The Docker network is properly configured
- The `SF_HOSTNAME_REGEX` is correctly matching the container name
- The Snowflake emulator is accepting connections

### Troubleshooting

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
