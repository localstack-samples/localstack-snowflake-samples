package com.example.claims;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * The Snowflake-Next emulator as a Testcontainers container.
 *
 * <p>Readiness is checked from the host: an HTTPS
 * GET of {@code /} on the mapped port must answer below 500 (the TLS certificate is for
 * {@code *.localhost.localstack.cloud}, hence {@code allowInsecure()} for this probe only;
 * JDBC verifies it normally). Clients connect to
 * {@code snowflake.localhost.localstack.cloud}, which resolves to 127.0.0.1, on the
 * mapped port.
 */
public final class SnowflakeEmulatorContainer extends GenericContainer<SnowflakeEmulatorContainer> {

    public static final String DEFAULT_IMAGE = "localstack/snowflake-next:latest";
    public static final String HOST = "snowflake.localhost.localstack.cloud";
    private static final int PORT = 4566;

    public SnowflakeEmulatorContainer() {
        super(DockerImageName.parse(System.getenv().getOrDefault("SNOWFLAKE_IMAGE", DEFAULT_IMAGE)));
        withExposedPorts(PORT);
        withEnv("LOCALSTACK_AUTH_TOKEN", authToken());
        waitingFor(Wait.forHttps("/")
                .forPort(PORT)
                .allowInsecure()
                .forStatusCodeMatching(status -> status < 500)
                .withStartupTimeout(Duration.ofMinutes(3)));
    }

    public String jdbcUrl() {
        return "jdbc:snowflake://" + HOST + ":" + getMappedPort(PORT) + "/";
    }

    private static String authToken() {
        String token = System.getenv("LOCALSTACK_AUTH_TOKEN");
        if (token == null || token.isBlank()) {
            throw new IllegalStateException("LOCALSTACK_AUTH_TOKEN is not set. Export it, or run `make test`, "
                    + "which reads it from ~/.localstack/default.env.");
        }
        return token;
    }
}
