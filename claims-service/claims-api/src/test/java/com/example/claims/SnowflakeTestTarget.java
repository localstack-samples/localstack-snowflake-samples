package com.example.claims;

import com.example.claims.config.PrivateKeys;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.test.context.DynamicPropertyRegistry;

/**
 * Where the suite runs, chosen by {@code TEST_TARGET}:
 * <ul>
 *   <li>{@code EMULATOR} (default): starts one {@link SnowflakeEmulatorContainer} for the
 *       whole run, generates a throwaway key pair and runs the {@code init/*.sf.sql}
 *       platform provisioning scripts as ACCOUNTADMIN;</li>
 *   <li>{@code SNOWFLAKE_CLOUD}: connects to the account in the {@code SNOWFLAKE_*}
 *       variables, creates a {@code CLAIMS_IT_<random>} database for this run and drops it
 *       when the JVM exits.</li>
 * </ul>
 * Nothing else differs between the two targets.
 */
final class SnowflakeTestTarget {

    private static final boolean EMULATOR = !"SNOWFLAKE_CLOUD".equals(
            System.getenv().getOrDefault("TEST_TARGET", "EMULATOR").toUpperCase(Locale.ROOT));

    private static final Duration READY_TIMEOUT = Duration.ofSeconds(90);
    private static SnowflakeTestTarget instance;

    private final Map<String, String> properties;

    private SnowflakeTestTarget(Map<String, String> properties) {
        this.properties = properties;
    }

    static synchronized SnowflakeTestTarget get() {
        if (instance == null) {
            instance = EMULATOR ? emulator() : cloud();
        }
        return instance;
    }

    void register(DynamicPropertyRegistry registry) {
        properties.forEach((name, value) -> registry.add(name, () -> value));
    }

    private static SnowflakeTestTarget emulator() {
        SnowflakeEmulatorContainer container = new SnowflakeEmulatorContainer();
        container.start(); // stopped by Testcontainers' Ryuk when the JVM exits
        Path key = generateThrowawayKey();
        String url = container.jdbcUrl();
        try (Connection admin = connectWhenReady(url, "snowflake", "SAMPLE_SVC", key, "ACCOUNTADMIN")) {
            for (Path script : initScripts(Path.of(System.getProperty("claims.init-dir", "../init")))) {
                runScript(admin, script);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Provisioning the emulator failed", e);
        }
        return new SnowflakeTestTarget(settings(url, "snowflake", "SAMPLE_SVC", key, "CLAIMS_APP", "CLAIMS_WH",
                "CLAIMS"));
    }

    private static SnowflakeTestTarget cloud() {
        String account = requireEnv("SNOWFLAKE_ACCOUNT");
        String host = System.getenv().getOrDefault("SNOWFLAKE_HOST", account + ".snowflakecomputing.com");
        String url = "jdbc:snowflake://" + host + ":443/";
        String user = requireEnv("SNOWFLAKE_USER");
        Path key = Path.of(requireEnv("SNOWFLAKE_PRIVATE_KEY_PATH"));
        String role = requireEnv("SNOWFLAKE_ROLE");
        String warehouse = requireEnv("SNOWFLAKE_WAREHOUSE");
        String database = "CLAIMS_IT_" + HexFormat.of().toHexDigits(ThreadLocalRandom.current().nextInt())
                .toUpperCase(Locale.ROOT);
        try (Connection admin = connectWhenReady(url, account, user, key, role);
             Statement statement = admin.createStatement()) {
            statement.execute("CREATE DATABASE " + database);
            statement.execute("CREATE SCHEMA " + database + ".CORE");
        } catch (SQLException e) {
            throw new IllegalStateException("Creating the test database failed", e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try (Connection admin = connect(url, account, user, key, role);
                 Statement statement = admin.createStatement()) {
                statement.execute("DROP DATABASE IF EXISTS " + database);
            } catch (SQLException e) {
                System.err.println("Could not drop test database " + database + ": " + e.getMessage());
            }
        }));
        return new SnowflakeTestTarget(settings(url, account, user, key, role, warehouse, database));
    }

    private static Map<String, String> settings(String url, String account, String user, Path key, String role,
                                                String warehouse, String database) {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("claims.snowflake.url", url);
        settings.put("claims.snowflake.account", account);
        settings.put("claims.snowflake.user", user);
        settings.put("claims.snowflake.private-key-path", key.toString());
        settings.put("claims.snowflake.role", role);
        settings.put("claims.snowflake.warehouse", warehouse);
        settings.put("claims.snowflake.database", database);
        settings.put("claims.snowflake.schema", "CORE");
        settings.put("claims.snowflake.query-tag", "claims-service-it");
        return settings;
    }

    /** The first connection retries until the emulator answers SQL, with a hard timeout. */
    private static Connection connectWhenReady(String url, String account, String user, Path key, String role)
            throws SQLException {
        Instant deadline = Instant.now().plus(READY_TIMEOUT);
        while (true) {
            try {
                Connection connection = connect(url, account, user, key, role);
                try (Statement statement = connection.createStatement()) {
                    statement.execute("SELECT 1");
                }
                return connection;
            } catch (SQLException e) {
                if (Instant.now().isAfter(deadline)) {
                    throw new SQLException("Snowflake at " + url + " did not answer SELECT 1 within "
                            + READY_TIMEOUT.toSeconds() + "s", e);
                }
                pause();
            }
        }
    }

    private static Connection connect(String url, String account, String user, Path key, String role)
            throws SQLException {
        Properties props = new Properties();
        props.put("account", account);
        props.put("user", user);
        props.put("authenticator", "SNOWFLAKE_JWT");
        props.put("privateKey", PrivateKeys.readPkcs8Pem(key));
        props.put("role", role);
        return DriverManager.getConnection(url, props);
    }

    /** The same {@code init/NN_name.sf.sql} files the compose emulator runs at startup, in name order. */
    private static List<Path> initScripts(Path dir) {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(f -> f.getFileName().toString().endsWith(".sf.sql")).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot list init scripts in " + dir.toAbsolutePath(), e);
        }
    }

    private static void runScript(Connection connection, Path script) throws SQLException {
        String sql;
        try {
            sql = Files.readString(script);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read platform script " + script.toAbsolutePath(), e);
        }
        String withoutComments = sql.lines()
                .filter(line -> !line.strip().startsWith("--"))
                .collect(Collectors.joining("\n"));
        List<String> statements = Arrays.stream(withoutComments.split(";\\s*(\\R|$)"))
                .map(String::strip)
                .filter(s -> !s.isEmpty())
                .toList();
        try (Statement statement = connection.createStatement()) {
            for (String each : statements) {
                statement.execute(each);
            }
        }
    }

    /** A fresh RSA key for this run, written outside the repository with owner-only permissions. */
    private static Path generateThrowawayKey() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            byte[] pkcs8 = generator.generateKeyPair().getPrivate().getEncoded();
            String pem = "-----BEGIN PRIVATE KEY-----\n"
                    + Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(pkcs8)
                    + "\n-----END PRIVATE KEY-----\n";
            Path file = FileSystems.getDefault().supportedFileAttributeViews().contains("posix")
                    ? Files.createTempFile("claims-it-", ".p8",
                            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
                    : Files.createTempFile("claims-it-", ".p8");
            Files.writeString(file, pem);
            file.toFile().deleteOnExit();
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("TEST_TARGET=SNOWFLAKE_CLOUD needs " + name);
        }
        return value;
    }

    private static void pause() {
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
