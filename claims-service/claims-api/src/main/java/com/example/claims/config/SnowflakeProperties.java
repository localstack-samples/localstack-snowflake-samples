package com.example.claims.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.nio.file.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Every Snowflake connection setting in one place. Values come from
 * {@code application.yml}, which maps the shared sample environment variables
 * ({@code SNOWFLAKE_ACCOUNT}, {@code SNOWFLAKE_USER}, ...) onto these fields.
 *
 * @param url            JDBC URL without parameters, e.g.
 *                       {@code jdbc:snowflake://snowflake.localhost.localstack.cloud:4566/}
 * @param account        account identifier ({@code snowflake} on the emulator)
 * @param user           service user that authenticates with a key pair
 * @param privateKeyPath unencrypted PKCS#8 PEM file holding the user's private key
 * @param role           role the service runs as; never ACCOUNTADMIN in production
 * @param warehouse      warehouse for queries, the procedure and the KPI task
 * @param database       database that holds the service schema
 * @param schema         schema owned by the service; Liquibase migrates it
 * @param queryTag       QUERY_TAG for every statement, for cost attribution in QUERY_HISTORY
 * @param maxPoolSize    HikariCP pool size
 */
@Validated
@ConfigurationProperties("claims.snowflake")
public record SnowflakeProperties(
        @NotBlank String url,
        @NotBlank String account,
        @NotBlank String user,
        @NotNull Path privateKeyPath,
        @NotBlank String role,
        @NotBlank String warehouse,
        @NotBlank String database,
        @NotBlank String schema,
        @DefaultValue("claims-service") String queryTag,
        @DefaultValue("5") @Min(1) int maxPoolSize) {
}
