package com.example.claims.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * HikariCP pool of Snowflake JDBC connections using key-pair (SNOWFLAKE_JWT) auth.
 *
 * <p>Role, warehouse, database and schema are set on every connection, so no
 * statement depends on a user's defaults. Snowflake has no default warehouse and a
 * new user's default role is PUBLIC.
 */
@Configuration(proxyBeanMethods = false)
public class SnowflakeDataSourceConfig {

    @Bean(destroyMethod = "close")
    public HikariDataSource dataSource(SnowflakeProperties props) {
        HikariConfig config = new HikariConfig();
        config.setPoolName("snowflake");
        config.setJdbcUrl(props.url());
        config.setMaximumPoolSize(props.maxPoolSize());
        config.setMinimumIdle(1);
        // Snowflake sessions expire after 4 hours of inactivity by default; recycle well before that.
        config.setMaxLifetime(Duration.ofMinutes(30).toMillis());

        config.addDataSourceProperty("account", props.account());
        config.addDataSourceProperty("user", props.user());
        config.addDataSourceProperty("authenticator", "SNOWFLAKE_JWT");
        config.addDataSourceProperty("privateKey", PrivateKeys.readPkcs8Pem(props.privateKeyPath()));
        config.addDataSourceProperty("role", props.role());
        config.addDataSourceProperty("warehouse", props.warehouse());
        config.addDataSourceProperty("db", props.database());
        config.addDataSourceProperty("schema", props.schema());
        config.addDataSourceProperty("application", "claims-service");
        config.addDataSourceProperty("QUERY_TAG", props.queryTag());
        // Fail fast on a bad role/warehouse/database instead of running with a NULL context.
        config.addDataSourceProperty("validateDefaultParameters", "true");
        return new HikariDataSource(config);
    }
}
