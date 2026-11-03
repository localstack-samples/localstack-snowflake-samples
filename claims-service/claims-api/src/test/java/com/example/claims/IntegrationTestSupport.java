package com.example.claims;

import com.example.claims.domain.NewClaim;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HexFormat;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Boots the whole service (HTTP on a random port, HikariCP, Liquibase) against the
 * target chosen by {@code TEST_TARGET}. All test classes share this configuration, so
 * Spring caches one context and the suite migrates one schema on one emulator.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.main.banner-mode=off")
abstract class IntegrationTestSupport {

    @Autowired
    protected TestRestTemplate http;

    @Autowired
    protected JdbcTemplate jdbc;

    @DynamicPropertySource
    static void snowflake(DynamicPropertyRegistry registry) {
        SnowflakeTestTarget.get().register(registry);
    }

    /** A claim id no other test (or earlier run on a shared database) uses. */
    protected static String newClaimId(String tag) {
        return "CLM-" + tag + "-" + HexFormat.of().toHexDigits(ThreadLocalRandom.current().nextInt())
                .toUpperCase(Locale.ROOT);
    }

    protected static NewClaim claim(String claimId, String policyId, String providerId, String diagnosisCode,
                                    String serviceDate, String billedAmount) {
        return new NewClaim(claimId, policyId, providerId, diagnosisCode, LocalDate.parse(serviceDate),
                new BigDecimal(billedAmount));
    }

    protected ResponseEntity<JsonNode> submit(Object claim) {
        return http.postForEntity("/api/claims", claim, JsonNode.class);
    }

    protected ResponseEntity<JsonNode> adjudicate(String claimId) {
        return http.postForEntity("/api/claims/{id}/adjudication", null, JsonNode.class, claimId);
    }

    protected ResponseEntity<JsonNode> getClaim(String claimId) {
        return http.getForEntity("/api/claims/{id}", JsonNode.class, claimId);
    }

    /**
     * Polls until the value satisfies the condition. EXECUTE TASK queues a task run and
     * returns, so tests wait for the observable result with a hard timeout instead of
     * sleeping.
     */
    protected static <T> T awaitValue(String what, Duration timeout, Supplier<T> probe, Predicate<T> done) {
        Instant deadline = Instant.now().plus(timeout);
        T value = probe.get();
        while (!done.test(value)) {
            if (Instant.now().isAfter(deadline)) {
                throw new AssertionError("Timed out after " + timeout.toSeconds() + "s waiting for " + what
                        + "; last value: " + value);
            }
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            value = probe.get();
        }
        return value;
    }
}
