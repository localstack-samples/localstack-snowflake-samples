package com.example.claims;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.example.claims.domain.BatchIntakeResult;
import com.example.claims.domain.BatchIntakeResult.Reason;
import com.example.claims.domain.MonthlyKpi;
import com.example.claims.domain.NewClaim;
import com.example.claims.seed.SyntheticClaims;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Happy paths end to end: REST -> JDBC -> Snowflake (UDF, procedure, stream, task).
 *
 * <p>Expected amounts are worked out by hand from the policy terms in
 * {@code 004-reference-data.sql} and the payout rule: payable = max(billed - deductible, 0)
 * x (1 - member share), where the member share is the policy coinsurance in network and
 * coinsurance + 0.30 out of network.
 */
class ClaimsIntegrationTest extends IntegrationTestSupport {

    @Test
    void approvesInNetworkClaimAndPaysBilledLessDeductibleLessCoinsurance() {
        // POL-1001: deductible 250.00, coinsurance 0.10; PRV-201 is in network.
        String id = newClaimId("OK");
        ResponseEntity<JsonNode> created = submit(claim(id, "POL-1001", "PRV-201", "E11.9", "2026-02-10", "1200.00"));
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getHeaders().getLocation()).hasToString("/api/claims/" + id);
        assertThat(created.getBody().get("status").asText()).isEqualTo("SUBMITTED");

        JsonNode adjudicated = adjudicate(id).getBody();

        assertThat(adjudicated.get("status").asText()).isEqualTo("APPROVED");
        assertThat(adjudicated.get("payableAmount").decimalValue()).isEqualByComparingTo("855.00"); // (1200-250)*0.90
        assertThat(adjudicated.get("denialReason").isNull()).isTrue();
        assertThat(adjudicated.get("adjudicatedAt").isNull()).isFalse();
        assertThat(jdbc.queryForMap(
                "SELECT DECISION, PAYABLE_AMOUNT, RULES_EVALUATED FROM ADJUDICATION_LOG WHERE CLAIM_ID = ?", id))
                .containsEntry("DECISION", "APPROVED")
                .containsEntry("RULES_EVALUATED", 3L)
                .hasEntrySatisfying("PAYABLE_AMOUNT", v -> assertThat((BigDecimal) v).isEqualByComparingTo("855.00"));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(textBlock = """
            # case,                                  policy,   provider, dx,     service date, billed,  status,   reason,           payable
            out of network adds 30 points of share,  POL-1002, PRV-205,  M54.50, 2026-03-02,   3000.00, APPROVED, ,                 1000.00
            billed below deductible pays nothing,    POL-1003, PRV-203,  J06.9,  2026-02-01,   800.00,  APPROVED, ,                 0.00
            last day of the policy period is covered,POL-1004, PRV-201,  I10,    2026-03-31,   1300.00, APPROVED, ,                 240.00
            day after termination is denied,         POL-1004, PRV-201,  I10,    2026-04-01,   1300.00, DENIED,   POLICY_INACTIVE,  0.00
            day before the policy starts is denied,  POL-1005, PRV-201,  I10,    2026-03-31,   400.00,  DENIED,   POLICY_INACTIVE,  0.00
            excluded diagnosis category is denied,   POL-1001, PRV-202,  Z41.1,  2026-02-14,   2500.00, DENIED,   NOT_COVERED,      0.00
            billed exactly at the claim limit,       POL-1006, PRV-201,  S93.401A,2026-05-05,  4000.00, APPROVED, ,                 900.00
            one cent over the claim limit is denied, POL-1006, PRV-201,  S93.401A,2026-05-05,  4000.01, DENIED,   OVER_CLAIM_LIMIT, 0.00
            first failing rule by priority wins,     POL-1004, PRV-202,  Z41.1,  2026-04-15,   9000.00, DENIED,   POLICY_INACTIVE,  0.00
            """)
    void adjudicationAppliesRulesInPriorityOrder(String scenario, String policy, String provider, String dx,
                                                 String serviceDate, String billed, String status, String reason,
                                                 String payable) {
        // POL-1002 1000.00/0.20 -> OON share 0.50: (3000-1000)*0.50 = 1000.00
        // POL-1003 2500.00/0.40: 800 < 2500 -> 0.00
        // POL-1004 1000.00/0.20, period ends 2026-03-31: (1300-1000)*0.80 = 240.00
        // POL-1005 period starts 2026-04-01
        // POL-1006 2500.00/0.40, limit 4000.00: (4000-2500)*0.60 = 900.00
        String id = newClaimId("RULE");
        assertThat(submit(claim(id, policy, provider, dx, serviceDate, billed)).getStatusCode())
                .isEqualTo(HttpStatus.CREATED);

        JsonNode adjudicated = adjudicate(id).getBody();

        assertThat(adjudicated.get("status").asText()).as(scenario).isEqualTo(status);
        assertThat(adjudicated.get("denialReason").isNull() ? null : adjudicated.get("denialReason").asText())
                .as(scenario).isEqualTo(reason);
        assertThat(adjudicated.get("payableAmount").decimalValue()).as(scenario).isEqualByComparingTo(payable);
    }

    @Test
    void bulkIntakeLoadsValidRowsWithOneBatchAndReportsEveryRejectedRow() {
        String prefix = newClaimId("B") + "-";
        List<NewClaim> valid = SyntheticClaims.generate(7, 250, prefix,
                List.of("POL-1001", "POL-1002", "POL-1003", "POL-1007", "POL-1008"),
                List.of("PRV-201", "PRV-202", "PRV-203", "PRV-204", "PRV-205", "PRV-206"));
        String existing = newClaimId("EXIST");
        assertThat(submit(claim(existing, "POL-1001", "PRV-201", "I10", "2026-01-20", "150.00")).getStatusCode())
                .isEqualTo(HttpStatus.CREATED);

        List<NewClaim> batch = new ArrayList<>(valid);
        batch.add(claim(prefix + "BAD-DX", "POL-1001", "PRV-201", "E11.", "2026-02-01", "100.00"));      // row 251
        batch.add(claim(prefix + "BAD-POL", "POL-9999", "PRV-201", "I10", "2026-02-01", "100.00"));      // row 252
        batch.add(claim(prefix + "BAD-PRV", "POL-1001", "PRV-999", "I10", "2026-02-01", "100.00"));      // row 253
        batch.add(valid.get(0));                                                                          // row 254
        batch.add(claim(existing, "POL-1001", "PRV-201", "I10", "2026-01-20", "150.00"));                // row 255

        ResponseEntity<BatchIntakeResult> response = http.exchange("/api/claims/batch", HttpMethod.POST,
                new HttpEntity<>(batch), BatchIntakeResult.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        BatchIntakeResult result = response.getBody();
        assertThat(result.received()).isEqualTo(255);
        assertThat(result.accepted()).isEqualTo(250);
        assertThat(result.rejected())
                .extracting(BatchIntakeResult.Rejection::row, BatchIntakeResult.Rejection::claimId,
                        BatchIntakeResult.Rejection::reason)
                .containsExactly(
                        tuple(251, prefix + "BAD-DX", Reason.INVALID_DIAGNOSIS_CODE),
                        tuple(252, prefix + "BAD-POL", Reason.UNKNOWN_POLICY),
                        tuple(253, prefix + "BAD-PRV", Reason.UNKNOWN_PROVIDER),
                        tuple(254, valid.get(0).claimId(), Reason.DUPLICATE_IN_BATCH),
                        tuple(255, existing, Reason.DUPLICATE_CLAIM_ID));

        BigDecimal expectedBilled = valid.stream().map(NewClaim::billedAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
        Map<String, Object> stored = jdbc.queryForMap("""
                SELECT COUNT(*) AS N, COUNT(DISTINCT CLAIM_ID) AS IDS, SUM(BILLED_AMOUNT) AS BILLED
                FROM CLAIMS WHERE STARTSWITH(CLAIM_ID, ?) AND STATUS = 'SUBMITTED'""", prefix);
        assertThat(stored).containsEntry("N", 250L).containsEntry("IDS", 250L);
        assertThat((BigDecimal) stored.get("BILLED")).isEqualByComparingTo(expectedBilled);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM CLAIM_INTAKE WHERE BATCH_ID = ?", Long.class,
                result.batchId())).isZero();
    }

    /**
     * Headline test: CLAIM_KPIS_MONTHLY is maintained incrementally by the
     * CLAIMS_CHANGES stream and the REFRESH_CLAIM_KPIS task, triggered with EXECUTE TASK.
     * The second refresh only sees one UPDATE (a DELETE plus an INSERT in the stream),
     * which must move the claim from SUBMITTED to APPROVED.
     */
    @Test
    void kpiTableReflectsNewAndAdjudicatedClaimsAfterExecuteTask() {
        // September 2026 is used by no other test.
        String a = newClaimId("KPIA");
        String b = newClaimId("KPIB");
        String c = newClaimId("KPIC");
        submit(claim(a, "POL-1001", "PRV-201", "I10", "2026-09-03", "500.00"));     // approve: (500-250)*0.90 = 225.00
        submit(claim(b, "POL-1002", "PRV-202", "M54.50", "2026-09-10", "1500.00")); // later: (1500-1000)*0.80 = 400.00
        submit(claim(c, "POL-1001", "PRV-203", "Z41.1", "2026-09-21", "900.00"));   // deny: NOT_COVERED
        adjudicate(a);
        adjudicate(c);

        assertThat(refreshAndAwaitSeptember("APPROVED=1/500.00/225.00 DENIED=1/900.00/0.00 SUBMITTED=1/1500.00/0.00"))
                .isEqualTo("APPROVED=1/500.00/225.00 DENIED=1/900.00/0.00 SUBMITTED=1/1500.00/0.00");

        adjudicate(b);

        assertThat(refreshAndAwaitSeptember("APPROVED=2/2000.00/625.00 DENIED=1/900.00/0.00"))
                .isEqualTo("APPROVED=2/2000.00/625.00 DENIED=1/900.00/0.00");
    }

    private String refreshAndAwaitSeptember(String expected) {
        assertThat(http.postForEntity("/api/kpis/refresh", null, Void.class).getStatusCode())
                .isEqualTo(HttpStatus.ACCEPTED);
        return awaitValue("September 2026 KPIs " + expected, Duration.ofSeconds(90),
                () -> september(http.exchange("/api/kpis/monthly", HttpMethod.GET, null,
                        new ParameterizedTypeReference<List<MonthlyKpi>>() { }).getBody()),
                expected::equals);
    }

    /** Renders September's rows as "STATUS=count/billed/payable ..." for one readable comparison. */
    private static String september(List<MonthlyKpi> kpis) {
        return kpis.stream()
                .filter(k -> k.serviceMonth().equals(LocalDate.of(2026, 9, 1)))
                .map(k -> k.status() + "=" + k.claimCount() + "/" + k.billedTotal().setScale(2) + "/"
                        + k.payableTotal().setScale(2))
                .sorted()
                .collect(Collectors.joining(" "));
    }

    @Test
    void listsClaimsByStatus() {
        String id = newClaimId("LIST");
        submit(claim(id, "POL-1008", "PRV-204", "F41.1", "2026-06-30", "640.00"));

        JsonNode submitted = http.getForEntity("/api/claims?status=SUBMITTED&limit=500", JsonNode.class).getBody();

        List<String> statuses = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        submitted.forEach(node -> {
            statuses.add(node.get("status").asText());
            ids.add(node.get("claimId").asText());
        });
        assertThat(ids).contains(id);
        assertThat(statuses).containsOnly("SUBMITTED");
        assertThat(http.getForEntity("/api/claims?limit=0", JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
