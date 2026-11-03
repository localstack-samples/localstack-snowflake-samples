package com.example.claims;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * Error paths: each one asserts the HTTP problem the client sees and that nothing was
 * written that should not have been.
 */
class AdjudicationErrorPathTest extends IntegrationTestSupport {

    @Autowired
    private DataSource dataSource;

    @ParameterizedTest
    @ValueSource(strings = {"E11.", "E119", "e11.9", "XYZ", "1E1.9"})
    void invalidIcd10CodeIsRejectedByTheUdfWith422AndNotStored(String diagnosisCode) {
        String id = newClaimId("DX");

        ResponseEntity<JsonNode> response = submit(claim(id, "POL-1001", "PRV-201", diagnosisCode, "2026-02-10",
                "300.00"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(response.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(response.getBody().get("type").asText()).isEqualTo("urn:claims:problem:invalid-diagnosis-code");
        assertThat(response.getBody().get("detail").asText()).contains(diagnosisCode);
        assertThat(getClaim(id).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void duplicateClaimIdIsRejectedWith409AndTheOriginalIsUnchanged() {
        String id = newClaimId("DUP");
        assertThat(submit(claim(id, "POL-1001", "PRV-201", "I10", "2026-03-01", "100.00")).getStatusCode())
                .isEqualTo(HttpStatus.CREATED);

        ResponseEntity<JsonNode> second = submit(claim(id, "POL-1002", "PRV-202", "J06.9", "2026-03-02", "999.00"));

        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(second.getBody().get("type").asText()).isEqualTo("urn:claims:problem:duplicate-claim");
        assertThat(second.getBody().get("claimId").asText()).isEqualTo(id);
        JsonNode stored = getClaim(id).getBody();
        assertThat(stored.get("policyId").asText()).isEqualTo("POL-1001");
        assertThat(stored.get("billedAmount").decimalValue()).isEqualByComparingTo("100.00");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM CLAIMS WHERE CLAIM_ID = ?", Long.class, id)).isEqualTo(1L);
    }

    @Test
    void adjudicatingTwiceRaisesTheProcedureExceptionMappedTo422() {
        String id = newClaimId("TWICE");
        submit(claim(id, "POL-1001", "PRV-201", "I10", "2026-03-05", "1000.00"));
        assertThat(adjudicate(id).getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<JsonNode> second = adjudicate(id);

        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(second.getBody().get("type").asText()).isEqualTo("urn:claims:problem:claim-not-pending");
        assertThat(second.getBody().get("snowflakeErrorCode").asInt()).isEqualTo(-20002);
        // The failed call changed nothing: still one decision, still the first payout.
        Map<String, Object> log = jdbc.queryForMap(
                "SELECT COUNT(*) AS N, MAX(PAYABLE_AMOUNT) AS PAYABLE FROM ADJUDICATION_LOG WHERE CLAIM_ID = ?", id);
        assertThat(log).containsEntry("N", 1L);
        assertThat(log.get("PAYABLE").toString()).isEqualTo("675.00"); // (1000-250)*0.90
        assertThat(getClaim(id).getBody().get("status").asText()).isEqualTo("APPROVED");
    }

    @Test
    void adjudicatingAnUnknownClaimIs404FromTheProcedureClaimNotFoundException() throws SQLException {
        ResponseEntity<JsonNode> response = adjudicate("CLM-DOES-NOT-EXIST");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody().get("type").asText()).isEqualTo("urn:claims:problem:claim-not-found");

        // The wire contract the service maps on: the custom exception's number and SQLSTATE P0001.
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            SQLException error = catchThrowableOfType(SQLException.class,
                    () -> statement.execute("CALL ADJUDICATE_CLAIM('CLM-DOES-NOT-EXIST')"));
            assertThat(error.getErrorCode()).isEqualTo(-20001);
            assertThat(error.getSQLState()).isEqualTo("P0001");
            assertThat(error.getMessage()).contains("Claim not found");
        }
    }

    @Test
    void unknownPolicyIsRejectedWith422() {
        String id = newClaimId("POL");

        ResponseEntity<JsonNode> response = submit(claim(id, "POL-0000", "PRV-201", "I10", "2026-03-05", "100.00"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(response.getBody().get("type").asText()).isEqualTo("urn:claims:problem:unknown-policy");
        assertThat(getClaim(id).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void malformedRequestIsRejectedWith400BeforeReachingSnowflake() {
        ResponseEntity<JsonNode> response = submit(Map.of(
                "claimId", "not-a-claim-id",
                "policyId", "POL-1001",
                "providerId", "PRV-201",
                "diagnosisCode", "I10",
                "serviceDate", "2026-03-05",
                "billedAmount", -5));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    }
}
