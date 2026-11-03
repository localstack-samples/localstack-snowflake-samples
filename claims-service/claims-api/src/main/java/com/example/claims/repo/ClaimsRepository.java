package com.example.claims.repo;

import com.example.claims.domain.BatchIntakeResult.Reason;
import com.example.claims.domain.BatchIntakeResult.Rejection;
import com.example.claims.domain.Claim;
import com.example.claims.domain.ClaimStatus;
import com.example.claims.domain.NewClaim;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * All SQL against the claim tables. Every value is a bind parameter; object names
 * are unqualified and resolve against the connection's database and schema.
 */
@Repository
public class ClaimsRepository {

    private static final String CLAIM_COLUMNS = """
            CLAIM_ID, POLICY_ID, PROVIDER_ID, DIAGNOSIS_CODE, SERVICE_DATE, BILLED_AMOUNT,
            STATUS, PAYABLE_AMOUNT, DENIAL_REASON, SUBMITTED_AT, ADJUDICATED_AT""";

    /**
     * Snowflake does not enforce PRIMARY KEY constraints, so "insert unless the id
     * exists" is a MERGE: concurrent MERGEs on one table serialize on its lock, so a
     * second submission of the same id sees the first and inserts nothing.
     */
    private static final String INSERT_IF_ABSENT = """
            MERGE INTO CLAIMS c
            USING (SELECT ? AS CLAIM_ID, ? AS POLICY_ID, ? AS PROVIDER_ID, ? AS DIAGNOSIS_CODE,
                          ?::DATE AS SERVICE_DATE, ?::NUMBER(12, 2) AS BILLED_AMOUNT) s
            ON c.CLAIM_ID = s.CLAIM_ID
            WHEN NOT MATCHED THEN INSERT
                (CLAIM_ID, POLICY_ID, PROVIDER_ID, DIAGNOSIS_CODE, SERVICE_DATE, BILLED_AMOUNT, STATUS)
            VALUES
                (s.CLAIM_ID, s.POLICY_ID, s.PROVIDER_ID, s.DIAGNOSIS_CODE, s.SERVICE_DATE, s.BILLED_AMOUNT, 'SUBMITTED')
            """;

    private static final String CHECK_REFERENCES = """
            SELECT IS_VALID_ICD10(?) AS DIAGNOSIS_VALID,
                   (SELECT COUNT(*) FROM POLICIES WHERE POLICY_ID = ?) > 0 AS POLICY_EXISTS,
                   (SELECT COUNT(*) FROM PROVIDERS WHERE PROVIDER_ID = ?) > 0 AS PROVIDER_EXISTS
            """;

    private static final String STAGE_INTAKE_ROW = """
            INSERT INTO CLAIM_INTAKE
                (BATCH_ID, ROW_NO, CLAIM_ID, POLICY_ID, PROVIDER_ID, DIAGNOSIS_CODE, SERVICE_DATE, BILLED_AMOUNT)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    /** Marks every staged row that must not be loaded, in rule order. */
    private static final String CLASSIFY_INTAKE = """
            UPDATE CLAIM_INTAKE t
            SET REJECT_REASON = s.REASON
            FROM (
                SELECT i.ROW_NO,
                       CASE
                           WHEN NOT IS_VALID_ICD10(i.DIAGNOSIS_CODE) THEN 'INVALID_DIAGNOSIS_CODE'
                           WHEN p.POLICY_ID IS NULL THEN 'UNKNOWN_POLICY'
                           WHEN v.PROVIDER_ID IS NULL THEN 'UNKNOWN_PROVIDER'
                           WHEN c.CLAIM_ID IS NOT NULL THEN 'DUPLICATE_CLAIM_ID'
                           WHEN ROW_NUMBER() OVER (PARTITION BY i.CLAIM_ID ORDER BY i.ROW_NO) > 1
                               THEN 'DUPLICATE_IN_BATCH'
                       END AS REASON
                FROM CLAIM_INTAKE i
                LEFT JOIN POLICIES p ON p.POLICY_ID = i.POLICY_ID
                LEFT JOIN PROVIDERS v ON v.PROVIDER_ID = i.PROVIDER_ID
                LEFT JOIN CLAIMS c ON c.CLAIM_ID = i.CLAIM_ID
                WHERE i.BATCH_ID = ?
            ) s
            WHERE t.BATCH_ID = ? AND t.ROW_NO = s.ROW_NO AND s.REASON IS NOT NULL
            """;

    private static final String PROMOTE_INTAKE = """
            MERGE INTO CLAIMS c
            USING (SELECT CLAIM_ID, POLICY_ID, PROVIDER_ID, DIAGNOSIS_CODE, SERVICE_DATE, BILLED_AMOUNT
                   FROM CLAIM_INTAKE
                   WHERE BATCH_ID = ? AND REJECT_REASON IS NULL) s
            ON c.CLAIM_ID = s.CLAIM_ID
            WHEN NOT MATCHED THEN INSERT
                (CLAIM_ID, POLICY_ID, PROVIDER_ID, DIAGNOSIS_CODE, SERVICE_DATE, BILLED_AMOUNT, STATUS)
            VALUES
                (s.CLAIM_ID, s.POLICY_ID, s.PROVIDER_ID, s.DIAGNOSIS_CODE, s.SERVICE_DATE, s.BILLED_AMOUNT, 'SUBMITTED')
            """;

    private final JdbcTemplate jdbc;

    public ClaimsRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Claim> findById(String claimId) {
        return jdbc.query("SELECT " + CLAIM_COLUMNS + " FROM CLAIMS WHERE CLAIM_ID = ?", CLAIM_ROW, claimId)
                .stream().findFirst();
    }

    public List<Claim> findRecent(ClaimStatus status, int limit) {
        String statusName = status == null ? null : status.name();
        return jdbc.query("SELECT " + CLAIM_COLUMNS + """
                 FROM CLAIMS
                WHERE (?::VARCHAR IS NULL OR STATUS = ?)
                ORDER BY SUBMITTED_AT DESC, CLAIM_ID
                LIMIT ?""", CLAIM_ROW, statusName, statusName, limit);
    }

    public ReferenceCheck checkReferences(NewClaim claim) {
        return jdbc.queryForObject(CHECK_REFERENCES,
                (rs, n) -> new ReferenceCheck(
                        rs.getBoolean("DIAGNOSIS_VALID"),
                        rs.getBoolean("POLICY_EXISTS"),
                        rs.getBoolean("PROVIDER_EXISTS")),
                claim.diagnosisCode(), claim.policyId(), claim.providerId());
    }

    /** Returns {@code false} when a claim with this id already exists. */
    public boolean insertIfAbsent(NewClaim claim) {
        return jdbc.update(INSERT_IF_ABSENT,
                claim.claimId(), claim.policyId(), claim.providerId(), claim.diagnosisCode(),
                claim.serviceDate().toString(), claim.billedAmount()) == 1;
    }

    /**
     * Stages a batch with one {@code JdbcTemplate.batchUpdate}: the Snowflake driver
     * sends all rows as array binds of a single INSERT (and switches to a temporary
     * stage upload for very large batches) instead of one round trip per row.
     */
    public void stageIntake(String batchId, List<NewClaim> claims) {
        jdbc.batchUpdate(STAGE_INTAKE_ROW, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                NewClaim claim = claims.get(i);
                ps.setString(1, batchId);
                ps.setInt(2, i + 1);
                ps.setString(3, claim.claimId());
                ps.setString(4, claim.policyId());
                ps.setString(5, claim.providerId());
                ps.setString(6, claim.diagnosisCode());
                ps.setString(7, claim.serviceDate().toString());
                ps.setBigDecimal(8, claim.billedAmount());
            }

            @Override
            public int getBatchSize() {
                return claims.size();
            }
        });
    }

    public void classifyIntake(String batchId) {
        jdbc.update(CLASSIFY_INTAKE, batchId, batchId);
    }

    /** Moves the batch's accepted rows into CLAIMS and returns how many were inserted. */
    public int promoteIntake(String batchId) {
        return jdbc.update(PROMOTE_INTAKE, batchId);
    }

    public List<Rejection> intakeRejections(String batchId) {
        return jdbc.query("""
                SELECT ROW_NO, CLAIM_ID, REJECT_REASON
                FROM CLAIM_INTAKE
                WHERE BATCH_ID = ? AND REJECT_REASON IS NOT NULL
                ORDER BY ROW_NO""",
                (rs, n) -> new Rejection(rs.getInt(1), rs.getString(2), Reason.valueOf(rs.getString(3))),
                batchId);
    }

    public void clearIntake(String batchId) {
        jdbc.update("DELETE FROM CLAIM_INTAKE WHERE BATCH_ID = ?", batchId);
    }

    /**
     * Runs the {@code ADJUDICATE_CLAIM} stored procedure, which owns its transaction,
     * and returns its decision ({@code APPROVED} or {@code DENIED}).
     */
    public String adjudicate(String claimId) {
        return jdbc.queryForObject("CALL ADJUDICATE_CLAIM(?)", String.class, claimId);
    }

    public List<String> policyIds() {
        return jdbc.queryForList("SELECT POLICY_ID FROM POLICIES ORDER BY POLICY_ID", String.class);
    }

    public List<String> providerIds() {
        return jdbc.queryForList("SELECT PROVIDER_ID FROM PROVIDERS ORDER BY PROVIDER_ID", String.class);
    }

    /** Result of the pre-insert checks that run in Snowflake. */
    public record ReferenceCheck(boolean diagnosisValid, boolean policyExists, boolean providerExists) {
    }

    private static final RowMapper<Claim> CLAIM_ROW = (rs, n) -> new Claim(
            rs.getString("CLAIM_ID"),
            rs.getString("POLICY_ID"),
            rs.getString("PROVIDER_ID"),
            rs.getString("DIAGNOSIS_CODE"),
            rs.getDate("SERVICE_DATE").toLocalDate(),
            rs.getBigDecimal("BILLED_AMOUNT"),
            ClaimStatus.valueOf(rs.getString("STATUS")),
            rs.getBigDecimal("PAYABLE_AMOUNT"),
            rs.getString("DENIAL_REASON"),
            instant(rs, "SUBMITTED_AT"),
            instant(rs, "ADJUDICATED_AT"));

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp ts = rs.getTimestamp(column);
        return ts == null ? null : ts.toInstant();
    }
}
