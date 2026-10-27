package com.example.claims.service;

import com.example.claims.domain.Claim;
import com.example.claims.repo.ClaimsRepository;
import java.sql.SQLException;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

/**
 * Adjudicates a claim by calling the {@code ADJUDICATE_CLAIM} SQL Scripting procedure.
 *
 * <p>The procedure owns its transaction (BEGIN ... COMMIT, ROLLBACK on error), so this
 * service deliberately runs it outside a Spring transaction. Business errors come back
 * as the procedure's custom exceptions, which Snowflake reports with the exception's
 * number as the JDBC error code and SQLSTATE {@code P0001}; they are matched by code,
 * never by message text (Snowflake prefixes the message with the raising line).
 */
@Service
public class AdjudicationService {

    /** {@code claim_not_found EXCEPTION (-20001, ...)} in 002-adjudicate-procedure.sql. */
    static final int CLAIM_NOT_FOUND = -20001;
    /** {@code claim_not_pending EXCEPTION (-20002, ...)} in 002-adjudicate-procedure.sql. */
    static final int CLAIM_NOT_PENDING = -20002;

    private final ClaimsRepository claims;

    public AdjudicationService(ClaimsRepository claims) {
        this.claims = claims;
    }

    public Claim adjudicate(String claimId) {
        try {
            claims.adjudicate(claimId);
        } catch (DataAccessException e) {
            if (e.getMostSpecificCause() instanceof SQLException sql) {
                switch (sql.getErrorCode()) {
                    case CLAIM_NOT_FOUND -> throw new ClaimNotFoundException(claimId);
                    case CLAIM_NOT_PENDING -> throw new ClaimNotAdjudicableException(claimId, sql.getErrorCode(), e);
                    default -> throw e;
                }
            }
            throw e;
        }
        return claims.findById(claimId).orElseThrow(() -> new ClaimNotFoundException(claimId));
    }
}
