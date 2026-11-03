package com.example.claims.service;

/**
 * {@code ADJUDICATE_CLAIM} raised its {@code claim_not_pending} exception: the claim
 * exists but is no longer {@code SUBMITTED}, so adjudicating it again is refused.
 */
public class ClaimNotAdjudicableException extends RuntimeException {

    private final String claimId;
    private final int snowflakeErrorCode;

    public ClaimNotAdjudicableException(String claimId, int snowflakeErrorCode, Throwable cause) {
        super("Claim " + claimId + " is not pending adjudication", cause);
        this.claimId = claimId;
        this.snowflakeErrorCode = snowflakeErrorCode;
    }

    public String claimId() {
        return claimId;
    }

    public int snowflakeErrorCode() {
        return snowflakeErrorCode;
    }
}
