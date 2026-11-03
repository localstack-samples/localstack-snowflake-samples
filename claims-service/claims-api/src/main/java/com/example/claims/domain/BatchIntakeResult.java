package com.example.claims.domain;

import java.util.List;

/**
 * Outcome of a bulk intake: every submitted row is either accepted or listed in
 * {@code rejected} with a reason.
 */
public record BatchIntakeResult(String batchId, int received, int accepted, List<Rejection> rejected) {

    /** Why a row of a batch was not loaded. */
    public enum Reason {
        INVALID_DIAGNOSIS_CODE,
        UNKNOWN_POLICY,
        UNKNOWN_PROVIDER,
        DUPLICATE_CLAIM_ID,
        DUPLICATE_IN_BATCH
    }

    public record Rejection(int row, String claimId, Reason reason) {
    }
}
