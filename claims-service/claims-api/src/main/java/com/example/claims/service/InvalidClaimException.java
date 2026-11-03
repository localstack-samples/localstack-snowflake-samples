package com.example.claims.service;

/**
 * The claim is well-formed but cannot be accepted: its diagnosis code fails the
 * {@code IS_VALID_ICD10} UDF, or it references an unknown policy or provider.
 */
public class InvalidClaimException extends RuntimeException {

    /** Machine-readable problem codes, also used as the problem type suffix. */
    public enum Problem {
        INVALID_DIAGNOSIS_CODE("invalid-diagnosis-code"),
        UNKNOWN_POLICY("unknown-policy"),
        UNKNOWN_PROVIDER("unknown-provider");

        private final String slug;

        Problem(String slug) {
            this.slug = slug;
        }

        public String slug() {
            return slug;
        }
    }

    private final Problem problem;

    public InvalidClaimException(Problem problem, String detail) {
        super(detail);
        this.problem = problem;
    }

    public Problem problem() {
        return problem;
    }
}
