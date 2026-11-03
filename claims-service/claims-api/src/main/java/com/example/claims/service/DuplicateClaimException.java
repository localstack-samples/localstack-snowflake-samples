package com.example.claims.service;

public class DuplicateClaimException extends RuntimeException {

    private final String claimId;

    public DuplicateClaimException(String claimId) {
        super("Claim " + claimId + " was already submitted");
        this.claimId = claimId;
    }

    public String claimId() {
        return claimId;
    }
}
