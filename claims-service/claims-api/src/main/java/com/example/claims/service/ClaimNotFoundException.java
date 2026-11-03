package com.example.claims.service;

public class ClaimNotFoundException extends RuntimeException {

    private final String claimId;

    public ClaimNotFoundException(String claimId) {
        super("Claim " + claimId + " does not exist");
        this.claimId = claimId;
    }

    public String claimId() {
        return claimId;
    }
}
