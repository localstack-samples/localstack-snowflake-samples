package com.example.claims.domain;

/** Lifecycle of a claim. Only {@code SUBMITTED} claims can be adjudicated. */
public enum ClaimStatus {
    SUBMITTED,
    APPROVED,
    DENIED
}
