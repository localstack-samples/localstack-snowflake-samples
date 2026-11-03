package com.example.claims.domain;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Instant;

/** A claim as stored in {@code CLAIMS}. Adjudication fills the last four fields. */
public record Claim(
        String claimId,
        String policyId,
        String providerId,
        String diagnosisCode,
        LocalDate serviceDate,
        BigDecimal billedAmount,
        ClaimStatus status,
        BigDecimal payableAmount,
        String denialReason,
        Instant submittedAt,
        Instant adjudicatedAt) {
}
