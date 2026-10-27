package com.example.claims.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

/** One row of {@code CLAIM_KPIS_MONTHLY}: claims per service month and current status. */
public record MonthlyKpi(
        LocalDate serviceMonth,
        ClaimStatus status,
        long claimCount,
        BigDecimal billedTotal,
        BigDecimal payableTotal) {
}
