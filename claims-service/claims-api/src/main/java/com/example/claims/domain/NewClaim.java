package com.example.claims.domain;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * A claim submitted by a provider. Bean Validation checks the shape; whether the
 * diagnosis code is a valid ICD-10 code, and whether the policy and provider exist,
 * is decided in Snowflake (the {@code IS_VALID_ICD10} SQL UDF and reference tables),
 * so batch loads and ad-hoc SQL apply exactly the same rules.
 */
public record NewClaim(
        @NotBlank @Pattern(regexp = "CLM-[A-Z0-9-]{4,26}", message = "must look like CLM-2026-000001") String claimId,
        @NotBlank @Size(max = 20) String policyId,
        @NotBlank @Size(max = 20) String providerId,
        @NotBlank @Size(max = 8) String diagnosisCode,
        @NotNull @PastOrPresent LocalDate serviceDate,
        @NotNull @Positive @Digits(integer = 10, fraction = 2) BigDecimal billedAmount) {
}
