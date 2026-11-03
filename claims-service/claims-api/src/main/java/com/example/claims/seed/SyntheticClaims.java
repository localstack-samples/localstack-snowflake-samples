package com.example.claims.seed;

import com.example.claims.domain.NewClaim;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Deterministic synthetic claims: the same seed always yields the same claims, so
 * seeding is repeatable and re-seeding is rejected as duplicates.
 */
public final class SyntheticClaims {

    /** Real ICD-10-CM codes of common outpatient diagnoses. */
    static final List<String> DIAGNOSIS_CODES = List.of(
            "I10",       // essential (primary) hypertension
            "E11.9",     // type 2 diabetes mellitus without complications
            "J06.9",     // acute upper respiratory infection, unspecified
            "M54.50",    // low back pain, unspecified
            "Z00.00",    // general adult medical examination without abnormal findings
            "K21.9",     // gastro-esophageal reflux disease without esophagitis
            "F41.1",     // generalized anxiety disorder
            "J45.909",   // unspecified asthma, uncomplicated
            "N39.0",     // urinary tract infection, site not specified
            "S93.401A",  // sprain of unspecified ligament of right ankle, initial encounter
            "R51.9",     // headache, unspecified
            "Z41.1");    // encounter for cosmetic surgery (excluded from coverage)

    static final LocalDate FIRST_SERVICE_DATE = LocalDate.of(2026, 1, 1);
    static final int SERVICE_DAYS = 181; // 2026-01-01 .. 2026-06-30

    private SyntheticClaims() {
    }

    public static List<NewClaim> generate(long seed, int count, String idPrefix,
                                          List<String> policyIds, List<String> providerIds) {
        if (policyIds.isEmpty() || providerIds.isEmpty()) {
            throw new IllegalArgumentException("reference data (policies, providers) is not loaded");
        }
        Random random = new Random(seed);
        return IntStream.rangeClosed(1, count)
                .mapToObj(i -> new NewClaim(
                        "%s%06d".formatted(idPrefix, i),
                        policyIds.get(random.nextInt(policyIds.size())),
                        providerIds.get(random.nextInt(providerIds.size())),
                        DIAGNOSIS_CODES.get(random.nextInt(DIAGNOSIS_CODES.size())),
                        FIRST_SERVICE_DATE.plusDays(random.nextInt(SERVICE_DAYS)),
                        BigDecimal.valueOf(5_000L + random.nextInt(495_000), 2))) // 50.00 .. 4999.99
                .toList();
    }
}
