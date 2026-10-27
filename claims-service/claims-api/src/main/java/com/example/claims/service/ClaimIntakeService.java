package com.example.claims.service;

import com.example.claims.domain.BatchIntakeResult;
import com.example.claims.domain.Claim;
import com.example.claims.domain.NewClaim;
import com.example.claims.repo.ClaimsRepository;
import com.example.claims.repo.ClaimsRepository.ReferenceCheck;
import com.example.claims.service.InvalidClaimException.Problem;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Accepts claims one at a time (REST) or in bulk (batch intake). */
@Service
public class ClaimIntakeService {

    private final ClaimsRepository claims;

    public ClaimIntakeService(ClaimsRepository claims) {
        this.claims = claims;
    }

    /**
     * Validates the claim in Snowflake and inserts it unless its id exists.
     *
     * @throws InvalidClaimException   bad ICD-10 code, unknown policy or provider (HTTP 422)
     * @throws DuplicateClaimException a claim with this id was already submitted (HTTP 409)
     */
    public Claim submit(NewClaim claim) {
        ReferenceCheck check = claims.checkReferences(claim);
        if (!check.diagnosisValid()) {
            throw new InvalidClaimException(Problem.INVALID_DIAGNOSIS_CODE,
                    "'" + claim.diagnosisCode() + "' is not a valid ICD-10-CM code");
        }
        if (!check.policyExists()) {
            throw new InvalidClaimException(Problem.UNKNOWN_POLICY, "Policy " + claim.policyId() + " does not exist");
        }
        if (!check.providerExists()) {
            throw new InvalidClaimException(Problem.UNKNOWN_PROVIDER,
                    "Provider " + claim.providerId() + " does not exist");
        }
        if (!claims.insertIfAbsent(claim)) {
            throw new DuplicateClaimException(claim.claimId());
        }
        return claims.findById(claim.claimId()).orElseThrow(() -> new ClaimNotFoundException(claim.claimId()));
    }

    /**
     * Loads a batch in one transaction: stage every row with a JDBC batch insert,
     * classify the rows in SQL (same UDF and reference checks as {@link #submit}),
     * MERGE the accepted ones into CLAIMS, and report the rest.
     */
    @Transactional
    public BatchIntakeResult intake(List<NewClaim> batch) {
        String batchId = UUID.randomUUID().toString();
        claims.stageIntake(batchId, batch);
        claims.classifyIntake(batchId);
        int accepted = claims.promoteIntake(batchId);
        List<BatchIntakeResult.Rejection> rejected = claims.intakeRejections(batchId);
        claims.clearIntake(batchId);
        return new BatchIntakeResult(batchId, batch.size(), accepted, rejected);
    }
}
