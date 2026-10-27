package com.example.claims.api;

import com.example.claims.domain.BatchIntakeResult;
import com.example.claims.domain.Claim;
import com.example.claims.domain.ClaimStatus;
import com.example.claims.domain.NewClaim;
import com.example.claims.repo.ClaimsRepository;
import com.example.claims.service.AdjudicationService;
import com.example.claims.service.ClaimIntakeService;
import com.example.claims.service.ClaimNotFoundException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/claims")
public class ClaimsController {

    static final int MAX_BATCH_SIZE = 10_000;

    private final ClaimIntakeService intake;
    private final AdjudicationService adjudication;
    private final ClaimsRepository claims;

    public ClaimsController(ClaimIntakeService intake, AdjudicationService adjudication, ClaimsRepository claims) {
        this.intake = intake;
        this.adjudication = adjudication;
        this.claims = claims;
    }

    /** 201 with the stored claim; 409 for a duplicate id; 422 for a bad code or reference. */
    @PostMapping
    public ResponseEntity<Claim> submit(@Valid @RequestBody NewClaim claim) {
        Claim stored = intake.submit(claim);
        return ResponseEntity.created(URI.create("/api/claims/" + stored.claimId())).body(stored);
    }

    /** Bulk intake: valid rows are loaded, the rest are reported with a reason. */
    @PostMapping("/batch")
    public BatchIntakeResult submitBatch(
            @RequestBody @NotEmpty @Size(max = MAX_BATCH_SIZE) List<@Valid NewClaim> batch) {
        return intake.intake(batch);
    }

    @GetMapping("/{claimId}")
    public Claim get(@PathVariable String claimId) {
        return claims.findById(claimId).orElseThrow(() -> new ClaimNotFoundException(claimId));
    }

    @GetMapping
    public List<Claim> list(@RequestParam(required = false) ClaimStatus status,
                            @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
        return claims.findRecent(status, limit);
    }

    /** Runs ADJUDICATE_CLAIM; 404 for an unknown claim, 422 if it was already adjudicated. */
    @PostMapping("/{claimId}/adjudication")
    public Claim adjudicate(@PathVariable String claimId) {
        return adjudication.adjudicate(claimId);
    }
}
