package com.example.claims.seed;

import com.example.claims.domain.BatchIntakeResult;
import com.example.claims.domain.NewClaim;
import com.example.claims.repo.ClaimsRepository;
import com.example.claims.service.AdjudicationService;
import com.example.claims.service.ClaimIntakeService;
import com.example.claims.service.KpiService;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.stereotype.Component;

/**
 * Loads deterministic sample claims through the bulk intake path, adjudicates the
 * first {@code claims.seed.adjudicate} newly accepted ones (each is one procedure call)
 * and refreshes the KPIs. Enabled with {@code claims.seed.enabled=true}
 * ({@code make seed}); safe to re-run: existing claims are rejected as duplicates.
 */
@Component
@ConditionalOnProperty(name = "claims.seed.enabled", havingValue = "true")
public class ClaimSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ClaimSeeder.class);

    private final SeedProperties props;
    private final ClaimsRepository claims;
    private final ClaimIntakeService intake;
    private final AdjudicationService adjudication;
    private final KpiService kpis;

    public ClaimSeeder(SeedProperties props, ClaimsRepository claims, ClaimIntakeService intake,
                       AdjudicationService adjudication, KpiService kpis) {
        this.props = props;
        this.claims = claims;
        this.intake = intake;
        this.adjudication = adjudication;
        this.kpis = kpis;
    }

    @Override
    public void run(ApplicationArguments args) {
        List<NewClaim> batch = SyntheticClaims.generate(props.randomSeed(), props.claims(), "CLM-SEED-",
                claims.policyIds(), claims.providerIds());
        BatchIntakeResult result = intake.intake(batch);
        Set<String> rejected = new HashSet<>();
        result.rejected().forEach(r -> rejected.add(r.claimId()));
        List<String> toAdjudicate = batch.stream()
                .map(NewClaim::claimId)
                .filter(id -> !rejected.contains(id))
                .limit(props.adjudicate())
                .toList();
        toAdjudicate.forEach(adjudication::adjudicate);
        kpis.refreshNow();
        log.info("Seeded {} claims: {} accepted, {} rejected, {} adjudicated",
                result.received(), result.accepted(), result.rejected().size(), toAdjudicate.size());
    }

    @ConfigurationProperties("claims.seed")
    public record SeedProperties(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("300") int claims,
            @DefaultValue("25") int adjudicate,
            @DefaultValue("20261024") long randomSeed) {
    }
}
