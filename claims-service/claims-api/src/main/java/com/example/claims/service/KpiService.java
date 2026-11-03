package com.example.claims.service;

import com.example.claims.domain.MonthlyKpi;
import com.example.claims.repo.KpiRepository;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * KPIs are maintained in Snowflake by a stream on CLAIMS and a scheduled task that
 * MERGEs only the changed rows into CLAIM_KPIS_MONTHLY; the service only reads them.
 */
@Service
public class KpiService {

    private final KpiRepository kpis;

    public KpiService(KpiRepository kpis) {
        this.kpis = kpis;
    }

    public List<MonthlyKpi> monthly() {
        return kpis.findMonthly();
    }

    /** Runs the refresh task now (operators, tests); normally it runs on its schedule. */
    public void refreshNow() {
        kpis.runRefreshTask();
    }
}
