package com.example.claims.repo;

import com.example.claims.domain.ClaimStatus;
import com.example.claims.domain.MonthlyKpi;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Reads the incrementally maintained KPI table and triggers its refresh task. */
@Repository
public class KpiRepository {

    static final String REFRESH_TASK = "REFRESH_CLAIM_KPIS";

    private final JdbcTemplate jdbc;

    public KpiRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<MonthlyKpi> findMonthly() {
        return jdbc.query("""
                SELECT SERVICE_MONTH, STATUS, CLAIM_COUNT, BILLED_TOTAL, PAYABLE_TOTAL
                FROM CLAIM_KPIS_MONTHLY
                ORDER BY SERVICE_MONTH, STATUS""",
                (rs, n) -> new MonthlyKpi(
                        rs.getDate(1).toLocalDate(),
                        ClaimStatus.valueOf(rs.getString(2)),
                        rs.getLong(3),
                        rs.getBigDecimal(4),
                        rs.getBigDecimal(5)));
    }

    /**
     * Starts one run of the KPI task now instead of waiting for its schedule.
     * On Snowflake the run is asynchronous: the call returns once the run is queued.
     */
    public void runRefreshTask() {
        jdbc.execute("EXECUTE TASK " + REFRESH_TASK);
    }
}
