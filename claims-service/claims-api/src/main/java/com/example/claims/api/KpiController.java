package com.example.claims.api;

import com.example.claims.domain.MonthlyKpi;
import com.example.claims.service.KpiService;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kpis")
public class KpiController {

    private final KpiService kpis;

    public KpiController(KpiService kpis) {
        this.kpis = kpis;
    }

    @GetMapping("/monthly")
    public List<MonthlyKpi> monthly() {
        return kpis.monthly();
    }

    /** 202: the task run is queued; poll GET /api/kpis/monthly for the result. */
    @PostMapping("/refresh")
    public ResponseEntity<Void> refresh() {
        kpis.refreshNow();
        return ResponseEntity.accepted().build();
    }
}
