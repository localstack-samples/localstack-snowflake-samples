package metering

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Requests rejected before any Snowflake call, so the service has no database.
func TestHandlerRejectsBadRequests(t *testing.T) {
	svc := NewService(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.now = func() time.Time { return now }
	h := NewHandler(svc, svc.log)

	tests := []struct {
		name, method, path, body string
		status                   int
		problemType              string
	}{
		{"truncated JSON", "POST", "/v1/events", `{"events": [`, 400, problemMalformed},
		{"quantity as a string", "POST", "/v1/events", `{"events": [{"event_id": "e1", "customer_id": "c1", "quantity": "10", "occurred_at": "2026-10-01T00:00:00Z"}]}`, 400, problemMalformed},
		{"unknown field", "POST", "/v1/events", `{"events": [], "tenant": "x"}`, 400, problemMalformed},
		{"two JSON documents", "POST", "/v1/events", `{"events": []} {}`, 400, problemMalformed},
		{"empty batch", "POST", "/v1/events", `{"events": []}`, 422, problemInvalid},
		{"invalid event", "POST", "/v1/events", `{"events": [{"event_id": "e1", "customer_id": "c1", "quantity": 0, "occurred_at": "2026-10-01T00:00:00Z"}]}`, 422, problemInvalid},
		{"bad usage range", "GET", "/v1/customers/c1/usage?from=2026-10-05&to=2026-10-01", "", 400, problemMalformed},
		{"bad month on close", "POST", "/v1/invoices/2026-13/close", "", 400, problemMalformed},
		{"current month cannot be closed", "POST", "/v1/invoices/2026-10/close", "", 409, problemConflict},
		{"bad month on export", "GET", "/v1/invoices/sept/export.csv", "", 400, problemMalformed},
		{"bad month on rollup", "POST", "/v1/rollups/2026-9", "", 400, problemMalformed},
		{"bad query id", "GET", "/v1/rollups/not-a-query-id", "", 400, problemMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body)))
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d; body %s", rec.Code, tt.status, rec.Body)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Errorf("Content-Type = %q", ct)
			}
			var p Problem
			if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
				t.Fatalf("body is not JSON: %v", err)
			}
			if p.Type != tt.problemType || p.Status != tt.status || p.Instance != strings.SplitN(tt.path, "?", 2)[0] {
				t.Errorf("problem = %+v", p)
			}
		})
	}
}

func TestHandlerReportsEveryInvalidField(t *testing.T) {
	svc := NewService(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.now = func() time.Time { return now }
	body := `{"events": [
	  {"event_id": "e1", "customer_id": "c1", "quantity": 5, "occurred_at": "2026-10-01T00:00:00Z"},
	  {"event_id": "", "customer_id": "c1", "quantity": -1}
	]}`
	rec := httptest.NewRecorder()
	NewHandler(svc, svc.log).ServeHTTP(rec, httptest.NewRequest("POST", "/v1/events", strings.NewReader(body)))
	var p Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	var fields []string
	for _, e := range p.Errors {
		fields = append(fields, e.Field)
	}
	want := "events[1].event_id events[1].quantity events[1].occurred_at"
	if rec.Code != http.StatusUnprocessableEntity || strings.Join(fields, " ") != want {
		t.Errorf("status %d, fields %v; want 422 and %s", rec.Code, fields, want)
	}
}
