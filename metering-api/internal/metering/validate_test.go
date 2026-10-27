package metering

import (
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func validEvent() Event {
	return Event{EventID: "evt_1", CustomerID: "cus_0001", Quantity: 10, OccurredAt: now.Add(-time.Hour)}
}

func TestValidateEvents(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Event)
		field  string // expected failing field, "" for valid
	}{
		{"valid", func(*Event) {}, ""},
		{"missing event id", func(e *Event) { e.EventID = "" }, "events[0].event_id"},
		{"event id too long", func(e *Event) { e.EventID = strings.Repeat("a", 65) }, "events[0].event_id"},
		{"event id with spaces", func(e *Event) { e.EventID = "evt 1" }, "events[0].event_id"},
		{"missing customer", func(e *Event) { e.CustomerID = "" }, "events[0].customer_id"},
		{"zero quantity", func(e *Event) { e.Quantity = 0 }, "events[0].quantity"},
		{"negative quantity", func(e *Event) { e.Quantity = -3 }, "events[0].quantity"},
		{"missing timestamp", func(e *Event) { e.OccurredAt = time.Time{} }, "events[0].occurred_at"},
		{"timestamp in the future", func(e *Event) { e.OccurredAt = now.Add(6 * time.Minute) }, "events[0].occurred_at"},
		{"small clock skew is fine", func(e *Event) { e.OccurredAt = now.Add(4 * time.Minute) }, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validEvent()
			tt.mutate(&e)
			errs := ValidateEvents([]Event{e}, now)
			if tt.field == "" {
				if len(errs) != 0 {
					t.Fatalf("unexpected errors: %+v", errs)
				}
				return
			}
			if len(errs) != 1 || errs[0].Field != tt.field {
				t.Fatalf("errors = %+v, want one on %s", errs, tt.field)
			}
		})
	}
}

func TestValidateEventsBatchSize(t *testing.T) {
	if errs := ValidateEvents(nil, now); len(errs) != 1 || errs[0].Field != "events" {
		t.Errorf("empty batch: %+v", errs)
	}
	big := make([]Event, MaxBatch+1)
	for i := range big {
		big[i] = validEvent()
	}
	if errs := ValidateEvents(big, now); len(errs) != 1 || errs[0].Field != "events" {
		t.Errorf("oversized batch: %+v", errs)
	}
	if errs := ValidateEvents(big[:MaxBatch], now); len(errs) != 0 {
		t.Errorf("full batch: %+v", errs)
	}
}

func TestParseUsageRange(t *testing.T) {
	from, to, err := parseUsageRange("", "", now)
	if err != nil || from.String() != "2026-10-01" || to.String() != "2026-11-01" {
		t.Errorf("default range = %s..%s, %v; want the current month", from, to, err)
	}
	from, to, err = parseUsageRange("2026-09-01", "2026-09-08", now)
	if err != nil || from.String() != "2026-09-01" || to.String() != "2026-09-08" {
		t.Errorf("explicit range = %s..%s, %v", from, to, err)
	}
	for _, bad := range [][2]string{
		{"2026-09-08", "2026-09-08"}, // empty
		{"2026-09-08", "2026-09-01"}, // reversed
		{"2025-01-01", "2026-01-03"}, // longer than 366 days
		{"2026-9-1", "2026-10-01"},   // not YYYY-MM-DD
		{"2026-09-01", "tomorrow"},
	} {
		if _, _, err := parseUsageRange(bad[0], bad[1], now); err == nil {
			t.Errorf("parseUsageRange(%q, %q) accepted", bad[0], bad[1])
		}
	}
}

func TestParseMonth(t *testing.T) {
	m, err := ParseMonth("2026-02")
	if err != nil {
		t.Fatal(err)
	}
	if got := m.End().Sub(m.Start()); got != 28*24*time.Hour {
		t.Errorf("February 2026 spans %s", got)
	}
	if m.Prev().String() != "2026-01" || MonthOf(now).String() != "2026-10" {
		t.Errorf("Prev or MonthOf wrong: %s, %s", m.Prev(), MonthOf(now))
	}
	for _, bad := range []string{"", "2026-13", "2026-1", "202602", "2026-02-01"} {
		if _, err := ParseMonth(bad); err == nil {
			t.Errorf("ParseMonth(%q) accepted", bad)
		}
	}
}

func TestInvoiceID(t *testing.T) {
	m, _ := ParseMonth("2026-09")
	if got := InvoiceID(m, "cus_0042"); got != "inv_202609_cus_0042" {
		t.Errorf("InvoiceID = %s", got)
	}
}
