package seed

import (
	"slices"
	"testing"
	"time"

	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/metering"
)

func months(t *testing.T, ss ...string) []metering.Month {
	t.Helper()
	var ms []metering.Month
	for _, s := range ss {
		m, err := metering.ParseMonth(s)
		if err != nil {
			t.Fatal(err)
		}
		ms = append(ms, m)
	}
	return ms
}

func TestPlanMix(t *testing.T) {
	d := Generate(Options{Seed: 42, Customers: 200, Months: months(t, "2026-09")})
	counts := map[string]int{}
	for _, c := range d.Customers {
		counts[c.PlanID]++
	}
	if counts["FREE"] != 120 || counts["PRO"] != 60 || counts["ENTERPRISE"] != 20 {
		t.Errorf("plan mix = %v, want FREE 120, PRO 60, ENTERPRISE 20", counts)
	}
}

func TestEventsAreValidAndInsideTheirMonth(t *testing.T) {
	window := months(t, "2026-08", "2026-09")
	d := Generate(Options{Seed: 1, Customers: 30, Months: window})
	if errs := metering.ValidateEvents(d.Events[:metering.MaxBatch], time.Now()); len(errs) > 0 {
		t.Fatalf("generated invalid events: %+v", errs[0])
	}
	seen := map[string]bool{}
	for _, e := range d.Events {
		if seen[e.EventID] {
			t.Fatalf("duplicate event id %s", e.EventID)
		}
		seen[e.EventID] = true
		if e.OccurredAt.Before(window[0].Start()) || !e.OccurredAt.Before(window[1].End()) {
			t.Fatalf("event %s at %s is outside %s..%s", e.EventID, e.OccurredAt, window[0], window[1])
		}
		if e.Quantity < 1 {
			t.Fatalf("event %s has quantity %d", e.EventID, e.Quantity)
		}
	}
}

// A customer-month yields the same events whichever window it is generated in,
// so re-running the seed in a later month does not duplicate usage.
func TestCustomerMonthsAreStableAcrossWindows(t *testing.T) {
	sept := func(d Dataset) []metering.Event {
		return slices.DeleteFunc(slices.Clone(d.Events), func(e metering.Event) bool {
			return e.OccurredAt.Month() != time.September
		})
	}
	a := Generate(Options{Seed: 7, Customers: 20, Months: months(t, "2026-07", "2026-08", "2026-09")})
	b := Generate(Options{Seed: 7, Customers: 20, Months: months(t, "2026-09", "2026-10")})
	if !slices.Equal(sept(a), sept(b)) || len(sept(a)) == 0 {
		t.Error("September differs between generation windows")
	}
	if !slices.Equal(a.Customers, b.Customers) {
		t.Error("customers differ between generation windows")
	}
	c := Generate(Options{Seed: 8, Customers: 20, Months: months(t, "2026-09")})
	if slices.Equal(sept(a), sept(c)) {
		t.Error("a different seed produced the same events")
	}
}

func TestLastCompleteMonths(t *testing.T) {
	got := LastCompleteMonths(time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC), 3)
	var s []string
	for _, m := range got {
		s = append(s, m.String())
	}
	if want := []string{"2025-10", "2025-11", "2025-12"}; !slices.Equal(s, want) {
		t.Errorf("LastCompleteMonths = %v, want %v", s, want)
	}
}
