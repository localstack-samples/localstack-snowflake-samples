package pricing

import (
	"errors"
	"slices"
	"testing"
)

// The published price list (init/03_price_plans.sf.sql), restated here.
var (
	free = Plan{ID: "FREE", Tiers: []Tier{
		{UpTo: 10_000},
		{UpTo: Unlimited, UnitPriceMicros: 2000}, // $2.00 per 1,000
	}}
	pro = Plan{ID: "PRO", Tiers: []Tier{
		{UpTo: 100_000, FlatFeeCents: 4900},     // $49 incl. 100k
		{UpTo: 1_000_000, UnitPriceMicros: 800}, // $0.80 per 1,000
		{UpTo: Unlimited, UnitPriceMicros: 500}, // $0.50 per 1,000
	}}
	enterprise = Plan{ID: "ENTERPRISE", Tiers: []Tier{
		{UpTo: 1_000_000, FlatFeeCents: 49900},   // $499 incl. 1M
		{UpTo: 10_000_000, UnitPriceMicros: 400}, // $0.40 per 1,000
		{UpTo: Unlimited, UnitPriceMicros: 250},  // $0.25 per 1,000
	}}
)

func TestRateTotals(t *testing.T) {
	tests := []struct {
		name      string
		plan      Plan
		calls     int64
		wantCents int64
		wantTiers []int64 // quantity per billed tier
	}{
		{"free, no usage", free, 0, 0, []int64{0}},
		{"free, exactly the allowance", free, 10_000, 0, []int64{10_000}},
		{"free, 2,500 over at $2.00/1k = $5.00", free, 12_500, 500, []int64{10_000, 2_500}},
		{"pro, no usage still pays the $49 fee", pro, 0, 4900, []int64{0}},
		{"pro, exactly the included calls", pro, 100_000, 4900, []int64{100_000}},
		{"pro, 150k at $0.80/1k = $120 + $49", pro, 250_000, 16_900, []int64{100_000, 150_000}},
		{"pro, top of tier 2: 900k at $0.80/1k = $720 + $49", pro, 1_000_000, 76_900, []int64{100_000, 900_000}},
		{"pro, 200k in tier 3 at $0.50/1k adds $100", pro, 1_200_000, 86_900, []int64{100_000, 900_000, 200_000}},
		{"enterprise, 9M at $0.40 + 2M at $0.25 + $499", enterprise, 12_000_000, 459_900, []int64{1_000_000, 9_000_000, 2_000_000}},
		{"pro, 7 calls over = 0.56 cents, rounds up", pro, 100_007, 4901, []int64{100_000, 7}},
		{"pro, 6 calls over = 0.48 cents, rounds down", pro, 100_006, 4900, []int64{100_000, 6}},
		{"free, 1 call over = 0.2 cents, rounds down", free, 10_001, 0, []int64{10_000, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines, total, err := tt.plan.Rate(tt.calls)
			if err != nil {
				t.Fatalf("Rate(%d): %v", tt.calls, err)
			}
			if total != tt.wantCents {
				t.Errorf("total = %d cents, want %d", total, tt.wantCents)
			}
			var qty []int64
			var sum, calls int64
			for i, l := range lines {
				if l.TierNo != i+1 {
					t.Errorf("line %d has tier %d", i, l.TierNo)
				}
				qty = append(qty, l.Quantity)
				sum += l.AmountCents
				calls += l.Quantity
			}
			if !slices.Equal(qty, tt.wantTiers) {
				t.Errorf("tier quantities = %v, want %v", qty, tt.wantTiers)
			}
			if sum != total {
				t.Errorf("lines sum to %d, total is %d", sum, total)
			}
			if calls != tt.calls {
				t.Errorf("lines bill %d calls, usage was %d", calls, tt.calls)
			}
		})
	}
}

func TestRateLineDetails(t *testing.T) {
	lines, _, err := pro.Rate(250_000)
	if err != nil {
		t.Fatal(err)
	}
	want := []Line{
		{TierNo: 1, Quantity: 100_000, UnitPriceMicros: 0, FlatFeeCents: 4900, AmountCents: 4900},
		{TierNo: 2, Quantity: 150_000, UnitPriceMicros: 800, FlatFeeCents: 0, AmountCents: 12_000},
	}
	if !slices.Equal(lines, want) {
		t.Errorf("lines = %+v\nwant    %+v", lines, want)
	}
}

func TestRateRejectsNegativeUsage(t *testing.T) {
	if _, _, err := pro.Rate(-1); !errors.Is(err, ErrNegativeUsage) {
		t.Errorf("Rate(-1) error = %v, want ErrNegativeUsage", err)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		plan Plan
		ok   bool
	}{
		{"published plans are valid", enterprise, true},
		{"single unlimited tier", Plan{ID: "FLAT", Tiers: []Tier{{UpTo: Unlimited, UnitPriceMicros: 100}}}, true},
		{"no tiers", Plan{ID: "EMPTY"}, false},
		{"last tier bounded", Plan{ID: "X", Tiers: []Tier{{UpTo: 10}}}, false},
		{"limits not ascending", Plan{ID: "X", Tiers: []Tier{{UpTo: 10}, {UpTo: 10}, {UpTo: Unlimited}}}, false},
		{"zero first limit", Plan{ID: "X", Tiers: []Tier{{UpTo: 0}, {UpTo: Unlimited}}}, false},
		{"negative unit price", Plan{ID: "X", Tiers: []Tier{{UpTo: Unlimited, UnitPriceMicros: -1}}}, false},
		{"negative flat fee", Plan{ID: "X", Tiers: []Tier{{UpTo: Unlimited, FlatFeeCents: -1}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plan.Validate()
			if (err == nil) != tt.ok {
				t.Errorf("Validate() = %v, want ok=%v", err, tt.ok)
			}
			if !tt.ok {
				if _, _, rateErr := tt.plan.Rate(1); rateErr == nil {
					t.Error("Rate accepted an invalid plan")
				}
			}
		})
	}
}
