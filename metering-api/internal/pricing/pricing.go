// Package pricing rates monthly API usage against graduated price tiers.
//
// A plan is an ordered list of tiers. Calls fill the tiers from the bottom up:
// each tier bills the calls between the previous tier's limit and its own at its
// unit price, plus its flat fee once usage reaches it. Tier 1 is always reached,
// so its flat fee is the plan's monthly fee.
//
// Money is integer: unit prices in micro-dollars per call, amounts in cents.
package pricing

import (
	"errors"
	"fmt"
	"math"
)

// Unlimited is the UpTo of the open-ended top tier.
const Unlimited int64 = math.MaxInt64

// microsPerCent converts micro-dollars to cents.
const microsPerCent = 10_000

// Tier is one price band.
type Tier struct {
	UpTo            int64 // last call in this tier, or Unlimited
	UnitPriceMicros int64 // price per call in this tier, in micro-dollars
	FlatFeeCents    int64 // charged once when usage reaches this tier
}

// Plan is a named list of tiers in ascending order.
type Plan struct {
	ID    string
	Tiers []Tier
}

// Line is the charge for one tier of one invoice.
type Line struct {
	TierNo          int
	Quantity        int64
	UnitPriceMicros int64
	FlatFeeCents    int64
	AmountCents     int64
}

// Validate checks that the tiers are well formed: at least one, strictly
// ascending limits, the last one Unlimited, and no negative prices.
func (p Plan) Validate() error {
	if len(p.Tiers) == 0 {
		return fmt.Errorf("plan %s: no tiers", p.ID)
	}
	var prev int64
	for i, t := range p.Tiers {
		if t.UnitPriceMicros < 0 || t.FlatFeeCents < 0 {
			return fmt.Errorf("plan %s tier %d: negative price", p.ID, i+1)
		}
		if t.UpTo <= prev {
			return fmt.Errorf("plan %s tier %d: limit %d is not above the previous tier's %d", p.ID, i+1, t.UpTo, prev)
		}
		prev = t.UpTo
	}
	if prev != Unlimited {
		return fmt.Errorf("plan %s: the last tier must be unlimited", p.ID)
	}
	return nil
}

// ErrNegativeUsage is returned by Rate for a negative call count.
var ErrNegativeUsage = errors.New("pricing: negative usage")

// Rate returns the invoice lines for a month with the given number of calls and
// their total in cents. Tiers the usage does not reach produce no line; tier 1
// always produces one, even for zero calls.
func (p Plan) Rate(calls int64) ([]Line, int64, error) {
	if calls < 0 {
		return nil, 0, ErrNegativeUsage
	}
	if err := p.Validate(); err != nil {
		return nil, 0, err
	}
	var (
		lines []Line
		total int64
		floor int64 // calls billed by the tiers below
	)
	for i, t := range p.Tiers {
		if i > 0 && calls <= floor {
			break
		}
		qty := min(calls, t.UpTo) - floor
		amount := t.FlatFeeCents + roundHalfUp(qty*t.UnitPriceMicros, microsPerCent)
		lines = append(lines, Line{
			TierNo:          i + 1,
			Quantity:        qty,
			UnitPriceMicros: t.UnitPriceMicros,
			FlatFeeCents:    t.FlatFeeCents,
			AmountCents:     amount,
		})
		total += amount
		floor = t.UpTo
	}
	return lines, total, nil
}

// roundHalfUp divides a non-negative n by d, rounding halves up.
func roundHalfUp(n, d int64) int64 {
	return (n + d/2) / d
}
