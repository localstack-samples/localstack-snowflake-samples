// Package seed generates a deterministic synthetic dataset: customers on the
// three plans and the usage reports an API gateway would send for them.
//
// Every customer and every (customer, month) has its own random stream derived
// from the seed, so the same customer-month always yields the same events with
// the same ids, whichever months are generated together. Loading a dataset twice
// is therefore a no-op.
package seed

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/metering"
)

// Options sizes a dataset.
type Options struct {
	Seed      uint64
	Customers int
	Months    []metering.Month
}

// Dataset is the generated data.
type Dataset struct {
	Customers []metering.Customer
	Events    []metering.Event
}

// profile is how a plan's customers use the API.
type profile struct {
	minCalls, maxCalls float64 // monthly calls, drawn log-uniformly
	eventsPerDay       int     // gateway reports per active day
	activeShare        float64 // probability that a customer is active on a day
}

var profiles = map[string]profile{
	"FREE":       {minCalls: 1_000, maxCalls: 20_000, eventsPerDay: 1, activeShare: 0.7},
	"PRO":        {minCalls: 40_000, maxCalls: 1_500_000, eventsPerDay: 2, activeShare: 1},
	"ENTERPRISE": {minCalls: 600_000, maxCalls: 15_000_000, eventsPerDay: 6, activeShare: 1},
}

// PlanFor is the plan of the customer at index i: 60% FREE, 30% PRO and 10%
// ENTERPRISE, interleaved so any prefix of the customers has the same mix.
func PlanFor(i int) string {
	switch i % 10 {
	case 9:
		return "ENTERPRISE"
	case 6, 7, 8:
		return "PRO"
	default:
		return "FREE"
	}
}

// CustomerID is the id of the customer at index i.
func CustomerID(i int) string { return fmt.Sprintf("cus_%04d", i+1) }

var (
	adjectives = []string{"Acme", "Blue", "Bright", "Cobalt", "Copper", "Delta", "Ember", "Granite", "Harbor", "Iron",
		"Juniper", "Lumen", "Maple", "Nimbus", "North", "Orbit", "Pine", "Quartz", "Silver", "Summit"}
	nouns = []string{"Analytics", "Apps", "Cloud", "Commerce", "Data", "Dynamics", "Health", "Labs", "Logistics",
		"Media", "Mobility", "Networks", "Payments", "Robotics", "Software", "Systems", "Travel", "Works"}
)

// signupEpoch is the earliest signup date; customers joined within 18 months of it.
var signupEpoch = time.Date(2024, time.January, 1, 9, 0, 0, 0, time.UTC)

// Generate builds the dataset.
func Generate(o Options) Dataset {
	var d Dataset
	for i := range o.Customers {
		rng := rand.New(rand.NewPCG(o.Seed, uint64(i)))
		c := metering.Customer{
			ID:        CustomerID(i),
			Name:      adjectives[rng.IntN(len(adjectives))] + " " + nouns[rng.IntN(len(nouns))],
			PlanID:    PlanFor(i),
			CreatedAt: signupEpoch.AddDate(0, 0, rng.IntN(540)),
		}
		d.Customers = append(d.Customers, c)
		for _, m := range o.Months {
			d.Events = append(d.Events, monthOfEvents(o.Seed, i, c, m)...)
		}
	}
	return d
}

func monthOfEvents(seed uint64, index int, c metering.Customer, m metering.Month) []metering.Event {
	p := profiles[c.PlanID]
	monthKey := uint64(m.Start().Year()*100 + int(m.Start().Month()))
	rng := rand.New(rand.NewPCG(seed^0x9e3779b97f4a7c15, uint64(index)<<32|monthKey))
	target := math.Exp(math.Log(p.minCalls) + rng.Float64()*(math.Log(p.maxCalls)-math.Log(p.minCalls)))
	days := int(m.End().Sub(m.Start()).Hours() / 24)
	perEvent := target / (float64(days) * float64(p.eventsPerDay) * p.activeShare)
	var events []metering.Event
	for day := range days {
		if rng.Float64() >= p.activeShare {
			continue
		}
		for slot := range p.eventsPerDay {
			// Reports land in their slot of the day, at a random second.
			slotLen := 24 * time.Hour / time.Duration(p.eventsPerDay)
			at := m.Start().AddDate(0, 0, day).Add(time.Duration(slot)*slotLen +
				time.Duration(rng.Int64N(int64(slotLen/time.Second)))*time.Second)
			qty := int64(math.Round(perEvent * (0.6 + 0.8*rng.Float64())))
			events = append(events, metering.Event{
				EventID:    fmt.Sprintf("evt_%016x", rng.Uint64()),
				CustomerID: c.ID,
				Quantity:   max(qty, 1),
				OccurredAt: at,
			})
		}
	}
	return events
}

// LastCompleteMonths returns the n months before the month of now, oldest first.
func LastCompleteMonths(now time.Time, n int) []metering.Month {
	months := make([]metering.Month, n)
	m := metering.MonthOf(now)
	for i := n - 1; i >= 0; i-- {
		m = m.Prev()
		months[i] = m
	}
	return months
}
