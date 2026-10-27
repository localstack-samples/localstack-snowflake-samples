//go:build integration

package metering_test

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/metering"
	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/seed"
)

// lastMonth is a complete month, so it can be closed.
var lastMonth = metering.MonthOf(time.Now()).Prev()

// 12 customers: 8 FREE, 3 PRO, 1 ENTERPRISE, about 530 events a month.
func smallDataset(seedValue uint64, months ...metering.Month) seed.Options {
	return seed.Options{Seed: seedValue, Customers: 12, Months: months}
}

// priceList restates the published price list (init/03_price_plans.sf.sql) as
// {last call of the tier, micro-dollars per call, flat fee in cents}.
var priceList = map[string][][3]int64{
	"FREE":       {{10_000, 0, 0}, {math.MaxInt64, 2000, 0}},
	"PRO":        {{100_000, 0, 4900}, {1_000_000, 800, 0}, {math.MaxInt64, 500, 0}},
	"ENTERPRISE": {{1_000_000, 0, 49900}, {10_000_000, 400, 0}, {math.MaxInt64, 250, 0}},
}

type wantLine struct {
	tier                     int
	qty, micros, flat, cents int64
}

// priceByHand bills calls against a plan's tiers: every tier the usage reaches
// bills the calls above the previous tier's limit, up to its own, plus its flat
// fee; amounts are rounded half up to the cent per line.
func priceByHand(plan string, calls int64) ([]wantLine, int64) {
	var lines []wantLine
	var total, below int64
	for i, t := range priceList[plan] {
		if i > 0 && calls <= below {
			break
		}
		qty := min(calls, t[0]) - below
		cents := t[2] + (qty*t[1]+5_000)/10_000
		lines = append(lines, wantLine{tier: i + 1, qty: qty, micros: t[1], flat: t[2], cents: cents})
		total += cents
		below = t[0]
	}
	return lines, total
}

// callsIn sums each customer's calls and counts the events in a month.
func callsIn(d seed.Dataset, m metering.Month) (map[string]int64, int) {
	calls := map[string]int64{}
	events := 0
	for _, e := range d.Events {
		if !e.OccurredAt.Before(m.Start()) && e.OccurredAt.Before(m.End()) {
			calls[e.CustomerID] += e.Quantity
			events++
		}
	}
	return calls, events
}

func TestIngestIsIdempotent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	d := e.seed(smallDataset(1, lastMonth))
	calls, events := callsIn(d, lastMonth)
	var wantCalls int64
	for _, c := range calls {
		wantCalls += c
	}

	// The gateway retries everything, in different batch sizes.
	res := e.ingest(d.Events, 250)
	if res.Received != events || res.Inserted != 0 || res.Duplicates != events {
		t.Errorf("re-ingest = %+v, want %d received, 0 inserted, %d duplicates", res, events, events)
	}
	if got := e.scalar(`SELECT COUNT(*) FROM USAGE_EVENTS`); got != int64(events) {
		t.Errorf("stored events = %d, want %d", got, events)
	}
	if got := e.scalar(`SELECT SUM(QUANTITY) FROM USAGE_EVENTS`); got != wantCalls {
		t.Errorf("stored calls = %d, want %d", got, wantCalls)
	}

	// One batch with a new event twice and an existing one: only one row is new.
	fresh := metering.Event{EventID: "evt_retry_1", CustomerID: d.Customers[6].ID, Quantity: 7, OccurredAt: lastMonth.Start().Add(time.Hour)}
	res = e.ingest([]metering.Event{fresh, fresh, d.Events[0]}, 10)
	if res != (metering.IngestResult{Received: 3, Inserted: 1, Duplicates: 2}) {
		t.Errorf("mixed batch = %+v, want 3 received, 1 inserted, 2 duplicates", res)
	}
	if got := e.scalar(`SELECT COUNT(*) FROM USAGE_EVENTS_STAGING`); got != 0 {
		t.Errorf("%d rows left in staging", got)
	}

	// The usage endpoint agrees with the dataset, per customer and per day.
	for _, c := range d.Customers[5:10] { // one FREE, the three PRO and the ENTERPRISE customer
		var u metering.Usage
		path := fmt.Sprintf("/v1/customers/%s/usage?from=%s&to=%s", c.ID, lastMonth.Start().Format(time.DateOnly), lastMonth.End().Format(time.DateOnly))
		e.call("GET", path, nil, http.StatusOK, &u)
		want := calls[c.ID]
		if c.ID == fresh.CustomerID {
			want += fresh.Quantity
		}
		var daily int64
		for _, day := range u.Daily {
			daily += day.Calls
		}
		if u.TotalCalls != want || daily != want || u.PlanID != c.PlanID {
			t.Errorf("%s usage = %d calls (%d by day) on %s, want %d on %s", c.ID, u.TotalCalls, daily, u.PlanID, want, c.PlanID)
		}
	}
}

func TestCloseMonthInvoicesAtTierPrices(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	d := e.seed(smallDataset(2, lastMonth.Prev(), lastMonth))
	// A PRO customer without usage still pays the monthly fee.
	idle := metering.Customer{ID: "cus_idle", Name: "Idle Labs", PlanID: "PRO", CreatedAt: lastMonth.Start().AddDate(-1, 0, 0)}
	if err := e.svc.UpsertCustomers(e.ctx, []metering.Customer{idle}); err != nil {
		t.Fatal(err)
	}
	customers := append(slices.Clone(d.Customers), idle)
	calls, events := callsIn(d, lastMonth)

	wantTotals := map[string]int64{}
	var wantTotal int64
	for _, c := range customers {
		_, cents := priceByHand(c.PlanID, calls[c.ID])
		wantTotals[c.ID] = cents
		wantTotal += cents
	}

	var res metering.CloseResult
	e.call("POST", "/v1/invoices/"+lastMonth.String()+"/close", nil, http.StatusCreated, &res)
	if res.Invoices != len(customers) || res.TotalCents != wantTotal || res.EventsInvoiced != int64(events) {
		t.Errorf("close = %+v, want %d invoices, %d cents, %d events", res, len(customers), wantTotal, events)
	}

	rows, err := e.db.QueryContext(e.ctx, `SELECT CUSTOMER_ID, TOTAL_CALLS, TOTAL_CENTS FROM INVOICES WHERE BILLING_MONTH = ?`, lastMonth.Start())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var id string
		var gotCalls, gotCents int64
		if err := rows.Scan(&id, &gotCalls, &gotCents); err != nil {
			t.Fatal(err)
		}
		seen++
		if gotCalls != calls[id] || gotCents != wantTotals[id] {
			t.Errorf("invoice of %s: %d calls, %d cents; want %d calls, %d cents", id, gotCalls, gotCents, calls[id], wantTotals[id])
		}
	}
	if err := rows.Err(); err != nil || seen != len(customers) {
		t.Errorf("read %d invoices (%v), want %d", seen, err, len(customers))
	}
	if got := e.scalar(`SELECT TOTAL_CENTS FROM INVOICES WHERE CUSTOMER_ID = 'cus_idle'`); got != 4900 {
		t.Errorf("idle PRO customer billed %d cents, want the 4900 monthly fee", got)
	}

	// The month's events are invoiced; the month before is untouched.
	if got := e.scalar(`SELECT COUNT(*) FROM USAGE_EVENTS WHERE INVOICE_ID IS NOT NULL`); got != int64(events) {
		t.Errorf("%d events invoiced, want %d", got, events)
	}
	_, earlier := callsIn(d, lastMonth.Prev())
	if got := e.scalar(`SELECT COUNT(*) FROM USAGE_EVENTS WHERE INVOICE_ID IS NULL`); got != int64(earlier) {
		t.Errorf("%d events uninvoiced, want the %d of %s", got, earlier, lastMonth.Prev())
	}

	// Invoices are issued once.
	var p metering.Problem
	e.call("POST", "/v1/invoices/"+lastMonth.String()+"/close", nil, http.StatusConflict, &p)
	if got := e.scalar(`SELECT COUNT(*) FROM INVOICES`); got != int64(len(customers)) {
		t.Errorf("second close left %d invoices, want %d", got, len(customers))
	}
}

func TestCloseMonthIsAtomic(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	d := e.seed(smallDataset(3, lastMonth))
	_, events := callsIn(d, lastMonth)

	var writtenInTx int64
	metering.SetBeforeMarkInvoiced(e.svc, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM INVOICES`).Scan(&writtenInTx); err != nil {
			return err
		}
		return errors.New("injected failure after writing invoices")
	})
	var p metering.Problem
	e.call("POST", "/v1/invoices/"+lastMonth.String()+"/close", nil, http.StatusInternalServerError, &p)
	if writtenInTx != int64(len(d.Customers)) {
		t.Fatalf("the transaction had written %d invoices before the failure, want %d", writtenInTx, len(d.Customers))
	}
	for query, want := range map[string]int64{
		`SELECT COUNT(*) FROM INVOICES`:                                  0,
		`SELECT COUNT(*) FROM INVOICE_LINES`:                             0,
		`SELECT COUNT(*) FROM USAGE_EVENTS WHERE INVOICE_ID IS NOT NULL`: 0,
		`SELECT COUNT(*) FROM USAGE_EVENTS WHERE INVOICE_ID IS NULL`:     int64(events),
	} {
		if got := e.scalar(query); got != want {
			t.Errorf("after the failed close, %s = %d, want %d", query, got, want)
		}
	}

	// Once the fault is gone, the same close succeeds.
	metering.SetBeforeMarkInvoiced(e.svc, nil)
	var res metering.CloseResult
	e.call("POST", "/v1/invoices/"+lastMonth.String()+"/close", nil, http.StatusCreated, &res)
	if res.Invoices != len(d.Customers) || res.EventsInvoiced != int64(events) {
		t.Errorf("retried close = %+v, want %d invoices and %d events", res, len(d.Customers), events)
	}
}

func TestRollupRunsAsync(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	d := e.seed(smallDataset(4, lastMonth.Prev(), lastMonth))

	plans := map[string]string{}
	for _, c := range d.Customers {
		plans[c.ID] = c.PlanID
	}
	type key struct{ plan, day string }
	agg := map[key]*metering.RollupRow{}
	active := map[key]map[string]bool{}
	for _, ev := range d.Events {
		if ev.OccurredAt.Before(lastMonth.Start()) || !ev.OccurredAt.Before(lastMonth.End()) {
			continue
		}
		k := key{plans[ev.CustomerID], metering.DateOf(ev.OccurredAt).String()}
		if agg[k] == nil {
			agg[k] = &metering.RollupRow{PlanID: k.plan, Date: metering.DateOf(ev.OccurredAt)}
			active[k] = map[string]bool{}
		}
		agg[k].Events++
		agg[k].Calls += ev.Quantity
		active[k][ev.CustomerID] = true
	}
	var want []metering.RollupRow
	for k, r := range agg {
		r.ActiveCustomers = int64(len(active[k]))
		want = append(want, *r)
	}
	slices.SortFunc(want, func(a, b metering.RollupRow) int {
		if c := a.Date.Time().Compare(b.Date.Time()); c != 0 {
			return c
		}
		return cmp.Compare(a.PlanID, b.PlanID)
	})

	var started struct {
		QueryID   string `json:"query_id"`
		StatusURL string `json:"status_url"`
	}
	header := e.call("POST", "/v1/rollups/"+lastMonth.String(), nil, http.StatusAccepted, &started)
	if !metering.ValidQueryID(started.QueryID) || header.Get("Location") != "/v1/rollups/"+started.QueryID {
		t.Fatalf("started = %+v, Location %q", started, header.Get("Location"))
	}

	deadline := time.Now().Add(2 * time.Minute)
	var r metering.Rollup
	for {
		e.call("GET", started.StatusURL, nil, http.StatusOK, &r)
		if r.Status != metering.RollupRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("rollup still running after 2 minutes")
		}
		time.Sleep(250 * time.Millisecond)
	}
	if r.Status != metering.RollupSucceeded {
		t.Fatalf("rollup %s: %s", r.Status, r.Error)
	}
	if !slices.Equal(r.Rows, want) {
		t.Errorf("rollup has %d rows, want %d; first got %+v, first want %+v", len(r.Rows), len(want), first(r.Rows), first(want))
	}
}

func TestInvoiceExportCSV(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	d := e.seed(smallDataset(5, lastMonth))
	calls, _ := callsIn(d, lastMonth)

	// The expected file, built from the dataset and the price list.
	var want bytes.Buffer
	w := csv.NewWriter(&want)
	_ = w.Write([]string{"invoice_id", "customer_id", "plan_id", "billing_month", "line_no", "tier_no",
		"quantity", "unit_price_micros", "flat_fee_cents", "amount_cents"})
	wantRows := 0
	for _, c := range d.Customers { // ids sort in generation order
		lines, _ := priceByHand(c.PlanID, calls[c.ID])
		for i, l := range lines {
			_ = w.Write([]string{"inv_" + lastMonth.Start().Format("200601") + "_" + c.ID, c.ID, c.PlanID, lastMonth.String(),
				strconv.Itoa(i + 1), strconv.Itoa(l.tier), fmt.Sprint(l.qty), fmt.Sprint(l.micros), fmt.Sprint(l.flat), fmt.Sprint(l.cents)})
			wantRows++
		}
	}
	w.Flush()

	path := "/v1/invoices/" + lastMonth.String() + "/export.csv"
	resp, _ := e.do("GET", path, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("export before close: status %d, want 404", resp.StatusCode)
	}
	e.call("POST", "/v1/invoices/"+lastMonth.String()+"/close", nil, http.StatusCreated, nil)

	resp, body := e.do("GET", path, nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/csv; charset=utf-8" {
		t.Fatalf("export: status %d, Content-Type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	records, err := csv.NewReader(bytes.NewReader(body)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != wantRows+1 {
		t.Errorf("export has %d rows plus header, want %d", len(records)-1, wantRows)
	}
	if got, wantSum := sha256.Sum256(body), sha256.Sum256(want.Bytes()); got != wantSum {
		t.Errorf("export checksum %x, want %x\ngot:\n%s\nwant:\n%s", got[:8], wantSum[:8], head(body), head(want.Bytes()))
	}
}

func TestErrorPaths(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	d := e.seed(smallDataset(6, lastMonth))
	events := e.scalar(`SELECT COUNT(*) FROM USAGE_EVENTS`)

	t.Run("unknown customer is a 404 problem", func(t *testing.T) {
		resp, body := e.do("GET", "/v1/customers/cus_9999/usage", nil)
		if resp.StatusCode != http.StatusNotFound || resp.Header.Get("Content-Type") != "application/problem+json" {
			t.Fatalf("status %d, Content-Type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
		}
		if !bytes.Contains(body, []byte(`"type":"urn:metering:problem:not-found"`)) || !bytes.Contains(body, []byte(`"status":404`)) {
			t.Errorf("body %s", body)
		}
	})
	t.Run("malformed payload is a 400 problem", func(t *testing.T) {
		var p metering.Problem
		e.call("POST", "/v1/events", `{"events": [{"event_id": "e1",`, http.StatusBadRequest, &p)
		if p.Type != "urn:metering:problem:malformed-request" {
			t.Errorf("problem %+v", p)
		}
	})
	t.Run("invalid events are a 422 problem", func(t *testing.T) {
		var p metering.Problem
		e.call("POST", "/v1/events", map[string]any{"events": []map[string]any{{"event_id": "e1", "customer_id": d.Customers[0].ID, "quantity": 0}}},
			http.StatusUnprocessableEntity, &p)
		if p.Type != "urn:metering:problem:validation-failed" || len(p.Errors) != 2 {
			t.Errorf("problem %+v", p)
		}
	})
	t.Run("a batch naming an unknown customer stores nothing", func(t *testing.T) {
		batch := []metering.Event{
			{EventID: "evt_ok", CustomerID: d.Customers[0].ID, Quantity: 5, OccurredAt: lastMonth.Start()},
			{EventID: "evt_ghost", CustomerID: "cus_ghost", Quantity: 5, OccurredAt: lastMonth.Start()},
		}
		var p metering.Problem
		e.call("POST", "/v1/events", map[string]any{"events": batch}, http.StatusUnprocessableEntity, &p)
		if p.Type != "urn:metering:problem:unknown-customer" || !slices.Equal(p.UnknownCustomers, []string{"cus_ghost"}) {
			t.Errorf("problem %+v", p)
		}
		if got := e.scalar(`SELECT COUNT(*) FROM USAGE_EVENTS`); got != events {
			t.Errorf("%d events stored, want %d", got, events)
		}
		if got := e.scalar(`SELECT COUNT(*) FROM USAGE_EVENTS_STAGING`); got != 0 {
			t.Errorf("%d rows left in staging", got)
		}
	})
	t.Run("unknown rollup query is a 404 problem", func(t *testing.T) {
		var p metering.Problem
		e.call("GET", "/v1/rollups/01bd0000-0000-0000-0000-000000000000", nil, http.StatusNotFound, &p)
		if p.Type != "urn:metering:problem:not-found" {
			t.Errorf("problem %+v", p)
		}
	})
}

func first[T any](s []T) any {
	if len(s) == 0 {
		return nil
	}
	return s[0]
}

func head(b []byte) []byte {
	lines := bytes.SplitAfterN(b, []byte("\n"), 6)
	return bytes.Join(lines[:min(5, len(lines))], nil)
}
