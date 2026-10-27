// Command demo walks through the metering API over HTTP: ingest, a retried
// (duplicate) batch, invoice close, an async rollup and the CSV export.
// It expects a seeded database (`make seed`) and a running API (`make run`, or
// let `make demo` start one).
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/metering"
)

const customer = "cus_0007"

var (
	base   string
	client = &http.Client{Timeout: 2 * time.Minute}
)

func main() {
	flag.StringVar(&base, "addr", "http://127.0.0.1:8080", "API base URL")
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "\ndemo:", err)
		os.Exit(1)
	}
}

func run() error {
	month := metering.MonthOf(time.Now()).Prev()
	usagePath := fmt.Sprintf("/v1/customers/%s/usage?from=%s&to=%s", customer,
		month.Start().Format(time.DateOnly), month.End().Format(time.DateOnly))
	fmt.Printf("Metering API demo against %s, billing month %s\n", base, month)

	var before metering.Usage
	if status, err := call("GET", usagePath, nil, &before); err != nil || status != http.StatusOK {
		return fmt.Errorf("customer %s not found (status %d, %v): run `make seed` first", customer, status, err)
	}

	// Hourly gateway reports for the last day of the month, with stable ids.
	events := make([]metering.Event, 24)
	lastDay := month.End().AddDate(0, 0, -1)
	for h := range events {
		events[h] = metering.Event{
			EventID:    fmt.Sprintf("evt_demo_%s_%s_%02d", customer, month.Start().Format("200601"), h),
			CustomerID: customer,
			Quantity:   1000,
			OccurredAt: lastDay.Add(time.Duration(h)*time.Hour + 59*time.Minute),
		}
	}
	body := map[string]any{"events": events}

	step(1, "Ingest a batch of usage reports")
	var res metering.IngestResult
	status, err := call("POST", "/v1/events", body, &res)
	if err != nil {
		return err
	}
	show("POST /v1/events", status, fmt.Sprintf("received %d, inserted %d, duplicates %d", res.Received, res.Inserted, res.Duplicates))

	step(2, "The gateway retries the same batch (same event ids)")
	if status, err = call("POST", "/v1/events", body, &res); err != nil {
		return err
	}
	show("POST /v1/events", status, fmt.Sprintf("received %d, inserted %d, duplicates %d", res.Received, res.Inserted, res.Duplicates))
	var after metering.Usage
	if _, err := call("GET", usagePath, nil, &after); err != nil {
		return err
	}
	show("GET  "+usagePath, http.StatusOK, fmt.Sprintf("%s calls in %d events (%s plan)", thousands(after.TotalCalls), after.Events, after.PlanID))
	note("before step 1: %s calls; step 1 added %s, the retry nothing",
		thousands(before.TotalCalls), thousands(after.TotalCalls-before.TotalCalls))

	step(3, "Close the month: rate usage, write invoices, mark usage invoiced (one transaction)")
	var closed metering.CloseResult
	path := "/v1/invoices/" + month.String() + "/close"
	if status, err = call("POST", path, nil, &closed); err != nil {
		return err
	}
	switch status {
	case http.StatusCreated:
		show("POST "+path, status, fmt.Sprintf("%d invoices, %s total, %s events invoiced",
			closed.Invoices, dollars(closed.TotalCents), thousands(closed.EventsInvoiced)))
	case http.StatusConflict:
		show("POST "+path, status, "already closed by an earlier run (invoices are issued once)")
	default:
		return fmt.Errorf("close: unexpected status %d", status)
	}

	step(4, "Start the monthly rollup as an async query and poll it")
	var started struct {
		QueryID   string `json:"query_id"`
		StatusURL string `json:"status_url"`
	}
	path = "/v1/rollups/" + month.String()
	if status, err = call("POST", path, nil, &started); err != nil {
		return err
	}
	show("POST "+path, status, "query "+started.QueryID)
	rollup, polls, err := poll(started.StatusURL)
	if err != nil {
		return err
	}
	show("GET  "+started.StatusURL, http.StatusOK, fmt.Sprintf("%s after %d poll(s), %d rows", rollup.Status, polls, len(rollup.Rows)))
	printRollup(rollup.Rows)

	step(5, "Export the month's invoice lines as CSV (streamed)")
	path = "/v1/invoices/" + month.String() + "/export.csv"
	lines, sum, head, err := download(path)
	if err != nil {
		return err
	}
	show("GET  "+path, http.StatusOK, fmt.Sprintf("%d rows plus header, sha256 %s...", lines-1, sum[:16]))
	for _, l := range head {
		fmt.Println("        " + l)
	}

	step(6, "Errors are problem details")
	var problem metering.Problem
	path = "/v1/customers/cus_9999/usage"
	status, _ = call("GET", path, nil, &problem)
	show("GET  "+path, status, problem.Type+": "+problem.Detail)
	problem = metering.Problem{}
	status, _ = call("POST", "/v1/events", map[string]any{"events": []map[string]any{{"event_id": "e1", "customer_id": customer, "quantity": -5}}}, &problem)
	detail := problem.Type
	for _, e := range problem.Errors {
		detail += fmt.Sprintf("\n             %s %s", e.Field, e.Detail)
	}
	show("POST /v1/events (quantity -5, no occurred_at)", status, detail)
	fmt.Println("\nDone.")
	return nil
}

func call(method, path string, body, out any) (int, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, base+path, reader)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: decode response: %w", method, path, err)
		}
	}
	return resp.StatusCode, nil
}

func poll(path string) (metering.Rollup, int, error) {
	deadline := time.Now().Add(2 * time.Minute)
	for n := 1; ; n++ {
		var r metering.Rollup
		if _, err := call("GET", path, nil, &r); err != nil {
			return r, n, err
		}
		switch r.Status {
		case metering.RollupSucceeded:
			return r, n, nil
		case metering.RollupFailed:
			return r, n, fmt.Errorf("rollup failed: %s", r.Error)
		}
		if time.Now().After(deadline) {
			return r, n, fmt.Errorf("rollup still %s after 2 minutes", r.Status)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func download(path string) (lines int, sum string, head []string, err error) {
	resp, err := client.Get(base + path)
	if err != nil {
		return 0, "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, "", nil, fmt.Errorf("export: status %d", resp.StatusCode)
	}
	h := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(resp.Body, h))
	for scanner.Scan() {
		if lines < 4 {
			head = append(head, scanner.Text())
		}
		lines++
	}
	return lines, hex.EncodeToString(h.Sum(nil)), head, scanner.Err()
}

func printRollup(rows []metering.RollupRow) {
	type total struct{ calls, events, peak int64 }
	totals := map[string]*total{}
	for _, r := range rows {
		t := totals[r.PlanID]
		if t == nil {
			t = &total{}
			totals[r.PlanID] = t
		}
		t.calls += r.Calls
		t.events += r.Events
		t.peak = max(t.peak, r.ActiveCustomers)
	}
	fmt.Printf("        %-11s %14s %8s %22s\n", "plan", "calls", "events", "peak daily customers")
	for _, plan := range []string{"FREE", "PRO", "ENTERPRISE"} {
		if t := totals[plan]; t != nil {
			fmt.Printf("        %-11s %14s %8s %22d\n", plan, thousands(t.calls), thousands(t.events), t.peak)
		}
	}
}

func step(n int, title string) { fmt.Printf("\n[%d/6] %s\n", n, title) }

func show(request string, status int, result string) {
	fmt.Printf("      %s\n        -> %d %s\n", request, status, result)
}

func note(format string, args ...any) { fmt.Printf("        (%s)\n", fmt.Sprintf(format, args...)) }

func dollars(cents int64) string {
	return fmt.Sprintf("$%s.%02d", thousands(cents/100), cents%100)
}

func thousands(n int64) string {
	s := fmt.Sprint(n)
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	return b.String()
}
