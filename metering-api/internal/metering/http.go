package metering

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// maxBodyBytes caps an ingest request body.
const maxBodyBytes = 1 << 20

// CSVHeader is the header row of the invoice export.
var CSVHeader = []string{
	"invoice_id", "customer_id", "plan_id", "billing_month", "line_no", "tier_no",
	"quantity", "unit_price_micros", "flat_fee_cents", "amount_cents",
}

type api struct {
	svc *Service
	log *slog.Logger
}

// NewHandler returns the HTTP API. Every route runs its Snowflake statements
// under its own query tag.
func NewHandler(svc *Service, log *slog.Logger) http.Handler {
	a := &api{svc: svc, log: log}
	mux := http.NewServeMux()
	route := func(pattern, tag string, h http.HandlerFunc) {
		mux.Handle(pattern, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h(w, r.WithContext(WithQueryTag(r.Context(), tag)))
		}))
	}
	route("GET /healthz", "healthz", a.health)
	route("POST /v1/events", "events.ingest", a.ingestEvents)
	route("GET /v1/customers/{id}/usage", "customers.usage", a.customerUsage)
	route("POST /v1/invoices/{month}/close", "invoices.close", a.closeMonth)
	route("GET /v1/invoices/{month}/export.csv", "invoices.export", a.exportInvoices)
	route("POST /v1/rollups/{month}", "rollups.start", a.startRollup)
	route("GET /v1/rollups/{queryId}", "rollups.get", a.getRollup)
	return a.logRequests(mux)
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.svc.Ping(ctx); err != nil {
		a.log.WarnContext(ctx, "health check failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *api) ingestEvents(w http.ResponseWriter, r *http.Request) {
	events, err := decodeEvents(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeProblem(w, r, Problem{Type: problemMalformed, Title: "Malformed request body", Status: http.StatusBadRequest,
			Detail: err.Error()})
		return
	}
	if errs := ValidateEvents(events, a.svc.now()); len(errs) > 0 {
		writeProblem(w, r, Problem{Type: problemInvalid, Title: "Invalid events", Status: http.StatusUnprocessableEntity,
			Detail: fmt.Sprintf("%d invalid field(s); nothing was stored", len(errs)), Errors: errs})
		return
	}
	res, err := a.svc.IngestEvents(r.Context(), events)
	var unknown *UnknownCustomersError
	switch {
	case errors.As(err, &unknown):
		writeProblem(w, r, Problem{Type: problemUnknownCustomer, Title: "Unknown customer", Status: http.StatusUnprocessableEntity,
			Detail: "the batch names customers that do not exist; nothing was stored", UnknownCustomers: unknown.IDs})
	case err != nil:
		a.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, res)
	}
}

func (a *api) customerUsage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, to, err := parseUsageRange(q.Get("from"), q.Get("to"), a.svc.now())
	if err != nil {
		writeProblem(w, r, Problem{Type: problemMalformed, Title: "Invalid date range", Status: http.StatusBadRequest, Detail: err.Error()})
		return
	}
	id := r.PathValue("id")
	usage, err := a.svc.CustomerUsage(r.Context(), id, from, to)
	switch {
	case errors.Is(err, ErrCustomerNotFound):
		writeProblem(w, r, Problem{Type: problemNotFound, Title: "Customer not found", Status: http.StatusNotFound,
			Detail: fmt.Sprintf("no customer with id %q", id)})
	case err != nil:
		a.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, usage)
	}
}

func (a *api) closeMonth(w http.ResponseWriter, r *http.Request) {
	m, ok := a.month(w, r)
	if !ok {
		return
	}
	res, err := a.svc.CloseMonth(r.Context(), m)
	switch {
	case errors.Is(err, ErrMonthClosed):
		writeProblem(w, r, Problem{Type: problemConflict, Title: "Month already closed", Status: http.StatusConflict,
			Detail: fmt.Sprintf("invoices for %s were already issued", m)})
	case errors.Is(err, ErrMonthNotOver):
		writeProblem(w, r, Problem{Type: problemConflict, Title: "Month not over", Status: http.StatusConflict,
			Detail: fmt.Sprintf("%s can be closed from %s", m, m.End().Format(time.DateOnly))})
	case err != nil:
		a.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusCreated, res)
	}
}

// exportInvoices streams a month's invoice lines as CSV. Rows go to the client
// as they arrive from Snowflake; the response is never held in memory.
func (a *api) exportInvoices(w http.ResponseWriter, r *http.Request) {
	m, ok := a.month(w, r)
	if !ok {
		return
	}
	cw := csv.NewWriter(w)
	flusher, _ := w.(http.Flusher)
	rows := 0
	err := a.svc.EachInvoiceLine(r.Context(), m, func(l InvoiceLine) error {
		if rows == 0 {
			w.Header().Set("Content-Type", "text/csv; charset=utf-8")
			w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="invoice-lines-%s.csv"`, m))
			if err := cw.Write(CSVHeader); err != nil {
				return err
			}
		}
		rows++
		if err := cw.Write(l.Record()); err != nil {
			return err
		}
		if rows%500 == 0 {
			cw.Flush()
			if flusher != nil {
				flusher.Flush()
			}
		}
		return cw.Error()
	})
	switch {
	case errors.Is(err, ErrNoInvoices):
		writeProblem(w, r, Problem{Type: problemNotFound, Title: "No invoices", Status: http.StatusNotFound,
			Detail: fmt.Sprintf("%s has not been closed", m)})
	case err != nil && rows == 0:
		a.internalError(w, r, err)
	case err != nil:
		// Headers are already sent: abort the response so the client sees a
		// truncated transfer instead of a short but well-formed file.
		a.log.ErrorContext(r.Context(), "export interrupted", "month", m.String(), "rows", rows, "err", err)
		panic(http.ErrAbortHandler)
	default:
		cw.Flush()
	}
}

// Record is the CSV row of the export, in CSVHeader order.
func (l InvoiceLine) Record() []string {
	return []string{
		l.InvoiceID, l.CustomerID, l.PlanID, l.Month.String(),
		strconv.FormatInt(l.LineNo, 10), strconv.FormatInt(l.TierNo, 10),
		strconv.FormatInt(l.Quantity, 10), strconv.FormatInt(l.UnitPriceMicros, 10),
		strconv.FormatInt(l.FlatFeeCents, 10), strconv.FormatInt(l.AmountCents, 10),
	}
}

func (a *api) startRollup(w http.ResponseWriter, r *http.Request) {
	m, ok := a.month(w, r)
	if !ok {
		return
	}
	queryID, err := a.svc.StartRollup(r.Context(), m)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	location := "/v1/rollups/" + queryID
	w.Header().Set("Location", location)
	writeJSON(w, http.StatusAccepted, map[string]string{"query_id": queryID, "status_url": location})
}

func (a *api) getRollup(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("queryId")
	if !ValidQueryID(id) {
		writeProblem(w, r, Problem{Type: problemMalformed, Title: "Invalid query id", Status: http.StatusBadRequest,
			Detail: fmt.Sprintf("%q is not a Snowflake query id", id)})
		return
	}
	rollup, err := a.svc.GetRollup(r.Context(), id)
	switch {
	case errors.Is(err, ErrQueryNotFound):
		writeProblem(w, r, Problem{Type: problemNotFound, Title: "Rollup not found", Status: http.StatusNotFound,
			Detail: fmt.Sprintf("no query with id %s", id)})
	case err != nil:
		a.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, rollup)
	}
}

func (a *api) month(w http.ResponseWriter, r *http.Request) (Month, bool) {
	m, err := ParseMonth(r.PathValue("month"))
	if err != nil {
		writeProblem(w, r, Problem{Type: problemMalformed, Title: "Invalid month", Status: http.StatusBadRequest, Detail: err.Error()})
		return Month{}, false
	}
	return m, true
}

func (a *api) internalError(w http.ResponseWriter, r *http.Request, err error) {
	a.log.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeProblem(w, r, Problem{Type: problemInternal, Title: "Internal error", Status: http.StatusInternalServerError})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Flush keeps streaming responses streaming through the recorder.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (a *api) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		a.log.InfoContext(r.Context(), "request", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration_ms", time.Since(start).Milliseconds())
	})
}
