package metering

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
)

// maxClockSkew is how far in the future an event's timestamp may be.
const maxClockSkew = 5 * time.Minute

// maxUsageRange bounds the usage query window.
const maxUsageRange = 366 * 24 * time.Hour

var idPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)

type ingestRequest struct {
	Events []Event `json:"events"`
}

// decodeEvents parses an ingest body. Any JSON error is a malformed request.
func decodeEvents(body io.Reader) ([]Event, error) {
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	var req ingestRequest
	if err := dec.Decode(&req); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("unexpected data after the JSON object")
	}
	return req.Events, nil
}

// ValidateEvents checks an ingest batch and returns one FieldError per problem.
func ValidateEvents(events []Event, now time.Time) []FieldError {
	var errs []FieldError
	add := func(field, format string, args ...any) {
		errs = append(errs, FieldError{Field: field, Detail: fmt.Sprintf(format, args...)})
	}
	switch {
	case len(events) == 0:
		add("events", "must contain at least one event")
	case len(events) > MaxBatch:
		add("events", "must contain at most %d events, got %d", MaxBatch, len(events))
	}
	for i, e := range events {
		at := func(f string) string { return fmt.Sprintf("events[%d].%s", i, f) }
		checkID(add, at("event_id"), e.EventID, 64)
		checkID(add, at("customer_id"), e.CustomerID, 32)
		if e.Quantity < 1 {
			add(at("quantity"), "must be a positive number of calls")
		}
		switch {
		case e.OccurredAt.IsZero():
			add(at("occurred_at"), "is required (RFC 3339 timestamp)")
		case e.OccurredAt.After(now.Add(maxClockSkew)):
			add(at("occurred_at"), "is in the future")
		}
	}
	return errs
}

func checkID(add func(string, string, ...any), field, v string, maxLen int) {
	switch {
	case v == "":
		add(field, "is required")
	case len(v) > maxLen:
		add(field, "must be at most %d characters", maxLen)
	case !idPattern.MatchString(v):
		add(field, "may contain only letters, digits and _ . : -")
	}
}

// parseUsageRange reads ?from=YYYY-MM-DD&to=YYYY-MM-DD (to exclusive). Both
// default to the current month.
func parseUsageRange(from, to string, now time.Time) (Date, Date, error) {
	month := MonthOf(now)
	start, end := DateOf(month.Start()), DateOf(month.End())
	var err error
	if from != "" {
		if start, err = ParseDate(from); err != nil {
			return Date{}, Date{}, fmt.Errorf("from: %w", err)
		}
	}
	if to != "" {
		if end, err = ParseDate(to); err != nil {
			return Date{}, Date{}, fmt.Errorf("to: %w", err)
		}
	}
	if !start.Time().Before(end.Time()) {
		return Date{}, Date{}, errors.New("from must be before to")
	}
	if end.Time().Sub(start.Time()) > maxUsageRange {
		return Date{}, Date{}, errors.New("the range may span at most 366 days")
	}
	return start, end, nil
}
