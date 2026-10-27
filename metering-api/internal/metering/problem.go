package metering

import (
	"encoding/json"
	"net/http"
)

// Problem is an RFC 9457 problem details response.
type Problem struct {
	Type             string       `json:"type"`
	Title            string       `json:"title"`
	Status           int          `json:"status"`
	Detail           string       `json:"detail,omitempty"`
	Instance         string       `json:"instance,omitempty"`
	Errors           []FieldError `json:"errors,omitempty"`
	UnknownCustomers []string     `json:"unknown_customers,omitempty"`
}

// FieldError points at one invalid field of a request body.
type FieldError struct {
	Field  string `json:"field"`
	Detail string `json:"detail"`
}

// Problem types, as URNs so they need no hosted documentation.
const (
	problemMalformed       = "urn:metering:problem:malformed-request"
	problemInvalid         = "urn:metering:problem:validation-failed"
	problemUnknownCustomer = "urn:metering:problem:unknown-customer"
	problemNotFound        = "urn:metering:problem:not-found"
	problemConflict        = "urn:metering:problem:conflict"
	problemInternal        = "urn:metering:problem:internal"
)

func writeProblem(w http.ResponseWriter, r *http.Request, p Problem) {
	p.Instance = r.URL.Path
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
