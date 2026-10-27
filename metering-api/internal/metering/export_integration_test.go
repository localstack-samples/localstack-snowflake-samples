//go:build integration

package metering

import (
	"context"
	"database/sql"
)

// SetBeforeMarkInvoiced installs a hook that runs inside the invoice-close
// transaction between writing the invoices and marking usage invoiced.
func SetBeforeMarkInvoiced(s *Service, fn func(context.Context, *sql.Tx) error) {
	s.beforeMarkInvoiced = fn
}
