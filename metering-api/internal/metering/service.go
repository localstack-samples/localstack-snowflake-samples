// Package metering is the usage-metering and invoicing service: event ingestion,
// usage queries, monthly invoice close, async rollups and CSV export, on
// Snowflake through database/sql and gosnowflake.
package metering

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"

	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/pricing"
)

// Service runs the metering use cases against one Snowflake schema.
type Service struct {
	db  *sql.DB
	log *slog.Logger
	now func() time.Time

	// beforeMarkInvoiced runs inside the invoice-close transaction after the
	// invoices are written and before usage is marked invoiced. Tests use it to
	// inject a failure mid-transaction.
	beforeMarkInvoiced func(context.Context, *sql.Tx) error
}

// NewService returns a Service on db, whose session must default to the
// metering schema.
func NewService(db *sql.DB, log *slog.Logger) *Service {
	return &Service{db: db, log: log, now: time.Now}
}

// Ping checks that Snowflake answers.
func (s *Service) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Customer is an account on the API platform.
type Customer struct {
	ID        string
	Name      string
	PlanID    string
	CreatedAt time.Time
}

// Errors the HTTP layer maps to problem responses.
var (
	ErrCustomerNotFound = errors.New("customer not found")
	ErrMonthClosed      = errors.New("month already closed")
	ErrMonthNotOver     = errors.New("month has not ended")
	ErrNoInvoices       = errors.New("no invoices for month")
	ErrQueryNotFound    = errors.New("query not found")
)

// UnknownCustomersError rejects an ingest batch that names customers that do
// not exist.
type UnknownCustomersError struct{ IDs []string }

func (e *UnknownCustomersError) Error() string {
	return fmt.Sprintf("%d unknown customer id(s)", len(e.IDs))
}

// WithQueryTag labels every Snowflake statement run with ctx, so QUERY_HISTORY
// can be filtered by endpoint (for example QUERY_TAG = 'metering-api:invoices.close').
func WithQueryTag(ctx context.Context, operation string) context.Context {
	return sf.WithQueryTag(ctx, "metering-api:"+operation)
}

// UpsertCustomers creates or updates customers by id, 100 per statement.
func (s *Service) UpsertCustomers(ctx context.Context, customers []Customer) error {
	const chunk = 100
	for start := 0; start < len(customers); start += chunk {
		part := customers[start:min(start+chunk, len(customers))]
		args := make([]any, 0, 4*len(part))
		values := make([]byte, 0, 16*len(part))
		for i, c := range part {
			if i > 0 {
				values = append(values, ", "...)
			}
			values = append(values, "(?, ?, ?, ?)"...)
			args = append(args, c.ID, c.Name, c.PlanID, c.CreatedAt.UTC())
		}
		query := `MERGE INTO CUSTOMERS t
USING (SELECT $1 AS CUSTOMER_ID, $2 AS NAME, $3 AS PLAN_ID, $4::TIMESTAMP_NTZ AS CREATED_AT
       FROM VALUES ` + string(values) + `) s
ON t.CUSTOMER_ID = s.CUSTOMER_ID
WHEN MATCHED THEN UPDATE SET NAME = s.NAME, PLAN_ID = s.PLAN_ID, CREATED_AT = s.CREATED_AT
WHEN NOT MATCHED THEN INSERT (CUSTOMER_ID, NAME, PLAN_ID, CREATED_AT)
  VALUES (s.CUSTOMER_ID, s.NAME, s.PLAN_ID, s.CREATED_AT)`
		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("upsert customers: %w", err)
		}
	}
	return nil
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// loadPlans reads PRICE_PLANS into validated pricing plans keyed by plan id.
func loadPlans(ctx context.Context, q queryer) (map[string]pricing.Plan, error) {
	rows, err := q.QueryContext(ctx, `SELECT PLAN_ID, UP_TO_CALLS, UNIT_PRICE_MICROS, FLAT_FEE_CENTS
FROM PRICE_PLANS ORDER BY PLAN_ID, TIER_NO`)
	if err != nil {
		return nil, fmt.Errorf("load price plans: %w", err)
	}
	defer rows.Close()
	plans := map[string]pricing.Plan{}
	for rows.Next() {
		var (
			id   string
			upTo sql.NullInt64
			tier pricing.Tier
		)
		if err := rows.Scan(&id, &upTo, &tier.UnitPriceMicros, &tier.FlatFeeCents); err != nil {
			return nil, err
		}
		tier.UpTo = pricing.Unlimited
		if upTo.Valid {
			tier.UpTo = upTo.Int64
		}
		p := plans[id]
		p.ID = id
		p.Tiers = append(p.Tiers, tier)
		plans[id] = p
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, p := range plans {
		if err := p.Validate(); err != nil {
			return nil, err
		}
	}
	return plans, nil
}
