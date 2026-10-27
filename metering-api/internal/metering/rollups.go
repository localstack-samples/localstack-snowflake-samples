package metering

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"
)

// The monthly rollup: calls, events and active customers per plan and day.
const rollupQuery = `SELECT c.PLAN_ID,
       TO_DATE(e.OCCURRED_AT) AS USAGE_DATE,
       COUNT(DISTINCT e.CUSTOMER_ID) AS ACTIVE_CUSTOMERS,
       COUNT(*) AS EVENTS,
       SUM(e.QUANTITY) AS CALLS
FROM USAGE_EVENTS e
JOIN CUSTOMERS c ON c.CUSTOMER_ID = e.CUSTOMER_ID
WHERE e.OCCURRED_AT >= ? AND e.OCCURRED_AT < ?
GROUP BY c.PLAN_ID, USAGE_DATE
ORDER BY USAGE_DATE, c.PLAN_ID`

// rollupTimeout bounds how long the driver keeps fetching a rollup's result in
// the background after the request that started it has returned.
const rollupTimeout = 15 * time.Minute

// RollupRow is one row of the monthly rollup.
type RollupRow struct {
	PlanID          string `json:"plan_id"`
	Date            Date   `json:"date"`
	ActiveCustomers int64  `json:"active_customers"`
	Events          int64  `json:"events"`
	Calls           int64  `json:"calls"`
}

// RollupStatus values.
const (
	RollupRunning   = "running"
	RollupSucceeded = "succeeded"
	RollupFailed    = "failed"
)

// Rollup is the state of a rollup query. Rows is set once it has succeeded.
type Rollup struct {
	QueryID string      `json:"query_id"`
	Status  string      `json:"status"`
	Error   string      `json:"error,omitempty"`
	Rows    []RollupRow `json:"rows,omitempty"`
}

// StartRollup submits the rollup for a month as an asynchronous Snowflake query
// and returns its query id without waiting for it. Any instance of the service
// can later read its status and result by that id (see GetRollup).
func (s *Service) StartRollup(ctx context.Context, m Month) (string, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	// The query outlives the HTTP request, so it must not be canceled with it.
	// The query tag on ctx is kept.
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollupTimeout)
	var queryID string
	err = conn.Raw(func(dc any) error {
		rows, err := dc.(driver.QueryerContext).QueryContext(sf.WithAsyncMode(bg), rollupQuery, []driver.NamedValue{
			{Ordinal: 1, Value: m.Start()},
			{Ordinal: 2, Value: m.End()},
		})
		if err != nil {
			return err
		}
		queryID = rows.(sf.SnowflakeRows).GetQueryID()
		// In async mode the driver fetches the result in the background; Close
		// waits for that and releases it. Results are read again by id later.
		go func() {
			defer cancel()
			_ = rows.Close()
		}()
		return nil
	})
	if err != nil {
		cancel()
		return "", fmt.Errorf("submit rollup: %w", err)
	}
	return queryID, nil
}

var queryIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// ValidQueryID reports whether id has the shape of a Snowflake query id.
func ValidQueryID(id string) bool { return queryIDPattern.MatchString(id) }

// GetRollup reports a rollup query's status and, once it has succeeded, its
// rows (fetched by query id). It returns ErrQueryNotFound for an unknown id.
func (s *Service) GetRollup(ctx context.Context, queryID string) (Rollup, error) {
	r := Rollup{QueryID: queryID}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return r, err
	}
	err = conn.Raw(func(dc any) error {
		_, err := dc.(sf.SnowflakeConnection).GetQueryStatus(ctx, queryID)
		return err
	})
	_ = conn.Close()
	var sfErr *sf.SnowflakeError
	switch {
	case err == nil:
		r.Status = RollupSucceeded
	case errors.As(err, &sfErr) && sfErr.Number == sf.ErrQueryIsRunning:
		r.Status = RollupRunning
		return r, nil
	case errors.As(err, &sfErr) && sfErr.Number == sf.ErrQueryReportedError:
		r.Status = RollupFailed
		r.Error = sfErr.Message
		return r, nil
	case errors.As(err, &sfErr) && sfErr.Number == sf.ErrQueryStatus:
		return r, ErrQueryNotFound
	default:
		return r, fmt.Errorf("rollup status: %w", err)
	}

	rows, err := s.db.QueryContext(sf.WithFetchResultByID(ctx, queryID), "")
	if err != nil {
		return r, fmt.Errorf("fetch rollup result: %w", err)
	}
	defer rows.Close()
	r.Rows = []RollupRow{}
	for rows.Next() {
		var (
			row RollupRow
			day time.Time
		)
		if err := rows.Scan(&row.PlanID, &day, &row.ActiveCustomers, &row.Events, &row.Calls); err != nil {
			return r, fmt.Errorf("read rollup result: %w", err)
		}
		row.Date = DateOf(day)
		r.Rows = append(r.Rows, row)
	}
	return r, rows.Err()
}
