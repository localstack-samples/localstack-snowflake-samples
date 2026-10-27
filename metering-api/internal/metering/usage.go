package metering

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Usage is a customer's usage over [From, To).
type Usage struct {
	CustomerID string     `json:"customer_id"`
	PlanID     string     `json:"plan_id"`
	From       Date       `json:"from"`
	To         Date       `json:"to"`
	TotalCalls int64      `json:"total_calls"`
	Events     int64      `json:"events"`
	Daily      []DayUsage `json:"daily"`
}

// DayUsage is one day of a Usage.
type DayUsage struct {
	Date   Date  `json:"date"`
	Calls  int64 `json:"calls"`
	Events int64 `json:"events"`
}

// CustomerUsage aggregates a customer's events per day. It returns
// ErrCustomerNotFound for an unknown customer.
func (s *Service) CustomerUsage(ctx context.Context, customerID string, from, to Date) (Usage, error) {
	u := Usage{CustomerID: customerID, From: from, To: to, Daily: []DayUsage{}}
	err := s.db.QueryRowContext(ctx, `SELECT PLAN_ID FROM CUSTOMERS WHERE CUSTOMER_ID = ?`, customerID).Scan(&u.PlanID)
	if errors.Is(err, sql.ErrNoRows) {
		return Usage{}, ErrCustomerNotFound
	}
	if err != nil {
		return Usage{}, fmt.Errorf("look up customer: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT TO_DATE(OCCURRED_AT) AS USAGE_DATE, SUM(QUANTITY), COUNT(*)
FROM USAGE_EVENTS
WHERE CUSTOMER_ID = ? AND OCCURRED_AT >= ? AND OCCURRED_AT < ?
GROUP BY USAGE_DATE
ORDER BY USAGE_DATE`, customerID, from.Time(), to.Time())
	if err != nil {
		return Usage{}, fmt.Errorf("aggregate usage: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			day time.Time
			d   DayUsage
		)
		if err := rows.Scan(&day, &d.Calls, &d.Events); err != nil {
			return Usage{}, err
		}
		d.Date = DateOf(day)
		u.TotalCalls += d.Calls
		u.Events += d.Events
		u.Daily = append(u.Daily, d)
	}
	return u, rows.Err()
}
