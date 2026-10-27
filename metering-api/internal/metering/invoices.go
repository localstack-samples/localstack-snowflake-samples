package metering

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"
)

// CloseResult summarizes a closed billing month.
type CloseResult struct {
	Month          Month `json:"month"`
	Invoices       int   `json:"invoices"`
	TotalCents     int64 `json:"total_cents"`
	EventsInvoiced int64 `json:"events_invoiced"`
}

// InvoiceID is the deterministic id of a customer's invoice for a month.
func InvoiceID(m Month, customerID string) string {
	return "inv_" + m.Start().Format("200601") + "_" + customerID
}

const usageToInvoice = `SELECT c.CUSTOMER_ID, c.PLAN_ID, COALESCE(SUM(e.QUANTITY), 0) AS CALLS
FROM CUSTOMERS c
LEFT JOIN USAGE_EVENTS e
  ON e.CUSTOMER_ID = c.CUSTOMER_ID
 AND e.OCCURRED_AT >= ? AND e.OCCURRED_AT < ?
 AND e.INVOICE_ID IS NULL
WHERE c.CREATED_AT < ?
GROUP BY c.CUSTOMER_ID, c.PLAN_ID
ORDER BY c.CUSTOMER_ID`

const markUsageInvoiced = `UPDATE USAGE_EVENTS e
SET INVOICE_ID = i.INVOICE_ID
FROM INVOICES i
WHERE i.CUSTOMER_ID = e.CUSTOMER_ID
  AND i.BILLING_MONTH = ?
  AND e.OCCURRED_AT >= ? AND e.OCCURRED_AT < ?
  AND e.INVOICE_ID IS NULL`

// CloseMonth invoices every customer for a finished month in one transaction:
// it rates each customer's uninvoiced usage against their plan's tiers, inserts
// the invoices and their lines, and marks the usage invoiced. Either all of it
// is committed or none of it is.
func (s *Service) CloseMonth(ctx context.Context, m Month) (CloseResult, error) {
	if s.now().Before(m.End()) {
		return CloseResult{}, ErrMonthNotOver
	}
	result := CloseResult{Month: m}
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var existing int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM INVOICES WHERE BILLING_MONTH = ?`,
			sf.DataTypeDate, m.Start()).Scan(&existing); err != nil {
			return fmt.Errorf("check month: %w", err)
		}
		if existing > 0 {
			return ErrMonthClosed
		}
		inv, lines, err := s.rateMonth(ctx, tx, m)
		if err != nil {
			return err
		}
		if err := insertInvoices(ctx, tx, inv, lines); err != nil {
			return err
		}
		result.Invoices = len(inv.ids)
		for _, c := range inv.totalCents {
			result.TotalCents += c
		}
		if s.beforeMarkInvoiced != nil {
			if err := s.beforeMarkInvoiced(ctx, tx); err != nil {
				return err
			}
		}
		res, err := tx.ExecContext(ctx, markUsageInvoiced, sf.DataTypeDate, m.Start(), m.Start(), m.End())
		if err != nil {
			return fmt.Errorf("mark usage invoiced: %w", err)
		}
		result.EventsInvoiced, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return CloseResult{}, err
	}
	return result, nil
}

// invoiceColumns and lineColumns hold rows column by column, ready for array binding.
type invoiceColumns struct {
	ids, customers, plans []string
	months, closedAt      []time.Time
	calls, totalCents     []int64
}

type lineColumns struct {
	invoiceIDs                                       []string
	lineNos, tierNos                                 []int64
	quantities, unitPrices, flatFees, amountsInCents []int64
}

func (s *Service) rateMonth(ctx context.Context, tx *sql.Tx, m Month) (invoiceColumns, lineColumns, error) {
	var inv invoiceColumns
	var lines lineColumns
	plans, err := loadPlans(ctx, tx)
	if err != nil {
		return inv, lines, err
	}
	rows, err := tx.QueryContext(ctx, usageToInvoice, m.Start(), m.End(), m.End())
	if err != nil {
		return inv, lines, fmt.Errorf("read usage: %w", err)
	}
	defer rows.Close()
	closedAt := s.now().UTC()
	for rows.Next() {
		var customerID, planID string
		var calls int64
		if err := rows.Scan(&customerID, &planID, &calls); err != nil {
			return inv, lines, err
		}
		plan, ok := plans[planID]
		if !ok {
			return inv, lines, fmt.Errorf("customer %s: unknown plan %q", customerID, planID)
		}
		rated, total, err := plan.Rate(calls)
		if err != nil {
			return inv, lines, fmt.Errorf("customer %s: %w", customerID, err)
		}
		id := InvoiceID(m, customerID)
		inv.ids = append(inv.ids, id)
		inv.customers = append(inv.customers, customerID)
		inv.plans = append(inv.plans, planID)
		inv.months = append(inv.months, m.Start())
		inv.closedAt = append(inv.closedAt, closedAt)
		inv.calls = append(inv.calls, calls)
		inv.totalCents = append(inv.totalCents, total)
		for i, l := range rated {
			lines.invoiceIDs = append(lines.invoiceIDs, id)
			lines.lineNos = append(lines.lineNos, int64(i+1))
			lines.tierNos = append(lines.tierNos, int64(l.TierNo))
			lines.quantities = append(lines.quantities, l.Quantity)
			lines.unitPrices = append(lines.unitPrices, l.UnitPriceMicros)
			lines.flatFees = append(lines.flatFees, l.FlatFeeCents)
			lines.amountsInCents = append(lines.amountsInCents, l.AmountCents)
		}
	}
	return inv, lines, rows.Err()
}

func insertInvoices(ctx context.Context, tx *sql.Tx, inv invoiceColumns, lines lineColumns) error {
	if len(inv.ids) == 0 {
		return nil
	}
	binds, err := arrays(inv.ids, inv.customers)
	if err != nil {
		return err
	}
	months, err := sf.Array(inv.months, sf.DateType)
	if err != nil {
		return err
	}
	rest, err := arrays(inv.plans, inv.calls, inv.totalCents)
	if err != nil {
		return err
	}
	closed, err := sf.Array(inv.closedAt, sf.TimestampNTZType)
	if err != nil {
		return err
	}
	binds = append(append(append(binds, months), rest...), closed)
	if _, err := tx.ExecContext(ctx, `INSERT INTO INVOICES
  (INVOICE_ID, CUSTOMER_ID, BILLING_MONTH, PLAN_ID, TOTAL_CALLS, TOTAL_CENTS, CLOSED_AT)
  VALUES (?, ?, ?, ?, ?, ?, ?)`, binds...); err != nil {
		return fmt.Errorf("insert invoices: %w", err)
	}
	lineBinds, err := arrays(lines.invoiceIDs, lines.lineNos, lines.tierNos, lines.quantities,
		lines.unitPrices, lines.flatFees, lines.amountsInCents)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO INVOICE_LINES
  (INVOICE_ID, LINE_NO, TIER_NO, QUANTITY, UNIT_PRICE_MICROS, FLAT_FEE_CENTS, AMOUNT_CENTS)
  VALUES (?, ?, ?, ?, ?, ?, ?)`, lineBinds...); err != nil {
		return fmt.Errorf("insert invoice lines: %w", err)
	}
	return nil
}

// InvoiceLine is one row of the invoice export.
type InvoiceLine struct {
	InvoiceID       string
	CustomerID      string
	PlanID          string
	Month           Month
	LineNo          int64
	TierNo          int64
	Quantity        int64
	UnitPriceMicros int64
	FlatFeeCents    int64
	AmountCents     int64
}

// EachInvoiceLine streams a closed month's invoice lines, ordered by invoice and
// line number, to fn one row at a time. It returns ErrNoInvoices, without
// calling fn, if the month has no invoices.
func (s *Service) EachInvoiceLine(ctx context.Context, m Month, fn func(InvoiceLine) error) error {
	rows, err := s.db.QueryContext(ctx, `SELECT i.INVOICE_ID, i.CUSTOMER_ID, i.PLAN_ID, l.LINE_NO, l.TIER_NO,
       l.QUANTITY, l.UNIT_PRICE_MICROS, l.FLAT_FEE_CENTS, l.AMOUNT_CENTS
FROM INVOICES i
JOIN INVOICE_LINES l ON l.INVOICE_ID = i.INVOICE_ID
WHERE i.BILLING_MONTH = ?
ORDER BY i.INVOICE_ID, l.LINE_NO`, sf.DataTypeDate, m.Start())
	if err != nil {
		return fmt.Errorf("export invoice lines: %w", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		l := InvoiceLine{Month: m}
		if err := rows.Scan(&l.InvoiceID, &l.CustomerID, &l.PlanID, &l.LineNo, &l.TierNo,
			&l.Quantity, &l.UnitPriceMicros, &l.FlatFeeCents, &l.AmountCents); err != nil {
			return err
		}
		if err := fn(l); err != nil {
			return err
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if n == 0 {
		return ErrNoInvoices
	}
	return nil
}
