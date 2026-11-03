package metering

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"
)

// Event is one usage report from the API gateway: Quantity calls made by a
// customer in the window ending at OccurredAt. EventID is the idempotency key:
// the gateway retries with the same id, and a retried event is never counted twice.
type Event struct {
	EventID    string    `json:"event_id"`
	CustomerID string    `json:"customer_id"`
	Quantity   int64     `json:"quantity"`
	OccurredAt time.Time `json:"occurred_at"`
}

// IngestResult reports how many events of a batch were new.
type IngestResult struct {
	Received   int `json:"received"`
	Inserted   int `json:"inserted"`
	Duplicates int `json:"duplicates"`
}

// MaxBatch is the largest ingest batch. It keeps one array-bound INSERT well
// below the driver's stage-binding threshold.
const MaxBatch = 1000

const mergeStagedEvents = `MERGE INTO USAGE_EVENTS t
USING (
  SELECT EVENT_ID, CUSTOMER_ID, QUANTITY, OCCURRED_AT
  FROM USAGE_EVENTS_STAGING
  WHERE BATCH_ID = ?
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY OCCURRED_AT) = 1
) s
ON t.EVENT_ID = s.EVENT_ID
WHEN NOT MATCHED THEN INSERT (EVENT_ID, CUSTOMER_ID, QUANTITY, OCCURRED_AT, RECEIVED_AT)
  VALUES (s.EVENT_ID, s.CUSTOMER_ID, s.QUANTITY, s.OCCURRED_AT, SYSDATE())`

// IngestEvents stores a batch idempotently, in one transaction:
//
//  1. one INSERT with array binds lands the batch in USAGE_EVENTS_STAGING;
//  2. the batch is rejected if it names an unknown customer;
//  3. a MERGE keyed by EVENT_ID inserts the events not stored yet;
//  4. the staging rows are deleted.
//
// Events must already be validated (see ValidateEvents).
func (s *Service) IngestEvents(ctx context.Context, events []Event) (IngestResult, error) {
	result := IngestResult{Received: len(events)}
	if len(events) == 0 {
		return result, nil
	}
	n := len(events)
	batch := make([]string, n)
	ids := make([]string, n)
	customers := make([]string, n)
	quantities := make([]int64, n)
	occurred := make([]time.Time, n)
	batchID := rand.Text()
	for i, e := range events {
		batch[i], ids[i], customers[i], quantities[i], occurred[i] = batchID, e.EventID, e.CustomerID, e.Quantity, e.OccurredAt.UTC()
	}
	binds, err := arrays(batch, ids, customers, quantities)
	if err != nil {
		return result, err
	}
	occurredBind, err := sf.Array(occurred, sf.TimestampNTZType)
	if err != nil {
		return result, err
	}
	binds = append(binds, occurredBind)

	err = s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO USAGE_EVENTS_STAGING
  (BATCH_ID, EVENT_ID, CUSTOMER_ID, QUANTITY, OCCURRED_AT) VALUES (?, ?, ?, ?, ?)`, binds...); err != nil {
			return fmt.Errorf("stage events: %w", err)
		}
		unknown, err := unknownCustomers(ctx, tx, batchID)
		if err != nil {
			return err
		}
		if len(unknown) > 0 {
			return &UnknownCustomersError{IDs: unknown}
		}
		res, err := tx.ExecContext(ctx, mergeStagedEvents, batchID)
		if err != nil {
			return fmt.Errorf("merge events: %w", err)
		}
		inserted, err := res.RowsAffected()
		if err != nil {
			return err
		}
		result.Inserted = int(inserted)
		if _, err := tx.ExecContext(ctx, `DELETE FROM USAGE_EVENTS_STAGING WHERE BATCH_ID = ?`, batchID); err != nil {
			return fmt.Errorf("clear staging: %w", err)
		}
		return nil
	})
	if err != nil {
		return IngestResult{}, err
	}
	result.Duplicates = result.Received - result.Inserted
	return result, nil
}

func unknownCustomers(ctx context.Context, tx *sql.Tx, batchID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT s.CUSTOMER_ID
FROM USAGE_EVENTS_STAGING s
LEFT JOIN CUSTOMERS c ON c.CUSTOMER_ID = s.CUSTOMER_ID
WHERE s.BATCH_ID = ? AND c.CUSTOMER_ID IS NULL
ORDER BY 1`, batchID)
	if err != nil {
		return nil, fmt.Errorf("check customers: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// arrays wraps each column slice for gosnowflake array binding.
func arrays(columns ...any) ([]any, error) {
	binds := make([]any, len(columns))
	for i, c := range columns {
		b, err := sf.Array(c)
		if err != nil {
			return nil, err
		}
		binds[i] = b
	}
	return binds, nil
}

// inTx runs fn in a transaction, committing if it returns nil and rolling back
// otherwise.
func (s *Service) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			s.log.ErrorContext(ctx, "rollback failed", "err", rbErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
