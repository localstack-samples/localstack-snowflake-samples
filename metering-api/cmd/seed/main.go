// Command seed loads the synthetic dataset: ~200 customers and the last three
// complete months of their usage. Events go through the same idempotent ingest
// path as POST /v1/events, so running it again inserts nothing.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/config"
	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/metering"
	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/seed"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
}

func run() error {
	customers := flag.Int("customers", 200, "number of customers")
	months := flag.Int("months", 3, "complete months of usage before the current one")
	seedValue := flag.Uint64("seed", 42, "random seed")
	flag.Parse()

	ctx := metering.WithQueryTag(context.Background(), "seed")
	cfg, err := config.SnowflakeFromEnv()
	if err != nil {
		return err
	}
	db, err := config.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	svc := metering.NewService(db, slog.New(slog.NewTextHandler(os.Stderr, nil)))

	window := seed.LastCompleteMonths(time.Now(), *months)
	data := seed.Generate(seed.Options{Seed: *seedValue, Customers: *customers, Months: window})
	plans := map[string]int{}
	for _, c := range data.Customers {
		plans[c.PlanID]++
	}
	fmt.Printf("Seeding %s (%s to %s, seed %d)\n", cfg.Database, window[0], window[len(window)-1], *seedValue)

	start := time.Now()
	if err := svc.UpsertCustomers(ctx, data.Customers); err != nil {
		return err
	}
	fmt.Printf("  customers  %d (FREE %d, PRO %d, ENTERPRISE %d)\n",
		len(data.Customers), plans["FREE"], plans["PRO"], plans["ENTERPRISE"])

	var total metering.IngestResult
	for i := 0; i < len(data.Events); i += metering.MaxBatch {
		batch := data.Events[i:min(i+metering.MaxBatch, len(data.Events))]
		res, err := svc.IngestEvents(ctx, batch)
		if err != nil {
			return fmt.Errorf("events %d-%d: %w", i, i+len(batch)-1, err)
		}
		total.Received += res.Received
		total.Inserted += res.Inserted
		total.Duplicates += res.Duplicates
	}
	fmt.Printf("  events     %d received, %d inserted, %d already present\n", total.Received, total.Inserted, total.Duplicates)
	fmt.Printf("Done in %s\n", time.Since(start).Round(time.Second))
	return nil
}
