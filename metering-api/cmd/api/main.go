// Command api serves the metering API.
//
// Configuration comes from the environment (see .env.example): SNOWFLAKE_* for
// the connection, HTTP_ADDR for the listen address, LOG_LEVEL for logging.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/config"
	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/metering"
)

func main() {
	if err := run(); err != nil {
		slog.Error("metering-api stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var level slog.Level
	if err := level.UnmarshalText([]byte(getenv("LOG_LEVEL", "INFO"))); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sfCfg, err := config.SnowflakeFromEnv()
	if err != nil {
		return err
	}
	db, err := config.Open(ctx, sfCfg)
	if err != nil {
		return err
	}
	defer db.Close()

	srv := &http.Server{
		Addr:              getenv("HTTP_ADDR", "127.0.0.1:8080"),
		Handler:           metering.NewHandler(metering.NewService(db, log), log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr, "snowflake", sfCfg.Host, "database", sfCfg.Database, "target", string(sfCfg.Target))
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func getenv(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
