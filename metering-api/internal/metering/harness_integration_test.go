//go:build integration

package metering_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/config"
	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/metering"
	"github.com/localstack-samples/localstack-snowflake-samples/metering-api/internal/seed"
)

// The integration suite runs against one of two targets, chosen by TEST_TARGET:
//
//   - EMULATOR (default): a fresh localstack/snowflake-next container started
//     with testcontainers-go; init/01_platform.sf.sql provisions it.
//   - SNOWFLAKE_CLOUD: the account in the SNOWFLAKE_* variables.
//
// Either way every test gets its own database, built from init/02 and init/03,
// and its own API instance on httptest. Nothing else differs between targets.

const initDir = "../../init"

var suite struct {
	app   config.Snowflake // the service's connection settings, minus the database
	admin *sql.DB          // creates and drops the per-test databases
	// grantTo is the role that must own each test database, when the admin
	// role is a different one.
	grantTo string
}

func TestMain(m *testing.M) {
	code, err := runSuite(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration suite:", err)
		code = 1
	}
	os.Exit(code)
}

func runSuite(m *testing.M) (int, error) {
	ctx := context.Background()
	target, err := config.TargetFromEnv()
	if err != nil {
		return 1, err
	}
	var stop func()
	switch target {
	case config.Emulator:
		stop, err = startEmulator(ctx)
	case config.Cloud:
		stop, err = useCloud(ctx)
	}
	if err != nil {
		return 1, err
	}
	defer stop()
	return m.Run(), nil
}

// startEmulator starts the emulator container, waits until it serves HTTPS on
// the mapped port (checked from the host), and provisions the account.
func startEmulator(ctx context.Context) (func(), error) {
	token := os.Getenv("LOCALSTACK_AUTH_TOKEN")
	if token == "" {
		return nil, fmt.Errorf("LOCALSTACK_AUTH_TOKEN is not set (export it, or run `make test`, which reads ~/.localstack/default.env)")
	}
	image := os.Getenv("SNOWFLAKE_IMAGE")
	if image == "" {
		image = "localstack/snowflake-next:latest"
	}
	const host = "snowflake.localhost.localstack.cloud" // resolves to 127.0.0.1
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"4566/tcp"},
			Env:          map[string]string{"LOCALSTACK_AUTH_TOKEN": token},
			WaitingFor: wait.ForHTTP("/").
				WithPort("4566/tcp").
				WithTLS(true, &tls.Config{ServerName: host}).
				WithStatusCodeMatcher(func(status int) bool { return status < 500 }).
				WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start emulator: %w", err)
	}
	stop := func() { _ = testcontainers.TerminateContainer(ctr) }
	port, err := ctr.MappedPort(ctx, "4566/tcp")
	if err != nil {
		stop()
		return nil, err
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048) // throwaway key for this run
	if err != nil {
		stop()
		return nil, err
	}
	suite.app = config.Snowflake{
		Target: config.Emulator, Account: "snowflake", Host: host, Port: int(port.Num()), Protocol: "https",
		User: "METERING_SVC", Role: "METERING_APP", Warehouse: "METERING_WH", PrivateKey: key,
	}
	// The admin session names no warehouse: 01_platform creates it.
	adminCfg := suite.app
	adminCfg.Role, adminCfg.Warehouse = "ACCOUNTADMIN", ""
	if suite.admin, err = openWhenReady(ctx, adminCfg); err != nil {
		stop()
		return nil, err
	}
	if err := applyScript(ctx, suite.admin, filepath.Join(initDir, "01_platform.sf.sql"), false); err != nil {
		stop()
		return nil, fmt.Errorf("provision emulator: %w", err)
	}
	suite.grantTo = "METERING_APP"
	return func() { _ = suite.admin.Close(); stop() }, nil
}

// useCloud connects to a real account. The role needs CREATE DATABASE.
func useCloud(ctx context.Context) (func(), error) {
	cfg, err := config.SnowflakeFromEnv()
	if err != nil {
		return nil, err
	}
	cfg.Database, cfg.Schema = "", ""
	suite.app = cfg
	if suite.admin, err = config.Open(ctx, cfg); err != nil {
		return nil, err
	}
	return func() { _ = suite.admin.Close() }, nil
}

// openWhenReady connects, retrying until the target answers SELECT 1.
func openWhenReady(ctx context.Context, cfg config.Snowflake) (*sql.DB, error) {
	deadline := time.Now().Add(90 * time.Second)
	for {
		db, err := config.Open(ctx, cfg)
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("no SQL answer within 90s: %w", err)
		}
		time.Sleep(2 * time.Second)
	}
}

// applyScript runs an init/*.sf.sql file statement by statement on one session.
// With databaseRelative set it skips the file's USE ROLE and USE DATABASE lines,
// so the objects land in the session's current database.
func applyScript(ctx context.Context, db *sql.DB, path string, databaseRelative bool) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var lines []string
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			lines = append(lines, line)
		}
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, stmt := range strings.SplitAfter(strings.Join(lines, "\n"), ";\n") {
		stmt = strings.TrimSuffix(strings.TrimSpace(stmt), ";")
		upper := strings.ToUpper(stmt)
		if stmt == "" || databaseRelative && (strings.HasPrefix(upper, "USE ROLE") || strings.HasPrefix(upper, "USE DATABASE")) {
			continue
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w\n%s", filepath.Base(path), err, stmt)
		}
	}
	return nil
}

// env is one test's database, service and API.
type env struct {
	t   *testing.T
	ctx context.Context
	db  *sql.DB
	svc *metering.Service
	srv *httptest.Server
}

// newEnv creates a database for the test, applies the schema and reference
// data, and starts the API on it. Everything is dropped when the test ends.
func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	name := "METERING_IT_" + strings.ToUpper(rand.Text()[:12])
	if _, err := suite.admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		if _, err := suite.admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name); err != nil {
			t.Logf("drop database %s: %v", name, err)
		}
	})
	if suite.grantTo != "" {
		if _, err := suite.admin.ExecContext(ctx, "GRANT OWNERSHIP ON DATABASE "+name+" TO ROLE "+suite.grantTo+" COPY CURRENT GRANTS"); err != nil {
			t.Fatalf("grant database: %v", err)
		}
	}

	cfg := suite.app
	cfg.Database = name
	ddl, err := config.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"02_schema.sf.sql", "03_price_plans.sf.sql"} {
		if err := applyScript(ctx, ddl, filepath.Join(initDir, file), true); err != nil {
			_ = ddl.Close()
			t.Fatal(err)
		}
	}
	_ = ddl.Close()

	cfg.Schema = "BILLING"
	db, err := config.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	log := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelWarn}))
	svc := metering.NewService(db, log)
	srv := httptest.NewServer(metering.NewHandler(svc, log))
	t.Cleanup(srv.Close)
	return &env{t: t, ctx: metering.WithQueryTag(ctx, "integration-test"), db: db, svc: svc, srv: srv}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

// seed loads a generated dataset: customers directly, events through the API.
func (e *env) seed(o seed.Options) seed.Dataset {
	e.t.Helper()
	d := seed.Generate(o)
	if err := e.svc.UpsertCustomers(e.ctx, d.Customers); err != nil {
		e.t.Fatal(err)
	}
	res := e.ingest(d.Events, 400)
	if res.Inserted != len(d.Events) || res.Duplicates != 0 {
		e.t.Fatalf("seeding %d events: %+v", len(d.Events), res)
	}
	return d
}

// ingest posts events in batches and sums the results.
func (e *env) ingest(events []metering.Event, batch int) metering.IngestResult {
	e.t.Helper()
	var total metering.IngestResult
	for i := 0; i < len(events); i += batch {
		var res metering.IngestResult
		e.call("POST", "/v1/events", map[string]any{"events": events[i:min(i+batch, len(events))]}, http.StatusOK, &res)
		total.Received += res.Received
		total.Inserted += res.Inserted
		total.Duplicates += res.Duplicates
	}
	return total
}

// call sends a JSON request, checks the status and decodes the response into out.
func (e *env) call(method, path string, body any, wantStatus int, out any) http.Header {
	e.t.Helper()
	resp, data := e.do(method, path, body)
	if resp.StatusCode != wantStatus {
		e.t.Fatalf("%s %s: status %d, want %d; body %s", method, path, resp.StatusCode, wantStatus, data)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			e.t.Fatalf("%s %s: decode %s: %v", method, path, data, err)
		}
	}
	return resp.Header
}

func (e *env) do(method, path string, body any) (*http.Response, []byte) {
	e.t.Helper()
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		reader = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			e.t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, reader)
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	return resp, data
}

// scalar runs a query that returns one integer.
func (e *env) scalar(query string, args ...any) int64 {
	e.t.Helper()
	var n sql.NullInt64
	if err := e.db.QueryRowContext(e.ctx, query, args...).Scan(&n); err != nil {
		e.t.Fatalf("%s: %v", query, err)
	}
	return n.Int64
}
