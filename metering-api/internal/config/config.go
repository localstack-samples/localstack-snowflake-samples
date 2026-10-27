// Package config reads the service's settings from the environment (12-factor)
// and turns the Snowflake settings into a database/sql connection pool.
//
// The same variables describe the local emulator and a real Snowflake account;
// TEST_TARGET picks which defaults apply. Only the account, host and key differ.
package config

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"
)

// Target selects the Snowflake the process talks to.
type Target string

const (
	// Emulator is the local Snowflake started by `make up` (the default).
	Emulator Target = "EMULATOR"
	// Cloud is a real Snowflake account.
	Cloud Target = "SNOWFLAKE_CLOUD"
)

const emulatorHost = "snowflake.localhost.localstack.cloud"

// Snowflake holds the connection settings. PrivateKey is loaded from
// SNOWFLAKE_PRIVATE_KEY_PATH by FromEnv; tests may set it directly.
type Snowflake struct {
	Target     Target
	Account    string
	Host       string
	Port       int
	Protocol   string
	User       string
	Role       string
	Warehouse  string
	Database   string
	Schema     string
	PrivateKey *rsa.PrivateKey
}

// TargetFromEnv reads TEST_TARGET (EMULATOR or SNOWFLAKE_CLOUD).
func TargetFromEnv() (Target, error) {
	switch t := Target(strings.ToUpper(env("TEST_TARGET", string(Emulator)))); t {
	case Emulator, Cloud:
		return t, nil
	default:
		return "", fmt.Errorf("TEST_TARGET must be %s or %s, got %q", Emulator, Cloud, t)
	}
}

// SnowflakeFromEnv reads the SNOWFLAKE_* variables. On the emulator every value
// has a working default; on a real account the account, user, role, warehouse
// and key path are required.
func SnowflakeFromEnv() (Snowflake, error) {
	target, err := TargetFromEnv()
	if err != nil {
		return Snowflake{}, err
	}
	c := Snowflake{
		Target:    target,
		Protocol:  "https",
		Database:  env("SNOWFLAKE_DATABASE", "METERING"),
		Schema:    env("SNOWFLAKE_SCHEMA", "BILLING"),
		Role:      env("SNOWFLAKE_ROLE", "METERING_APP"),
		Warehouse: env("SNOWFLAKE_WAREHOUSE", "METERING_WH"),
	}
	keyPath := env("SNOWFLAKE_PRIVATE_KEY_PATH", ".secrets/rsa_key.p8")
	switch target {
	case Emulator:
		c.Account = env("SNOWFLAKE_ACCOUNT", "snowflake")
		c.Host = env("SNOWFLAKE_HOST", emulatorHost)
		c.User = env("SNOWFLAKE_USER", "METERING_SVC")
		if c.Port, err = strconv.Atoi(env("SNOWFLAKE_PORT", "4566")); err != nil {
			return Snowflake{}, fmt.Errorf("SNOWFLAKE_PORT: %w", err)
		}
	case Cloud:
		var missing []string
		for _, name := range []string{"SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_PRIVATE_KEY_PATH"} {
			if os.Getenv(name) == "" {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			return Snowflake{}, fmt.Errorf("TEST_TARGET=%s needs %s", Cloud, strings.Join(missing, ", "))
		}
		c.Account = os.Getenv("SNOWFLAKE_ACCOUNT")
		c.Host = env("SNOWFLAKE_HOST", c.Account+".snowflakecomputing.com")
		c.User = os.Getenv("SNOWFLAKE_USER")
		c.Port = 443
	}
	if c.PrivateKey, err = ReadPrivateKey(keyPath); err != nil {
		return Snowflake{}, err
	}
	return c, nil
}

// ReadPrivateKey loads an unencrypted PKCS#8 RSA key in PEM form, as written by
// `openssl genpkey` (see `make keys`).
func ReadPrivateKey(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read private key (run `make keys`?): %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%s: expected a PEM \"PRIVATE KEY\" (PKCS#8) block", path)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s: key is %T, want RSA", path, key)
	}
	return rsaKey, nil
}

// DriverConfig is the gosnowflake configuration for these settings.
func (c Snowflake) DriverConfig() sf.Config {
	return sf.Config{
		Account:       c.Account,
		User:          c.User,
		Host:          c.Host,
		Port:          c.Port,
		Protocol:      c.Protocol,
		Authenticator: sf.AuthTypeJwt,
		PrivateKey:    c.PrivateKey,
		Role:          c.Role,
		Warehouse:     c.Warehouse,
		Database:      c.Database,
		Schema:        c.Schema,
		Application:   "metering-api",
	}
}

// Open returns a connection pool and checks that Snowflake answers.
func Open(ctx context.Context, c Snowflake) (*sql.DB, error) {
	if c.PrivateKey == nil {
		return nil, errors.New("snowflake: no private key configured")
	}
	db := sql.OpenDB(sf.NewConnector(sf.SnowflakeDriver{}, c.DriverConfig()))
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to Snowflake at %s:%d as %s: %w", c.Host, c.Port, c.User, err)
	}
	return db, nil
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
