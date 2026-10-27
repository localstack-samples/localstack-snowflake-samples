package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var allVars = []string{
	"TEST_TARGET", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_HOST", "SNOWFLAKE_PORT", "SNOWFLAKE_USER",
	"SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_PRIVATE_KEY_PATH",
}

// cleanEnv clears every variable the package reads, for the test's duration.
func cleanEnv(t *testing.T) {
	t.Helper()
	for _, name := range allVars {
		t.Setenv(name, "")
	}
}

func writeKey(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "rsa_key.p8")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestEmulatorDefaults(t *testing.T) {
	cleanEnv(t)
	t.Setenv("SNOWFLAKE_PRIVATE_KEY_PATH", writeKey(t))
	t.Setenv("SNOWFLAKE_PORT", "34566")
	c, err := SnowflakeFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	got := []any{c.Target, c.Account, c.Host, c.Port, c.Protocol, c.User, c.Role, c.Warehouse, c.Database, c.Schema}
	want := []any{Emulator, "snowflake", "snowflake.localhost.localstack.cloud", 34566, "https", "METERING_SVC",
		"METERING_APP", "METERING_WH", "METERING", "BILLING"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("field %d = %v, want %v", i, got[i], want[i])
		}
	}
	d := c.DriverConfig()
	if d.PrivateKey == nil || d.Authenticator.String() != "SNOWFLAKE_JWT" {
		t.Errorf("driver config is not key-pair auth: %v", d.Authenticator)
	}
}

func TestCloudNeedsAccountSettings(t *testing.T) {
	cleanEnv(t)
	t.Setenv("TEST_TARGET", "snowflake_cloud")
	t.Setenv("SNOWFLAKE_USER", "ci_user")
	_, err := SnowflakeFromEnv()
	if err == nil {
		t.Fatal("accepted a cloud target without an account")
	}
	for _, name := range []string{"SNOWFLAKE_ACCOUNT", "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_PRIVATE_KEY_PATH"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
	if strings.Contains(err.Error(), "SNOWFLAKE_USER") {
		t.Errorf("error %q names a variable that is set", err)
	}
}

func TestCloudDerivesHostAndIgnoresEmulatorPort(t *testing.T) {
	cleanEnv(t)
	t.Setenv("TEST_TARGET", "SNOWFLAKE_CLOUD")
	t.Setenv("SNOWFLAKE_ACCOUNT", "myorg-prod")
	t.Setenv("SNOWFLAKE_USER", "ci_user")
	t.Setenv("SNOWFLAKE_ROLE", "CI_ROLE")
	t.Setenv("SNOWFLAKE_WAREHOUSE", "CI_WH")
	t.Setenv("SNOWFLAKE_PORT", "34566")
	t.Setenv("SNOWFLAKE_PRIVATE_KEY_PATH", writeKey(t))
	c, err := SnowflakeFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.Host != "myorg-prod.snowflakecomputing.com" || c.Port != 443 || c.User != "ci_user" || c.Role != "CI_ROLE" {
		t.Errorf("cloud config = %+v", c)
	}
}

func TestInvalidSettings(t *testing.T) {
	cleanEnv(t)
	t.Setenv("TEST_TARGET", "staging")
	if _, err := SnowflakeFromEnv(); err == nil {
		t.Error("accepted TEST_TARGET=staging")
	}
	t.Setenv("TEST_TARGET", "")
	t.Setenv("SNOWFLAKE_PORT", "https")
	if _, err := SnowflakeFromEnv(); err == nil {
		t.Error("accepted a non-numeric port")
	}
}

func TestReadPrivateKeyErrors(t *testing.T) {
	dir := t.TempDir()
	notPEM := filepath.Join(dir, "plain.txt")
	pkcs1 := filepath.Join(dir, "pkcs1.pem")
	_ = os.WriteFile(notPEM, []byte("not a key"), 0o600)
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	_ = os.WriteFile(pkcs1, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600)
	for _, path := range []string{filepath.Join(dir, "missing.p8"), notPEM, pkcs1} {
		if _, err := ReadPrivateKey(path); err == nil {
			t.Errorf("ReadPrivateKey(%s) accepted", filepath.Base(path))
		}
	}
}
