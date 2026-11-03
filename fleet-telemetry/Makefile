# Fleet Telemetry: an ASP.NET Core service on Snowflake.Data, developed against the
# Snowflake-Next emulator.
#
#   make up          dev key, pull the emulator image, start it, wait until it answers SQL
#   make seed        generate 3 days of synthetic telemetry and bulk-load it (idempotent)
#   make run         the API on http://localhost:5080 against the emulator
#   make demo        walkthrough: backfill, live POST, trips, events, faults, daily KPIs
#   make test        unit tests, then integration tests on their own Testcontainers emulator
#   make test-cloud  the same suite against Snowflake (TEST_TARGET=SNOWFLAKE_CLOUD)
#   make check       build with warnings as errors and verify formatting
#   make down        stop and remove the emulator

-include $(HOME)/.localstack/default.env
export LOCALSTACK_AUTH_TOKEN
SNOWFLAKE_PORT ?= 4566
SNOWFLAKE_IMAGE ?= localstack/snowflake-next:latest
export SNOWFLAKE_PORT SNOWFLAKE_IMAGE

COMPOSE := docker compose -p snfs-fleet-telemetry
DEV_KEY := .secrets/rsa_key.p8
DATA_DIR := .cache/telemetry
# make run, seed and demo use appsettings.Development.json (the local emulator) and the dev key.
DEV_ENV := DOTNET_ENVIRONMENT=Development ASPNETCORE_ENVIRONMENT=Development \
	SNOWFLAKE_PRIVATE_KEY_PATH=$(CURDIR)/$(DEV_KEY)
INGEST := $(DEV_ENV) dotnet run --project src/FleetTelemetry.Ingest --

.PHONY: up seed run demo test test-unit test-integration test-cloud check down logs install start stop

up: $(DEV_KEY)
	$(COMPOSE) pull
	$(COMPOSE) up -d
	scripts/wait-for-emulator.sh 180

seed:
	$(INGEST) generate $(DATA_DIR)
	$(INGEST) backfill $(DATA_DIR)

run:
	$(DEV_ENV) dotnet run --project src/FleetTelemetry.Api --urls http://localhost:5080

demo:
	scripts/demo.sh

test: test-unit test-integration

test-unit:
	dotnet test --project tests/FleetTelemetry.UnitTests

test-integration:
	dotnet test --project tests/FleetTelemetry.IntegrationTests

# Reads SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_ROLE and
# SNOWFLAKE_WAREHOUSE; each test class creates and drops a FLEET_IT_* database.
test-cloud:
	TEST_TARGET=SNOWFLAKE_CLOUD dotnet test --project tests/FleetTelemetry.IntegrationTests

check:
	dotnet build -warnaserror
	dotnet format --verify-no-changes

down:
	$(COMPOSE) down -v --remove-orphans

logs:
	$(COMPOSE) logs --tail=200 snowflake

# Throwaway unencrypted PKCS#8 key pair for key-pair (JWT) authentication.
$(DEV_KEY):
	mkdir -p $(dir $@)
	openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out $@
	openssl rsa -in $@ -pubout -out $(dir $@)rsa_key.pub
	chmod 600 $@

# The targets the repository CI runs for every sample (see the root README)
install:
	@echo "Nothing to install, make up and make test fetch the dependencies"

start: up

stop: down
