# Metering API: a Go usage-metering and invoicing service on Snowflake.
# Run `make help` for the targets.

-include $(HOME)/.localstack/default.env
export LOCALSTACK_AUTH_TOKEN
SNOWFLAKE_PORT ?= 4566
SNOWFLAKE_IMAGE ?= localstack/snowflake-next:latest
SNOWFLAKE_PRIVATE_KEY_PATH ?= $(CURDIR)/.secrets/rsa_key.p8
HTTP_ADDR ?= 127.0.0.1:8080
export SNOWFLAKE_PORT SNOWFLAKE_IMAGE SNOWFLAKE_PRIVATE_KEY_PATH HTTP_ADDR

COMPOSE := docker compose -p snfs-metering-api
GO_TEST_INTEGRATION := go test -tags integration -count=1 -timeout 30m

.DEFAULT_GOAL := help
.PHONY: help keys up seed run demo test test-unit test-integration test-cloud lint down logs install start stop

help: ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(firstword $(MAKEFILE_LIST)) | awk -F':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

keys: .secrets/rsa_key.p8 ## Generate the throwaway key pair used for SNOWFLAKE_JWT auth

.secrets/rsa_key.p8:
	@mkdir -p .secrets && chmod 700 .secrets
	@openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out $@ 2>/dev/null
	@openssl rsa -in $@ -pubout -out .secrets/rsa_key.pub 2>/dev/null
	@chmod 600 $@
	@echo "Generated .secrets/rsa_key.p8 (throwaway, for local development)"

up: keys ## Pull and start the local Snowflake, wait until it answers SQL
	$(COMPOSE) pull --quiet
	$(COMPOSE) up -d
	./scripts/wait-for-emulator.sh 180
	go mod download

seed: ## Load ~200 synthetic customers and three months of usage (idempotent)
	go run ./cmd/seed

run: ## Run the API on http://$(HTTP_ADDR) against the local Snowflake
	go run ./cmd/api

demo: ## Walk through ingest, duplicate re-ingest, invoice close, async rollup and export
	./scripts/demo.sh

test: test-unit test-integration ## Unit tests, then integration tests on a testcontainers emulator

test-unit: ## Pricing, validation and handler tests (no database)
	go test -count=1 ./...

test-integration: ## Integration tests: own emulator via testcontainers-go, one database per test
	$(GO_TEST_INTEGRATION) ./internal/metering/

test-cloud: ## The integration tests against real Snowflake (billable; needs SNOWFLAKE_* env)
	TEST_TARGET=SNOWFLAKE_CLOUD $(GO_TEST_INTEGRATION) ./internal/metering/

lint: ## go vet and gofmt
	go vet ./... && go vet -tags integration ./...
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }

down: ## Stop and remove the local Snowflake
	$(COMPOSE) down -v --remove-orphans

logs: ## Tail the emulator log (init hook results included)
	$(COMPOSE) logs --tail=200 snowflake

# The targets the repository CI runs for every sample (see the root README)
install: ## Nothing to install up front: make up and make test fetch the dependencies
	@echo "Nothing to install, make up and make test fetch the dependencies"

start: up ## Same as make up

stop: down ## Same as make down
