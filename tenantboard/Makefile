# Tenantboard: a multi-tenant analytics API whose tenant isolation and PII
# masking are enforced by Snowflake policies. Run `make help` for the targets.

-include $(HOME)/.localstack/default.env
export LOCALSTACK_AUTH_TOKEN
SNOWFLAKE_PORT ?= 4566
export SNOWFLAKE_PORT
COMPOSE := docker compose -p snfs-tenantboard

SNOWFLAKE_PRIVATE_KEY_PATH ?= $(CURDIR)/.secrets/rsa_key.p8
export SNOWFLAKE_PRIVATE_KEY_PATH

UV_RUN := uv run --frozen
# At most 4 workers: each one clones its own database from the template.
PYTEST_ARGS ?= -n 4
API_PORT ?= 8000
WEB_PORT ?= 5173
export API_PORT WEB_PORT

.DEFAULT_GOAL := help
.PHONY: help keys up seed test demo run web web-build down test-cloud logs install start stop

help: ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(firstword $(MAKEFILE_LIST)) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

keys: .secrets/rsa_key.p8 ## Generate the throwaway key pair used for SNOWFLAKE_JWT auth

.secrets/rsa_key.p8:
	@mkdir -p .secrets && chmod 700 .secrets
	@openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out .secrets/rsa_key.p8 2>/dev/null
	@openssl rsa -in .secrets/rsa_key.p8 -pubout -out .secrets/rsa_key.pub 2>/dev/null
	@chmod 600 .secrets/rsa_key.p8
	@echo "Generated .secrets/rsa_key.p8 (throwaway, for local development)"

up: keys ## Start the emulator, install the Python env, wait until SQL answers
	$(COMPOSE) up -d
	uv sync --frozen --quiet
	./scripts/wait-for-emulator.sh 180
	$(UV_RUN) python -m scripts.wait_for_init

seed: ## Load the synthetic merchants, customers and orders into TENANTBOARD (idempotent)
	$(UV_RUN) python -m scripts.seed

test: ## Run the suite against the running emulator (each worker clones its own database)
	$(UV_RUN) pytest $(PYTEST_ARGS)

demo: ## Headline demo: a raw SELECT * FROM orders as tenant ACME, with and without the policy
	$(UV_RUN) python -m scripts.demo

web/node_modules: web/package-lock.json
	cd web && npm ci --no-audit --no-fund
	@touch $@

web: web/node_modules ## Install the dashboard's npm dependencies

web-build: web ## Type-check and build the dashboard into web/dist
	cd web && npm run build

run: web ## Serve the API on :8000 and the dashboard on :5173 (Ctrl-C stops both)
	./scripts/run.sh

down: ## Stop the emulator and remove its volumes
	$(COMPOSE) down -v --remove-orphans

test-cloud: ## Run the same suite against real Snowflake (billable; needs SNOWFLAKE_* env)
	TEST_TARGET=SNOWFLAKE_CLOUD $(UV_RUN) pytest $(PYTEST_ARGS)

logs: ## Tail the emulator log (init hook results included)
	$(COMPOSE) logs --tail=200 snowflake

# The targets the repository CI runs for every sample (see the root README)
install: ## Nothing to install up front: make up and make test fetch the dependencies
	@echo "Nothing to install, make up and make test fetch the dependencies"

start: up ## Same as make up

stop: down ## Same as make down
