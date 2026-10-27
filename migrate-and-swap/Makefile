# Migrate and Swap: blue/green schema releases against the Snowflake-Next emulator.
# Run `make help` for the targets.

-include $(HOME)/.localstack/default.env
export LOCALSTACK_AUTH_TOKEN
SNOWFLAKE_PORT ?= 4566
export SNOWFLAKE_PORT
COMPOSE := docker compose -p snfs-migrate-and-swap
UV_RUN := uv run --locked
PYTEST_ARGS ?=

# Optional local overrides (copy .env.example to .env).
-include .env
PYTEST_WORKERS ?= 4
export TEST_TARGET SNOWFLAKE_IMAGE INVENTORY_DATABASE INVENTORY_SCHEMA KEEP_TEST_DB PYTEST_WORKERS \
       SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_ROLE SNOWFLAKE_WAREHOUSE SNOWFLAKE_PRIVATE_KEY_PATH

.DEFAULT_GOAL := help
.PHONY: help keys up seed test demo release rollback status down test-cloud install start stop logs

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-11s %s\n", $$1, $$2}'

keys: .secrets/rsa_key.p8 ## Generate the throwaway key pair used for SNOWFLAKE_JWT auth

# Throwaway, unencrypted dev key pair for local key-pair (SNOWFLAKE_JWT) auth.
.secrets/rsa_key.p8:
	@mkdir -p .secrets && chmod 700 .secrets
	@openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out .secrets/rsa_key.p8 2>/dev/null
	@openssl rsa -in .secrets/rsa_key.p8 -pubout -out .secrets/rsa_key.pub 2>/dev/null
	@chmod 600 .secrets/rsa_key.p8
	@echo "Generated .secrets/rsa_key.p8 (throwaway, for local use)"

up: keys ## Pull and start the emulator, install the Python env, wait until SQL answers
	$(COMPOSE) pull -q
	$(COMPOSE) up -d
	uv sync --locked --quiet
	./scripts/wait-for-emulator.sh 180

seed: ## (Re)create RETAIL with live INVENTORY at V1.0.0 and the synthetic data set
	$(UV_RUN) python scripts/seed.py

test: ## Run the suite against the running emulator (each worker builds its own databases)
	$(UV_RUN) pytest -n $(PYTEST_WORKERS) $(PYTEST_ARGS)

demo: seed ## Bad release blocked, good release swapped in, rollback, recovery drills
	$(UV_RUN) python -m release.demo

release: ## Release migrations/ to live: clone, migrate green, validate, swap
	$(UV_RUN) python -m release release

rollback: ## Swap the previous version back in
	$(UV_RUN) python -m release rollback

status: ## Show the version of live and green
	$(UV_RUN) python -m release status

down: ## Stop the emulator and remove its volumes
	$(COMPOSE) down -v --remove-orphans

test-cloud: ## Run the same suite against real Snowflake (billable; needs SNOWFLAKE_* env)
	TEST_TARGET=SNOWFLAKE_CLOUD $(UV_RUN) pytest -n $(PYTEST_WORKERS) $(PYTEST_ARGS)

# The targets the repository CI runs for every sample (see the root README)
install: ## Nothing to install up front: make up and make test fetch the dependencies
	@echo "Nothing to install, make up and make test fetch the dependencies"

start: up ## Same as make up

stop: down ## Same as make down

logs: ## Tail the emulator log (init hook results included)
	$(COMPOSE) logs --tail=200 snowflake
