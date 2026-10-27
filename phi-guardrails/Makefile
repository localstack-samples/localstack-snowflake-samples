# PHI Guardrails: governance as code against the Snowflake-Next emulator.
# Targets: up, migrate, seed, test, demo, gate, down, test-cloud (see README.md).

-include $(HOME)/.localstack/default.env
export LOCALSTACK_AUTH_TOKEN
SNOWFLAKE_PORT ?= 4566
export SNOWFLAKE_PORT
COMPOSE := docker compose -p snfs-phi-guardrails
UV_RUN := uv run --locked

# Optional local overrides (copy .env.example to .env).
-include .env
export TEST_TARGET SNOWFLAKE_IMAGE PHI_DATABASE KEEP_TEST_DB \
       SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_ROLE SNOWFLAKE_WAREHOUSE SNOWFLAKE_PRIVATE_KEY_PATH

.PHONY: up migrate seed test demo gate down test-cloud install start stop logs

up: .secrets/rsa_key.p8
	$(COMPOSE) pull -q
	$(COMPOSE) up -d
	./scripts/wait-for-emulator.sh 180

# Throwaway, unencrypted key pair for local runs; never reuse it elsewhere.
.secrets/rsa_key.p8:
	@mkdir -p .secrets
	openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out .secrets/rsa_key.p8
	openssl rsa -in .secrets/rsa_key.p8 -pubout -out .secrets/rsa_key.pub

migrate:  ## apply pending schemachange migrations to $(PHI_DATABASE) (default PHI)
	$(UV_RUN) python -m phi_guardrails.deploy

seed: migrate  ## download (once), verify and load the Synthea extract; idempotent
	$(UV_RUN) python scripts/load_synthea.py

test:  ## the full suite; deploys and drops its own database
	$(UV_RUN) pytest

demo: seed  ## the access matrix, a new PHI column, and a caught regression
	$(UV_RUN) python -m phi_guardrails.demo

gate: migrate  ## the policy-coverage gate on $(PHI_DATABASE); exits 1 on an unprotected column
	$(UV_RUN) python -m phi_guardrails.coverage

down:
	$(COMPOSE) down -v --remove-orphans

test-cloud:  ## same suite against a real account (consumes credits)
	TEST_TARGET=SNOWFLAKE_CLOUD $(UV_RUN) pytest

# The targets the repository CI runs for every sample (see the root README)
install: ## Nothing to install up front: make up and make test fetch the dependencies
	@echo "Nothing to install, make up and make test fetch the dependencies"

start: up  ## Same as make up

stop: down  ## Same as make down

logs: ## Tail the emulator log (init hook results included)
	$(COMPOSE) logs --tail=200 snowflake
