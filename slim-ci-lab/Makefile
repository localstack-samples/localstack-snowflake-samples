# Slim CI Lab: dbt Slim CI against the Snowflake-Next emulator.
# Run `make help` for the targets.

-include $(HOME)/.localstack/default.env
export LOCALSTACK_AUTH_TOKEN
SNOWFLAKE_PORT ?= 4566
export SNOWFLAKE_PORT
COMPOSE := docker compose -p snfs-slim-ci-lab

SNOWFLAKE_PRIVATE_KEY_PATH ?= $(CURDIR)/.secrets/rsa_key.p8
export SNOWFLAKE_PRIVATE_KEY_PATH
export DBT_PROFILES_DIR := $(CURDIR)/dbt
export DBT_PROJECT_DIR := $(CURDIR)/dbt

UV_RUN := uv run --frozen
PYTEST_ARGS ?=

.DEFAULT_GOAL := help
.PHONY: help keys up seed test demo demo-bad-pr ci-local down test-cloud install start stop logs

help: ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(firstword $(MAKEFILE_LIST)) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

keys: .secrets/rsa_key.p8 ## Generate the throwaway key pair used for SNOWFLAKE_JWT auth

.secrets/rsa_key.p8:
	@mkdir -p .secrets && chmod 700 .secrets
	@openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out .secrets/rsa_key.p8 2>/dev/null
	@openssl rsa -in .secrets/rsa_key.p8 -pubout -out .secrets/rsa_key.pub 2>/dev/null
	@chmod 600 .secrets/rsa_key.p8
	@echo "Generated .secrets/rsa_key.p8 (throwaway, for local runs)"

up: keys ## Start the emulator, install the Python env, wait until SQL answers
	$(COMPOSE) up -d
	uv sync --frozen --quiet
	./scripts/wait-for-emulator.sh 180

seed: ## Download, verify and downsample the TLC month, then PUT + COPY it into RAW.TRIPS
	$(UV_RUN) slim-ci-load

test: ## Run the test suite against the running emulator (builds its own database)
	$(UV_RUN) pytest $(PYTEST_ARGS)

demo: demo-bad-pr ## Headline demo: the bad-PR second-run MERGE failure

demo-bad-pr: ## Apply demos/bad-unique-key.patch and show the second run fail with 100090
	$(UV_RUN) slim-ci-demo-bad-pr

ci-local: ## Reproduce the GitHub Actions Slim CI pipeline locally
	./scripts/ci_local.sh

down: ## Stop the emulator and remove its volumes
	$(COMPOSE) down -v --remove-orphans

test-cloud: ## Run the same suite against real Snowflake (billable; needs SNOWFLAKE_* env)
	TEST_TARGET=SNOWFLAKE_CLOUD $(UV_RUN) pytest $(PYTEST_ARGS)

# The targets the repository CI runs for every sample (see the root README)
install: ## Nothing to install up front: make up and make test fetch the dependencies
	@echo "Nothing to install, make up and make test fetch the dependencies"

start: up ## Same as make up

stop: down ## Same as make down

logs: ## Tail the emulator log (init hook results included)
	$(COMPOSE) logs --tail=200 snowflake
