VENV_BIN = python3 -m venv
VENV_DIR ?= .venv
VENV_ACTIVATE = $(VENV_DIR)/bin/activate
VENV_RUN = . $(VENV_ACTIVATE)

usage:		## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

venv:
	test -d $(VENV_DIR) || $(VENV_BIN) $(VENV_DIR)

install: venv   ## Install dependencies
	$(VENV_RUN); which awslocal || pip install 'awscli-local[ver1]'
	$(VENV_RUN); which soda || pip install soda-core-snowflake
	$(VENV_RUN); which snow || pip install snowflake-cli

start:     ## Start LocalStack for Snowflake
	LOCALSTACK_SF_LOG=trace lstk start --type snowflake --non-interactive --timeout 3m

logs:      ## Show the LocalStack logs
	lstk logs --tail 300

init:      ## Initialize the environment and seed test data into local Snowflake
	$(VENV_RUN) && \
	    soda test-connection -d local_snowflake -c configuration.yml && \
	    snow sql -c localstack --query "CREATE OR REPLACE TABLE test_table (id INT, name TEXT)" && \
	    snow sql -c localstack --query "INSERT INTO test_table (id, name) VALUES (1, 'Alice')" && \
	    snow sql -c localstack --query "INSERT INTO test_table (id, name) VALUES (2, 'Bob')"

scan:      ## Scan for any data quality issues
	$(VENV_RUN); soda scan --verbose -d local_snowflake -c configuration.yml checks.yml

test:      ## Seed the table, run the scan and check its results
	./test.sh

.PHONY: usage install venv start logs init scan test
