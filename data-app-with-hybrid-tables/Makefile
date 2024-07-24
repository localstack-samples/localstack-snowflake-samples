VENV_BIN = python3 -m venv
VENV_DIR ?= .venv
VENV_ACTIVATE = $(VENV_DIR)/bin/activate
VENV_RUN = . $(VENV_ACTIVATE)

usage:		## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:    ## Install dependencies
	test -d .venv || $(VENV_BIN) .venv
	$(VENV_RUN); pip install -r requirements.txt

start:      ## Start the flask application
	$(VENV_RUN); flask --app app/app run

seed:       ## Seed test data into LocalStack Snowflake
	snow sql -c local -f sql/seeds.sql

.PHONY: usage install start init
