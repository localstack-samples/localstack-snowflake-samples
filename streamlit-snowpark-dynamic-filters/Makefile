usage:		## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:    ## Nothing to install on the host, the test runs the app in a python:3.8 container
	@echo "Nothing to install"

start-localstack: ## Start the LocalStack Snowflake Next emulator
	lstk start --type snowflake --image localstack/snowflake-next:latest --non-interactive --timeout 3m

logs:       ## Show the LocalStack logs
	lstk logs --tail 300

init:       ## Initialize the test data
	snow sql -c local -f .assets/setup.sql

start:      ## Start up streamlit app
	streamlit run main.py

test:       ## Load the data, start the app and check its filters
	./test.sh

.PHONY: usage install start-localstack logs init start test
