usage:		## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:    ## Install dependencies
	which awslocal || pip install 'awscli-local[ver1]'
	which soda || pip install soda-core-snowflake

init:      ## Initialize the environment and seed test data into local Snowflake
	soda test-connection -d local_snowflake -c configuration.yml
	snow sql -c local --query "CREATE TABLE test_table (id INT, name TEXT)"
	snow sql -c local --query "INSERT INTO test_table (id, name) VALUES (1, 'Alice')"
	snow sql -c local --query "INSERT INTO test_table (id, name) VALUES (2, 'Bob')"

scan:      ## Scan for any data quality issues
	soda scan --verbose -d local_snowflake -c configuration.yml checks.yml

.PHONY: usage install init scan
