FUNCTION_NAME = localstack-snowflake-lambda-example
# where the function reaches the Snowflake emulator: its service name on the
# lambda-snowpark Docker network, over plain http
SNOWFLAKE_ENV = Variables={SNOWFLAKE_HOST=snowflake,SNOWFLAKE_PORT=4566,SNOWFLAKE_PROTOCOL=http}

usage:                    ## Show this help
	@grep -Fh "##" $(MAKEFILE_LIST) | grep -Fv fgrep | sed -e 's/:.*##\s*/##/g' | awk -F'##' '{ printf "%-25s %s\n", $$1, $$2 }'

install:     ## Install dependencies for the Lambda runtime (Python 3.12, x86_64)
	pip3 install \
		--platform manylinux2014_x86_64 \
		--python-version 3.12 \
		--implementation cp \
		--only-binary=:all: --upgrade \
		--target ./libs \
		snowflake-connector-python==3.12.4

zip:        ## Create the Lambda zip archive
	rm -rf build && mkdir -p build
	cp -r libs/* build/
	cp handler.py build/
	(cd build && zip -q -r function-py.zip .)

create-function:  ## Create the Lambda function in LocalStack
	awslocal lambda create-function \
	--function-name $(FUNCTION_NAME) \
	--runtime python3.12 \
	--timeout 180 \
	--zip-file fileb://build/function-py.zip \
	--handler handler.lambda_handler \
	--environment '$(SNOWFLAKE_ENV)' \
	--role arn:aws:iam::000000000000:role/lambda-role
	awslocal lambda wait function-active-v2 --function-name $(FUNCTION_NAME)

local-invoke:   ## Invoke the Lambda locally
	echo '{"body": "test"}' > build/event.json
	awslocal lambda invoke --function-name $(FUNCTION_NAME) \
	--cli-read-timeout 300 \
	--payload fileb://build/event.json output.txt

test:          ## Deploy and invoke the function, then check its results
	./test.sh

start-localstack: ## Start LocalStack for AWS and the Snowflake emulator, and wait until both are healthy
	SF_LOG=trace docker compose up -d --wait

logs:          ## Show the logs of both containers
	docker compose logs --tail 300

stop:          ## Stop and remove both containers
	docker compose down

.PHONY: usage install zip create-function local-invoke test start-localstack logs stop
