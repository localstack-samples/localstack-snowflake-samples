FUNCTION_NAME = localstack-snowflake-lambda-example

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
	--role arn:aws:iam::000000000000:role/lambda-role
	awslocal lambda wait function-active-v2 --function-name $(FUNCTION_NAME)

local-invoke:   ## Invoke the Lambda locally
	echo '{"body": "test"}' > build/event.json
	awslocal lambda invoke --function-name $(FUNCTION_NAME) \
	--cli-read-timeout 300 \
	--payload fileb://build/event.json output.txt

test:          ## Deploy and invoke the function, then check its results
	./test.sh

start-localstack: ## Start LocalStack for Snowflake
	LOCALSTACK_DEBUG=1 LOCALSTACK_SF_LOG=trace LOCALSTACK_LAMBDA_RUNTIME_ENVIRONMENT_TIMEOUT=180 \
		lstk start --type snowflake --non-interactive --timeout 3m

logs:          ## Show the LocalStack logs
	lstk logs --tail 300

stop:          ## Stop LocalStack
	lstk stop

.PHONY: usage install zip create-function local-invoke test start-localstack logs stop
