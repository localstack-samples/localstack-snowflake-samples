usage:                    ## Show this help
	@grep -Fh "##" $(MAKEFILE_LIST) | grep -Fv fgrep | sed -e 's/:.*##\s*/##/g' | awk -F'##' '{ printf "%-25s %s\n", $$1, $$2 }'

install:     ## Install dependencies
	pip3 install \
		--platform manylinux2010_x86_64 \
		--implementation cp \
		--only-binary=:all: --upgrade \
		--target ./libs \
 		snowflake-connector-python==2.7.9 boto3==1.26.153 botocore==1.29.153

zip:        ## Create the Lambda zip archive
	mkdir -p build
	cp -r libs/* build/
	(cd build && zip -q -r function-py.zip .)

create-function:  ## Create the Lambda function in LocalStack
	awslocal lambda create-function \
	--function-name localstack-snowflake-lambda-example \
	--runtime python3.10 \
	--timeout 180 \
	--zip-file fileb://build/function-py.zip \
	--handler handler.lambda_handler \
	--role arn:aws:iam::000000000000:role/lambda-role

local-invoke:   ## Invoke the Lambda locally
	awslocal lambda invoke --function-name localstack-snowflake-lambda-example \
	--payload '{"body": "test" }' output.txt

start-localstack:
	DOCKER_FLAGS="-e DEBUG=1 -e SF_LOG=trace" LAMBDA_RUNTIME_ENVIRONMENT_TIMEOUT=180 localstack start -d

ready:         ## Check if the LocalStack container is up and running.
	localstack wait -t 20 && echo "LocalStack is ready to use!"

stop:          ## Stop LocalStack infrastructure
	localstack stop
