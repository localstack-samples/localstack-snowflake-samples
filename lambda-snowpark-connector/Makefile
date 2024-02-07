install:
	pip3 uninstall \
		--platform manylinux2010_x86_64 \
		--implementation cp \
		--only-binary=:all: --upgrade \
		--target . \
 		snowflake-connector-python==2.7.9 boto3==1.26.153 botocore==1.29.153
		
zip:
	zip -r function-py.zip .

create-function:
	awslocal lambda create-function \
	--function-name localstack-snowflake-lambda-example \
	--runtime python3.10 \
	--timeout 180 \
	--zip-file fileb://function-py.zip \
	--handler handler.lambda_handler \
	--role arn:aws:iam::000000000000:role/lambda-role

local-invoke:
	awslocal lambda invoke --function-name localstack-snowflake-lambda-example \
	--cli-binary-format raw-in-base64-out \
	--payload '{"body": "test" }' output.txt
