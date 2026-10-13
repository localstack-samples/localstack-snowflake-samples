usage:			## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:		## Install tflocal and awslocal
	which tflocal || pip install terraform-local
	which awslocal || pip install awscli-local

start:			## Start LocalStack for Snowflake with init.sf.sql mounted (see .lstk/config.toml)
	lstk start --non-interactive --timeout 3m

stop:			## Stop LocalStack
	lstk stop

logs:			## Show the LocalStack logs
	lstk logs --tail 300

deploy:			## Deploy and run the Glue job
	./deploy.sh

test:			## Deploy and run the Glue job, then check it reached Snowflake
	./test.sh

.PHONY: usage install start stop logs deploy test
