usage:			## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:		## Install tflocal, awslocal and the Snowflake CLI
	which tflocal || pip install terraform-local
	which awslocal || pip install awscli-local
	which snow || pip install snowflake-cli

start:			## Start LocalStack AWS and the Snowflake emulator, with init.sf.sql mounted
	docker compose up -d --wait

stop:			## Stop and remove both containers
	docker compose down

logs:			## Show the logs of both containers
	docker logs --tail 300 glue-localstack-aws
	docker logs --tail 150 glue-snowflake

deploy:			## Deploy and run the Glue job
	./deploy.sh

test:			## Run the Glue job, then check it read from and wrote to Snowflake
	./test.sh

.PHONY: usage install start stop logs deploy test
