usage:		## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:	## Nothing to install, the sample only needs Docker
	@echo "Nothing to install"

start:		## Start both containers and wait until they are healthy
	docker compose up -d --wait

stop:		## Stop and remove both containers
	docker compose down

logs:		## Show the logs of both containers
	docker compose ps
	docker logs --tail 150 localstack-aws
	docker logs --tail 150 localstack-snowflake

test:		## Check the containers can reach each other and load data from S3 into Snowflake
	./test.sh

.PHONY: usage install start stop logs test
