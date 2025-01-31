usage:			## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'


start:			## Start the localstack
	DOCKER_FLAGS='-v ./init.sf.sql:/etc/localstack/init/ready.d/init.sf.sql' IMAGE_NAME=localstack/snowflake localstack start;

stop:			## Stop the localstack
	localstack stop

deploy:			## Deploy and Test the job
	./deploy.sh
