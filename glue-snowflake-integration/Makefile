usage:			## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'


start:			## Start the localstack
	DEBUG=1 localstack start;

stop:			## Stop the localstack
	localstack stop

deploy:			## Deploy and Test the job
	./deploy.sh
