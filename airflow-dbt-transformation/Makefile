usage:		## Shows usage for this Makefile
	@cat Makefile | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:    ## Install dependencies
	which awslocal || pip install 'awscli-local[ver1]'

init:      ## Initialize the Airflow environment in LocalStack MWAA
	awslocal s3 mb s3://snowflake-airflow
	awslocal mwaa create-environment --dag-s3-path /dags \
		--execution-role-arn arn:aws:iam::000000000000:role/airflow-role \
		--network-configuration {} \
		--source-bucket-arn arn:aws:s3:::snowflake-airflow \
		--airflow-configuration-options agent.code=007,agent.name=bond \
		--name my-mwaa-env \
		--endpoint-url http://localhost.localstack.cloud:4566

deploy:     ## Deploy the DAG to the local Airflow instance
	awslocal s3 cp requirements.txt s3://snowflake-airflow/
	awslocal s3 cp packages.yml s3://snowflake-airflow/dags/
	awslocal s3 cp dbt_project.yml s3://snowflake-airflow/dags/
	awslocal s3 cp --recursive models s3://snowflake-airflow/dags/models
	awslocal s3 cp --recursive seeds s3://snowflake-airflow/dags/seeds
	awslocal s3 cp airflow_dag.py s3://snowflake-airflow/dags/

.PHONY: usage install deploy start
