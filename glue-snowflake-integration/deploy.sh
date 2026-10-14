#!/bin/bash
set -euo pipefail

# Root Directory
ROOT_DIR=$(pwd)

echo "🚀 Initializing Terraform..."
cd tf
tflocal init -input=false > /dev/null
tflocal apply -input=false --auto-approve > /dev/null

# Get Outputs
bucket_name=$(tflocal output -raw bucket_name)
job_name=$(tflocal output -raw job_name)

cd $ROOT_DIR

echo "✅ Terraform setup complete."
echo "📂 Bucket Name: $bucket_name"
echo "🛠️ Job Name: $job_name"

# Prepare Jars
echo "📥 Downloading dependencies..."
rm -rf jars && mkdir jars

# Glue 4.0 runs Spark 3.3, so use the connector build for Spark 3.3
wget -q "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.20.0/snowflake-jdbc-3.20.0.jar" -P jars
wget -q "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.16.0-spark_3.3/spark-snowflake_2.12-2.16.0-spark_3.3.jar" -P jars

# Upload jars to S3
echo "☁️ Uploading JARs to S3..."
awslocal s3 cp jars s3://$bucket_name/jars/ --recursive > /dev/null 2>&1
echo "✅ JARs uploaded successfully."

# Upload script to S3
echo "☁️ Uploading script to S3..."
awslocal s3 cp script/job.py s3://$bucket_name/script/job.py > /dev/null 2>&1
echo "✅ Script uploaded successfully."

# Start Glue Job
echo "🚀 Starting AWS Glue job..."
job_run_id=$(awslocal glue start-job-run --job-name $job_name --output text --query 'JobRunId')
echo "🆔 Job Run ID: $job_run_id"
echo "$job_run_id" > .last-job-run-id
# Check Job Status
JOB_STATUS=$(awslocal glue get-job-run --job-name $job_name --run-id $job_run_id --query 'JobRun.JobRunState' --output text)

while [[ "$JOB_STATUS" != "SUCCEEDED" && "$JOB_STATUS" != "FAILED" && "$JOB_STATUS" != "STOPPED" ]]; do
  echo "⏳ Job is still running..."
  sleep 5
  JOB_STATUS=$(awslocal glue get-job-run --job-name $job_name --run-id $job_run_id --query 'JobRun.JobRunState' --output text)
done

# Final Status
if [[ "$JOB_STATUS" == "SUCCEEDED" ]]; then
  echo "🎉 Job finished successfully!"
elif [[ "$JOB_STATUS" == "FAILED" ]]; then
  echo "❌ Job failed!"
  exit 1
else
  echo "⏹️ Job stopped!"
  exit 1
fi
