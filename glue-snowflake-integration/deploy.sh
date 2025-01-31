#!/bin/bash

# Root Directory
ROOT_DIR=$(pwd)

echo "🚀 Initializing Terraform..."
cd tf
tflocal init > /dev/null 2>&1
tflocal apply --auto-approve > /dev/null 2>&1

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

wget -q --show-progress "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.20.0/snowflake-jdbc-3.20.0.jar" -P jars
wget -q --show-progress "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.5.4-spark_2.4/spark-snowflake_2.12-2.5.4-spark_2.4.jar" -P jars

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

# Check Job Status
JOB_STATUS=$(awslocal glue get-job-run --job-name $job_name --run-id $job_run_id --query 'JobRun.JobRunState' --output text)

while [[ "$JOB_STATUS" != "SUCCEEDED" && "$JOB_STATUS" != "FAILED" && "$JOB_STATUS" != "STOPPED" ]]; do
  echo "⏳ Job is still running..."
  sleep 2
  JOB_STATUS=$(awslocal glue get-job-run --job-name $job_name --run-id $job_run_id --query 'JobRun.JobRunState' --output text)
done

# Final Status
if [[ "$JOB_STATUS" == "SUCCEEDED" ]]; then
  echo "🎉 Job finished successfully!"
elif [[ "$JOB_STATUS" == "FAILED" ]]; then
  echo "❌ Job failed!"
else
  echo "⏹️ Job stopped!"
fi
