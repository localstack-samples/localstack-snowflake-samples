#### IAM Setup
# Assume policy
data "aws_iam_policy_document" "glue_assume_policy_document" {
  statement {
    sid     = "GlueAssumeRole"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
  }
}

# Execution policy
data "aws_iam_policy_document" "glue_execution_policy_document" {
  statement {
    sid = "GlueExecutionPolicy"
    actions = [
      "s3:*",
      "cloudwatch:*",
      "logs:*",
      "secretsmanager:*",
      "glue:*",
    ]

    resources = ["*"]
  }
}

# Policy
resource "aws_iam_policy" "glue_execution_policy" {
  name        = "glue-execution-policy"
  description = "Policy for Glue Execution Role"
  policy      = data.aws_iam_policy_document.glue_execution_policy_document.json
}

# Role
resource "aws_iam_role" "glue_execution_role" {
  name               = "glue-execution-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_policy_document.json
}

# Role policy attachment
resource "aws_iam_role_policy_attachment" "glue_execution_policy" {
  role       = aws_iam_role.glue_execution_role.name
  policy_arn = aws_iam_policy.glue_execution_policy.arn
}


# S3 Bucket for Glue Assets
resource "aws_s3_bucket" "glue_assets" {
  bucket_prefix = "glue-assets"
}

# Glue job
resource "aws_glue_job" "glue_job" {
  name         = "glue-job"
  role_arn     = aws_iam_role.glue_execution_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.glue_assets.bucket}/script/job.py"
  }
  default_arguments = {
    "--class"                            = "GlueApp"
    "--enable-continuous-cloudwatch-log" = "true"
    "--extra-jars"                       = "s3://${aws_s3_bucket.glue_assets.bucket}/jars/snowflake-jdbc-3.20.0.jar,s3://${aws_s3_bucket.glue_assets.bucket}/jars/spark-snowflake_2.12-2.5.4-spark_2.4.jar"
  }
}


### outputs
output "bucket_name" {
  value = aws_s3_bucket.glue_assets.bucket
}

output "job_name" {
  value = aws_glue_job.glue_job.name
}
