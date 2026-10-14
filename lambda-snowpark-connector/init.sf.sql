-- Run by the Snowflake emulator at startup (SNOWFLAKE_INIT_ACCOUNT=TEST), so the
-- database and schema the Lambda function connects to already exist.
CREATE DATABASE IF NOT EXISTS test;
CREATE SCHEMA IF NOT EXISTS test.test_schema;
