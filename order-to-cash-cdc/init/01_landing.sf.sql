-- Order-to-Cash CDC: account objects and the Kafka landing zone.
--
-- Take ACCOUNTADMIN to create the warehouse, the ingest role and user, and
-- the database the pipeline uses.
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS O2C_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  INITIALLY_SUSPENDED = TRUE;

-- Least-privilege role for the Kafka connector's service user. On real
-- Snowflake, register the public key with:
--   ALTER USER SAMPLE_SVC SET RSA_PUBLIC_KEY = '<contents of .secrets/rsa_key.pub>';
CREATE ROLE IF NOT EXISTS O2C_INGEST;
CREATE USER IF NOT EXISTS SAMPLE_SVC
  DEFAULT_ROLE = O2C_INGEST
  DEFAULT_WAREHOUSE = O2C_WH
  COMMENT = 'Kafka connector service user (key-pair auth)';
GRANT ROLE O2C_INGEST TO USER SAMPLE_SVC;

CREATE DATABASE IF NOT EXISTS O2C;
USE DATABASE O2C;
GRANT USAGE ON DATABASE O2C TO ROLE O2C_INGEST;

-- @deploy: everything below this line is database-relative. The test suite
-- and `make demo` run it inside a fresh database per run.

CREATE SCHEMA IF NOT EXISTS RAW;
GRANT USAGE ON SCHEMA RAW TO ROLE O2C_INGEST;

-- Landing table written by the Snowflake Kafka connector (Snowpipe Streaming,
-- non-schematized). One row per Kafka record: RECORD_METADATA carries topic,
-- partition, offset and key; RECORD_CONTENT is the Debezium change envelope.
CREATE TABLE IF NOT EXISTS RAW.ORDERS_CDC (
  RECORD_METADATA VARIANT,
  RECORD_CONTENT  VARIANT
)
COMMENT = 'Debezium-style change events for TPC-H orders and customer, landed by the Kafka connector';
GRANT SELECT, INSERT ON TABLE RAW.ORDERS_CDC TO ROLE O2C_INGEST;

-- Standard (delta) stream of unprocessed change events. OPS.APPLY_ORDERS_CDC
-- consumes it inside one transaction, so the offset advances only when the
-- merge commits.
CREATE STREAM IF NOT EXISTS RAW.ORDERS_CDC_STREAM ON TABLE RAW.ORDERS_CDC;
