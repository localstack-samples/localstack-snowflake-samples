-- Platform objects a data team gets from its Snowflake admins: a warehouse,
-- a TRANSFORMER role for dbt and the loader, a service user, and the
-- analytics database. Runs once at emulator startup (as role PUBLIC).
-- The loader creates RAW objects idempotently itself, so the same pipeline
-- also runs on a real account where this script is applied by an admin.
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS TRANSFORMING
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE ROLE IF NOT EXISTS TRANSFORMER;
GRANT ROLE TRANSFORMER TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE TRANSFORMING TO ROLE TRANSFORMER;
-- The test suite creates one throwaway database per run.
GRANT CREATE DATABASE ON ACCOUNT TO ROLE TRANSFORMER;

CREATE USER IF NOT EXISTS SAMPLE_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = TRANSFORMER
  DEFAULT_WAREHOUSE = TRANSFORMING;
GRANT ROLE TRANSFORMER TO USER SAMPLE_SVC;

CREATE DATABASE IF NOT EXISTS SLIM_CI;
GRANT OWNERSHIP ON DATABASE SLIM_CI TO ROLE TRANSFORMER COPY CURRENT GRANTS;
