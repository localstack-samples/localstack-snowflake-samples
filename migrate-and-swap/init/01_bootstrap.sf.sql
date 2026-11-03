-- Runs once when the emulator starts, in account SNOWFLAKE as role PUBLIC.
-- Everything inside the inventory database (schema, tables, views, grants) is
-- owned by the schemachange migrations in migrations/. This file only provides
-- what a fresh account needs before the first deploy: a warehouse and the
-- release service user, and the reader role the migrations grant to.
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS RELEASE_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE USER IF NOT EXISTS SAMPLE_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = ACCOUNTADMIN
  DEFAULT_WAREHOUSE = RELEASE_WH
  COMMENT = 'release service user for the Migrate and Swap sample';
GRANT ROLE ACCOUNTADMIN TO USER SAMPLE_SVC;

-- The account-level role the migrations grant to. V1.0.0 creates it too (IF
-- NOT EXISTS); creating it here means it exists before the first deploy runs.
CREATE ROLE IF NOT EXISTS INVENTORY_READER;
GRANT ROLE INVENTORY_READER TO ROLE SYSADMIN;
