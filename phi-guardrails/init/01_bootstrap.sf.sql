-- Runs once when the emulator starts, in account SNOWFLAKE as role PUBLIC.
-- Everything else (roles, schema, tags, policies) is owned by the schemachange
-- migrations in migrations/, so this file only provides what a fresh account
-- needs before the first deploy: a warehouse and the deploy service user.
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS PHI_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE USER IF NOT EXISTS SAMPLE_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = ACCOUNTADMIN
  DEFAULT_WAREHOUSE = PHI_WH
  COMMENT = 'schemachange deploy user for the PHI Guardrails sample';
GRANT ROLE ACCOUNTADMIN TO USER SAMPLE_SVC;
