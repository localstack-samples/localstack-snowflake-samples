-- Account-level provisioning: what a Snowflake administrator runs once per
-- environment (often from Terraform or a platform repository).
--
-- The emulator runs every init/*.sf.sql file at startup (docker compose mounts
-- this directory as its init hooks). The integration tests run this file on
-- their own emulator, then the database-scoped files 02 to 05 once per test
-- class in a fresh database. Keep one statement per line ending in ';'.

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS FLEET_WH
    WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

-- The service role owns the telemetry schema. Granting it to SYSADMIN keeps it
-- inside the standard role hierarchy.
CREATE ROLE IF NOT EXISTS FLEET_APP;
GRANT ROLE FLEET_APP TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE FLEET_WH TO ROLE FLEET_APP;
-- Development and CI only: the integration tests create one database per test class.
GRANT CREATE DATABASE ON ACCOUNT TO ROLE FLEET_APP;

CREATE DATABASE IF NOT EXISTS FLEET;
CREATE SCHEMA IF NOT EXISTS FLEET.TELEMETRY;
GRANT OWNERSHIP ON DATABASE FLEET TO ROLE FLEET_APP COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA FLEET.TELEMETRY TO ROLE FLEET_APP COPY CURRENT GRANTS;

-- Service user for the API and the ingest tool, key-pair authentication only.
-- On a Snowflake account, register the public key that `make up` writes:
--   ALTER USER FLEET_SVC SET RSA_PUBLIC_KEY = '<.secrets/rsa_key.pub without the PEM header lines>';
CREATE USER IF NOT EXISTS FLEET_SVC TYPE = SERVICE DEFAULT_ROLE = FLEET_APP DEFAULT_WAREHOUSE = FLEET_WH;
GRANT ROLE FLEET_APP TO USER FLEET_SVC;
