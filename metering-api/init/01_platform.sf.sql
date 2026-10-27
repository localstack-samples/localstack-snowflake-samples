-- Account objects for the metering API: what a Snowflake administrator runs once
-- per environment (in production usually Terraform or a platform repository).
--
-- The local emulator runs every init/*.sf.sql file at startup. The integration
-- tests run this file over the driver, then apply 02 and 03 to a fresh database
-- per test. Keep it to plain statements, each ending with ';' at the end of a line.

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS METERING_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- The service's role. It owns the METERING database and everything in it.
CREATE ROLE IF NOT EXISTS METERING_APP;
GRANT ROLE METERING_APP TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE METERING_WH TO ROLE METERING_APP;

CREATE DATABASE IF NOT EXISTS METERING;
GRANT OWNERSHIP ON DATABASE METERING TO ROLE METERING_APP COPY CURRENT GRANTS;

-- Service user with key-pair auth only. On a real account, register the key with:
--   ALTER USER METERING_SVC SET RSA_PUBLIC_KEY = '<.secrets/rsa_key.pub without the PEM lines>';
CREATE USER IF NOT EXISTS METERING_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = METERING_APP
  DEFAULT_WAREHOUSE = METERING_WH
  COMMENT = 'Metering API service user (key-pair auth)';
GRANT ROLE METERING_APP TO USER METERING_SVC;
