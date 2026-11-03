-- Platform provisioning for the claims service: what a Snowflake administrator runs
-- once per environment (in production usually Terraform or a platform repo).
-- The service itself only migrates objects inside CLAIMS.CORE with Liquibase.
--
-- Used in two places:
--   * docker-compose.yml mounts init/ as the emulator's init hooks (make up);
--   * the Testcontainers suite runs init/*.sf.sql over JDBC after the container starts.
-- Keep it to plain statements, each ending with ';' at the end of a line.

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS CLAIMS_WH
    WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

CREATE ROLE IF NOT EXISTS CLAIMS_APP;
GRANT USAGE ON WAREHOUSE CLAIMS_WH TO ROLE CLAIMS_APP;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE CLAIMS_APP;
-- Custom roles roll up to SYSADMIN, so administrators can manage what CLAIMS_APP owns.
GRANT ROLE CLAIMS_APP TO ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS CLAIMS;
CREATE SCHEMA IF NOT EXISTS CLAIMS.CORE;
GRANT USAGE ON DATABASE CLAIMS TO ROLE CLAIMS_APP;
GRANT OWNERSHIP ON SCHEMA CLAIMS.CORE TO ROLE CLAIMS_APP COPY CURRENT GRANTS;

-- Service user with key-pair auth only. On a real account also run:
--   ALTER USER SAMPLE_SVC SET RSA_PUBLIC_KEY = '<contents of rsa_key.pub without the PEM lines>';
CREATE USER IF NOT EXISTS SAMPLE_SVC TYPE = SERVICE DEFAULT_ROLE = CLAIMS_APP DEFAULT_WAREHOUSE = CLAIMS_WH;
GRANT ROLE CLAIMS_APP TO USER SAMPLE_SVC;
