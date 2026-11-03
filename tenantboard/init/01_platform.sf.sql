-- Tenantboard: account objects (warehouse, shared roles, service user) and the
-- TENANTBOARD database.
--
-- Account-level objects are created as ACCOUNTADMIN.
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS TB_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  INITIALLY_SUSPENDED = TRUE;

-- Role model. Per-tenant roles are created when a tenant is provisioned
-- (app/provisioning.py), never by hand:
--   TENANT_<ID>         viewer role of one tenant; the row access policy keys on its name
--   TENANT_<ID>_ADMIN   that tenant's admin: inherits TENANT_<ID> and TENANT_ADMIN
--   TENANT_READER       shared read grants, inherited by every TENANT_<ID>
--   TENANT_ADMIN        shared write grants; the email masking policy checks it
--   PLATFORM_ADMIN      platform operators: every tenant's rows, emails still masked
CREATE ROLE IF NOT EXISTS TENANT_READER;
CREATE ROLE IF NOT EXISTS TENANT_ADMIN;
CREATE ROLE IF NOT EXISTS PLATFORM_ADMIN;
-- Custom roles roll up to SYSADMIN, Snowflake's recommended hierarchy.
GRANT ROLE PLATFORM_ADMIN TO ROLE SYSADMIN;

-- The API's service user. It holds PLATFORM_ADMIN (tenant directory only) and
-- each tenant's two roles, and opens one session per role it serves. On real
-- Snowflake, register the public key with:
--   ALTER USER TENANTBOARD_SVC SET RSA_PUBLIC_KEY = '<contents of .secrets/rsa_key.pub>';
CREATE USER IF NOT EXISTS TENANTBOARD_SVC
  DEFAULT_WAREHOUSE = TB_WH
  COMMENT = 'Tenantboard API service user (key-pair auth)';
GRANT ROLE PLATFORM_ADMIN TO USER TENANTBOARD_SVC;
GRANT USAGE ON WAREHOUSE TB_WH TO ROLE TENANT_READER;
GRANT USAGE ON WAREHOUSE TB_WH TO ROLE PLATFORM_ADMIN;

CREATE DATABASE IF NOT EXISTS TENANTBOARD;
USE DATABASE TENANTBOARD;

-- @deploy: everything below this line is database-relative. The test suite
-- builds its template database from these files in order.

CREATE SCHEMA IF NOT EXISTS APP;
