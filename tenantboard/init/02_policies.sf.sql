-- Tenantboard: tenant isolation and PII masking, enforced by Snowflake.
-- The policies exist before the tables, which attach them at creation
-- (03_tables.sf.sql), so no table is ever readable without them.
USE ROLE ACCOUNTADMIN;
USE DATABASE TENANTBOARD;

-- @deploy: everything below this line is database-relative.

USE SCHEMA APP;

-- A session sees a row only when the tenant's own role is active in it
-- (directly or inherited, so TENANT_<ID>_ADMIN qualifies), or when it is a
-- platform operator. The app's WHERE TENANT_ID = ? filter is a second,
-- independent layer: the leak test removes it and this policy alone holds.
CREATE ROW ACCESS POLICY IF NOT EXISTS TENANT_ISOLATION
  AS (TENANT_ID VARCHAR) RETURNS BOOLEAN ->
  IS_ROLE_IN_SESSION('PLATFORM_ADMIN') OR IS_ROLE_IN_SESSION('TENANT_' || TENANT_ID)
  COMMENT = 'Row visible to TENANT_<TENANT_ID> (and roles inheriting it) and PLATFORM_ADMIN';

-- Customer email is clear only for tenant admins (TENANT_<ID>_ADMIN inherits
-- TENANT_ADMIN). Viewers and platform operators see ***MASKED***.
CREATE MASKING POLICY IF NOT EXISTS EMAIL_MASK
  AS (VAL VARCHAR) RETURNS VARCHAR ->
  CASE WHEN IS_ROLE_IN_SESSION('TENANT_ADMIN') THEN VAL ELSE '***MASKED***' END
  COMMENT = 'Clear email for TENANT_ADMIN only';
