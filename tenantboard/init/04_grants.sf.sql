-- Tenantboard: least-privilege grants for the shared roles.
USE ROLE ACCOUNTADMIN;
USE DATABASE TENANTBOARD;
GRANT USAGE ON DATABASE TENANTBOARD TO ROLE TENANT_READER;
GRANT USAGE ON DATABASE TENANTBOARD TO ROLE PLATFORM_ADMIN;

-- @deploy: everything below this line is database-relative. It is also applied
-- to every clone (app/provisioning.py), so each test database carries exactly
-- the grants production has.

USE SCHEMA APP;

-- Tenant viewers read the tenant tables; the row access policy decides which rows.
GRANT USAGE ON SCHEMA APP TO ROLE TENANT_READER;
GRANT SELECT ON TABLE MERCHANTS  TO ROLE TENANT_READER;
GRANT SELECT ON TABLE CUSTOMERS  TO ROLE TENANT_READER;
GRANT SELECT ON TABLE ORDERS     TO ROLE TENANT_READER;
GRANT SELECT ON TABLE LINE_ITEMS TO ROLE TENANT_READER;

-- Tenant admins upload offline orders: write_pandas stages a temporary table,
-- then one MERGE writes ORDERS (still subject to the row access policy).
GRANT CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA APP TO ROLE TENANT_ADMIN;
GRANT INSERT, UPDATE ON TABLE ORDERS TO ROLE TENANT_ADMIN;

-- Platform operators read every tenant's rows (the policy lets PLATFORM_ADMIN
-- through) for the tenant directory and support, with customer email masked.
GRANT USAGE ON SCHEMA APP TO ROLE PLATFORM_ADMIN;
GRANT SELECT ON TABLE MERCHANTS  TO ROLE PLATFORM_ADMIN;
GRANT SELECT ON TABLE CUSTOMERS  TO ROLE PLATFORM_ADMIN;
GRANT SELECT ON TABLE ORDERS     TO ROLE PLATFORM_ADMIN;
GRANT SELECT ON TABLE LINE_ITEMS TO ROLE PLATFORM_ADMIN;
