-- V1.0.0: clinical role hierarchy, one test user per persona, schemas and tables.
--
-- Roles and users are account-level, so every statement here is idempotent:
-- the same migrations deploy into several databases (one per test run) in one
-- account. Grants are explicit per object: PHI never relies on future grants.

-- Functional roles ---------------------------------------------------------
CREATE ROLE IF NOT EXISTS CLINICIAN      COMMENT = 'Treats patients: clear PHI, rows of own facilities only';
CREATE ROLE IF NOT EXISTS CLINICIAN_LEAD COMMENT = 'Leads a care team: everything CLINICIAN has, plus more facilities';
CREATE ROLE IF NOT EXISTS ANALYST        COMMENT = 'Population analytics: ANALYTICS views only, PHI masked';
CREATE ROLE IF NOT EXISTS AUDITOR        COMMENT = 'Privacy audit: all rows, identities clear, other PHI masked';
CREATE ROLE IF NOT EXISTS DATA_ENGINEER  COMMENT = 'Operates pipelines: may write, reads no patient rows';

GRANT ROLE CLINICIAN TO ROLE CLINICIAN_LEAD;

-- One test user per persona. Key-pair only (no password); sessions use the
-- persona role alone (no secondary roles) so each test sees exactly one
-- role hierarchy.
CREATE USER IF NOT EXISTS PERSONA_CLINICIAN      TYPE = PERSON DEFAULT_ROLE = CLINICIAN      DEFAULT_WAREHOUSE = {{ warehouse }} DEFAULT_SECONDARY_ROLES = ();
CREATE USER IF NOT EXISTS PERSONA_CLINICIAN_LEAD TYPE = PERSON DEFAULT_ROLE = CLINICIAN_LEAD DEFAULT_WAREHOUSE = {{ warehouse }} DEFAULT_SECONDARY_ROLES = ();
CREATE USER IF NOT EXISTS PERSONA_ANALYST        TYPE = PERSON DEFAULT_ROLE = ANALYST        DEFAULT_WAREHOUSE = {{ warehouse }} DEFAULT_SECONDARY_ROLES = ();
CREATE USER IF NOT EXISTS PERSONA_AUDITOR        TYPE = PERSON DEFAULT_ROLE = AUDITOR        DEFAULT_WAREHOUSE = {{ warehouse }} DEFAULT_SECONDARY_ROLES = ();
CREATE USER IF NOT EXISTS PERSONA_DATA_ENGINEER  TYPE = PERSON DEFAULT_ROLE = DATA_ENGINEER  DEFAULT_WAREHOUSE = {{ warehouse }} DEFAULT_SECONDARY_ROLES = ();

ALTER USER PERSONA_CLINICIAN      SET RSA_PUBLIC_KEY = '{{ persona_public_key }}';
ALTER USER PERSONA_CLINICIAN_LEAD SET RSA_PUBLIC_KEY = '{{ persona_public_key }}';
ALTER USER PERSONA_ANALYST        SET RSA_PUBLIC_KEY = '{{ persona_public_key }}';
ALTER USER PERSONA_AUDITOR        SET RSA_PUBLIC_KEY = '{{ persona_public_key }}';
ALTER USER PERSONA_DATA_ENGINEER  SET RSA_PUBLIC_KEY = '{{ persona_public_key }}';

GRANT ROLE CLINICIAN      TO USER PERSONA_CLINICIAN;
GRANT ROLE CLINICIAN_LEAD TO USER PERSONA_CLINICIAN_LEAD;
GRANT ROLE ANALYST        TO USER PERSONA_ANALYST;
GRANT ROLE AUDITOR        TO USER PERSONA_AUDITOR;
GRANT ROLE DATA_ENGINEER  TO USER PERSONA_DATA_ENGINEER;

GRANT USAGE ON WAREHOUSE {{ warehouse }} TO ROLE CLINICIAN;
GRANT USAGE ON WAREHOUSE {{ warehouse }} TO ROLE ANALYST;
GRANT USAGE ON WAREHOUSE {{ warehouse }} TO ROLE AUDITOR;
GRANT USAGE ON WAREHOUSE {{ warehouse }} TO ROLE DATA_ENGINEER;

-- Schemas ------------------------------------------------------------------
-- CLINICAL holds the raw tables, GOVERNANCE the tag and policies (owned by the
-- deploy role, never granted to readers), ANALYTICS the views analysts read.
CREATE SCHEMA IF NOT EXISTS {{ database }}.CLINICAL   COMMENT = 'Raw clinical tables (PHI)';
CREATE SCHEMA IF NOT EXISTS {{ database }}.GOVERNANCE COMMENT = 'PHI tag, masking and row access policies';
CREATE SCHEMA IF NOT EXISTS {{ database }}.ANALYTICS  COMMENT = 'Secure views for analysts';

CREATE TABLE IF NOT EXISTS {{ database }}.CLINICAL.FACILITIES (
    FACILITY_ID            NUMBER(6, 0)  NOT NULL PRIMARY KEY,
    NAME                   VARCHAR(200)  NOT NULL,
    ADDRESS                VARCHAR(200),
    CITY                   VARCHAR(100),
    STATE                  VARCHAR(2),
    ZIP                    VARCHAR(10),
    PHONE                  VARCHAR(20),
    SOURCE_ORGANIZATION_ID VARCHAR(36)   NOT NULL
) COMMENT = 'Hospitals (Synthea organizations); directory data, not PHI';

CREATE TABLE IF NOT EXISTS {{ database }}.CLINICAL.PATIENTS (
    PATIENT_ID          VARCHAR(36)  NOT NULL PRIMARY KEY COMMENT 'Random surrogate key',
    PRIMARY_FACILITY_ID NUMBER(6, 0) NOT NULL REFERENCES {{ database }}.CLINICAL.FACILITIES (FACILITY_ID),
    FIRST_NAME          VARCHAR(100) NOT NULL,
    LAST_NAME           VARCHAR(100) NOT NULL,
    BIRTH_DATE          DATE         NOT NULL,
    DEATH_DATE          DATE,
    SSN                 VARCHAR(11)  NOT NULL,
    DRIVERS_LICENSE     VARCHAR(20),
    PASSPORT            VARCHAR(20),
    GENDER              VARCHAR(1),
    RACE                VARCHAR(30),
    ETHNICITY           VARCHAR(30),
    MARITAL_STATUS      VARCHAR(1),
    ADDRESS             VARCHAR(200),
    CITY                VARCHAR(100),
    COUNTY              VARCHAR(100),
    STATE               VARCHAR(50),
    ZIP                 VARCHAR(10)
) COMMENT = 'Synthea patients seen at a sample facility since 2010';

CREATE TABLE IF NOT EXISTS {{ database }}.CLINICAL.ENCOUNTERS (
    ENCOUNTER_ID       VARCHAR(36)    NOT NULL PRIMARY KEY,
    PATIENT_ID         VARCHAR(36)    NOT NULL REFERENCES {{ database }}.CLINICAL.PATIENTS (PATIENT_ID),
    FACILITY_ID        NUMBER(6, 0)   NOT NULL REFERENCES {{ database }}.CLINICAL.FACILITIES (FACILITY_ID),
    START_TS           TIMESTAMP_NTZ  NOT NULL COMMENT 'UTC',
    STOP_TS            TIMESTAMP_NTZ           COMMENT 'UTC',
    ENCOUNTER_CLASS    VARCHAR(20)    NOT NULL,
    CODE               VARCHAR(20)    NOT NULL COMMENT 'SNOMED CT',
    DESCRIPTION        VARCHAR(200),
    REASON_CODE        VARCHAR(20),
    REASON_DESCRIPTION VARCHAR(200),
    TOTAL_CLAIM_COST   NUMBER(12, 2),
    PAYER_COVERAGE     NUMBER(12, 2)
) COMMENT = 'Synthea encounters at sample facilities since 2010';

-- Grants -------------------------------------------------------------------
GRANT USAGE ON DATABASE {{ database }} TO ROLE CLINICIAN;
GRANT USAGE ON DATABASE {{ database }} TO ROLE ANALYST;
GRANT USAGE ON DATABASE {{ database }} TO ROLE AUDITOR;
GRANT USAGE ON DATABASE {{ database }} TO ROLE DATA_ENGINEER;

GRANT USAGE ON SCHEMA {{ database }}.CLINICAL TO ROLE CLINICIAN;
GRANT USAGE ON SCHEMA {{ database }}.CLINICAL TO ROLE AUDITOR;
GRANT USAGE ON SCHEMA {{ database }}.CLINICAL TO ROLE DATA_ENGINEER;
-- ANALYST reads the facility directory, but no patient-level table.
GRANT USAGE ON SCHEMA {{ database }}.CLINICAL TO ROLE ANALYST;

GRANT SELECT ON TABLE {{ database }}.CLINICAL.FACILITIES TO ROLE CLINICIAN;
GRANT SELECT ON TABLE {{ database }}.CLINICAL.FACILITIES TO ROLE ANALYST;
GRANT SELECT ON TABLE {{ database }}.CLINICAL.FACILITIES TO ROLE AUDITOR;
GRANT SELECT ON TABLE {{ database }}.CLINICAL.FACILITIES TO ROLE DATA_ENGINEER;

GRANT SELECT ON TABLE {{ database }}.CLINICAL.PATIENTS   TO ROLE CLINICIAN;
GRANT SELECT ON TABLE {{ database }}.CLINICAL.PATIENTS   TO ROLE AUDITOR;
GRANT SELECT ON TABLE {{ database }}.CLINICAL.ENCOUNTERS TO ROLE CLINICIAN;
GRANT SELECT ON TABLE {{ database }}.CLINICAL.ENCOUNTERS TO ROLE AUDITOR;

-- Data engineers run the load jobs: they write, and the row access policy in
-- V1.2.0 keeps every patient row invisible to them.
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE {{ database }}.CLINICAL.PATIENTS   TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE {{ database }}.CLINICAL.ENCOUNTERS TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE {{ database }}.CLINICAL.FACILITIES TO ROLE DATA_ENGINEER;
