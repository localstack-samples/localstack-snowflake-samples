-- V1.1.0: the PHI_TYPE tag and tag-based masking.
--
-- Columns are classified with one tag. The tag carries one masking policy per
-- data type, so any VARCHAR, DATE or TIMESTAMP_NTZ column that is tagged later
-- is masked with no new policy code (V1.3.0 relies on this). A tagged column
-- of any other data type is NOT masked: tests/test_policy_coverage.py is the
-- gate that catches it.
--
-- Who sees what:
--   CLINICIAN (and CLINICIAN_LEAD, which inherits it)  every value in clear
--   AUDITOR   names in clear (to identify whose record was accessed), the rest masked
--   everyone else                                       masked
-- Masked strings read '***MASKED***'; masked dates and timestamps keep only the
-- year (HIPAA Safe Harbor allows the year), so year-level analytics still work.
--
-- IS_ROLE_IN_SESSION, not CURRENT_ROLE: it honours the role hierarchy, so a
-- role that inherits CLINICIAN is treated as a clinician.

CREATE TAG IF NOT EXISTS {{ database }}.GOVERNANCE.PHI_TYPE
    ALLOWED_VALUES 'name', 'address', 'zip', 'birth_date', 'death_date', 'service_date',
                   'phone', 'email', 'ssn', 'mrn', 'license_number', 'passport_number'
    COMMENT = 'HIPAA identifier class of a column; drives masking';

CREATE MASKING POLICY IF NOT EXISTS {{ database }}.GOVERNANCE.PHI_MASK_STRING
    AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN IS_ROLE_IN_SESSION('CLINICIAN') THEN val
        WHEN IS_ROLE_IN_SESSION('AUDITOR')
             AND SYSTEM$GET_TAG_ON_CURRENT_COLUMN('{{ database }}.GOVERNANCE.PHI_TYPE') = 'name' THEN val
        ELSE '***MASKED***'
    END
    COMMENT = 'PHI strings: clear for clinicians, names clear for auditors';

CREATE MASKING POLICY IF NOT EXISTS {{ database }}.GOVERNANCE.PHI_MASK_DATE
    AS (val DATE) RETURNS DATE ->
    CASE
        WHEN IS_ROLE_IN_SESSION('CLINICIAN') THEN val
        ELSE DATE_TRUNC('YEAR', val)
    END
    COMMENT = 'PHI dates: clear for clinicians, year only otherwise';

CREATE MASKING POLICY IF NOT EXISTS {{ database }}.GOVERNANCE.PHI_MASK_TIMESTAMP
    AS (val TIMESTAMP_NTZ) RETURNS TIMESTAMP_NTZ ->
    CASE
        WHEN IS_ROLE_IN_SESSION('CLINICIAN') THEN val
        ELSE DATE_TRUNC('YEAR', val)
    END
    COMMENT = 'PHI timestamps: clear for clinicians, year only otherwise';

ALTER TAG {{ database }}.GOVERNANCE.PHI_TYPE SET
    MASKING POLICY {{ database }}.GOVERNANCE.PHI_MASK_STRING,
    MASKING POLICY {{ database }}.GOVERNANCE.PHI_MASK_DATE,
    MASKING POLICY {{ database }}.GOVERNANCE.PHI_MASK_TIMESTAMP;

-- Classify the existing columns -------------------------------------------
-- One column per statement, so each classification is its own reviewable line
-- and a later diff shows exactly which column changed.
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN FIRST_NAME      SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'name';
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN LAST_NAME       SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'name';
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN BIRTH_DATE      SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'birth_date';
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN DEATH_DATE      SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'death_date';
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN SSN             SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'ssn';
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN DRIVERS_LICENSE SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'license_number';
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN PASSPORT        SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'passport_number';
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN ADDRESS         SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'address';
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN CITY            SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'address';
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN COUNTY          SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'address';
ALTER TABLE {{ database }}.CLINICAL.PATIENTS   MODIFY COLUMN ZIP             SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'zip';

ALTER TABLE {{ database }}.CLINICAL.ENCOUNTERS MODIFY COLUMN START_TS        SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'service_date';
ALTER TABLE {{ database }}.CLINICAL.ENCOUNTERS MODIFY COLUMN STOP_TS         SET TAG {{ database }}.GOVERNANCE.PHI_TYPE = 'service_date';
