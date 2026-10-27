-- V1.2.0: facility-scoped row access.
--
-- Each facility has an access role FACILITY_<id>. A row is visible when the
-- session holds the role of the row's facility, or a role that reads across
-- facilities:
--   AUDITOR  audits every facility
--   ANALYST  aggregates across facilities, but only through ANALYTICS views
--            (it has no SELECT grant on the patient-level tables)
-- DATA_ENGINEER holds no facility role, so it sees no patient rows at all.
--
-- The role-to-facility mapping lives in the role hierarchy, so who may see
-- which facility is an ordinary grant: visible in SHOW GRANTS, reviewed like
-- any other privilege, and evaluated without a lookup table.

CREATE ROLE IF NOT EXISTS FACILITY_101 COMMENT = 'Row access: HALLMARK HEALTH SYSTEM';
CREATE ROLE IF NOT EXISTS FACILITY_102 COMMENT = 'Row access: CAMBRIDGE HEALTH ALLIANCE';
CREATE ROLE IF NOT EXISTS FACILITY_103 COMMENT = 'Row access: NEWTON-WELLESLEY HOSPITAL';

-- Facility roles are granted to the functional roles, so row access follows
-- the role hierarchy and a session needs only its primary role.
GRANT ROLE FACILITY_101 TO ROLE CLINICIAN;       -- clinicians work at facility 101
GRANT ROLE FACILITY_102 TO ROLE CLINICIAN_LEAD;  -- the lead also covers 102 (and 101 through CLINICIAN)

CREATE ROW ACCESS POLICY IF NOT EXISTS {{ database }}.GOVERNANCE.FACILITY_ROW_ACCESS
    AS (facility_id NUMBER) RETURNS BOOLEAN ->
        IS_ROLE_IN_SESSION('AUDITOR')
        OR IS_ROLE_IN_SESSION('ANALYST')
        OR IS_ROLE_IN_SESSION('FACILITY_' || facility_id)
    COMMENT = 'Rows of facilities whose FACILITY_<id> role is in session';

ALTER TABLE {{ database }}.CLINICAL.PATIENTS
    ADD ROW ACCESS POLICY {{ database }}.GOVERNANCE.FACILITY_ROW_ACCESS ON (PRIMARY_FACILITY_ID);
ALTER TABLE {{ database }}.CLINICAL.ENCOUNTERS
    ADD ROW ACCESS POLICY {{ database }}.GOVERNANCE.FACILITY_ROW_ACCESS ON (FACILITY_ID);
