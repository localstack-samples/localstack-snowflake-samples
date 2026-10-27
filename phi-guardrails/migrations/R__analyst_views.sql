-- Repeatable: the analyst-facing views. schemachange re-runs this file
-- whenever it changes, after all versioned migrations.
--
-- Both views are SECURE, so their definitions and the base tables stay hidden
-- from analysts. Policies on the base columns still apply to whoever queries
-- the view: there is no masking logic here, and none is needed.

-- Encounter volume and cost per facility, year and class. Cells covering fewer
-- than 11 patients are suppressed (the CMS cell-size suppression threshold),
-- so small groups cannot be used to re-identify a patient. YEAR() works on the
-- masked START_TS because masking keeps the year.
CREATE OR REPLACE SECURE VIEW {{ database }}.ANALYTICS.ENCOUNTER_SUMMARY
    COPY GRANTS
    COMMENT = 'Encounters per facility, year and class; cells under 11 patients suppressed'
AS
SELECT
    e.FACILITY_ID,
    f.NAME                       AS FACILITY_NAME,
    YEAR(e.START_TS)             AS ENCOUNTER_YEAR,
    e.ENCOUNTER_CLASS,
    COUNT(*)                     AS ENCOUNTERS,
    COUNT(DISTINCT e.PATIENT_ID) AS PATIENTS,
    SUM(e.TOTAL_CLAIM_COST)      AS TOTAL_CLAIM_COST
FROM {{ database }}.CLINICAL.ENCOUNTERS AS e
JOIN {{ database }}.CLINICAL.FACILITIES AS f
    ON f.FACILITY_ID = e.FACILITY_ID
GROUP BY e.FACILITY_ID, f.NAME, YEAR(e.START_TS), e.ENCOUNTER_CLASS
HAVING COUNT(DISTINCT e.PATIENT_ID) >= 11;

-- Patient-level cohort for building study populations. PHI columns pass
-- through as-is and arrive masked for analysts, because their tags travel with
-- them; EMAIL (added in V1.3.0) needed no change beyond listing it here.
CREATE OR REPLACE SECURE VIEW {{ database }}.ANALYTICS.PATIENT_COHORT
    COPY GRANTS
    COMMENT = 'Patient demographics for cohort building; PHI masked by tag'
AS
SELECT
    PATIENT_ID,
    PRIMARY_FACILITY_ID,
    BIRTH_DATE,
    GENDER,
    RACE,
    ETHNICITY,
    STATE,
    ZIP,
    EMAIL
FROM {{ database }}.CLINICAL.PATIENTS;

GRANT USAGE ON SCHEMA {{ database }}.ANALYTICS TO ROLE ANALYST;
GRANT SELECT ON VIEW {{ database }}.ANALYTICS.ENCOUNTER_SUMMARY TO ROLE ANALYST;
GRANT SELECT ON VIEW {{ database }}.ANALYTICS.PATIENT_COHORT TO ROLE ANALYST;
