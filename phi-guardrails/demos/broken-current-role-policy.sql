-- A plausible "simplification" of the string masking policy, as it might
-- arrive in a pull request: compare the session's role by name instead of
-- calling IS_ROLE_IN_SESSION.
--
-- It reads the same and still works for CLINICIAN and AUDITOR. It breaks
-- CLINICIAN_LEAD: CURRENT_ROLE() is the session's primary role only, so a role
-- that inherits CLINICIAN no longer counts as a clinician, and every text PHI
-- column turns to '***MASKED***' for clinical leads. Dates and timestamps are
-- unaffected (their policies are untouched), which makes the bug easy to miss
-- in a manual spot check.
--
-- `make demo` applies this to the sample database, shows the access matrix
-- catching it, and restores the original body. Rendered like a migration:
-- {{ database }} is the target database.

ALTER MASKING POLICY {{ database }}.GOVERNANCE.PHI_MASK_STRING SET BODY ->
    CASE
        WHEN CURRENT_ROLE() = 'CLINICIAN' THEN val
        WHEN CURRENT_ROLE() = 'AUDITOR'
             AND SYSTEM$GET_TAG_ON_CURRENT_COLUMN('{{ database }}.GOVERNANCE.PHI_TYPE') = 'name' THEN val
        ELSE '***MASKED***'
    END;
