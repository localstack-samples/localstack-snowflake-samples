--liquibase formatted sql

--changeset claims:001-icd10-function runOnChange:true splitStatements:false
-- TRUE when CODE is a structurally valid ICD-10-CM code in canonical dotted form:
--   * a category of 3 characters: a letter, a digit, then a digit or letter (E11, C4A);
--   * optionally a dot and 1 to 4 more digits or letters (E11.9, S93.401A).
-- Codes are stored trimmed and upper case, so anything else, including NULL, is FALSE.
-- REGEXP_LIKE matches the whole string.
CREATE OR REPLACE FUNCTION IS_VALID_ICD10(CODE VARCHAR)
RETURNS BOOLEAN
COMMENT = 'TRUE when CODE is a structurally valid ICD-10-CM code in canonical dotted form'
AS
$$
    COALESCE(REGEXP_LIKE(CODE, '[A-Z][0-9][0-9A-Z]([.][0-9A-Z]{1,4})?'), FALSE)
$$
--rollback DROP FUNCTION IS_VALID_ICD10(VARCHAR);
