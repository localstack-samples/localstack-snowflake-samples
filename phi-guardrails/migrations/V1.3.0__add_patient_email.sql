-- V1.3.0: a new PHI column.
--
-- The column is classified at creation, and that is the whole change: the
-- PHI_TYPE tag already carries a VARCHAR masking policy, so EMAIL is masked for
-- every non-clinical role with no new policy code. access-contract.yml gains the
-- matching rows, and the coverage gate confirms the column is protected.

ALTER TABLE {{ database }}.CLINICAL.PATIENTS
    ADD COLUMN EMAIL VARCHAR(254)
    WITH TAG ({{ database }}.GOVERNANCE.PHI_TYPE = 'email');

-- Describe the column for catalog users.
ALTER TABLE {{ database }}.CLINICAL.PATIENTS ALTER COLUMN EMAIL COMMENT 'Patient contact email';
