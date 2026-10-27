--liquibase formatted sql

-- Synthetic reference data. Names, NPIs and policies are invented.

--changeset claims:004-policies
INSERT INTO POLICIES
    (POLICY_ID, MEMBER_NAME, PLAN_TIER, EFFECTIVE_DATE, TERMINATION_DATE, DEDUCTIBLE, COINSURANCE_RATE, CLAIM_LIMIT)
VALUES
    ('POL-1001', 'Avery Chen',     'GOLD',   '2025-01-01', '2026-12-31',  250.00, 0.100, 25000.00),
    ('POL-1002', 'Jordan Patel',   'SILVER', '2025-07-01', '2027-06-30', 1000.00, 0.200, 15000.00),
    ('POL-1003', 'Sam Okafor',     'BRONZE', '2026-01-01', '2026-12-31', 2500.00, 0.400, 10000.00),
    ('POL-1004', 'Riley Novak',    'SILVER', '2024-01-01', '2026-03-31', 1000.00, 0.200, 15000.00),
    ('POL-1005', 'Morgan Silva',   'GOLD',   '2026-04-01', '2027-03-31',  250.00, 0.100, 25000.00),
    ('POL-1006', 'Casey Lindqvist','BRONZE', '2025-01-01', '2026-12-31', 2500.00, 0.400,  4000.00),
    ('POL-1007', 'Taylor Moreau',  'SILVER', '2026-01-01', '2026-12-31', 1000.00, 0.200, 15000.00),
    ('POL-1008', 'Drew Haddad',    'GOLD',   '2025-01-01', '2027-12-31',  250.00, 0.100, 25000.00);
--rollback DELETE FROM POLICIES WHERE POLICY_ID LIKE 'POL-10%';

--changeset claims:004-providers
INSERT INTO PROVIDERS (PROVIDER_ID, NAME, NPI, SPECIALTY, IN_NETWORK)
VALUES
    ('PRV-201', 'Lakeside Family Medicine',  '1902837465', 'Family Medicine',     TRUE),
    ('PRV-202', 'Northgate Orthopedics',     '1548293071', 'Orthopedic Surgery',  TRUE),
    ('PRV-203', 'Harbor Urgent Care',        '1265019384', 'Urgent Care',         TRUE),
    ('PRV-204', 'Summit Behavioral Health',  '1730468259', 'Psychiatry',          TRUE),
    ('PRV-205', 'Westfield Imaging Center',  '1384726150', 'Diagnostic Radiology', FALSE),
    ('PRV-206', 'Riverbend Medical Group',   '1659204837', 'Internal Medicine',   FALSE);
--rollback DELETE FROM PROVIDERS WHERE PROVIDER_ID LIKE 'PRV-2%';

--changeset claims:004-adjudication-rules
INSERT INTO ADJUDICATION_RULES (RULE_CODE, PRIORITY, DENIAL_REASON, DESCRIPTION)
VALUES
    ('POLICY_ACTIVE',      10, 'POLICY_INACTIVE',  'The service date falls within the policy period'),
    ('DIAGNOSIS_COVERED',  20, 'NOT_COVERED',      'The diagnosis category is not excluded from coverage'),
    ('WITHIN_CLAIM_LIMIT', 30, 'OVER_CLAIM_LIMIT', 'The billed amount does not exceed the policy per-claim limit');
--rollback DELETE FROM ADJUDICATION_RULES;

--changeset claims:004-coverage-exclusions
INSERT INTO COVERAGE_EXCLUSIONS (ICD10_CATEGORY, DESCRIPTION)
VALUES
    ('Z41', 'Encounter for procedures for purposes other than remedying health state (cosmetic and elective)');
--rollback DELETE FROM COVERAGE_EXCLUSIONS;
