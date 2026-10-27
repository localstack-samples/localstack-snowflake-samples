--liquibase formatted sql

-- Snowflake records PRIMARY KEY constraints but does not enforce them; the service
-- prevents duplicate claim ids with MERGE (see ClaimsRepository).

--changeset claims:001-policies
CREATE TABLE POLICIES (
    POLICY_ID        VARCHAR(20)   NOT NULL PRIMARY KEY,
    MEMBER_NAME      VARCHAR(100)  NOT NULL,
    PLAN_TIER        VARCHAR(10)   NOT NULL,
    EFFECTIVE_DATE   DATE          NOT NULL,
    TERMINATION_DATE DATE          NOT NULL,
    DEDUCTIBLE       NUMBER(12, 2) NOT NULL,
    COINSURANCE_RATE NUMBER(4, 3)  NOT NULL,
    CLAIM_LIMIT      NUMBER(12, 2) NOT NULL
) COMMENT = 'Member policies: coverage period and cost-sharing terms (synthetic)';
--rollback DROP TABLE POLICIES;

--changeset claims:001-providers
CREATE TABLE PROVIDERS (
    PROVIDER_ID VARCHAR(20)  NOT NULL PRIMARY KEY,
    NAME        VARCHAR(100) NOT NULL,
    NPI         VARCHAR(10)  NOT NULL,
    SPECIALTY   VARCHAR(50)  NOT NULL,
    IN_NETWORK  BOOLEAN      NOT NULL
) COMMENT = 'Billing providers (synthetic)';
--rollback DROP TABLE PROVIDERS;

--changeset claims:001-claims
CREATE TABLE CLAIMS (
    CLAIM_ID       VARCHAR(30)   NOT NULL PRIMARY KEY,
    POLICY_ID      VARCHAR(20)   NOT NULL,
    PROVIDER_ID    VARCHAR(20)   NOT NULL,
    DIAGNOSIS_CODE VARCHAR(8)    NOT NULL,
    SERVICE_DATE   DATE          NOT NULL,
    BILLED_AMOUNT  NUMBER(12, 2) NOT NULL,
    STATUS         VARCHAR(12)   NOT NULL,
    PAYABLE_AMOUNT NUMBER(12, 2),
    DENIAL_REASON  VARCHAR(30),
    SUBMITTED_AT   TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ADJUDICATED_AT TIMESTAMP_LTZ
) COMMENT = 'Insurance claims: one row per claim, STATUS SUBMITTED, APPROVED or DENIED';
--rollback DROP TABLE CLAIMS;

--changeset claims:001-claim-intake
CREATE TRANSIENT TABLE CLAIM_INTAKE (
    BATCH_ID       VARCHAR(36)   NOT NULL,
    ROW_NO         NUMBER(9, 0)  NOT NULL,
    CLAIM_ID       VARCHAR(30)   NOT NULL,
    POLICY_ID      VARCHAR(20)   NOT NULL,
    PROVIDER_ID    VARCHAR(20)   NOT NULL,
    DIAGNOSIS_CODE VARCHAR(8)    NOT NULL,
    SERVICE_DATE   DATE          NOT NULL,
    BILLED_AMOUNT  NUMBER(12, 2) NOT NULL,
    REJECT_REASON  VARCHAR(30)
) COMMENT = 'Staging for bulk intake; rows live only for the duration of one batch transaction';
--rollback DROP TABLE CLAIM_INTAKE;

--changeset claims:001-adjudication-rules
CREATE TABLE ADJUDICATION_RULES (
    RULE_CODE     VARCHAR(30)  NOT NULL PRIMARY KEY,
    PRIORITY      NUMBER(4, 0) NOT NULL,
    DENIAL_REASON VARCHAR(30)  NOT NULL,
    DESCRIPTION   VARCHAR(200) NOT NULL,
    ENABLED       BOOLEAN      NOT NULL DEFAULT TRUE
) COMMENT = 'Rules ADJUDICATE_CLAIM evaluates in PRIORITY order; the first failing rule denies the claim';
--rollback DROP TABLE ADJUDICATION_RULES;

--changeset claims:001-coverage-exclusions
CREATE TABLE COVERAGE_EXCLUSIONS (
    ICD10_CATEGORY VARCHAR(3)   NOT NULL PRIMARY KEY,
    DESCRIPTION    VARCHAR(200) NOT NULL
) COMMENT = 'ICD-10 categories (first three characters) that policies do not cover';
--rollback DROP TABLE COVERAGE_EXCLUSIONS;

--changeset claims:001-adjudication-log
CREATE TABLE ADJUDICATION_LOG (
    CLAIM_ID        VARCHAR(30)   NOT NULL,
    DECISION        VARCHAR(12)   NOT NULL,
    DENIAL_REASON   VARCHAR(30),
    PAYABLE_AMOUNT  NUMBER(12, 2) NOT NULL,
    RULES_EVALUATED NUMBER(4, 0)  NOT NULL,
    ADJUDICATED_AT  TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Audit trail: one row per adjudication decision';
--rollback DROP TABLE ADJUDICATION_LOG;
