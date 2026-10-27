--liquibase formatted sql

--changeset claims:002-adjudicate-claim runOnChange:true splitStatements:false
-- Adjudicates one SUBMITTED claim:
--   1. raises claim_not_found (-20001) or claim_not_pending (-20002) for bad input;
--   2. walks the enabled ADJUDICATION_RULES with a declared cursor, in PRIORITY order,
--      and denies the claim with the first failing rule's DENIAL_REASON;
--   3. computes the payable amount for approved claims from the policy terms;
--   4. writes the decision and an audit row in one transaction, guarding against a
--      concurrent adjudication with SQLROWCOUNT.
-- The claim's values are read into variables once with SELECT ... INTO, so the rules
-- cursor is a plain query over ADJUDICATION_RULES and the loop only compares variables.
-- The procedure returns the decision; callers read the updated claim row itself.
CREATE OR REPLACE PROCEDURE ADJUDICATE_CLAIM(P_CLAIM_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    claim_not_found EXCEPTION (-20001, 'Claim not found');
    claim_not_pending EXCEPTION (-20002, 'Claim is not pending adjudication');

    rule_rs RESULTSET DEFAULT (
        SELECT RULE_CODE, DENIAL_REASON
        FROM ADJUDICATION_RULES
        WHERE ENABLED
        ORDER BY PRIORITY, RULE_CODE);
    rule_cursor CURSOR FOR rule_rs;

    v_found INTEGER DEFAULT 0;
    v_status VARCHAR;
    v_service_date DATE;
    v_billed NUMBER(12, 2);
    v_diagnosis VARCHAR;
    v_effective DATE;
    v_termination DATE;
    v_deductible NUMBER(12, 2);
    v_coinsurance NUMBER(4, 3);
    v_claim_limit NUMBER(12, 2);
    v_in_network BOOLEAN;
    v_excluded INTEGER DEFAULT 0;

    v_decision VARCHAR DEFAULT 'APPROVED';
    v_denial_reason VARCHAR DEFAULT NULL;
    v_member_share NUMBER(4, 3);
    v_payable NUMBER(12, 2) DEFAULT 0;
    v_rules_evaluated INTEGER DEFAULT 0;
BEGIN
    SELECT COUNT(*) INTO :v_found FROM CLAIMS WHERE CLAIM_ID = :p_claim_id;
    IF (v_found = 0) THEN
        RAISE claim_not_found;
    END IF;

    SELECT c.STATUS, c.SERVICE_DATE, c.BILLED_AMOUNT, c.DIAGNOSIS_CODE,
           p.EFFECTIVE_DATE, p.TERMINATION_DATE, p.DEDUCTIBLE, p.COINSURANCE_RATE, p.CLAIM_LIMIT,
           v.IN_NETWORK
      INTO :v_status, :v_service_date, :v_billed, :v_diagnosis,
           :v_effective, :v_termination, :v_deductible, :v_coinsurance, :v_claim_limit,
           :v_in_network
      FROM CLAIMS c
      JOIN POLICIES p ON p.POLICY_ID = c.POLICY_ID
      JOIN PROVIDERS v ON v.PROVIDER_ID = c.PROVIDER_ID
     WHERE c.CLAIM_ID = :p_claim_id;
    IF (v_status <> 'SUBMITTED') THEN
        RAISE claim_not_pending;
    END IF;

    SELECT COUNT(*) INTO :v_excluded
      FROM COVERAGE_EXCLUSIONS
     WHERE ICD10_CATEGORY = LEFT(:v_diagnosis, 3);

    FOR r IN rule_cursor DO
        v_rules_evaluated := v_rules_evaluated + 1;
        IF ((r.RULE_CODE = 'POLICY_ACTIVE' AND (v_service_date < v_effective OR v_service_date > v_termination))
            OR (r.RULE_CODE = 'DIAGNOSIS_COVERED' AND v_excluded > 0)
            OR (r.RULE_CODE = 'WITHIN_CLAIM_LIMIT' AND v_billed > v_claim_limit)) THEN
            v_decision := 'DENIED';
            v_denial_reason := r.DENIAL_REASON;
            BREAK;
        END IF;
    END FOR;

    IF (v_decision = 'APPROVED') THEN
        -- Out-of-network care carries 30 points more coinsurance.
        v_member_share := IFF(v_in_network, v_coinsurance, LEAST(v_coinsurance + 0.3, 1));
        v_payable := ROUND(GREATEST(v_billed - v_deductible, 0) * (1 - v_member_share), 2);
    END IF;

    BEGIN TRANSACTION;
    UPDATE CLAIMS
       SET STATUS = :v_decision,
           PAYABLE_AMOUNT = :v_payable,
           DENIAL_REASON = :v_denial_reason,
           ADJUDICATED_AT = CURRENT_TIMESTAMP()
     WHERE CLAIM_ID = :p_claim_id AND STATUS = 'SUBMITTED';
    IF (SQLROWCOUNT = 0) THEN
        -- Another session adjudicated the claim since we read it.
        ROLLBACK;
        RAISE claim_not_pending;
    END IF;
    INSERT INTO ADJUDICATION_LOG (CLAIM_ID, DECISION, DENIAL_REASON, PAYABLE_AMOUNT, RULES_EVALUATED)
    VALUES (:p_claim_id, :v_decision, :v_denial_reason, :v_payable, :v_rules_evaluated);
    COMMIT;
    RETURN v_decision;
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
$$
