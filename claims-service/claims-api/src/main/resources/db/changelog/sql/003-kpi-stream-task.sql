--liquibase formatted sql

--changeset claims:003-kpi-table
CREATE TABLE CLAIM_KPIS_MONTHLY (
    SERVICE_MONTH DATE          NOT NULL,
    STATUS        VARCHAR(12)   NOT NULL,
    CLAIM_COUNT   NUMBER(18, 0) NOT NULL,
    BILLED_TOTAL  NUMBER(18, 2) NOT NULL,
    PAYABLE_TOTAL NUMBER(18, 2) NOT NULL,
    UPDATED_AT    TIMESTAMP_LTZ NOT NULL,
    PRIMARY KEY (SERVICE_MONTH, STATUS)
) COMMENT = 'Claims per service month and current status, maintained incrementally by REFRESH_CLAIM_KPIS';
--rollback DROP TABLE CLAIM_KPIS_MONTHLY;

--changeset claims:003-claims-stream
CREATE STREAM CLAIMS_CHANGES ON TABLE CLAIMS COMMENT = 'Changes to CLAIMS not yet folded into CLAIM_KPIS_MONTHLY';
-- The KPI task reads the stream as the service role.
GRANT SELECT ON STREAM CLAIMS_CHANGES TO ROLE ${appRole};
--rollback DROP STREAM CLAIMS_CHANGES;

--changeset claims:003-kpi-task runOnChange:true splitStatements:false
-- One MERGE over the stream's net changes: an INSERT row adds a claim to its
-- (month, status) bucket, a DELETE row (the old image of an UPDATE) removes it, so a
-- status change moves the claim between buckets. Committing the MERGE advances the
-- stream offset, so the task body is that one statement.
CREATE OR REPLACE TASK REFRESH_CLAIM_KPIS
    WAREHOUSE = ${warehouse}
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Folds CLAIMS_CHANGES into CLAIM_KPIS_MONTHLY'
    WHEN SYSTEM$STREAM_HAS_DATA('${appSchema}.CLAIMS_CHANGES')
AS
MERGE INTO ${appSchema}.CLAIM_KPIS_MONTHLY k
USING (
    SELECT DATE_TRUNC('MONTH', SERVICE_DATE) AS SERVICE_MONTH,
           STATUS,
           SUM(IFF(METADATA$ACTION = 'INSERT', 1, -1)) AS D_COUNT,
           SUM(IFF(METADATA$ACTION = 'INSERT', BILLED_AMOUNT, -BILLED_AMOUNT)) AS D_BILLED,
           SUM(IFF(METADATA$ACTION = 'INSERT', 1, -1) * COALESCE(PAYABLE_AMOUNT, 0)) AS D_PAYABLE
    FROM ${appSchema}.CLAIMS_CHANGES
    GROUP BY 1, 2
) s
ON k.SERVICE_MONTH = s.SERVICE_MONTH AND k.STATUS = s.STATUS
WHEN MATCHED AND k.CLAIM_COUNT + s.D_COUNT = 0 THEN DELETE
WHEN MATCHED THEN UPDATE SET
    CLAIM_COUNT = k.CLAIM_COUNT + s.D_COUNT,
    BILLED_TOTAL = k.BILLED_TOTAL + s.D_BILLED,
    PAYABLE_TOTAL = k.PAYABLE_TOTAL + s.D_PAYABLE,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.D_COUNT <> 0 THEN INSERT
    (SERVICE_MONTH, STATUS, CLAIM_COUNT, BILLED_TOTAL, PAYABLE_TOTAL, UPDATED_AT)
VALUES
    (s.SERVICE_MONTH, s.STATUS, s.D_COUNT, s.D_BILLED, s.D_PAYABLE, CURRENT_TIMESTAMP());
--rollback DROP TASK REFRESH_CLAIM_KPIS;

--changeset claims:003-kpi-task-resume context:scheduled runAlways:true
-- Production only (Liquibase context "scheduled"): CREATE OR REPLACE TASK leaves the
-- task suspended, so resume it after every deployment.
ALTER TASK REFRESH_CLAIM_KPIS RESUME;
