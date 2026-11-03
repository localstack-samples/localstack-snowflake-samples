-- Order-to-Cash CDC: current-state and history tables, plus operational tables.
USE ROLE ACCOUNTADMIN;
USE DATABASE O2C;

-- @deploy: everything below this line is database-relative.

CREATE SCHEMA IF NOT EXISTS CORE;
CREATE SCHEMA IF NOT EXISTS MART;
CREATE SCHEMA IF NOT EXISTS OPS;

-- Current state of every order the source has ever emitted. Deletes are soft:
-- the row stays with IS_DELETED = TRUE so late or replayed events for that key
-- cannot resurrect it (their SOURCE_LSN is lower).
CREATE TABLE IF NOT EXISTS CORE.ORDERS (
  O_ORDERKEY      NUMBER(38, 0)    NOT NULL,
  O_CUSTKEY       NUMBER(38, 0)    NOT NULL,
  O_ORDERSTATUS   VARCHAR(1)       NOT NULL,
  O_TOTALPRICE    NUMBER(12, 2)    NOT NULL,
  O_ORDERDATE     DATE             NOT NULL,
  O_ORDERPRIORITY VARCHAR(15)      NOT NULL,
  O_CLERK         VARCHAR(15)      NOT NULL,
  IS_DELETED      BOOLEAN          NOT NULL,
  SOURCE_LSN      NUMBER(38, 0)    NOT NULL,
  SOURCE_TS       TIMESTAMP_NTZ(3) NOT NULL,
  APPLIED_AT      TIMESTAMP_NTZ(3) NOT NULL,
  CONSTRAINT PK_ORDERS PRIMARY KEY (O_ORDERKEY)
)
COMMENT = 'Current state per order key (soft delete), newest source LSN wins';

-- Type 2 history of customers. Validity is half-open: [VALID_FROM, VALID_TO).
-- A delete closes the open version and opens none.
CREATE TABLE IF NOT EXISTS CORE.DIM_CUSTOMER_HISTORY (
  C_CUSTKEY    NUMBER(38, 0)    NOT NULL,
  C_NAME       VARCHAR(25)      NOT NULL,
  C_NATIONKEY  NUMBER(38, 0)    NOT NULL,
  C_ACCTBAL    NUMBER(12, 2)    NOT NULL,
  C_MKTSEGMENT VARCHAR(10)      NOT NULL,
  VALID_FROM   TIMESTAMP_NTZ(3) NOT NULL,
  VALID_TO     TIMESTAMP_NTZ(3),
  IS_CURRENT   BOOLEAN          NOT NULL,
  SOURCE_LSN   NUMBER(38, 0)    NOT NULL,
  CONSTRAINT PK_DIM_CUSTOMER_HISTORY PRIMARY KEY (C_CUSTKEY, SOURCE_LSN)
)
COMMENT = 'SCD2 history per customer key, one row per applied version';

-- Working set of the current task run, parsed from the stream. Rewritten at
-- the start of every run (inside the run's transaction), kept afterwards for
-- debugging.
CREATE TRANSIENT TABLE IF NOT EXISTS OPS.APPLY_BATCH (
  KAFKA_TOPIC     VARCHAR,
  KAFKA_PARTITION NUMBER(38, 0),
  KAFKA_OFFSET    NUMBER(38, 0),
  SOURCE_TABLE    VARCHAR,
  OP              VARCHAR,
  ENTITY_KEY      NUMBER(38, 0),
  SOURCE_LSN      NUMBER(38, 0),
  SOURCE_TS       TIMESTAMP_NTZ(3),
  IMAGE           VARIANT,
  REJECT_REASON   VARCHAR,
  RECORD_CONTENT  VARIANT,
  RECORD_METADATA VARIANT
)
COMMENT = 'Parsed change events of the latest OPS.APPLY_ORDERS_CDC run';

-- Events that fail validation. Keyed by Kafka coordinates so re-reading the
-- same record never dead-letters it twice.
CREATE TABLE IF NOT EXISTS OPS.DEAD_LETTER (
  KAFKA_TOPIC      VARCHAR          NOT NULL,
  KAFKA_PARTITION  NUMBER(38, 0)    NOT NULL,
  KAFKA_OFFSET     NUMBER(38, 0)    NOT NULL,
  REASON           VARCHAR          NOT NULL,
  RECORD_CONTENT   VARIANT,
  RECORD_METADATA  VARIANT,
  DEAD_LETTERED_AT TIMESTAMP_NTZ(3) NOT NULL,
  CONSTRAINT PK_DEAD_LETTER PRIMARY KEY (KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)
)
COMMENT = 'Rejected change events with the reason, for replay after a fix';

-- Operator switches for the pipeline. FAIL_NEXT_RUN makes the apply
-- procedure raise after its merges, which exercises the rollback path.
CREATE TABLE IF NOT EXISTS OPS.PIPELINE_CONTROL (
  PIPELINE_NAME VARCHAR          NOT NULL,
  FAIL_NEXT_RUN BOOLEAN          NOT NULL,
  UPDATED_AT    TIMESTAMP_NTZ(3) NOT NULL,
  CONSTRAINT PK_PIPELINE_CONTROL PRIMARY KEY (PIPELINE_NAME)
)
COMMENT = 'Operator switches for the order-to-cash pipeline';

MERGE INTO OPS.PIPELINE_CONTROL t
USING (SELECT 'orders_cdc' AS PIPELINE_NAME) s
  ON t.PIPELINE_NAME = s.PIPELINE_NAME
WHEN NOT MATCHED THEN
  INSERT (PIPELINE_NAME, FAIL_NEXT_RUN, UPDATED_AT)
  VALUES (s.PIPELINE_NAME, FALSE, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
