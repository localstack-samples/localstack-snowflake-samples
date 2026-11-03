-- Order-to-Cash CDC: the apply procedure, the stream-driven task and the mart.
USE ROLE ACCOUNTADMIN;
USE DATABASE O2C;

-- @deploy: everything below this line is database-relative.

-- Applies every unprocessed change event in one transaction:
--   1. parse the stream into OPS.APPLY_BATCH (this read consumes the stream),
--   2. dead-letter invalid events,
--   3. MERGE orders into CORE.ORDERS (newest LSN wins, soft delete),
--   4. SCD2 MERGE customers into CORE.DIM_CUSTOMER_HISTORY,
--   5. COMMIT, which is also when the stream offset advances.
-- Any error rolls the whole run back, so the stream keeps its offset and the
-- next run sees the same events again. Every MERGE is guarded by SOURCE_LSN,
-- which makes re-applying an already applied event a no-op.
CREATE OR REPLACE PROCEDURE OPS.APPLY_ORDERS_CDC()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  injected_failure EXCEPTION (-20001, 'Injected failure: OPS.PIPELINE_CONTROL.FAIL_NEXT_RUN is TRUE for orders_cdc');
  -- The control flag is read through a declared cursor.
  control_cur CURSOR FOR
    SELECT FAIL_NEXT_RUN FROM OPS.PIPELINE_CONTROL WHERE PIPELINE_NAME = 'orders_cdc';
  fail_next_run BOOLEAN DEFAULT FALSE;
  events_read INTEGER DEFAULT 0;
  dead_lettered INTEGER DEFAULT 0;
  orders_applied INTEGER DEFAULT 0;
  customer_rows INTEGER DEFAULT 0;
BEGIN
  BEGIN TRANSACTION;

  DELETE FROM OPS.APPLY_BATCH;

  INSERT INTO OPS.APPLY_BATCH (
    KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, SOURCE_TABLE, OP, ENTITY_KEY,
    SOURCE_LSN, SOURCE_TS, IMAGE, REJECT_REASON, RECORD_CONTENT, RECORD_METADATA
  )
  SELECT
    kafka_topic, kafka_partition, kafka_offset, source_table, op, entity_key,
    source_lsn, source_ts, image,
    CASE
      WHEN source_table IS NULL OR source_table NOT IN ('orders', 'customer') THEN 'UNKNOWN_SOURCE_TABLE'
      WHEN op IS NULL OR op NOT IN ('c', 'r', 'u', 'd') THEN 'UNKNOWN_OP'
      WHEN source_lsn IS NULL THEN 'MISSING_LSN'
      WHEN source_ts IS NULL THEN 'MISSING_SOURCE_TS'
      WHEN entity_key IS NULL THEN 'MISSING_KEY'
    END,
    record_content, record_metadata
  FROM (
    SELECT
      e.*,
      CASE e.source_table
        WHEN 'orders' THEN e.image:o_orderkey::NUMBER(38, 0)
        WHEN 'customer' THEN e.image:c_custkey::NUMBER(38, 0)
      END AS entity_key
    FROM (
      SELECT
        s.RECORD_METADATA:topic::VARCHAR AS kafka_topic,
        s.RECORD_METADATA:partition::NUMBER(38, 0) AS kafka_partition,
        s.RECORD_METADATA:offset::NUMBER(38, 0) AS kafka_offset,
        s.RECORD_CONTENT:source:table::VARCHAR AS source_table,
        s.RECORD_CONTENT:op::VARCHAR AS op,
        s.RECORD_CONTENT:source:lsn::NUMBER(38, 0) AS source_lsn,
        TO_TIMESTAMP_NTZ(s.RECORD_CONTENT:source:ts_ms::NUMBER(38, 0), 3) AS source_ts,
        -- Debezium puts the row in "after" for c/r/u and in "before" for d.
        IFF(s.RECORD_CONTENT:op::VARCHAR = 'd', s.RECORD_CONTENT:before, s.RECORD_CONTENT:after) AS image,
        s.RECORD_CONTENT AS record_content,
        s.RECORD_METADATA AS record_metadata
      FROM RAW.ORDERS_CDC_STREAM s
      WHERE s.METADATA$ACTION = 'INSERT'
    ) e
  ) p;
  events_read := SQLROWCOUNT;

  MERGE INTO OPS.DEAD_LETTER t
  USING (
    SELECT * FROM OPS.APPLY_BATCH WHERE REJECT_REASON IS NOT NULL
  ) s
    ON  t.KAFKA_TOPIC = s.KAFKA_TOPIC
    AND t.KAFKA_PARTITION = s.KAFKA_PARTITION
    AND t.KAFKA_OFFSET = s.KAFKA_OFFSET
  WHEN NOT MATCHED THEN
    INSERT (KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, REASON, RECORD_CONTENT, RECORD_METADATA, DEAD_LETTERED_AT)
    VALUES (s.KAFKA_TOPIC, s.KAFKA_PARTITION, s.KAFKA_OFFSET, s.REJECT_REASON, s.RECORD_CONTENT, s.RECORD_METADATA,
            CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
  dead_lettered := SQLROWCOUNT;

  -- Current state: keep the newest event per order key in this batch (this
  -- also collapses redelivered duplicates), then apply it only if it is newer
  -- than what the table already holds (late events lose).
  MERGE INTO CORE.ORDERS t
  USING (
    SELECT
      ENTITY_KEY AS o_orderkey,
      IMAGE:o_custkey::NUMBER(38, 0) AS o_custkey,
      IMAGE:o_orderstatus::VARCHAR AS o_orderstatus,
      IMAGE:o_totalprice::NUMBER(12, 2) AS o_totalprice,
      -- Debezium encodes DATE columns as days since the epoch.
      DATEADD(DAY, IMAGE:o_orderdate::INTEGER, '1970-01-01'::DATE) AS o_orderdate,
      IMAGE:o_orderpriority::VARCHAR AS o_orderpriority,
      IMAGE:o_clerk::VARCHAR AS o_clerk,
      OP = 'd' AS is_deleted,
      SOURCE_LSN AS source_lsn,
      SOURCE_TS AS source_ts
    FROM OPS.APPLY_BATCH
    WHERE SOURCE_TABLE = 'orders' AND REJECT_REASON IS NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ENTITY_KEY ORDER BY SOURCE_LSN DESC, KAFKA_PARTITION, KAFKA_OFFSET
    ) = 1
  ) s
    ON t.O_ORDERKEY = s.o_orderkey
  WHEN MATCHED AND s.source_lsn > t.SOURCE_LSN THEN UPDATE SET
    O_CUSTKEY = s.o_custkey,
    O_ORDERSTATUS = s.o_orderstatus,
    O_TOTALPRICE = s.o_totalprice,
    O_ORDERDATE = s.o_orderdate,
    O_ORDERPRIORITY = s.o_orderpriority,
    O_CLERK = s.o_clerk,
    IS_DELETED = s.is_deleted,
    SOURCE_LSN = s.source_lsn,
    SOURCE_TS = s.source_ts,
    APPLIED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  -- A delete for a key never seen before still inserts a tombstone (from the
  -- "before" image), so a late create cannot bring the order back.
  WHEN NOT MATCHED THEN INSERT (
    O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY,
    O_CLERK, IS_DELETED, SOURCE_LSN, SOURCE_TS, APPLIED_AT
  ) VALUES (
    s.o_orderkey, s.o_custkey, s.o_orderstatus, s.o_totalprice, s.o_orderdate, s.o_orderpriority,
    s.o_clerk, s.is_deleted, s.source_lsn, s.source_ts, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  );
  orders_applied := SQLROWCOUNT;

  -- SCD2: every customer event newer than the newest applied version becomes
  -- a version (deletes only close). The source rows are:
  --   * one row per new version with a NULL merge key (never matches: INSERT),
  --   * one row per customer with its key (matches the open version: close it
  --     at the timestamp of the first change in this batch).
  MERGE INTO CORE.DIM_CUSTOMER_HISTORY t
  USING (
    WITH applied AS (
      SELECT C_CUSTKEY, MAX(SOURCE_LSN) AS max_lsn
      FROM CORE.DIM_CUSTOMER_HISTORY
      GROUP BY C_CUSTKEY
    ),
    changes AS (
      SELECT b.*
      FROM OPS.APPLY_BATCH b
      LEFT JOIN applied a ON a.C_CUSTKEY = b.ENTITY_KEY
      WHERE b.SOURCE_TABLE = 'customer'
        AND b.REJECT_REASON IS NULL
        AND (a.max_lsn IS NULL OR b.SOURCE_LSN > a.max_lsn)
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY b.ENTITY_KEY, b.SOURCE_LSN ORDER BY b.KAFKA_PARTITION, b.KAFKA_OFFSET
      ) = 1
    ),
    versions AS (
      SELECT
        c.*,
        LEAD(c.SOURCE_TS) OVER (PARTITION BY c.ENTITY_KEY ORDER BY c.SOURCE_LSN) AS next_ts,
        ROW_NUMBER() OVER (PARTITION BY c.ENTITY_KEY ORDER BY c.SOURCE_LSN) AS change_seq
      FROM changes c
    )
    SELECT
      NULL::NUMBER(38, 0) AS merge_key,
      ENTITY_KEY AS c_custkey,
      IMAGE:c_name::VARCHAR AS c_name,
      IMAGE:c_nationkey::NUMBER(38, 0) AS c_nationkey,
      IMAGE:c_acctbal::NUMBER(12, 2) AS c_acctbal,
      IMAGE:c_mktsegment::VARCHAR AS c_mktsegment,
      SOURCE_TS AS valid_from,
      next_ts AS valid_to,
      SOURCE_LSN AS source_lsn
    FROM versions
    WHERE OP <> 'd'
    UNION ALL
    SELECT
      ENTITY_KEY, ENTITY_KEY, NULL, NULL, NULL, NULL, SOURCE_TS, NULL, SOURCE_LSN
    FROM versions
    WHERE change_seq = 1
  ) s
    ON t.C_CUSTKEY = s.merge_key AND t.IS_CURRENT
  WHEN MATCHED THEN UPDATE SET
    VALID_TO = s.valid_from,
    IS_CURRENT = FALSE
  WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT (
    C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL, C_MKTSEGMENT, VALID_FROM, VALID_TO, IS_CURRENT, SOURCE_LSN
  ) VALUES (
    s.c_custkey, s.c_name, s.c_nationkey, s.c_acctbal, s.c_mktsegment, s.valid_from, s.valid_to,
    s.valid_to IS NULL, s.source_lsn
  );
  customer_rows := SQLROWCOUNT;

  -- Failure injection point: after every merge, before COMMIT.
  OPEN control_cur;
  FETCH control_cur INTO fail_next_run;
  CLOSE control_cur;
  IF (fail_next_run) THEN
    RAISE injected_failure;
  END IF;

  COMMIT;
  RETURN 'events_read=' || events_read || ' dead_lettered=' || dead_lettered ||
         ' orders_applied=' || orders_applied || ' customer_rows=' || customer_rows;
EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    RAISE;
END;
$$;

-- Runs the procedure whenever the stream has data. Created suspended, as
-- Snowflake does; tests and the demo trigger runs with EXECUTE TASK. To run it
-- on its schedule: ALTER TASK OPS.APPLY_ORDERS_CDC RESUME;
CREATE OR REPLACE TASK OPS.APPLY_ORDERS_CDC
  WAREHOUSE = O2C_WH
  SCHEDULE = '1 MINUTE'
  COMMENT = 'Apply new change events from RAW.ORDERS_CDC_STREAM'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.ORDERS_CDC_STREAM')
AS
  CALL OPS.APPLY_ORDERS_CDC();

-- Daily bookings by market segment, from live (not deleted) orders and each
-- customer's current segment. Orders whose customer has no open version
-- (deleted or not yet seen) fall into UNKNOWN.
CREATE OR REPLACE DYNAMIC TABLE MART.DAILY_BOOKINGS
  TARGET_LAG = '1 minute'
  WAREHOUSE = O2C_WH
  COMMENT = 'Order count and gross bookings per order date and market segment'
AS
  SELECT
    o.O_ORDERDATE AS BOOKING_DATE,
    COALESCE(c.C_MKTSEGMENT, 'UNKNOWN') AS MARKET_SEGMENT,
    COUNT(*) AS ORDER_COUNT,
    SUM(o.O_TOTALPRICE) AS GROSS_BOOKINGS
  FROM CORE.ORDERS o
  LEFT JOIN CORE.DIM_CUSTOMER_HISTORY c
    ON c.C_CUSTKEY = o.O_CUSTKEY AND c.IS_CURRENT
  WHERE NOT o.IS_DELETED
  GROUP BY o.O_ORDERDATE, COALESCE(c.C_MKTSEGMENT, 'UNKNOWN');
