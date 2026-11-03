-- Reference data: the three public plans, as graduated tiers (idempotent).
--
--   FREE        first 10,000 calls free, then $2.00 per 1,000
--   PRO         $49 per month incl. 100,000 calls, $0.80 per 1,000 up to 1M, then $0.50
--   ENTERPRISE  $499 per month incl. 1M calls, $0.40 per 1,000 up to 10M, then $0.25
--
-- Unit prices are in micro-dollars per call ($0.80 per 1,000 calls = 800).

USE ROLE METERING_APP;
USE DATABASE METERING;
USE SCHEMA BILLING;

MERGE INTO PRICE_PLANS t
USING (
  SELECT * FROM VALUES
    ('FREE', 1, 10000, 0, 0),
    ('FREE', 2, NULL, 2000, 0),
    ('PRO', 1, 100000, 0, 4900),
    ('PRO', 2, 1000000, 800, 0),
    ('PRO', 3, NULL, 500, 0),
    ('ENTERPRISE', 1, 1000000, 0, 49900),
    ('ENTERPRISE', 2, 10000000, 400, 0),
    ('ENTERPRISE', 3, NULL, 250, 0)
    AS v (PLAN_ID, TIER_NO, UP_TO_CALLS, UNIT_PRICE_MICROS, FLAT_FEE_CENTS)
) s
ON t.PLAN_ID = s.PLAN_ID AND t.TIER_NO = s.TIER_NO
WHEN MATCHED THEN UPDATE SET
  UP_TO_CALLS = s.UP_TO_CALLS,
  UNIT_PRICE_MICROS = s.UNIT_PRICE_MICROS,
  FLAT_FEE_CENTS = s.FLAT_FEE_CENTS
WHEN NOT MATCHED THEN INSERT (PLAN_ID, TIER_NO, UP_TO_CALLS, UNIT_PRICE_MICROS, FLAT_FEE_CENTS)
  VALUES (s.PLAN_ID, s.TIER_NO, s.UP_TO_CALLS, s.UNIT_PRICE_MICROS, s.FLAT_FEE_CENTS);
