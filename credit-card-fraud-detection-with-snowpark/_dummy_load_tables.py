# Run this Python script in order to replicate the script `00_load_tables.py`.
# This script only loads enough information to run subsequent sections of the
# demo.
#
# This is important in order to run `01_feature_engineering.py` after restarting
# `localstack/localstack-pro`, since the database is not persisted between runs.
#
# Using a separate Python script in order to avoid overwriting the workarounds
# and TODO statements added in-line for the actual Python demo.
#
# TODO: Leverage the LocalStack persistence functionality to avoid load data
# operations where possible. If the expected state file does not exist, then
# generate it once using this logic.

import logging
import os

from dotenv import load_dotenv
import snowflake.snowpark.functions as functions  # type: ignore
from snowflake.snowpark import Session  # type: ignore

# NOTE: Change this to run against Snowflake or LocalStack.
ENV_FILE = ".env.localstack"

logging.basicConfig(level=logging.INFO)
load_dotenv(ENV_FILE)


localstack_connection_parameters = {
    "account": os.getenv("ACCOUNTNAME"),
    "user": os.getenv("USERNAME"),
    "password": os.getenv("PASSWORD"),
    "database": os.getenv("DBNAME"),
    "host": os.getenv("HOST"),
}
session = Session.builder.configs(localstack_connection_parameters).create()

session.sql(f"CREATE OR REPLACE STAGE FRAUD_DATA").collect()

session.file.put(
    "data/fraud_transactions.min.csv.gz",
    f"@FRAUD_DATA",
    auto_compress=False,
    overwrite=True,
)

# Need to add in `OR REPLACE` in order to run idempotently
sql = """
CREATE OR REPLACE TABLE "CUSTOMER_TRANSACTIONS_FRAUD" AS
SELECT
  CAST("_COL1" AS INT) AS "TRANSACTION_ID",
  CAST("_COL2" AS TIMESTAMP) AS "TX_DATETIME",
  CAST("_COL3" AS INT) AS "CUSTOMER_ID",
  CAST("_COL4" AS INT) AS "TERMINAL_ID",
  CAST("_COL5" AS REAL) AS "TX_AMOUNT",
  CAST("_COL6" AS INT) AS "TX_TIME_SECONDS",
  CAST("_COL7" AS INT) AS "TX_TIME_DAYS",
  CAST("_COL8" AS INT) AS "TX_FRAUD",
  CAST("_COL9" AS INT) AS "TX_FRAUD_SCENARIO"
FROM
  "LOAD_DATA"(
    '@FRAUD_DATA/fraud_transactions.min.csv.gz',
    'KCAoRklMRV9GT1JNQVQgPT4gJyJ0ZXN0Ii4iUFVCTElDIi5TTk9XUEFSS19URU1QX0ZJTEVfRk9STUFUX1dMR0w4QzdLTFEnKQ=='
  ) AS "_TMP"(
    "_COL1" TEXT, "_COL2" TEXT, "_COL3" TEXT,
    "_COL4" TEXT, "_COL5" TEXT, "_COL6" TEXT,
    "_COL7" TEXT, "_COL8" TEXT, "_COL9" TEXT
  );
"""

session.sql(sql).collect()

dfCustTrxFraudTb = session.table("CUSTOMER_TRANSACTIONS_FRAUD")

dfCustomers = (
    dfCustTrxFraudTb.select(functions.col("CUSTOMER_ID"))
    .distinct()
    .sort(functions.col("CUSTOMER_ID"))
)

dfTerminals = (
    dfCustTrxFraudTb.select(functions.col("TERMINAL_ID"))
    .distinct()
    .sort(functions.col("TERMINAL_ID"))
)

ret2 = dfCustomers.write.mode("overwrite").saveAsTable("CUSTOMERS")

ret3 = dfTerminals.write.mode("overwrite").saveAsTable("TERMINALS")
