#!/usr/bin/env python3
"""
Credit Card Fraud Prediction - Loading Dataset using Snowpark Python

This example is based on the handbook "Machine Learning for Credit Card Fraud
Detection": https://fraud-detection-handbook.github.io/fraud-detection-handbook/
"""

import logging
import os

from dotenv import load_dotenv
import snowflake.snowpark as snowpark  # type: ignore
import snowflake.snowpark.functions as functions  # type: ignore
import snowflake.snowpark.types as types  # type: ignore
from snowflake.snowpark import Session  # type: ignore

# NOTE: Change this to run against Snowflake or LocalStack.
# ENV_FILE = ".env.snowflake"
ENV_FILE = ".env.localstack"

FRAUD_TRANSACTIONS_CSV_GZ = "fraud_transactions.min.csv.gz"

logging.basicConfig(level=logging.INFO)
load_dotenv(ENV_FILE)

# Loading Credit Card Transactions into Snowflake - import the dependencies and
# connect to Snowflake.

logging.info(f"Snowpark for Python version {snowpark.__version__}")

# NOTE: Change this to run against Snowflake or LocalStack.
# snowflake_connection_parameters = {
#     "account": os.getenv("ACCOUNTNAME"),
#     "user": os.getenv("USERNAME"),
#     "password": os.getenv("PASSWORD"),
#     "role": os.getenv("ROLENAME"),
#     "database": os.getenv("DBNAME"),
#     "schema": os.getenv("SCHEMANAME"),
#     "warehouse": os.getenv("WAREHOUSENAME"),
# }
# session = Session.builder.configs(snowflake_connection_parameters).create()

localstack_connection_parameters = {
    "account": os.getenv("ACCOUNTNAME"),
    "user": os.getenv("USERNAME"),
    "password": os.getenv("PASSWORD"),
    "database": os.getenv("DBNAME"),
    "host": os.getenv("HOST"),
}
session = Session.builder.configs(localstack_connection_parameters).create()

# Define Staging Area and the Schema for the transaction table
#
# Using SQL we can create a internal stage and then use the **put** function to
# upload the **fraud_transactions.csv.gz** file to it.

stage_name = "FRAUD_DATA"

# Create a internal staging area for uploading the source file
session.sql(f"CREATE OR REPLACE STAGE {stage_name}").collect()

# Upload the source file to the stage
putResult = session.file.put(
    f"data/{FRAUD_TRANSACTIONS_CSV_GZ}",
    f"@{stage_name}",
    auto_compress=False,
    overwrite=True,
)

logging.info(putResult)

# Define the schema for our **CUSTOMER_TRANSACTIONS_FRAUD** table.

dfCustTrxFraudSchema = types.StructType(
    [
        types.StructField("TRANSACTION_ID", types.IntegerType()),
        types.StructField("TX_DATETIME", types.TimestampType()),
        types.StructField("CUSTOMER_ID", types.IntegerType()),
        types.StructField("TERMINAL_ID", types.IntegerType()),
        types.StructField("TX_AMOUNT", types.FloatType()),
        types.StructField("TX_TIME_SECONDS", types.IntegerType()),
        types.StructField("TX_TIME_DAYS", types.IntegerType()),
        types.StructField("TX_FRAUD", types.IntegerType()),
        types.StructField("TX_FRAUD_SCENARIO", types.IntegerType()),
    ]
)

# Load the CSV gzip to a DataFrame reader and save into a table

# Create a reader
dfReader = session.read.schema(dfCustTrxFraudSchema)

# Get the data into the data frame
dfCustTrxFraudRd = dfReader.csv(f"@{stage_name}/{FRAUD_TRANSACTIONS_CSV_GZ}")

########## BEGIN LOCALSTACK WORKAROUND ##########

# NOTE: Stop execution before this line, using `import ipdb; ipdb.set_trace()`,
# and run the commented-out SQL solution manually, before continuing execution.

# # Write the dataframe in a table
# ret = dfCustTrxFraudRd.write.mode("overwrite").saveAsTable(
#     "CUSTOMER_TRANSACTIONS_FRAUD"
# )

# Currently, the commented-out line of Python runs the following SQL against
# PostgreSQL:
#
# ```sql
# CREATE TABLE "CUSTOMER_TRANSACTIONS_FRAUD" (
#   "TRANSACTION_ID", "TX_DATETIME",
#   "CUSTOMER_ID", "TERMINAL_ID", "TX_AMOUNT" NOT NULL,
#   "TX_TIME_SECONDS", "TX_TIME_DAYS",
#   "TX_FRAUD", "TX_FRAUD_SCENARIO"
# ) AS
# SELECT
#   *
# FROM
#   (
#     SELECT
#       CAST("_COL1" AS INT) AS "TRANSACTION_ID",
#       CAST("_COL2" AS TIMESTAMP) AS "TX_DATETIME",
#       CAST("_COL3" AS INT) AS "CUSTOMER_ID",
#       CAST("_COL4" AS INT) AS "TERMINAL_ID",
#       CAST("_COL5" AS REAL) AS "TX_AMOUNT",
#       CAST("_COL6" AS INT) AS "TX_TIME_SECONDS",
#       CAST("_COL7" AS INT) AS "TX_TIME_DAYS",
#       CAST("_COL8" AS INT) AS "TX_FRAUD",
#       CAST("_COL9" AS INT) AS "TX_FRAUD_SCENARIO"
#     FROM
#       "LOAD_DATA"(
#         '@FRAUD_DATA/fraud_transactions.min.csv.gz',
#         'KCAoRklMRV9GT1JNQVQgPT4gJyJ0ZXN0Ii4iUFVCTElDIi5TTk9XUEFSS19URU1QX0ZJTEVfRk9STUFUX1dMR0w4QzdLTFEnKQ=='
#       ) AS "_TMP"(
#         "_COL1" TEXT, "_COL2" TEXT, "_COL3" TEXT,
#         "_COL4" TEXT, "_COL5" TEXT, "_COL6" TEXT,
#         "_COL7" TEXT, "_COL8" TEXT, "_COL9" TEXT
#       )
#   ) AS _tmp
# ```
#
# Instead, connect to the underlying PostgreSQL instance, and run this SQL
# instead:
#
# ```sql
# CREATE TABLE "CUSTOMER_TRANSACTIONS_FRAUD" AS
# SELECT
#   CAST("_COL1" AS INT) AS "TRANSACTION_ID",
#   CAST("_COL2" AS TIMESTAMP) AS "TX_DATETIME",
#   CAST("_COL3" AS INT) AS "CUSTOMER_ID",
#   CAST("_COL4" AS INT) AS "TERMINAL_ID",
#   CAST("_COL5" AS REAL) AS "TX_AMOUNT",
#   CAST("_COL6" AS INT) AS "TX_TIME_SECONDS",
#   CAST("_COL7" AS INT) AS "TX_TIME_DAYS",
#   CAST("_COL8" AS INT) AS "TX_FRAUD",
#   CAST("_COL9" AS INT) AS "TX_FRAUD_SCENARIO"
# FROM
#   "LOAD_DATA"(
#     '@FRAUD_DATA/fraud_transactions.min.csv.gz',
#     'KCAoRklMRV9GT1JNQVQgPT4gJyJ0ZXN0Ii4iUFVCTElDIi5TTk9XUEFSS19URU1QX0ZJTEVfRk9STUFUX1dMR0w4QzdLTFEnKQ=='
#   ) AS "_TMP"(
#     "_COL1" TEXT, "_COL2" TEXT, "_COL3" TEXT,
#     "_COL4" TEXT, "_COL5" TEXT, "_COL6" TEXT,
#     "_COL7" TEXT, "_COL8" TEXT, "_COL9" TEXT
#   );
# ```

########## END LOCALSTACK WORKAROUND ##########

# Read the data from the staging area and create CUSTOMER_TRANSACTIONS_FRAUD,
# CUSTOMERS and TERMINALS tables

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

logging.info(dfCustTrxFraudTb.show())


########## BEGIN LOCALSTACK TODO ##########

# No action needed immediately, but the LocalStack Snowflake emulator currently
# does not close any sessions, as function `CANCEL_ALL_QUERIES` does not exist.
# This does not block execution of the demo, but may be a problem for
# long-running Snowflake emulators. Trace below:
#
# ```bash
# Traceback (most recent call last):
#   File "/usr/local/lib/python3.8/site-packages/snowflake/snowpark/session.py", line 507, in close
#     self.cancel_all()
#   File "/usr/local/lib/python3.8/site-packages/snowflake/snowpark/session.py", line 593, in cancel_all
#     self._conn.run_query(f"select system$cancel_all_queries({self._session_id})")
#   File "/usr/local/lib/python3.8/site-packages/snowflake/snowpark/_internal/server_connection.py", line 123, in wrap
#     raise ex
#   File "/usr/local/lib/python3.8/site-packages/snowflake/snowpark/_internal/server_connection.py", line 117, in wrap
#     return func(*args, **kwargs)
#   File "/usr/local/lib/python3.8/site-packages/snowflake/snowpark/_internal/server_connection.py", line 417, in run_query
#     raise ex
#   File "/usr/local/lib/python3.8/site-packages/snowflake/snowpark/_internal/server_connection.py", line 402, in run_query
#     results_cursor = self.execute_and_notify_query_listener(
#   File "/usr/local/lib/python3.8/site-packages/snowflake/snowpark/_internal/server_connection.py", line 354, in execute_and_notify_query_listener
#     results_cursor = self._cursor.execute(query, **kwargs)
#   File "/usr/local/lib/python3.8/site-packages/snowflake/connector/cursor.py", line 1080, in execute
#     Error.errorhandler_wrapper(self.connection, self, error_class, errvalue)
#   File "/usr/local/lib/python3.8/site-packages/snowflake/connector/errors.py", line 290, in errorhandler_wrapper
#     handed_over = Error.hand_to_other_handler(
#   File "/usr/local/lib/python3.8/site-packages/snowflake/connector/errors.py", line 345, in hand_to_other_handler
#     cursor.errorhandler(connection, cursor, error_class, error_value)
#   File "/usr/local/lib/python3.8/site-packages/snowflake/connector/errors.py", line 221, in default_errorhandler
#     raise error_class(
# snowflake.connector.errors.ProgrammingError: 002002 (42710): b74129c0-0470-46eb-8837-0969d2c400c7: function SYSTEM$CANCEL_ALL_QUERIES(integer) does not exist
# ```

########## END LOCALSTACK TODO ##########

session.close()
