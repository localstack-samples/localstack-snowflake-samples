#!/usr/bin/env python3
"""
Credit Card Fraud Prediction - Feature Engineering

This example is based on the handbook "Machine Learning for Credit Card Fraud
Detection": https://fraud-detection-handbook.github.io/fraud-detection-handbook/

NOTE: Currently cannot replicate the X11-based settings for matplotlib, so
removing mentions of that. Wait to migrate to Docker image
quay.io/jupyter/datascience-notebook.
"""

########## BEGIN LOCALSTACK WORKAROUND ##########

# Run `00_load_tables.dummy.py` before running this file, in order to set up
# prior Snowflake emulator state from scratch.
import _dummy_load_tables

########## END LOCALSTACK WORKAROUND ##########

import logging
import os

from dotenv import load_dotenv
import pandas
import snowflake.snowpark as snowpark  # type: ignore
import snowflake.snowpark.functions as functions  # type: ignore
import snowflake.snowpark.types as types  # type: ignore
from snowflake.snowpark import Session, Window  # type: ignore

# NOTE: Change this to run against Snowflake or LocalStack.
# ENV_FILE = ".env.snowflake"
ENV_FILE = ".env.localstack"

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

# Define a DataFrame
#
# We start by defining a Snowpark Dataframe that reference the
# **CUSTOMER_TRANSACTIONS_FRAUD** table in our database. No data will be pulled
# back and the **dfCustTrxFraud** is basicly only containing the SQL needed to
# use the table. The below image gives a illustration on what type of data it
# has.
#
# Using the **show** command brings back 10 rows to the client, meaning the SQL
# to use the table are executed in Snowflake.

df_cust_trx_fraud = session.table("CUSTOMER_TRANSACTIONS_FRAUD")
logging.info(df_cust_trx_fraud.show())

# Data Understanding
#
# Let's start by getting some basic understanding of our data.
#
# We can use the **describe** function on our **numeric** columns to get some
# basic statistics. The only column in our current dataset would be TX_AMOUNT.

import ipdb
ipdb.set_trace()

logging.info(df_cust_trx_fraud.describe().show())

# We can see that we have about 6 months  of transactions and around 9k
# transactions/day.

########## BEGIN LOCALSTACK WORKAROUND ##########

# NOTE: Stop execution before this line, using `import ipdb; ipdb.set_trace()`,
# and run the commented-out SQL solution manually, before continuing execution.

# # Let's count the number of fraudulent and none fraudulent transactions, by
# # using the **call_function** function we can also use the use
# # **RATIO_TO_REPORT** function (that currently is not exposed in the Snowpark
# # API) to also get percentages
# logging.info(
#     df_cust_trx_fraud.group_by(functions.col("TX_FRAUD"))
#     .agg(functions.count(functions.col("TRANSACTION_ID")).as_("NB_TX_DAY"))
#     .select(
#         functions.col("TX_FRAUD"),
#         functions.col("NB_TX_DAY"),
#         (
#             functions.call_function(
#                 "RATIO_TO_REPORT", functions.col("NB_TX_DAY")
#             ).over()
#             * 100
#         ).as_("percentage"),
#     )
#     .show()
# )
#
# LocalStack Snowflake emulator SQL:
#
# ```sql
# SELECT
#   "TX_FRAUD",
#   "NB_TX_DAY",
#   (
#     CAST(
#       ROUND(
#         CAST("NB_TX_DAY" AS DECIMAL) / sum("NB_TX_DAY") OVER (),
#         6
#       ) AS TEXT
#     ) * CAST(100 AS INT)
#   ) AS "PERCENTAGE"
# FROM
#   (
#     SELECT
#       "TX_FRAUD",
#       COUNT("TRANSACTION_ID") AS "NB_TX_DAY"
#     FROM
#       (
#         SELECT
#           *
#         FROM
#           "CUSTOMER_TRANSACTIONS_FRAUD"
#       ) AS _tmpc2e410ae
#     GROUP BY
#       "TX_FRAUD"
#   ) AS _tm p436ba9cf
# LIMIT
#   10
# ```
#
# Snowflake SQL:
#
# ```sql
# SELECT
#   "TX_FRAUD",
#   "NB_TX_DAY",
#   (
#     RATIO_TO_REPORT("NB_TX_DAY") OVER () * 100 :: INT
#   ) AS "PERCENTAGE"
# FROM
#   (
#     SELECT
#       "TX_FRAUD",
#       count("TRANSACTION_ID") AS "NB_TX_DAY"
#     FROM
#       (
#         SELECT
#           *
#         FROM
#           CUSTOMER_TRANSACTIONS_FRAUD
#       )
#     GROUP BY
#       "TX_FRAUD"
#   )
# ```
#
result1_as_pd_df = pandas.read_parquet(
    "tempdata/01_feature_engineering.result1.parquet"
)
# TODO: Implement Snowpark dataframe using schema=result1_schema. This currently
# does not work because using SnowPark types makes a database call to run
# `CREATE SCOPED TEMP TABLE`, while Pandas columns do not.
#
# result1_schema = types.StructType(
#     [
#         types.StructField("TX_FRAUD", types.LongType(), nullable=True),
#         types.StructField("NB_TX_DAY", types.LongType(), nullable=False),
#         types.StructField("PERCENTAGE", types.DecimalType(38, 6), nullable=True),
#     ]
# )
# result1 = session.create_dataframe(
#     result1_as_pd_df.values.tolist(), schema=result1_schema
# )
result1 = session.create_dataframe(
    result1_as_pd_df.values.tolist(), schema=result1_as_pd_df.columns.tolist()
)
logging.info(result1.show())

########## END LOCALSTACK WORKAROUND ##########


pd_day_by_card = (
    df_cust_trx_fraud.filter(functions.col("TX_FRAUD") == 1)
    .group_by(functions.to_date(functions.col("TX_DATETIME")))
    .agg(
        [
            functions.sum(functions.col("TX_FRAUD")).as_("NBR_FRAUD_TRX"),
            functions.count_distinct(functions.col("CUSTOMER_ID")).as_(
                "NBR_FRAUD_CARD"
            ),
        ]
    )
    .sort(functions.col("TO_DATE(TX_DATETIME)"))
    .to_pandas()
)

logging.info(pd_day_by_card)


# Date and time transformations
#
# We will create two new binary features from the transaction dates and times:
#
# The first will characterize whether a transaction occurs during a weekday
# (value 0) or a weekend (1), and will be called TX_DURING_WEEKEND
#
# The second will characterize whether a transaction occurs during the day (0)
# or during the night (1). The night is defined as hours that are between 20pm
# and 6am. It will be called TX_DURING_NIGHT.
#
# This can be done using the built in date functions in Snowflake that are
# exposed in the Snowpark API and will be based on the **TX_DATETIME** column,
# as illustrated in the image below.
def gen_time_feat(df):
    df_date_time_feat = df.with_columns(
        ["TX_DURING_WEEKEND", "TX_DURING_NIGHT"],
        [
            functions.iff(
                (functions.dayofweek(functions.col("TX_DATETIME")) == functions.lit(6))
                | (
                    functions.dayofweek(functions.col("TX_DATETIME"))
                    == functions.lit(0)
                ),
                functions.lit(1),
                functions.lit(0),
            ),
            functions.iff(
                (functions.hour(functions.col("TX_DATETIME")) < functions.lit(6))
                | (functions.hour(functions.col("TX_DATETIME")) > functions.lit(20)),
                functions.lit(1),
                functions.lit(0),
            ),
        ],
    )
    return df_date_time_feat


########## BEGIN LOCALSTACK WORKAROUND ##########

# NOTE: Stop execution before this line, using `import ipdb; ipdb.set_trace()`,
# and run the commented-out SQL solution manually, before continuing execution.

# df_date_time_feat = gen_time_feat(df_cust_trx_fraud)
#
# LocalStack Snowflake emulator SQL:
#
# ```sql
# SELECT
#   "TRANSACTION_ID",
#   "TX_DATETIME",
#   "CUSTOMER_ID",
#   "TERMINAL_ID",
#   "TX_AMOUNT",
#   "TX_TIME_SECONDS",
#   "TX_TIME_DAYS",
#   "TX_FRAUD",
#   "TX_FRAUD_SCENARIO",
#   CASE WHEN (
#     (
#       "DAYOFWEEK"("TX_DATETIME") = CAST(6 AS INT)
#     )
#     OR (
#       "DAYOFWEEK"("TX_DATETIME") = CAST(0 AS INT)
#     )
#   ) THEN 1 ELSE 0 END AS "TX_DURING_WEEKEND",
#   CASE WHEN (
#     (
#       "HOUR"("TX_DATETIME") < CAST(6 AS INT)
#     )
#     OR (
#       "HOUR"("TX_DATETIME") > CAST(20 AS INT)
#     )
#   ) THEN 1 ELSE 0 END AS "TX_DURING_NIGHT"
# FROM
#   "CUSTOMER_TRANSACTIONS_FRAUD"
# LIMIT
#   10
# ```
#
# Snowflake SQL
#
# ```sql
# SELECT
#   "TRANSACTION_ID",
#   "TX_DATETIME",
#   "CUSTOMER_ID",
#   "TERMINAL_ID",
#   "TX_AMOUNT",
#   "TX_TIME_SECONDS",
#   "TX_TIME_DAYS",
#   "TX_FRAUD",
#   "TX_FRAUD_SCENARIO",
#   iff(
#     (
#       (
#         dayofweek("TX_DATETIME") = 6 :: INT
#       )
#       OR (
#         dayofweek("TX_DATETIME") = 0 :: INT
#       )
#     ),
#     1,
#     0
#   ) AS "TX_DURING_WEEKEND",
#   iff(
#     (
#       (
#         hour("TX_DATETIME") < 6 :: INT
#       )
#       OR (
#         hour("TX_DATETIME") > 20 :: INT
#       )
#     ),
#     1,
#     0
#   ) AS "TX_DURING_NIGHT"
# FROM
#   CUSTOMER_TRANSACTIONS_FRAUD
# ```
#

########## WHERE I STOPPED FILING BUGS ##########
########## WHERE I STOPPED FILING BUGS ##########
########## WHERE I STOPPED FILING BUGS ##########
df_date_time_feat_as_pd_df = pandas.read_parquet(
    "tempdata/01_feature_engineering.df_date_time_feat.parquet"
)
df_date_time_feat_schema = types.StructType(
    [
        types.StructField("TRANSACTION_ID", types.LongType(), nullable=True),
        types.StructField(
            "TX_DATETIME", types.TimestampType(tz=types.ntz), nullable=True
        ),
        types.StructField("CUSTOMER_ID", types.LongType(), nullable=True),
        types.StructField("TERMINAL_ID", types.LongType(), nullable=True),
        types.StructField("TX_AMOUNT", types.DoubleType(), nullable=True),
        types.StructField("TX_TIME_SECONDS", types.LongType(), nullable=True),
        types.StructField("TX_TIME_DAYS", types.LongType(), nullable=True),
        types.StructField("TX_FRAUD", types.LongType(), nullable=True),
        types.StructField("TX_FRAUD_SCENARIO", types.LongType(), nullable=True),
        types.StructField("TX_DURING_WEEKEND", types.LongType(), nullable=False),
        types.StructField("TX_DURING_NIGHT", types.LongType(), nullable=False),
    ]
)

df_date_time_feat = session.create_dataframe(
    df_date_time_feat_as_pd_df.values.tolist(),
    schema=df_date_time_feat_as_pd_df.columns.to_list(),
)

########## END LOCALSTACK WORKAROUND ##########

logging.debug(df_date_time_feat.sort(functions.col("TRANSACTION_ID")).show())

########## BEGIN LOCALSTACK WORKAROUND ##########

# NOTE: Stop execution before this line, using `import ipdb; ipdb.set_trace()`,
# and run the commented-out SQL solution manually, before continuing execution.

# # Is there differences between the number of fraud cases during
# # weekdays/weekdays and day/Night?
#
# window = Window.partition_by(
#     functions.col("TX_DURING_WEEKEND"), functions.col("TX_DURING_NIGHT")
# )
# result_one = (
#     df_date_time_feat.group_by(
#         functions.col("TX_DURING_WEEKEND"),
#         functions.col("TX_DURING_NIGHT"),
#         functions.col("TX_FRAUD"),
#     )
#     .count()
#     .sort(
#         functions.col("TX_DURING_WEEKEND"),
#         functions.col("TX_DURING_NIGHT"),
#         functions.col("TX_FRAUD"),
#     )
#     .select(
#         functions.col("TX_DURING_WEEKEND"),
#         functions.col("TX_DURING_NIGHT"),
#         functions.col("TX_FRAUD"),
#         (
#             functions.call_function("RATIO_TO_REPORT", functions.col("COUNT")).over(
#                 window
#             )
#             * 100
#         ).as_("percentage"),
#     )
#     .sort(
#         functions.col("TX_DURING_WEEKEND"),
#         functions.col("TX_DURING_NIGHT"),
#         functions.col("TX_FRAUD"),
#     )
# )
# logging.info(result_one.show())
#
# # We can see that basicly the number of fradulent transactions are the same
# # during the days


# Currently, the commented-out line of Python runs the following SQL against
# PostgreSQL:
#
# ```sql
# SELECT
#   "TX_DURING_WEEKEND",
#   "TX_DURING_NIGHT",
#   "TX_FRAUD",
#   COUNT(1) AS "COUNT"
# FROM
#   (
#     SELECT
#       "TRANSACTION_ID",
#       "TX_DATETIME",
#       "CUSTOMER_ID",
#       "TERMINAL_ID",
#       "TX_AMOUNT",
#       "TX_TIME_SECONDS",
#       "TX_TIME_DAYS",
#       "TX_FRAUD",
#       "TX_FRAUD_SCENARIO",
#       CASE WHEN (
#         (
#           "DAYOFWEEK"("TX_DATETIME") = CAST(6 AS INT)
#         )
#         OR (
#           "DAYOFWEEK"("TX_DATETIME") = CAST(0 AS INT)
#         )
#       ) THEN 1 ELSE 0 END AS "TX_DURING_WEEKEND",
#       CASE WHEN (
#         (
#           "HOUR"("TX_DATETIME") < CAST(6 AS INT)
#         )
#         OR (
#           "HOUR"("TX_DATETIME") > CAST(20 AS INT)
#         )
#       ) THEN 1 ELSE 0 END AS "TX_DURING_NIGHT"
#     FROM
#       (
#         SELECT
#           *
#         FROM
#           "CUSTOMER_TRANSACTIONS_FRAUD"
#       ) AS _tmpe7b1e2f3
#   ) AS _tmp62e9da4e
# GROUP BY
#   "TX_DURING_WEEKEND",
#   "TX_DURING_NIGHT",
#   "TX_FRAUD"
# ```
#
# TODO: Implement workaround

########## END LOCALSTACK WORKAROUND ##########


########## BEGIN LOCALSTACK WORKAROUND ##########

# NOTE: Stop execution before this line, using `import ipdb; ipdb.set_trace()`,
# and run the commented-out Python solution manually, before continuing
# execution.

# Customer spending behaviour transformations
#
# We will compute two customer spending behaviour features.
#
# The first feature will be the number of transactions that occur within a time
# window (Frequency). The second will be the average amount spent in these
# transactions (Monetary value). The time windows will be set to one, seven, and
# thirty days.
#
# The values is to be calculated based on day level where our transactions is on
# seconds level, the table below show a example of the output for the 1 day
# window.
#
# Since we want to aggregate by day and also take include dates that has no
# transactions (so our windows are real days) we need to first create a new data
# frame that has for each customer one row for each date between the minimum
# transaction date and maximum transaction date. Snowpark has a function,
# **range** , that can be used to generate n number of rows. Since we want to
# generate a row for each date between minimum and maximum we need to calculate
# that first. Once we have that dataframe we can do a cross join with our
# **CUSTOMER** table to create a new dataframe that has now one row for each
# date between the minimum transaction date and maximum transaction date and
# customer, as illustrated in the image below.
#
# date_info = df_cust_trx_fraud.select(
#     functions.min(functions.col("TX_DATETIME")).as_("END_DATE"),
#     functions.datediff(
#         "DAY", functions.col("END_DATE"), functions.max(functions.col("TX_DATETIME"))
#     ).as_("NO_DAYS"),
# )

# Currently, the commented-out line of Python above runs the following
# equivalent SQL against PostgreSQL:
#
# ```sql
# SELECT
#   MIN("TX_DATETIME") AS "END_DATE",
#   CAST(
#     EXTRACT(
#       epoch
#       FROM
#         CAST(
#           MAX("TX_DATETIME") AS TIMESTAMP
#         ) - CAST("END_DATE" AS TIMESTAMP)
#     ) / 86400 AS BIGINT
#   ) AS "NO_DAYS"
# FROM
#   "CUSTOMER_TRANSACTIONS_FRAUD"
# ```
#
# LocalStack Engineering: Assuming that this `SELECT` statement is silently
# wrapped by a transaction, it appears that the Snowflake SQL parser parses from
# left-to-right **within a single transaction** instead of parsing on a
# per-transaction basis, which is pretty surprising. This breaks the aliasing
# logic when transpiling for our Snowflake emulator, which assumes that aliases
# created in one transaction / CTE will only be made available in the next
# transaction / CTE. In order to resolve this issue, we would need to replace
# aliases with their original statements, but only within the context of that
# transaction / CTE, since afterwards the underlying data could have changed
# (e.g. in the example above, if "TX_DATETIME" was aliased to another column
# within that transaction, MIN("TX_DATETIME") may return a different result).
# This is a non-trivial bug, and therefore we recommend manually replacing
# aliases in the same transaction with the original statement (e.g.
# `functions.col("END_DATE")` with
# `functions.min(functions.col("TX_DATETIME"))`), to get the equivalent SQL:
#
# ```sql
# SELECT
#   MIN("TX_DATETIME") AS "END_DATE",
#   CAST(
#     EXTRACT(
#       epoch
#       FROM
#         CAST(
#           MAX("TX_DATETIME") AS TIMESTAMP
#         ) - CAST(
#           MIN("TX_DATETIME") AS TIMESTAMP
#         )
#     ) / 86400 AS BIGINT
#   ) AS "NO_DAYS"
# FROM
#   "CUSTOMER_TRANSACTIONS_FRAUD"
# ```
#
# Yet to be answered: if "TX_DATETIME" is aliased within the same transaction,
# then which column would `MIN("TX_DATETIME")` reference, the first or the
# second? E.g.
#
# ```sql
# SELECT
#   "TX_DATETIME" AS "TX_DATETIME_2",
#   "_TX_DATETIME" AS "TX_DATETIME",
#   MIN("TX_DATETIME") AS "END_DATE"
# FROM
#   "CUSTOMER_TRANSACTIONS_FRAUD"
# ```
#
# Q: If it is executed from left-to-right, would this imply that this SQL...is
# actually procedural, instead of declarative, like SQL is supposed to be...?
#
date_info = df_cust_trx_fraud.select(
    functions.min(functions.col("TX_DATETIME")).as_("END_DATE"),
    functions.datediff(
        "DAY",
        functions.min(functions.col("TX_DATETIME")),
        functions.max(functions.col("TX_DATETIME")),
    ).as_("NO_DAYS"),
)

########## END LOCALSTACK WORKAROUND ##########

date_info_as_pandas = date_info.to_pandas()

days = int(date_info_as_pandas["NO_DAYS"].values[0])
start_date = str(date_info_as_pandas["END_DATE"].values[0].astype("datetime64[D]"))

########## BEGIN LOCALSTACK WORKAROUND ##########

# NOTE: Stop execution before this line, using `import ipdb; ipdb.set_trace()`,
# and run the commented-out SQL solution manually, before continuing execution.

# # Create a dataframe with one row for each date between the min and max
# # transaction date
# df_days = session.range(days).with_column(
#     "TX_DATE",
#     functions.to_date(
#         functions.dateadd("DAY", functions.seq4(), functions.lit(start_date))
#     ),
# )

# Currently, the commented-out line of Python runs the following SQL against
# PostgreSQL:
#
# ```sql
# SELECT
#   (
#     ROW_NUMBER() OVER (
#       ORDER BY
#         "SEQ8"()
#     ) -1
#   ) * (1) + (0) AS "ID"
# FROM
#   (
#     "GENERATOR"("ROWCOUNT" => 153)
#   ) AS _tmp4e72a70e
# ```
#
# This is equivalent to `session.range(days)` only, since `.with_column()` is
# only evaluated afterwards. To replace:
#
# ```python
# session.range(days)
# ```
#
# Use:
#
# ```python
# session_as_days = session.create_dataframe([
#     snowpark.Row(ID = idx)
#     for idx
#     in range(0, days - 1)
# ])
# ```
#
# TODO: Add ticket for the rest of the Python method reproduction
#
# As a temporary workaround, read the Parquet file
# `tempdata/01_feature_engineering.df_days.parquet` into a Pandas dataframe, and
# then create a Snowpark dataframe as `df_days` from that.
df_days_as_pd_df = pandas.read_parquet(
    "tempdata/01_feature_engineering.df_days.parquet"
)
df_days = session.create_dataframe(
    df_days_as_pd_df.values.tolist(), schema=df_days_as_pd_df.columns.to_list()
)

########## END LOCALSTACK WORKAROUND ##########

# Since we aggregate by customer and day and not all customers have transactions
# for all dates we cross join our date dataframe with our customer table so each
# customer witll have one row for each date
df_customers = session.table("CUSTOMERS").select("CUSTOMER_ID")


########## WHERE I LEFT OFF ##########
########## WHERE I LEFT OFF ##########
########## WHERE I LEFT OFF ##########


# TODO: This line here is broken - checkout `main`, grab the result as a
# DataFrame and export to Parquet, then switch back to this branch, and then
# read `df_cust_day` from a dataframe.
#
# Query as run by SnowFlake:
#
# TODO: Add
#
# Query as run by PostgreSQL:
#
# ```sql
# SELECT "ID" AS "ID", "TX_DATE" AS "TX_DATE" FROM (SELECT "ID", TO_DATE(CAST("TX_DATE" AS TEXT), 'YYYY/MM/DD') AS "TX_DATE" FROM (SELECT "_COL1" AS "ID", "_COL2" AS "TX_DATE" FROM (VALUES (CAST(0 AS BIGINT), CAST('a' AS TEXT))) AS _tmp90d4a10d("_COL1", "_COL2")) AS _tmp2b46038c) AS _tmp
# ```
#
df_cust_day = df_days.join(df_customers)

logging.debug(df_cust_day.show())

# We can now use the new data frame, **df_cust_day**, to aggregate the number of
# transaction and transaction amount by day, for days that a customer has no
# transaction we will use 0. The picture below illustrates what we are doing.
#
# Earlier in the data understanding part we used **call_function** to use a
# Snowflake functions that is not exposed in the Snowpark API, we can also use
# **function** to assign the function to a variable that we then can use instead
# how having to write call_builtin each time.

zero_if_null = functions.function("ZEROIFNULL")

df_cust_trx_day = (
    df_cust_trx_fraud.join(
        df_cust_day,
        (df_cust_trx_fraud["CUSTOMER_ID"] == df_cust_day["CUSTOMER_ID"])
        & (
            functions.to_date(df_cust_trx_fraud["TX_DATETIME"])
            == df_cust_day["TX_DATE"]
        ),
        "rightouter",
    )
    .select(
        df_cust_day.col("CUSTOMER_ID").as_("CUSTOMER_ID"),
        df_cust_day.col("TX_DATE"),
        zero_if_null(df_cust_trx_fraud["TX_AMOUNT"]).as_("TX_AMOUNT"),
        functions.iff(
            functions.col("TX_AMOUNT") > functions.lit(0),
            functions.lit(1),
            functions.lit(0),
        ).as_("NO_TRX"),
    )
    .group_by(functions.col("CUSTOMER_ID"), functions.col("TX_DATE"))
    .agg(
        [
            functions.sum(functions.col("TX_AMOUNT")).as_("TOT_AMOUNT"),
            functions.sum(functions.col("NO_TRX")).as_("NO_TRX"),
        ]
    )
)

# Now when we have the number of transactions and amount by customer and day we
# can aggregate by our windows (1, 7 and 30 days).
#
# For getting values previous day we will use the **lag** function since it's
# only one value we want and for 7 and 30 days we will use sum over a window.
# Since we do not want to include future values we will not include the current
# day in the windows.

cust_date = Window.partition_by(functions.col("customer_id")).orderBy(
    functions.col("TX_DATE")
)
win_7d_cust = cust_date.rowsBetween(-7, -1)
win_30d_cust = cust_date.rowsBetween(-30, -1)

df_cust_feat_day = df_cust_trx_day.select(
    functions.col("TX_DATE"),
    functions.col("CUSTOMER_ID"),
    functions.col("NO_TRX"),
    functions.col("TOT_AMOUNT"),
    functions.lag(functions.col("NO_TRX"), 1).over(cust_date).as_("CUST_TX_PREV_1"),
    functions.sum(functions.col("NO_TRX")).over(win_7d_cust).as_("CUST_TX_PREV_7"),
    functions.sum(functions.col("NO_TRX")).over(win_30d_cust).as_("CUST_TX_PREV_30"),
    functions.lag(functions.col("TOT_AMOUNT"), 1)
    .over(cust_date)
    .as_("CUST_TOT_AMT_PREV_1"),
    functions.sum(functions.col("TOT_AMOUNT"))
    .over(win_7d_cust)
    .as_("CUST_TOT_AMT_PREV_7"),
    functions.sum(functions.col("TOT_AMOUNT"))
    .over(win_30d_cust)
    .as_("CUST_TOT_AMT_PREV_30"),
)

logging.debug(df_cust_feat_day.show())

# Now we know for each customer and day the number of transactions and amount
# for previous 1, 7 and 30 days and we add that to our transactions.
#
# In this step we will also use a window function to count the number of
# transactions and total amount for the current date, in order to only include
# the previous transactions for the same date we will create a partion key that
# consists of transaction date and customer id. By using that we get a rolling
# sum of all previous rows that is for the same date and customer.

win_cur_date = (
    Window.partition_by(functions.col("PARTITION_KEY"))
    .order_by(functions.col("TX_DATETIME"))
    .rangeBetween(Window.unboundedPreceding, Window.currentRow)
)

df_cust_behaviur_feat = (
    df_date_time_feat.join(
        df_cust_feat_day,
        (df_date_time_feat["CUSTOMER_ID"] == df_cust_feat_day["CUSTOMER_ID"])
        & (
            functions.to_date(df_date_time_feat["TX_DATETIME"])
            == df_cust_feat_day["TX_DATE"]
        ),
    )
    .with_column(
        "PARTITION_KEY",
        functions.concat(
            df_date_time_feat["CUSTOMER_ID"],
            functions.to_date(df_date_time_feat["TX_DATETIME"]),
        ),
    )
    .with_columns(
        ["CUR_DAY_TRX", "CUR_DAY_AMT"],
        [
            functions.count(df_date_time_feat["CUSTOMER_ID"]).over(win_cur_date),
            functions.sum(df_date_time_feat["TX_AMOUNT"]).over(win_cur_date),
        ],
    )
    .select(
        df_date_time_feat["TRANSACTION_ID"],
        df_date_time_feat["CUSTOMER_ID"].as_("CUSTOMER_ID"),
        df_date_time_feat["TERMINAL_ID"],
        df_date_time_feat["TX_DATETIME"].as_("TX_DATETIME"),
        df_date_time_feat["TX_AMOUNT"],
        df_date_time_feat["TX_TIME_SECONDS"],
        df_date_time_feat["TX_TIME_DAYS"],
        df_date_time_feat["TX_FRAUD"],
        df_date_time_feat["TX_FRAUD_SCENARIO"],
        df_date_time_feat["TX_DURING_WEEKEND"],
        df_date_time_feat["TX_DURING_NIGHT"],
        (
            zero_if_null(df_cust_feat_day["CUST_TX_PREV_1"])
            + functions.col("CUR_DAY_TRX")
        ).as_("CUST_CNT_TX_1"),
        (
            (
                zero_if_null(df_cust_feat_day["CUST_TOT_AMT_PREV_1"])
                + functions.col("CUR_DAY_AMT")
            )
            / functions.col("CUST_CNT_TX_1")
        ).as_("CUST_AVG_AMOUNT_1"),
        (
            zero_if_null(df_cust_feat_day["CUST_TX_PREV_7"])
            + functions.col("CUR_DAY_TRX")
        ).as_("CUST_CNT_TX_7"),
        (
            (
                zero_if_null(df_cust_feat_day["CUST_TOT_AMT_PREV_7"])
                + functions.col("CUR_DAY_AMT")
            )
            / functions.col("CUST_CNT_TX_7")
        ).as_("CUST_AVG_AMOUNT_7"),
        (
            zero_if_null(df_cust_feat_day["CUST_TX_PREV_30"])
            + functions.col("CUR_DAY_TRX")
        ).as_("CUST_CNT_TX_30"),
        (
            (
                zero_if_null(df_cust_feat_day["CUST_TOT_AMT_PREV_30"])
                + functions.col("CUR_DAY_AMT")
            )
            / functions.col("CUST_CNT_TX_30")
        ).as_("CUST_AVG_AMOUNT_30"),
    )
)

logging.info(df_cust_behaviur_feat.show())

# Terminal ID transformations
#
# The main goal with the Terminal ID transformations will be to extract a risk
# score, that assesses the exposure of a given terminal ID to fraudulent
# transactions. The risk score will be defined as the average number of
# fraudulent transactions that occurred on a terminal ID over a time window. As
# for customer ID transformations, we will use three window sizes, of 1, 7, and
# 30 days.
#
# Contrary to customer ID transformations, the time windows will not directly
# precede a given transaction. Instead, they will be shifted back by a delay
# period. The delay period accounts for the fact that, in practice, the
# fraudulent transactions are only discovered after a fraud investigation or a
# customer complaint. Hence, the fraudulent labels, which are needed to compute
# the risk score, are only available after this delay period. To a first
# approximation, this delay period will be set to one week.
#
# Part from above the logic is rather similar to how we calculated for customer.

# Since we aggregate by terminal and day and not all terminals have transactions
# for all dates we cross join our date dataframe with our terminal table so each
# terminal will have one row for each date
df_terminals = session.table("TERMINALS").select("TERMINAL_ID")
df_term_day = df_days.join(df_terminals)

# Aggregate number of transactions and amount by terminal and date, for dates
# where a terminal do not have any ttransactions we ad a 0
df_term_trx_by_day = (
    df_cust_trx_fraud.join(
        df_term_day,
        (df_cust_trx_fraud["TERMINAL_ID"] == df_term_day["TERMINAL_ID"])
        & (
            functions.to_date(df_cust_trx_fraud["TX_DATETIME"])
            == df_term_day["TX_DATE"]
        ),
        "rightouter",
    )
    .select(
        df_term_day["TERMINAL_ID"].as_("TERMINAL_ID"),
        df_term_day["TX_DATE"],
        zero_if_null(df_cust_trx_fraud["TX_FRAUD"]).as_("NB_FRAUD"),
        functions.when(
            functions.is_null(df_cust_trx_fraud["TX_FRAUD"]), functions.lit(0)
        )
        .otherwise(functions.lit(1))
        .as_("NO_TRX"),
    )
    .groupBy(functions.col("TERMINAL_ID"), functions.col("TX_DATE"))
    .agg(
        [
            functions.sum(functions.col("NB_FRAUD")).as_("NB_FRAUD"),
            functions.sum(functions.col("NO_TRX")).as_("NO_TRX"),
        ]
    )
)

# Aggregate by our windows.
term_date = Window.partitionBy(functions.col("TERMINAL_ID")).orderBy(
    functions.col("TX_DATE")
)
win_delay = term_date.rowsBetween(-7, -1)
win_1d_term = term_date.rowsBetween(
    -8, -1
)  # We need to add the Delay period to our windows
win_7d_term = term_date.rowsBetween(-14, -1)
win_30d_term = term_date.rowsBetween(-37, -1)

df_term_feat_day = df_term_trx_by_day.select(
    functions.col("TX_DATE"),
    functions.col("TERMINAL_ID"),
    functions.col("NO_TRX"),
    functions.col("NB_FRAUD"),
    functions.sum(functions.col("NB_FRAUD")).over(win_delay).as_("NB_FRAUD_DELAY"),
    functions.sum(functions.col("NO_TRX")).over(win_delay).as_("NB_TX_DELAY"),
    functions.sum(functions.col("NO_TRX"))
    .over(win_1d_term)
    .as_("NB_TX_DELAY_WINDOW_1"),
    functions.sum(functions.col("NO_TRX"))
    .over(win_1d_term)
    .as_("NB_TX_DELAY_WINDOW_7"),
    functions.sum(functions.col("NO_TRX"))
    .over(win_30d_term)
    .as_("NB_TX_DELAY_WINDOW_30"),
    functions.sum(functions.col("NB_FRAUD"))
    .over(win_1d_term)
    .as_("NB_FRAUD_DELAY_WINDOW_1"),
    functions.sum(functions.col("NB_FRAUD"))
    .over(win_1d_term)
    .as_("NB_FRAUD_DELAY_WINDOW_7"),
    functions.sum(functions.col("NB_FRAUD"))
    .over(win_30d_term)
    .as_("NB_FRAUD_DELAY_WINDOW_30"),
)

df_term_behaviur_feat = (
    df_cust_behaviur_feat.join(
        df_term_feat_day,
        (df_cust_behaviur_feat["TERMINAL_ID"] == df_term_feat_day["TERMINAL_ID"])
        & (
            functions.to_date(df_cust_behaviur_feat["TX_DATETIME"])
            == df_term_feat_day["TX_DATE"]
        ),
    )
    .with_columns(
        ["PARTITION_KEY", "CUR_DAY_TRX", "CUR_DAY_FRAUD"],
        [
            functions.concat(
                df_cust_behaviur_feat["TERMINAL_ID"],
                functions.to_date(df_cust_behaviur_feat["TX_DATETIME"]),
            ),
            functions.count(df_cust_behaviur_feat["TERMINAL_ID"]).over(win_cur_date),
            functions.sum(df_cust_behaviur_feat["TX_FRAUD"]).over(win_cur_date),
        ],
    )
    .with_columns(
        [
            "NB_TX_DELAY",
            "NB_FRAUD_DELAY",
            "NB_TX_DELAY_WINDOW_1",
            "NB_FRAUD_DELAY_WINDOW_1",
            "NB_TX_DELAY_WINDOW_7",
            "NB_FRAUD_DELAY_WINDOW_7",
            "NB_TX_DELAY_WINDOW_30",
            "NB_FRAUD_DELAY_WINDOW_30",
        ],
        [
            df_term_feat_day.col("NB_TX_DELAY") + functions.col("CUR_DAY_TRX"),
            functions.col("NB_FRAUD_DELAY") + functions.col("CUR_DAY_FRAUD"),
            functions.col("NB_TX_DELAY_WINDOW_1") + functions.col("CUR_DAY_TRX"),
            functions.col("NB_FRAUD_DELAY_WINDOW_1") + functions.col("CUR_DAY_FRAUD"),
            functions.col("NB_TX_DELAY_WINDOW_7") + functions.col("CUR_DAY_TRX"),
            functions.col("NB_FRAUD_DELAY_WINDOW_7") + functions.col("CUR_DAY_FRAUD"),
            functions.col("NB_TX_DELAY_WINDOW_30") + functions.col("CUR_DAY_TRX"),
            functions.col("NB_FRAUD_DELAY_WINDOW_30") + functions.col("CUR_DAY_FRAUD"),
        ],
    )
    .select(
        df_cust_behaviur_feat["TRANSACTION_ID"],
        df_cust_behaviur_feat["TX_DATETIME"].as_("TX_DATETIME"),
        df_cust_behaviur_feat["CUSTOMER_ID"].as_("CUSTOMER_ID"),
        df_cust_behaviur_feat["TERMINAL_ID"].as_("TERMINAL_ID"),
        df_cust_behaviur_feat["TX_TIME_SECONDS"],
        df_cust_behaviur_feat["TX_TIME_DAYS"],
        df_cust_behaviur_feat["TX_AMOUNT"],
        df_cust_behaviur_feat["TX_FRAUD"],
        df_cust_behaviur_feat["TX_FRAUD_SCENARIO"],
        df_cust_behaviur_feat["TX_DURING_WEEKEND"],
        df_cust_behaviur_feat["TX_DURING_NIGHT"],
        df_cust_behaviur_feat["CUST_AVG_AMOUNT_1"],
        df_cust_behaviur_feat["CUST_CNT_TX_1"],
        df_cust_behaviur_feat["CUST_AVG_AMOUNT_7"],
        df_cust_behaviur_feat["CUST_CNT_TX_7"],
        df_cust_behaviur_feat["CUST_AVG_AMOUNT_30"],
        df_cust_behaviur_feat["CUST_CNT_TX_30"],
        (functions.col("NB_TX_DELAY_WINDOW_1") - functions.col("NB_TX_DELAY")).as_(
            "NB_TX_WINDOW_1"
        ),
        functions.iff(
            functions.col("NB_FRAUD_DELAY_WINDOW_1") - functions.col("NB_FRAUD_DELAY")
            > 0,
            (functions.col("NB_FRAUD_DELAY_WINDOW_1") - functions.col("NB_FRAUD_DELAY"))
            / functions.col("NB_TX_WINDOW_1"),
            functions.lit(0),
        ).as_("TERM_RISK_1"),
        (functions.col("NB_TX_DELAY_WINDOW_7") - functions.col("NB_TX_DELAY")).as_(
            "NB_TX_WINDOW_7"
        ),
        functions.iff(
            functions.col("NB_FRAUD_DELAY_WINDOW_7") - functions.col("NB_FRAUD_DELAY")
            > 0,
            (functions.col("NB_FRAUD_DELAY_WINDOW_7") - functions.col("NB_FRAUD_DELAY"))
            / functions.col("NB_TX_WINDOW_7"),
            functions.lit(0),
        ).as_("TERM_RISK_7"),
        (functions.col("NB_TX_DELAY_WINDOW_30") - functions.col("NB_TX_DELAY")).as_(
            "NB_TX_WINDOW_30"
        ),
        functions.iff(
            functions.col("NB_FRAUD_DELAY_WINDOW_30") - functions.col("NB_FRAUD_DELAY")
            > 0,
            (
                functions.col("NB_FRAUD_DELAY_WINDOW_30")
                - functions.col("NB_FRAUD_DELAY")
            )
            / functions.col("NB_TX_WINDOW_30"),
            functions.lit(0),
        ).as_("TERM_RISK_30"),
    )
)

logging.info(df_term_behaviur_feat.show())

# We now have our new features and can save it into a new table that we then can
# use for traing our model for predicting fraud.
#
# Start by checking the schema of our new dataframe and decide if we will keep
# all columns.
for col in df_term_behaviur_feat.schema.fields:
    logging.info(f"{col.name}, Nullable: {col.nullable}, {col.datatype}")

# As mentioned we are not actually executing anything in Snowflake, unless when
# using .show(), and has just created a execution plan.
#
# We can get the SQL needed to prefrom all our steps by calling **queries**
logging.info(df_term_behaviur_feat.queries)


# We can quicly scale up our Warhouse (compute) before we load our new table and
# after we done the load we can scale it down again.
#
# By creating a function for it we can simplify the code and make it easier for
# the developers.
def scale_wh(sess, wh, size):
    if len(wh) == 0:
        return False
    if len(size) == 0:
        return False

    alter_SQL = "ALTER WAREHOUSE " + wh + " SET WAREHOUSE_SIZE = " + size
    sess.sql(alter_SQL).collect()
    return True


scale_wh(session, session.get_current_warehouse(), "LARGE")

# We we run **saveAsTable** Snowpark will generate the SQL for all the previous
# step, execute it in Snowflake and store the result in the table
# **customer_trx_fraud_features**.
df_term_behaviur_feat.write.mode("overwrite").save_as_table(
    "customer_trx_fraud_features"
)

scale_wh(session, session.get_current_warehouse(), "SMALL")

session.close()
