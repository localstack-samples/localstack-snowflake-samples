#!/usr/bin/env python3
"""
Credit Card Fraud Prediction - Feature Engineering

This example is based on the handbook "Machine Learning for Credit Card Fraud
Detection": https://fraud-detection-handbook.github.io/fraud-detection-handbook/

NOTE: Currently cannot replicate the X11-based settings for matplotlib, so
removing mentions of that. Wait to migrate to Docker image
quay.io/jupyter/datascience-notebook.
"""

import logging
import os
from pprint import pprint

from dotenv import load_dotenv
import snowflake.snowpark as snowpark  # type: ignore
import snowflake.snowpark.functions as functions  # type: ignore
from snowflake.snowpark import Session, Window  # type: ignore

from utils import create_data_checkpoint

# NOTE: Change this to run against Snowflake or LocalStack.
ENV_FILE = ".env.snowflake"
# ENV_FILE = '.env.localstack'

logging.basicConfig(level=logging.INFO)
load_dotenv(ENV_FILE)

# Loading Credit Card Transactions into Snowflake - import the dependencies and
# connect to Snowflake.

logging.info(f"Snowpark for Python version {snowpark.__version__}")

connection_parameters = {
    "account": os.getenv("ACCOUNTNAME"),
    "user": os.getenv("USERNAME"),
    "password": os.getenv("PASSWORD"),
    "role": os.getenv("ROLENAME"),
    "database": os.getenv("DBNAME"),
    "schema": os.getenv("SCHEMANAME"),
    "warehouse": os.getenv("WAREHOUSENAME"),
}

session = Session.builder.configs(connection_parameters).create()

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

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(df_cust_trx_fraud, "df_cust_trx_fraud", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

logging.info(df_cust_trx_fraud.show())

# Data Understanding
#
# Let's start by getting some basic understanding of our data.
#
# We can use the **describe** function on our **numeric** columns to get some
# basic statistics. The only column in our current dataset would be TX_AMOUNT.

logging.info(df_cust_trx_fraud.describe().show())

# We can see that we have about 6 months  of transactions and around 9k
# transactions/day.
#
# Let's count the number of fraudulent and none fraudulent transactions, by
# using the **call_function** function we can also use the use
# **RATIO_TO_REPORT** function (that currently is not exposed in the Snowpark
# API) to also get percentages

result1 = (
    df_cust_trx_fraud.group_by(functions.col("TX_FRAUD"))
    .agg(functions.count(functions.col("TRANSACTION_ID")).as_("NB_TX_DAY"))
    .select(
        functions.col("TX_FRAUD"),
        functions.col("NB_TX_DAY"),
        (
            functions.call_function(
                "RATIO_TO_REPORT", functions.col("NB_TX_DAY")
            ).over()
            * 100
        ).as_("percentage"),
    )
)

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(result1, "result1", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

logging.info(result1.show())

df_day_by_card = (
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
)

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(df_day_by_card, "df_day_by_card", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

pd_day_by_card = df_day_by_card.to_pandas()

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


df_date_time_feat = gen_time_feat(df_cust_trx_fraud)

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(df_date_time_feat, "df_date_time_feat", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

result2 = df_date_time_feat.sort(functions.col("TRANSACTION_ID"))

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(result2, "result2", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

logging.info(result2.show())

# Is there differences between the number of fraud cases during
# weekdays/weekdays and day/Night?

window = Window.partition_by(
    functions.col("TX_DURING_WEEKEND"), functions.col("TX_DURING_NIGHT")
)

result3 = (
    df_date_time_feat.group_by(
        functions.col("TX_DURING_WEEKEND"),
        functions.col("TX_DURING_NIGHT"),
        functions.col("TX_FRAUD"),
    )
    .count()
    .sort(
        functions.col("TX_DURING_WEEKEND"),
        functions.col("TX_DURING_NIGHT"),
        functions.col("TX_FRAUD"),
    )
    .select(
        functions.col("TX_DURING_WEEKEND"),
        functions.col("TX_DURING_NIGHT"),
        functions.col("TX_FRAUD"),
        (
            functions.call_function("RATIO_TO_REPORT", functions.col("COUNT")).over(
                window
            )
            * 100
        ).as_("percentage"),
    )
    .sort(
        functions.col("TX_DURING_WEEKEND"),
        functions.col("TX_DURING_NIGHT"),
        functions.col("TX_FRAUD"),
    )
)

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(result3, "result3", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

logging.info(result3.show())

# We can see that basicly the number of fradulent transactions are the same
# during the days

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

result4 = df_cust_trx_fraud.select(
    functions.min(functions.col("TX_DATETIME")).as_("END_DATE"),
    functions.datediff(
        "DAY", functions.col("END_DATE"), functions.max(functions.col("TX_DATETIME"))
    ).as_("NO_DAYS"),
)

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(result4, "result4", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

date_info = result4.to_pandas()


days = int(date_info["NO_DAYS"].values[0])
start_date = str(date_info["END_DATE"].values[0].astype("datetime64[D]"))

# Create a dataframe with one row for each date between the min and max
# transaction date
df_days = session.range(days).with_column(
    "TX_DATE",
    functions.to_date(
        functions.dateadd("DAY", functions.seq4(), functions.lit(start_date))
    ),
)

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(df_days, "df_days", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

# Since we aggregate by customer and day and not all customers have transactions
# for all dates we cross join our date dataframe with our customer table so each
# customer witll have one row for each date
df_customers = session.table("CUSTOMERS").select("CUSTOMER_ID")
df_cust_day = df_days.join(df_customers)

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(df_cust_day, "df_cust_day", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

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

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(df_cust_trx_day, "df_cust_trx_day", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

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

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(df_cust_feat_day, "df_cust_feat_day", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

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

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(
    df_cust_behaviur_feat, "df_cust_behaviur_feat", "01_feature_engineering"
)

########## END DATA CHECKPOINT ##########

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

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(df_term_day, "df_term_day", "01_feature_engineering")

########## END DATA CHECKPOINT ##########

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

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(
    df_term_trx_by_day, "df_term_trx_by_day", "01_feature_engineering"
)

########## END DATA CHECKPOINT ##########

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

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(df_term_feat_day, "df_term_feat_day", "01_feature_engineering")

########## END DATA CHECKPOINT ##########


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

########## BEGIN DATA CHECKPOINT ##########

create_data_checkpoint(
    df_term_behaviur_feat, "df_term_behaviur_feat", "01_feature_engineering"
)

########## END DATA CHECKPOINT ##########

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
