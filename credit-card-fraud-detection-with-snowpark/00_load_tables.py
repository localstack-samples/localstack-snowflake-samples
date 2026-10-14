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

from utils import connection_parameters

# `.env.localstack` connects to the LocalStack Snowflake emulator, and
# `.env.snowflake` to Snowflake Cloud. The Makefile sets ENV_FILE.
ENV_FILE = os.getenv("ENV_FILE", ".env.localstack")

FRAUD_TRANSACTIONS_CSV_GZ = "fraud_transactions.min.csv.gz"

logging.basicConfig(level=logging.INFO)
load_dotenv(ENV_FILE)

# Loading Credit Card Transactions into Snowflake - import the dependencies and
# connect to Snowflake.

logging.info(f"Snowpark for Python version {snowpark.__version__}")

session = Session.builder.configs(connection_parameters()).create()

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

# Copy the staged file into a new CUSTOMER_TRANSACTIONS_FRAUD table
session.sql("DROP TABLE IF EXISTS CUSTOMER_TRANSACTIONS_FRAUD").collect()
ret = dfCustTrxFraudRd.copy_into_table("CUSTOMER_TRANSACTIONS_FRAUD")

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


session.close()
