#!/bin/bash
# Load the simulated transactions with 00_load_tables.py and build the fraud
# features with 01_feature_engineering.py, both in the sample's Docker image,
# then check the tables they create in Snowflake.
set -euo pipefail

PORT=${PORT:-4566}
[ -f .env.localstack ] || cp .env.sample .env.localstack

query() {
  snow sql -c local --port "$PORT" --format json -q "$1" | jq -r '.. | objects | .N? // empty' | head -1
}

check() {
  local description=$1 actual=$2 expected=$3
  if [ "$actual" != "$expected" ]; then
    echo "Test failed: $description is ${actual:-empty}, expected $expected"
    exit 1
  fi
  echo "PASS: $description is $actual"
}

snow sql -c local --port "$PORT" -q "CREATE DATABASE IF NOT EXISTS test" > /dev/null

make load-tables DOCKER_RUN_FLAGS=

# data/fraud_transactions.min.csv.gz holds 11033 transactions from 50
# customers on 50 terminals, 590 of them frauds, from 2019-04-01 to 2019-07-31
check "CUSTOMER_TRANSACTIONS_FRAUD rows" "$(query "SELECT COUNT(*) AS N FROM test.public.customer_transactions_fraud")" 11033
check "CUSTOMERS rows" "$(query "SELECT COUNT(*) AS N FROM test.public.customers")" 50
check "TERMINALS rows" "$(query "SELECT COUNT(*) AS N FROM test.public.terminals")" 50
check "fraudulent transactions" "$(query "SELECT SUM(tx_fraud) AS N FROM test.public.customer_transactions_fraud")" 590

make feature-engineering DOCKER_RUN_FLAGS=

# The daily customer and terminal windows span from the first day up to, not
# including, the last one, so the 106 transactions of 2019-07-31 get no
# features: 10927 rows with 579 frauds remain
features=test.public.customer_trx_fraud_features
check "CUSTOMER_TRX_FRAUD_FEATURES rows" "$(query "SELECT COUNT(*) AS N FROM $features")" 10927
check "fraudulent transactions with features" "$(query "SELECT SUM(tx_fraud) AS N FROM $features")" 579
check "rows with a 7 day average amount" "$(query "SELECT COUNT(cust_avg_amount_7) AS N FROM $features")" 10927
check "feature columns" "$(query "SELECT COUNT(*) AS N FROM test.information_schema.columns WHERE table_name = 'CUSTOMER_TRX_FRAUD_FEATURES'")" 23

echo "Test passed: the transactions were loaded and the fraud features built in Snowflake"
