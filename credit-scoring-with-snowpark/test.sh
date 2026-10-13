#!/bin/bash
# Run every cell of the notebook against the Snowflake emulator, then check
# that both CSV files were written to Snowflake in full.
set -euo pipefail

. .venv/bin/activate
jupyter nbconvert --to notebook --execute credit-scoring-eda.ipynb \
  --output executed.ipynb --ExecutePreprocessor.timeout=900
echo "PASS: every cell of the notebook ran"

python - <<'PY'
import sys
import pandas as pd
from snowflake.snowpark import Session

session = Session.builder.configs({
    "user": "test", "password": "test", "account": "test", "warehouse": "test",
    "host": "snowflake.localhost.localstack.cloud",
}).create()
for table, csv in (("CREDIT_FILES", "credit_files.csv"), ("CREDIT_REQUESTS", "credit_request.csv")):
    expected = len(pd.read_csv(csv))
    rows = session.table(f"CREDIT_BANK.PUBLIC.{table}").count()
    if rows != expected:
        sys.exit(f"Test failed: {table} has {rows} rows, expected {expected} from {csv}")
    print(f"PASS: {table} has all {rows} rows from {csv}")
PY

echo "Test passed: the credit scoring notebook ran against Snowflake"
