#!/bin/bash
# Run every cell of the notebook against the Snowflake emulator, then check
# the customers table it writes and the quality of the regression it trains.
set -euo pipefail

. .venv/bin/activate

# Diagnostic: which write_pandas inputs the emulator accepts (does not fail the test)
python - <<'PY' || true
import pandas as pd
from snowflake.snowpark import Session

base = {"user": "test", "password": "test", "account": "test", "warehouse": "test",
        "host": "snowflake.localhost.localstack.cloud"}
customers = pd.read_csv("EcommerceCustomers.csv")
customers.columns = [c.upper().replace(".", "").replace(" ", "_") for c in customers.columns]
variants = {
    "floats only, test.test": (customers[["TIME_ON_APP", "YEARLY_AMOUNT_SPENT"]], {"database": "test", "schema": "test"}),
    "strings only, test.test": (customers[["EMAIL", "AVATAR"]], {"database": "test", "schema": "test"}),
    "address with newlines, test.test": (customers[["ADDRESS"]], {"database": "test", "schema": "test"}),
    "all columns, test.test": (customers, {"database": "test", "schema": "test"}),
    "all columns, new database": (customers, {}),
}
for i, (name, (df, location)) in enumerate(variants.items()):
    session = Session.builder.configs({**base, **location}).create()
    if not location:
        session.sql("CREATE OR REPLACE DATABASE probe_db").collect()
        session.sql("USE SCHEMA probe_db.public").collect()
    try:
        session.write_pandas(df, f"PROBE_{i}", auto_create_table=True)
        print(f"PROBE ok:     {name}")
    except Exception as e:
        print(f"PROBE failed: {name}: {str(e)[:150]}")
PY

jupyter nbconvert --to notebook --execute linear-regression-model.ipynb \
  --output executed.ipynb --ExecutePreprocessor.timeout=900
echo "PASS: every cell of the notebook ran"

python - <<'PY'
import json
import re
import sys
import pandas as pd
from snowflake.snowpark import Session

session = Session.builder.configs({
    "user": "test", "password": "test", "account": "test", "warehouse": "test",
    "database": "test", "schema": "test", "host": "snowflake.localhost.localstack.cloud",
}).create()
expected = len(pd.read_csv("EcommerceCustomers.csv"))
rows = session.table("CUSTOMERS_NEW").count()
if rows != expected:
    sys.exit(f"Test failed: CUSTOMERS_NEW has {rows} rows, expected {expected}")
print(f"PASS: CUSTOMERS_NEW has all {rows} customers")

notebook = json.load(open("executed.ipynb"))
text = "".join(
    "".join(output.get("text", ""))
    for cell in notebook["cells"] if cell["cell_type"] == "code"
    for output in cell.get("outputs", [])
)
match = re.search(r"R-squared: ([0-9.]+)", text)
if not match or float(match.group(1)) < 0.9:
    sys.exit(f"Test failed: expected an R-squared above 0.9, got {match.group(1) if match else 'none'}")
print(f"PASS: the model explains the spend with R-squared {float(match.group(1)):.3f}")
PY

echo "Test passed: the customer spend notebook ran against Snowflake"
