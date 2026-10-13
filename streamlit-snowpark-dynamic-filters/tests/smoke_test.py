"""Run the app's Snowpark logic without a browser.

Builds the three filters the way main.py does, enables them with fixed widget
values, chains them over the CUSTOMERS table like draw_main_ui and checks the
result against the same filter written in SQL.
"""
import sys
import types

import streamlit as st
import toml
from snowflake.snowpark.session import Session

import lib.filterwidget as filterwidget
from lib.chart_helpers import mk_labels, mk_links
from lib.filterwidget import MyFilter


def check(condition, message):
    if not condition:
        print(f"Test failed: {message}")
        sys.exit(1)
    print(f"PASS: {message}")


secrets = toml.load(".streamlit/secrets.toml")["snowpark"]
session = Session.builder.configs(secrets).create()
session.sql("USE DATABASE STREAMLIT_DEMO").collect()
session.sql("USE SCHEMA STREAMLIT_USER_PROFILER").collect()
MyFilter.session = session
MyFilter.table_name = "CUSTOMERS"

# The filters read their widget values from st.session_state, which only exists
# inside a running Streamlit app, so give them a plain dict instead.
widget_values = {}
filterwidget.st = types.SimpleNamespace(
    session_state=widget_values, select_slider=st.select_slider, checkbox=st.checkbox
)

filters = (
    MyFilter("Current customer", "is_current_customer", "current_customer", st.checkbox),
    MyFilter("Tenure", "years_tenure", "tenure_slider", st.select_slider),
    MyFilter("Weekly workout count", "average_weekly_workout_count", "workouts_slider", st.select_slider),
)
current, tenure, workouts = filters

customers = session.table("CUSTOMERS")
total = customers.count()
check(total == 10000, f"CUSTOMERS holds the 10000 generated rows (found {total})")
check(0 < tenure.max_value <= 24, f"tenure slider maximum is in range (found {tenure.max_value})")
check(0 < workouts.max_value <= 7, f"workout slider maximum is in range (found {workouts.max_value})")

widget_values["current_customer"] = True
widget_values["tenure_slider"] = (5, tenure.max_value)
widget_values["workouts_slider"] = (2, 5)

table_sequence = [customers]
for _filter in filters:
    _filter.enable()
    last_table = table_sequence[-1]
    table_sequence.append(last_table[_filter(last_table)])

links = mk_links(table_sequence)
counts = links["value"]
print(f"Rows after each filter: {counts}")
check(counts == sorted(counts, reverse=True), "each filter keeps a subset of the previous rows")

expected = session.sql(
    "SELECT COUNT(*) FROM CUSTOMERS WHERE is_current_customer "
    f"AND years_tenure BETWEEN 5 AND {tenure.max_value} "
    "AND average_weekly_workout_count BETWEEN 2 AND 5"
).collect()[0][0]
check(0 < counts[-1] == expected, f"the filtered result matches the SQL filter ({counts[-1]} rows)")

labels = mk_labels(f.human_name for f in filters)
check(len(labels) == len(table_sequence) + 1, "the Sankey chart has a label per step")

# The app previews the result with .sample(n=5), which runs
# SELECT ... FROM (<subquery>) SAMPLE (5 ROWS). The Snowflake emulator rejects
# SAMPLE on a subquery, so report that and check the preview data with LIMIT.
try:
    preview = table_sequence[-1].sample(n=5).to_pandas()
except Exception as e:
    if "unexpected 'SELECT'" not in str(e):
        raise
    print("KNOWN EMULATOR ISSUE: SAMPLE on a subquery fails, checking the preview with LIMIT instead")
    preview = table_sequence[-1].limit(5).to_pandas()
check(len(preview) == 5, "the result preview returns 5 rows")

statements = [t.queries["queries"][0] for t in table_sequence]
check(all(statements), "every step has a SQL statement for the statement sequence")

print("Test passed: the app's filters work against Snowflake")
