"""dbt models and snapshot, checked against values computed independently."""

from __future__ import annotations

import csv
import datetime as dt
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd

from slim_ci_lab.dbt_runner import copy_project, dbt_env, run_dbt, targets

# Business rules, restated here rather than imported from the project:
# revenue is the passenger-paid total of billable trips (total >= 0)
# picked up in January 2024, attributed to the pickup zone and day.
MONTH_START, MONTH_END = pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")
JFK_AIRPORT, KNOWN_DAY = 132, dt.date(2024, 1, 15)
CENT = Decimal("0.01")


def _billable_trips(sample_file) -> pd.DataFrame:
    df = pd.read_parquet(sample_file, columns=["tpep_pickup_datetime", "PULocationID", "total_amount"])
    in_month = (df.tpep_pickup_datetime >= MONTH_START) & (df.tpep_pickup_datetime < MONTH_END)
    return df[in_month & (df.total_amount >= 0)]


def _money(value: float) -> Decimal:
    return Decimal(str(float(value))).quantize(CENT, rounding=ROUND_HALF_UP)


def test_daily_zone_revenue_matches_pandas(conn, prod_state, sample_file):
    trips = _billable_trips(sample_file)
    known = trips[(trips.PULocationID == JFK_AIRPORT) & (trips.tpep_pickup_datetime.dt.date == KNOWN_DAY)]
    assert len(known) > 10, "fixture precondition: the extract has enough JFK trips on the known day"
    expected = (len(known), sum((_money(v) for v in known.total_amount), Decimal("0.00")))

    actual = conn.cursor().execute(
        f"""SELECT trip_count, revenue, pickup_zone
            FROM {prod_state.database}.MARTS.FCT_DAILY_ZONE_REVENUE
            WHERE pickup_location_id = %s AND pickup_date = %s""",
        (JFK_AIRPORT, KNOWN_DAY),
    ).fetchall()

    assert actual == [(expected[0], expected[1], "JFK Airport")]


def test_incremental_second_run_merges_without_duplicates(conn, prod_state, sample_file):
    expected_rows = len(_billable_trips(sample_file))
    table = f"{prod_state.database}.INTERMEDIATE.INT_TRIPS_ENRICHED"

    second = run_dbt(["run", "--target", targets()[0], "--select", "int_trips_enriched"], prod_state.project_dir, prod_state.env)

    assert second.returncode == 0, second.stdout[-3000:]
    rows, keys = conn.cursor().execute(f"SELECT COUNT(*), COUNT(DISTINCT trip_id) FROM {table}").fetchone()
    assert (rows, keys) == (expected_rows, expected_rows)


def test_snapshot_records_zone_change_and_hard_delete(conn, database, first_load, tmp_path):
    project = copy_project(tmp_path / "dbt")
    env = dbt_env(DBT_DATABASE=database, DBT_PR_NUMBER="snapshot_test")
    build = [
        "build", "--target", targets()[1], "--indirect-selection", "cautious",
        "--select", "taxi_zone_lookup", "stg_taxi_zones", "dim_zone",
    ]
    first = run_dbt(build, project, env)
    assert first.returncode == 0, first.stdout[-3000:]

    seed = project / "seeds" / "taxi_zone_lookup.csv"
    with seed.open(newline="") as fh:
        rows = list(csv.reader(fh))
    header, zones = rows[0], rows[1:]
    assert zones[0][:3] == ["1", "EWR", "Newark Airport"] and zones[-1][0] == "265"
    zones[0][2] = "Newark Liberty International Airport"  # zone renamed upstream
    zones = zones[:-1]  # zone 265 removed upstream
    with seed.open("w", newline="") as fh:
        csv.writer(fh).writerows([header, *zones])

    second = run_dbt(build, project, env)
    assert second.returncode == 0, second.stdout[-3000:]

    history = conn.cursor().execute(
        f"""SELECT location_id, zone_name, dbt_valid_to IS NULL AS is_current, dbt_is_deleted
            FROM {database}.PR_SNAPSHOT_TEST.DIM_ZONE
            WHERE location_id IN (1, 265)
            ORDER BY location_id, dbt_valid_from, dbt_valid_to NULLS LAST"""
    ).fetchall()
    (total,) = conn.cursor().execute(f"SELECT COUNT(*) FROM {database}.PR_SNAPSHOT_TEST.DIM_ZONE").fetchone()

    assert history == [
        (1, "Newark Airport", False, "False"),
        (1, "Newark Liberty International Airport", True, "False"),
        (265, "Outside of NYC", False, "False"),
        (265, "Outside of NYC", True, "True"),
    ]
    assert total == 265 + 2
