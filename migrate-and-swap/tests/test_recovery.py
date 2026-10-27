"""Recovery drills: UNDROP and time travel to just before a bad statement.

Every statement a drill travels back to is executed inside the test, after
seeding, so it is always inside the time-travel window.
"""

from __future__ import annotations

import pytest
from conftest import query
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from release import recovery
from release.dataset import Dataset
from release.settings import Settings

STOCK_COLUMNS = "STORE_ID, SKU, ON_HAND, ALLOCATED, LOCATION"


def stock_rows(conn: SnowflakeConnection, table: str) -> set[tuple]:
    return set(query(conn, f"SELECT {STOCK_COLUMNS} FROM {table}"))


def seeded_stock(data: Dataset) -> set[tuple]:
    return {(s["STORE_ID"], s["SKU"], s["ON_HAND"], s["ALLOCATED"], s["LOCATION"]) for s in data.stock_levels}


def test_undrop_restores_a_dropped_table(conn: SnowflakeConnection, env: Settings, data: Dataset) -> None:
    table = f"{env.database}.{env.schema}.PURCHASE_ORDERS"
    conn.cursor().execute(f"DROP TABLE {table}")
    with pytest.raises(ProgrammingError) as dropped:
        conn.cursor().execute(f"SELECT COUNT(*) FROM {table}")
    assert dropped.value.errno == 2003

    recovery.undrop(conn, table)

    restored = query(conn, f"SELECT PO_ID, STORE_ID, SKU, ORDERED_QTY, STATUS FROM {table}")
    assert sorted(restored) == sorted(
        (p["PO_ID"], p["STORE_ID"], p["SKU"], p["ORDERED_QTY"], p["STATUS"]) for p in data.purchase_orders
    )
    open_orders = query(conn, f"SELECT COUNT(*) FROM {env.database}.{env.schema}.V_OPEN_PURCHASE_ORDERS")
    assert open_orders == [(sum(p["STATUS"] == "OPEN" for p in data.purchase_orders),)]


def test_restore_before_a_bad_update(conn: SnowflakeConnection, env: Settings, data: Dataset) -> None:
    table = f"{env.database}.{env.schema}.STOCK_LEVELS"
    conn.cursor().execute(f"UPDATE {table} SET ON_HAND = 0 WHERE STORE_ID = 'S03'")  # meant: one SKU
    bad_statement = recovery.last_query_id(conn)
    damaged = stock_rows(conn, table)
    assert len(seeded_stock(data) - damaged) == sum(
        1 for s in data.stock_levels if s["STORE_ID"] == "S03" and s["ON_HAND"] != 0
    )

    restored = recovery.restore_before(conn, table, bad_statement)

    assert restored == len(data.stock_levels)
    assert stock_rows(conn, table) == seeded_stock(data)
    leftovers = query(
        conn,
        f"SELECT TABLE_NAME FROM {env.database}.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME LIKE 'STOCK_LEVELS_BEFORE_%%'",
        (env.schema,),
    )
    assert leftovers == [], "the before-image clone is dropped once the restore is done"


def test_restore_keeps_later_statements_out(conn: SnowflakeConnection, env: Settings, data: Dataset) -> None:
    """BEFORE(STATEMENT => bad) is a point in time: a later write is undone too, which the runbook must say."""
    table = f"{env.database}.{env.schema}.STOCK_LEVELS"
    cur = conn.cursor()
    cur.execute(f"UPDATE {table} SET ALLOCATED = 0 WHERE STORE_ID = 'S05'")
    bad_statement = recovery.last_query_id(conn)
    cur.execute(f"UPDATE {table} SET ON_HAND = ON_HAND + 1 WHERE STORE_ID = 'S06'")
    recovery.restore_before(conn, table, bad_statement)
    assert stock_rows(conn, table) == seeded_stock(data)


def test_undrop_of_a_table_that_exists_fails(conn: SnowflakeConnection, env: Settings) -> None:
    with pytest.raises(ProgrammingError) as exc:
        recovery.undrop(conn, f"{env.database}.{env.schema}.PURCHASE_ORDERS")
    assert (exc.value.errno, exc.value.sqlstate) == (2002, "42710")
    assert "Object 'PURCHASE_ORDERS' already exists" in exc.value.msg


def test_time_travel_before_the_table_existed_fails(conn: SnowflakeConnection, env: Settings) -> None:
    schema = f"{env.database}.{env.schema}"
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {schema}.STORES")
    earlier = recovery.last_query_id(conn)
    cur.execute(f"CREATE TABLE {schema}.REPLENISHMENT_RUNS (RUN_ID NUMBER)")
    with pytest.raises(ProgrammingError) as exc:
        recovery.restore_before(conn, f"{schema}.REPLENISHMENT_RUNS", earlier)
    assert (exc.value.errno, exc.value.sqlstate) == (707, "02000")
    assert "Time travel data is not available for table REPLENISHMENT_RUNS" in exc.value.msg
