"""Blue/green releases: the swap, the validation gate, rollback, and grants across the swap."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

from conftest import (
    BASELINE_STOCK_COLUMNS,
    RELEASED_STOCK_COLUMNS,
    baseline_plus,
    columns,
    live_snapshot,
    query,
    release_set,
)
from snowflake.connector import SnowflakeConnection

from release.dataset import Dataset
from release.release import Exit, Releaser
from release.settings import SAMPLE_ROOT, Settings

BAD_MIGRATION = "demos/V1.3.0__bad_negative_stock.sql"


# -- expected values, from the data set and the rules stated in the migrations --


def expected_reorder_points(data: Dataset) -> dict[tuple[str, str], int]:
    """V1.1.0: CEIL(units sold in the 28 days up to the newest movement / 4), 0 without sales."""
    newest = max(m["MOVED_AT"] for m in data.stock_movements)
    sold: dict[tuple[str, str], int] = {}
    for m in data.stock_movements:
        if m["MOVEMENT_TYPE"] == "SALE" and m["MOVED_AT"] > newest - timedelta(days=28):
            key = (m["STORE_ID"], m["SKU"])
            sold[key] = sold.get(key, 0) - m["QUANTITY"]
    return {(s["STORE_ID"], s["SKU"]): -(-sold.get((s["STORE_ID"], s["SKU"]), 0) // 4) for s in data.stock_levels}


def expected_shelf(location: str | None) -> tuple[str | None, str | None]:
    """V1.2.0: 'a07-b12 ' -> ('A07', 'B12'); NULL stays NULL."""
    if location is None:
        return None, None
    aisle, bay = location.strip().upper().split("-")
    return aisle, bay


def backordered(data: Dataset) -> set[tuple[str, str]]:
    return {(s["STORE_ID"], s["SKU"]) for s in data.stock_levels if s["ALLOCATED"] > s["ON_HAND"]}


def failed_checks(results) -> set[str]:
    return {r.check.name for r in results if not r.passed}


# -- the release --


def test_release_swaps_in_the_new_shape(
    releaser: Releaser, conn: SnowflakeConnection, env: Settings, data: Dataset
) -> None:
    assert releaser.release() is Exit.OK

    live, green = releaser.sides()
    assert live.version == "1.2.0"
    assert sorted(r[1] for r in live.history) == [
        "A__grants.sql",
        "A__grants.sql",
        "R__inventory_views.sql",
        "V1.0.0__baseline.sql",
        "V1.1.0__add_reorder_point.sql",
        "V1.2.0__split_location.sql",
    ]
    assert green.version == "1.0.0", "the previous version stays in green for rollback"
    assert columns(conn, env, env.schema, "STOCK_LEVELS") == RELEASED_STOCK_COLUMNS
    assert columns(conn, env, env.green_schema, "STOCK_LEVELS") == BASELINE_STOCK_COLUMNS

    table = f"{env.database}.{env.schema}.STOCK_LEVELS"
    rows = query(conn, f"SELECT STORE_ID, SKU, ON_HAND, ALLOCATED, REORDER_POINT, AISLE, BAY FROM {table}")
    reorder = expected_reorder_points(data)
    expected = {
        (s["STORE_ID"], s["SKU"]): (
            s["ON_HAND"],
            s["ALLOCATED"],
            reorder[s["STORE_ID"], s["SKU"]],
            *expected_shelf(s["LOCATION"]),
        )
        for s in data.stock_levels
    }
    assert {(r[0], r[1]): tuple(r[2:]) for r in rows} == expected
    assert releaser.row_counts(env.schema) == data.counts()


def test_views_serve_the_new_shape_from_live_tables(
    releaser: Releaser, conn: SnowflakeConnection, env: Settings, data: Dataset
) -> None:
    assert releaser.release() is Exit.OK
    live = f"{env.database}.{env.schema}"

    # Unqualified table names in the view bodies resolve in the view's own
    # schema, so after the swap the views read live's (the migrated) tables.
    position = query(conn, f"SELECT STORE_ID, SKU, AVAILABLE, REORDER_POINT, AISLE, BAY FROM {live}.V_STOCK_POSITION")
    reorder = expected_reorder_points(data)
    assert {(r[0], r[1]): r[2:] for r in position} == {
        (s["STORE_ID"], s["SKU"]): (
            s["ON_HAND"] - s["ALLOCATED"],
            reorder[s["STORE_ID"], s["SKU"]],
            *expected_shelf(s["LOCATION"]),
        )
        for s in data.stock_levels
    }

    on_order = {(p["STORE_ID"], p["SKU"]) for p in data.purchase_orders if p["STATUS"] == "OPEN"}
    expected_candidates = {
        (s["STORE_ID"], s["SKU"])
        for s in data.stock_levels
        if 0 < reorder[s["STORE_ID"], s["SKU"]]
        and s["ON_HAND"] - s["ALLOCATED"] <= reorder[s["STORE_ID"], s["SKU"]]
        and (s["STORE_ID"], s["SKU"]) not in on_order
    }
    candidates = query(conn, f"SELECT STORE_ID, SKU FROM {live}.V_REORDER_CANDIDATES")
    assert expected_candidates, "the data set must exercise the reorder view"
    assert set(candidates) == expected_candidates
    assert len(candidates) == len(expected_candidates)


def test_bad_migration_blocks_the_swap_and_leaves_live_untouched(
    releaser: Releaser, conn: SnowflakeConnection, env: Settings, data: Dataset
) -> None:
    """The headline: V1.3.0 drives ON_HAND negative on backordered rows; the gate stops the swap."""
    before = live_snapshot(conn, releaser)
    with release_set(BAD_MIGRATION) as root:
        cli = subprocess.run(
            [sys.executable, "-m", "release", "release", "--root-folder", str(root)],
            env=os.environ | {"INVENTORY_DATABASE": env.database},
            cwd=SAMPLE_ROOT,
            capture_output=True,
            text=True,
        )
    assert cli.returncode == Exit.VALIDATION_FAILED, cli.stdout + cli.stderr
    assert "FAIL no_negative_on_hand" in cli.stdout
    assert "RELEASE BLOCKED" in cli.stdout

    assert live_snapshot(conn, releaser) == before, "live must be byte-for-byte untouched"
    _, green = releaser.sides()
    assert green.version == "1.3.0", "the failed candidate stays in green for inspection"

    results = releaser.validate()
    assert failed_checks(results) == {"no_negative_on_hand"}
    [negative] = [r for r in results if r.check.name == "no_negative_on_hand"]
    assert {(store, sku) for store, sku, _ in negative.violations} == backordered(data)
    expected_on_hand = {(s["STORE_ID"], s["SKU"]): s["ON_HAND"] - s["ALLOCATED"] for s in data.stock_levels}
    assert all(on_hand == expected_on_hand[store, sku] < 0 for store, sku, on_hand in negative.violations)


def test_rollback_restores_the_previous_version(releaser: Releaser, conn: SnowflakeConnection, env: Settings) -> None:
    before = live_snapshot(conn, releaser)
    assert releaser.release() is Exit.OK
    released = releaser.side(env.schema).history

    assert releaser.rollback() is Exit.OK
    assert live_snapshot(conn, releaser) == before
    assert columns(conn, env, env.schema, "V_STOCK_POSITION")[-1] == "LOCATION"
    _, green = releaser.sides()
    assert green.version == "1.2.0"
    assert green.history == released

    # A second rollback would be a roll-forward: refused, nothing changes.
    assert releaser.rollback() is Exit.PRECONDITION
    assert live_snapshot(conn, releaser) == before


def test_failed_migration_exits_before_the_swap(releaser: Releaser, conn: SnowflakeConnection) -> None:
    before = live_snapshot(conn, releaser)
    broken = "UPDATE {{ database }}.{{ schema }}.STOCK_LEVELS SET ON_HAND = ON_HAND + ;\n"
    with release_set(tmp_sql={"V1.3.0__typo.sql": broken}) as root:
        assert releaser.release(root) is Exit.MIGRATION_FAILED
    assert live_snapshot(conn, releaser) == before


def test_release_requires_a_bootstrapped_live_schema(env: Settings, conn: SnowflakeConnection) -> None:
    empty = env.with_database(f"{env.database}_EMPTY")
    conn.cursor().execute(f"CREATE DATABASE {empty.database}")
    try:
        assert Releaser(empty, conn, out=lambda _: None).release() is Exit.PRECONDITION
        assert Releaser(empty, conn, out=lambda _: None).rollback() is Exit.PRECONDITION
    finally:
        conn.cursor().execute(f"DROP DATABASE {empty.database}")


# -- the gate catches each class of defect --


def test_writes_to_live_after_the_clone_fail_row_count_parity(
    releaser: Releaser, conn: SnowflakeConnection, env: Settings, data: Dataset
) -> None:
    """A write that reaches live mid-release would be lost by the swap; the parity check sees it."""
    releaser.clone()
    with release_set() as root:
        releaser.migrate(root)
    conn.cursor().execute(
        f"INSERT INTO {env.database}.{env.schema}.STOCK_MOVEMENTS "
        "(MOVEMENT_ID, STORE_ID, SKU, MOVEMENT_TYPE, QUANTITY, PO_ID, MOVED_AT) "
        "VALUES (999999, 'S01', 'SKU-10001', 'SALE', -1, NULL, '2026-10-06 09:00:00')"
    )
    results = releaser.validate()
    assert failed_checks(results) == {"row_count_parity"}
    [parity] = [r for r in results if r.check.name == "row_count_parity"]
    movements = len(data.stock_movements)
    assert parity.violations == [("STOCK_MOVEMENTS", movements + 1, movements)]


def test_orphan_movements_are_caught(releaser: Releaser, data: Dataset) -> None:
    delist = "DELETE FROM {{ database }}.{{ schema }}.STOCK_LEVELS WHERE SKU = 'SKU-10407';\n"
    with baseline_plus({"V1.1.0__delist_sku.sql": delist}) as root:
        assert releaser.release(root) is Exit.VALIDATION_FAILED
    results = releaser.validate()
    assert failed_checks(results) == {"row_count_parity", "no_orphan_movements"}
    [orphans] = [r for r in results if r.check.name == "no_orphan_movements"]
    expected = sorted(m["MOVEMENT_ID"] for m in data.stock_movements if m["SKU"] == "SKU-10407")
    assert expected, "the data set must have movements for the delisted SKU"
    assert [movement_id for movement_id, _, _ in orphans.violations] == expected


def test_dropped_view_is_caught(releaser: Releaser) -> None:
    drop = "DROP VIEW {{ database }}.{{ schema }}.V_OPEN_PURCHASE_ORDERS;\n"
    with baseline_plus({"V1.1.0__drop_view.sql": drop}) as root:
        assert releaser.release(root) is Exit.VALIDATION_FAILED
    results = releaser.validate()
    assert failed_checks(results) == {"no_view_dropped"}
    [dropped] = [r for r in results if r.check.name == "no_view_dropped"]
    assert dropped.violations == [("V_OPEN_PURCHASE_ORDERS",)]


def test_view_broken_by_a_dropped_table_is_caught(releaser: Releaser) -> None:
    """A migration replaces PURCHASE_ORDERS but forgets the view that reads it."""
    replace = (
        "CREATE TABLE {{ database }}.{{ schema }}.PURCHASE_ORDER_LINES AS\n"
        "    SELECT * FROM {{ database }}.{{ schema }}.PURCHASE_ORDERS;\n"
        "DROP TABLE {{ database }}.{{ schema }}.PURCHASE_ORDERS;\n"
    )
    with baseline_plus({"V1.1.0__replace_purchase_orders.sql": replace}) as root:
        assert releaser.release(root) is Exit.VALIDATION_FAILED
    results = {r.check.name: r for r in releaser.validate()}
    assert {name for name, r in results.items() if not r.passed} == {
        "row_count_parity",
        "receipts_match_purchase_orders",
        "view_selectable[V_OPEN_PURCHASE_ORDERS]",
    }
    assert results["view_selectable[V_OPEN_PURCHASE_ORDERS]"].error.startswith("002037")
    assert results["view_selectable[V_STOCK_POSITION]"].passed


# -- grants across CLONE and SWAP --


def _grantees(conn: SnowflakeConnection, on: str, privilege: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"SHOW GRANTS ON {on}")
    names = [d.name for d in cur.description]
    return {row[names.index("grantee_name")] for row in cur.fetchall() if row[names.index("privilege")] == privilege}


def test_clone_does_not_copy_grants_on_the_schema_itself(conn: SnowflakeConnection, env: Settings) -> None:
    """Why A__grants.sql exists: the swapped-in schema would otherwise lack USAGE for readers."""
    live = f"{env.database}.{env.schema}"
    clone = f"{env.database}.GRANT_PROBE"
    assert "INVENTORY_READER" in _grantees(conn, f"SCHEMA {live}", "USAGE")
    conn.cursor().execute(f"CREATE SCHEMA {clone} CLONE {live}")
    assert "INVENTORY_READER" not in _grantees(conn, f"SCHEMA {clone}", "USAGE")


def test_readers_keep_access_across_the_swap(releaser: Releaser, conn: SnowflakeConnection, env: Settings) -> None:
    assert releaser.release() is Exit.OK
    live = f"{env.database}.{env.schema}"
    assert "INVENTORY_READER" in _grantees(conn, f"SCHEMA {live}", "USAGE")
    for view in ("V_STOCK_POSITION", "V_REORDER_CANDIDATES", "V_OPEN_PURCHASE_ORDERS"):
        assert "INVENTORY_READER" in _grantees(conn, f"VIEW {live}.{view}", "SELECT"), view
    cur = conn.cursor()
    cur.execute(f"GRANT USAGE ON WAREHOUSE {env.warehouse} TO ROLE INVENTORY_READER")  # environment, not schema
    cur.execute("USE ROLE INVENTORY_READER")
    assert cur.execute(f"SELECT COUNT(*) FROM {live}.V_REORDER_CANDIDATES").fetchone()[0] > 0
