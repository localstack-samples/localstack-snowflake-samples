"""``make demo``: a blocked release, a good release, a rollback, two recovery drills.

Runs against the sample database (``$INVENTORY_DATABASE``, default ``RETAIL``)
right after ``make seed``, so live starts at version 1.0.0.
"""

from __future__ import annotations

from snowflake.connector import SnowflakeConnection

from release import recovery
from release import settings as cfg
from release.deploy import migration_files, staged
from release.release import Exit, Releaser
from release.settings import Settings

BAD_MIGRATION = "demos/V1.3.0__bad_negative_stock.sql"


def heading(text: str) -> None:
    print(f"\n== {text}")


def indented(line: str) -> None:
    print(f"   {line}")


def expect(code: Exit, wanted: Exit) -> None:
    indented(f"exit code {int(code)} ({code.name})")
    if code is not wanted:
        raise SystemExit(f"demo: expected {wanted.name}, got {code.name}")


def snapshot(releaser: Releaser) -> tuple[dict[str, int], frozenset]:
    """What "live untouched" means here: every table's row count and the change history."""
    return releaser.row_counts(releaser.settings.schema), releaser.side(releaser.settings.schema).history


def columns(conn: SnowflakeConnection, settings: Settings, table: str) -> list[str]:
    rows = (
        conn.cursor()
        .execute(
            f"SELECT COLUMN_NAME FROM {settings.database}.INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            (settings.schema, table),
        )
        .fetchall()
    )
    return [name for (name,) in rows]


def main() -> None:
    settings = cfg.load()
    live = f"{settings.database}.{settings.schema}"
    with cfg.connect(settings) as conn:
        releaser = Releaser(settings, conn, out=indented)

        heading(f"0. Production today: {live}")
        releaser.status()
        indented(f"STOCK_LEVELS columns: {', '.join(columns(conn, settings, 'STOCK_LEVELS'))}")

        heading(f"1. A release with a bad migration stacked on top: {BAD_MIGRATION}")
        before = snapshot(releaser)
        with staged(BAD_MIGRATION, base=migration_files()) as root:
            expect(releaser.release(root, label=f"migrations/ + {BAD_MIGRATION}"), Exit.VALIDATION_FAILED)
        after = snapshot(releaser)
        indented(f"live row counts unchanged: {before[0] == after[0]}")
        indented(f"live change history unchanged: {before[1] == after[1]}")

        heading("2. The good release: migrations/ (V1.1.0, V1.2.0, R__inventory_views, A__grants)")
        expect(releaser.release(), Exit.OK)
        indented(f"STOCK_LEVELS columns: {', '.join(columns(conn, settings, 'STOCK_LEVELS'))}")
        row = (
            conn.cursor()
            .execute(
                f"SELECT STORE_ID, SKU, AVAILABLE, REORDER_POINT, AISLE, BAY FROM {live}.V_STOCK_POSITION "
                "ORDER BY STORE_ID, SKU LIMIT 1"
            )
            .fetchone()
        )
        indented(f"V_STOCK_POSITION: STORE_ID, SKU, AVAILABLE, REORDER_POINT, AISLE, BAY = {row}")
        releaser.status()

        heading("3. Roll back with one more swap")
        expect(releaser.rollback(), Exit.OK)
        releaser.status()
        indented(f"STOCK_LEVELS columns: {', '.join(columns(conn, settings, 'STOCK_LEVELS'))}")
        indented("A second rollback would be a roll-forward, so it is refused:")
        expect(releaser.rollback(), Exit.PRECONDITION)

        heading(f"4. Recovery drill: DROP TABLE {live}.PURCHASE_ORDERS, then UNDROP")
        cur = conn.cursor()
        count = cur.execute(f"SELECT COUNT(*) FROM {live}.PURCHASE_ORDERS").fetchone()[0]
        cur.execute(f"DROP TABLE {live}.PURCHASE_ORDERS")
        indented(f"dropped a table of {count} rows")
        recovery.undrop(conn, f"{live}.PURCHASE_ORDERS")
        restored = cur.execute(f"SELECT COUNT(*) FROM {live}.PURCHASE_ORDERS").fetchone()[0]
        open_pos = cur.execute(f"SELECT COUNT(*) FROM {live}.V_OPEN_PURCHASE_ORDERS").fetchone()[0]
        indented(f"UNDROP TABLE: {restored} rows back; V_OPEN_PURCHASE_ORDERS answers again ({open_pos} open)")

        heading("5. Recovery drill: a bad UPDATE, restored with BEFORE(STATEMENT => LAST_QUERY_ID())")
        total = f"SELECT SUM(ON_HAND) FROM {live}.STOCK_LEVELS"
        good = cur.execute(total).fetchone()[0]
        cur.execute(f"UPDATE {live}.STOCK_LEVELS SET ON_HAND = 0 WHERE STORE_ID = 'S03'")  # meant: one SKU
        bad_statement = recovery.last_query_id(conn)
        indented(f"UPDATE ... SET ON_HAND = 0 WHERE STORE_ID = 'S03' (query id {bad_statement})")
        indented(f"SUM(ON_HAND): {good} before, {cur.execute(total).fetchone()[0]} after the bad statement")
        rows = recovery.restore_before(conn, f"{live}.STOCK_LEVELS", bad_statement)
        indented(f"restored {rows} rows from a clone BEFORE(STATEMENT => ...)")
        indented(f"SUM(ON_HAND) = {cur.execute(total).fetchone()[0]}")
    print()


if __name__ == "__main__":
    main()
