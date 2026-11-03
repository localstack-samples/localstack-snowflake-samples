"""Recovery runbook for the two incidents a release cannot roll back.

A schema swap restores *structure*. It does not help when someone drops a live
table or runs a bad ``UPDATE`` between releases. Those are time-travel jobs:

``undrop``
    ``UNDROP TABLE`` brings back the most recent dropped version of a table,
    with its data, inside the table's retention period
    (``DATA_RETENTION_TIME_IN_DAYS``, 1 by default).

``restore_before``
    Rebuilds a table as it was immediately before one statement:

    1. ``CREATE TABLE <t>_BEFORE_<id> CLONE <t> BEFORE(STATEMENT => '<query id>')``:
       a zero-copy image of the table before the bad statement, kept until the
       restore is verified, so the damage can be diffed first;
    2. ``INSERT OVERWRITE INTO <t> SELECT * FROM <clone>``: restore in place, so
       the table keeps its identity, grants and dependent views;
    3. drop the clone.

    The statement id comes from the bad session (``SELECT LAST_QUERY_ID()``
    right after it), from ``QUERY_HISTORY``, or from Snowsight. Time travel
    only reaches statements inside the retention period; anything older fails
    with ``000707 Time travel data is not available``.
"""

from __future__ import annotations

from snowflake.connector import SnowflakeConnection


def undrop(conn: SnowflakeConnection, table: str) -> None:
    """Restore the most recently dropped version of ``table`` (``DB.SCHEMA.TABLE``)."""
    conn.cursor().execute(f"UNDROP TABLE {table}")


def last_query_id(conn: SnowflakeConnection) -> str:
    """The id of the statement this session ran last (the one to undo)."""
    return conn.cursor().execute("SELECT LAST_QUERY_ID()").fetchone()[0]


def restore_before(conn: SnowflakeConnection, table: str, query_id: str) -> int:
    """Restore ``table`` to its state just before statement ``query_id``; returns the restored row count."""
    before = f"{table}_BEFORE_{query_id.replace('-', '')[:12].upper()}"
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {before} CLONE {table} BEFORE(STATEMENT => %s)", (query_id,))
    try:
        restored = cur.execute(f"INSERT OVERWRITE INTO {table} SELECT * FROM {before}").fetchone()[0]
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {before}")
    return restored
