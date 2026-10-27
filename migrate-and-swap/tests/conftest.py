"""Shared fixtures: every test gets its own freshly seeded inventory database.

``env`` creates ``MAS_TEST_<random>``, bootstraps its live ``INVENTORY``
schema at version 1.0.0 with the real schemachange baseline and loads the
deterministic data set (a few seconds), then drops the database afterwards
(``KEEP_TEST_DB=1`` keeps it for debugging). Tests never share a database, so
they run in parallel (``pytest -n``) and in any order.

Expected values in the tests come from ``data`` (the generated rows) and the
business rules written in the migrations, never from reading the result of
the code under test.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from snowflake.connector import SnowflakeConnection

from release import dataset
from release import settings as cfg
from release.deploy import migration_files, staged
from release.release import Releaser
from release.settings import Settings

BASELINE_STOCK_COLUMNS = ["STORE_ID", "SKU", "ON_HAND", "ALLOCATED", "LOCATION", "UPDATED_AT"]
RELEASED_STOCK_COLUMNS = ["STORE_ID", "SKU", "ON_HAND", "ALLOCATED", "UPDATED_AT", "REORDER_POINT", "AISLE", "BAY"]


def unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8].upper()}"


@pytest.fixture(scope="session")
def data() -> dataset.Dataset:
    return dataset.generate()


@pytest.fixture
def env(data: dataset.Dataset) -> Iterator[Settings]:
    """Settings for a database whose live schema is at V1.0.0 with the data set loaded."""
    settings = cfg.load().with_database(unique("MAS_TEST"))
    try:
        dataset.seed(settings, data)
        yield settings
    finally:
        if not os.getenv("KEEP_TEST_DB"):
            with cfg.connect(settings) as conn:
                conn.cursor().execute(f"DROP DATABASE IF EXISTS {settings.database}")


@pytest.fixture
def conn(env: Settings) -> Iterator[SnowflakeConnection]:
    """A session with no current database or schema: every name is fully qualified."""
    connection = cfg.connect(env)
    yield connection
    connection.close()


@pytest.fixture
def releaser(env: Settings, conn: SnowflakeConnection) -> Releaser:
    return Releaser(env, conn, out=lambda _line: None)


@contextmanager
def release_set(*extra: str | Path, tmp_sql: dict[str, str] | None = None) -> Iterator[Path]:
    """A schemachange root folder: migrations/ plus ``extra`` files plus ``tmp_sql`` {name: body} scripts."""
    with staged(*extra, base=migration_files()) as root:
        for name, body in (tmp_sql or {}).items():
            (root / name).write_text(body, encoding="utf-8")
        yield root


@contextmanager
def baseline_plus(scripts: dict[str, str]) -> Iterator[Path]:
    """A root folder holding only the bootstrap scripts plus ``scripts``: a release of just those changes."""
    with staged(*dataset.BASELINE_SCRIPTS) as root:
        for name, body in scripts.items():
            (root / name).write_text(body, encoding="utf-8")
        yield root


def query(conn: SnowflakeConnection, sql: str, params: tuple = ()) -> list[tuple]:
    return conn.cursor().execute(sql, params).fetchall()


def columns(conn: SnowflakeConnection, settings: Settings, schema: str, table: str) -> list[str]:
    return [
        name
        for (name,) in query(
            conn,
            f"SELECT COLUMN_NAME FROM {settings.database}.INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            (schema, table),
        )
    ]


def live_snapshot(conn: SnowflakeConnection, releaser: Releaser) -> dict:
    """Everything "live untouched" covers: row counts, change history, table shapes, view list."""
    settings = releaser.settings
    tables = sorted(releaser.row_counts(settings.schema))
    return {
        "row_counts": releaser.row_counts(settings.schema),
        "history": releaser.side(settings.schema).history,
        "columns": {t: columns(conn, settings, settings.schema, t) for t in tables},
        "views": query(
            conn,
            f"SELECT TABLE_NAME FROM {settings.database}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = %s ORDER BY 1",
            (settings.schema,),
        ),
    }
