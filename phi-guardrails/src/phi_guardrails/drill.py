"""Apply a policy change temporarily, then put the original policy back.

Used by the regression test and ``make demo`` to show that the access matrix
catches a broken masking policy, without leaving the database broken.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import jinja2
from snowflake.connector import SnowflakeConnection

from phi_guardrails.settings import SAMPLE_ROOT

BROKEN_POLICY_SQL = SAMPLE_ROOT / "demos" / "broken-current-role-policy.sql"
STRING_POLICY = "GOVERNANCE.PHI_MASK_STRING"


def render(path: Path, database: str) -> str:
    """Render a SQL file the way schemachange renders migrations."""
    return jinja2.Template(path.read_text(encoding="utf-8")).render(database=database)


def policy_body(conn: SnowflakeConnection, policy: str) -> str:
    cur = conn.cursor().execute(f"DESCRIBE MASKING POLICY {policy}")
    columns = [d[0].lower() for d in cur.description]
    return cur.fetchone()[columns.index("body")]


@contextmanager
def temporarily_applied(
    conn: SnowflakeConnection,
    database: str,
    sql_file: Path = BROKEN_POLICY_SQL,
    policy: str = STRING_POLICY,
) -> Iterator[None]:
    """Run ``sql_file`` (which alters ``policy``), and restore ``policy``'s body on exit."""
    qualified = f"{database}.{policy}"
    original = policy_body(conn, qualified)
    conn.execute_string(render(sql_file, database))
    try:
        yield
    finally:
        conn.cursor().execute(f"ALTER MASKING POLICY {qualified} SET BODY -> {original}")
