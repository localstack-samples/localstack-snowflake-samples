"""Policy-coverage gate: every PHI_TYPE-tagged column must be masked.

Usage: ``python -m phi_guardrails.coverage [--database NAME]`` (exit 1 on an unprotected column)

Tag-based masking only protects a tagged column when the tag carries a policy
for that column's data type. A ZIP stored as NUMBER, or a timestamp stored as
TIMESTAMP_TZ, keeps its tag and silently stays in clear. The gate lists every
tagged column in the database, looks up its effective masking policy, and
fails on any column that has none.

Only INFORMATION_SCHEMA table functions are used (TAG_REFERENCES_ALL_COLUMNS,
POLICY_REFERENCES): unlike the ACCOUNT_USAGE views they have no ingestion
latency, so the gate can run straight after a deploy.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from snowflake.connector import SnowflakeConnection

from phi_guardrails import settings as cfg

TAG_SCHEMA = "GOVERNANCE"
TAG_NAME = "PHI_TYPE"


@dataclass(frozen=True)
class TaggedColumn:
    table: str  # SCHEMA.TABLE
    column: str
    data_type: str
    phi_type: str
    masking_policy: str | None  # SCHEMA.POLICY, or None when unprotected

    @property
    def protected(self) -> bool:
        return self.masking_policy is not None


def _tables(cur, database: str) -> list[tuple[str, str]]:
    cur.execute(
        f"SELECT TABLE_SCHEMA, TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA' "
        "ORDER BY 1, 2"
    )
    return cur.fetchall()


def scan(conn: SnowflakeConnection, database: str) -> list[TaggedColumn]:
    """Every column of every table in ``database`` that carries the PHI_TYPE tag."""
    cur = conn.cursor()
    cur.execute(f"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM {database}.INFORMATION_SCHEMA.COLUMNS")
    data_types = {(s, t, c): d for s, t, c, d in cur.fetchall()}
    found: list[TaggedColumn] = []
    for schema, table in _tables(cur, database):
        fq = f"{database}.{schema}.{table}"
        cur.execute(
            "SELECT COLUMN_NAME, TAG_VALUE "
            f"FROM TABLE({database}.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(%s, 'table')) "
            "WHERE TAG_DATABASE = %s AND TAG_SCHEMA = %s AND TAG_NAME = %s",
            (fq, database, TAG_SCHEMA, TAG_NAME),
        )
        tagged = dict(cur.fetchall())
        if not tagged:
            continue
        cur.execute(
            "SELECT REF_COLUMN_NAME, POLICY_SCHEMA || '.' || POLICY_NAME "
            f"FROM TABLE({database}.INFORMATION_SCHEMA.POLICY_REFERENCES("
            "REF_ENTITY_NAME => %s, REF_ENTITY_DOMAIN => 'table')) "
            "WHERE POLICY_KIND = 'MASKING_POLICY' AND POLICY_STATUS = 'ACTIVE'",
            (fq,),
        )
        policies = dict(cur.fetchall())
        found.extend(
            TaggedColumn(
                table=f"{schema}.{table}",
                column=column,
                data_type=data_types.get((schema, table, column), "?"),
                phi_type=phi_type,
                masking_policy=policies.get(column),
            )
            for column, phi_type in sorted(tagged.items())
        )
    return found


def uncovered(columns: list[TaggedColumn]) -> list[TaggedColumn]:
    return [c for c in columns if not c.protected]


def report(columns: list[TaggedColumn]) -> str:
    lines = [f"{'column':<40} {'data type':<14} {'phi_type':<16} masking policy"]
    for c in columns:
        policy = c.masking_policy or "NONE  <-- unprotected"
        lines.append(f"{c.table + '.' + c.column:<40} {c.data_type:<14} {c.phi_type:<16} {policy}")
    gaps = uncovered(columns)
    lines.append(
        f"{len(columns)} tagged column(s), {len(gaps)} without a masking policy" + ("" if gaps else ": gate passed")
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fail if a PHI_TYPE-tagged column has no masking policy.")
    parser.add_argument("--database", help="database to scan (default: $PHI_DATABASE or PHI)")
    args = parser.parse_args()
    settings = cfg.load()
    database = (args.database or settings.database).upper()
    with cfg.connect(settings) as conn:
        columns = scan(conn, database)
    print(report(columns))
    sys.exit(1 if uncovered(columns) else 0)


if __name__ == "__main__":
    main()
