"""Load the pinned Synthea extract into a deployed database (idempotent).

The load runs as the deploy identity in one transaction: tables are truncated
and refilled, so re-running it leaves exactly one copy of the data. TRUNCATE
(not DELETE) empties every row: the deploy role is not a facility role, so a
DELETE would only see the rows the facility row access policy shows it.
"""

from __future__ import annotations

from phi_guardrails import settings as cfg
from phi_guardrails.settings import Settings
from phi_guardrails.synthea import Dataset, load_dataset

LOAD_ORDER = ("CLINICAL.FACILITIES", "CLINICAL.PATIENTS", "CLINICAL.ENCOUNTERS")


def load(settings: Settings, dataset: Dataset | None = None) -> dict[str, int]:
    dataset = dataset or load_dataset()
    counts: dict[str, int] = {}
    with cfg.connect(settings) as conn:
        cur = conn.cursor()
        cur.execute(f"USE DATABASE IDENTIFIER('{settings.database}')")
        cur.execute("BEGIN")
        try:
            for table in reversed(LOAD_ORDER):
                cur.execute(f"TRUNCATE TABLE {table}")
            for table in LOAD_ORDER:
                rows = dataset.table(table)
                columns = list(rows[0])
                placeholders = ", ".join(["%s"] * len(columns))
                cur.executemany(
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                    [tuple(row[c] for c in columns) for row in rows],
                )
                counts[table] = len(rows)
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
    return counts
