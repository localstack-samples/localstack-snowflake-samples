"""Loading the Synthea extract is idempotent."""

from __future__ import annotations

from phi_guardrails import settings as cfg
from phi_guardrails.load import LOAD_ORDER, load
from phi_guardrails.settings import Settings
from phi_guardrails.synthea import Dataset


def test_reloading_leaves_exactly_one_copy(deployed: Settings, dataset: Dataset) -> None:
    load(deployed, dataset)
    load(deployed, dataset)

    # AUDITOR passes the facility row access policy, so it counts every row.
    with cfg.connect(deployed, user="PERSONA_AUDITOR", role="AUDITOR") as conn:
        cur = conn.cursor()
        cur.execute(f"USE DATABASE {deployed.database}")
        counts = {table: cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in LOAD_ORDER}

    assert counts == {table: len(dataset.table(table)) for table in LOAD_ORDER}
