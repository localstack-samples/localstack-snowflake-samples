"""schemachange deploys: change history and idempotency."""

from __future__ import annotations

from snowflake.connector import SnowflakeConnection

from phi_guardrails.deploy import change_history_table, deploy
from phi_guardrails.settings import SAMPLE_ROOT, Settings

MIGRATIONS = sorted(p.name for p in (SAMPLE_ROOT / "migrations").glob("*.sql"))


def _history(admin: SnowflakeConnection, database: str) -> list[tuple]:
    return (
        admin.cursor()
        .execute(f"SELECT VERSION, SCRIPT, STATUS FROM {change_history_table(database)} ORDER BY INSTALLED_ON, SCRIPT")
        .fetchall()
    )


def test_change_history_lists_every_migration(admin: SnowflakeConnection, deployed: Settings) -> None:
    history = _history(admin, deployed.database)
    assert sorted(script for _, script, _ in history) == MIGRATIONS
    assert {status for _, _, status in history} == {"Success"}
    versions = [version for version, script, _ in history if script.startswith("V")]
    assert versions == ["1.0.0", "1.1.0", "1.2.0", "1.3.0"]


def test_redeploy_applies_nothing(admin: SnowflakeConnection, deployed: Settings) -> None:
    before = _history(admin, deployed.database)
    deploy(deployed)
    assert _history(admin, deployed.database) == before
