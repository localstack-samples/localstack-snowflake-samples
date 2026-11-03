"""Run the schemachange migrations against one database.

Usage: ``python -m phi_guardrails.deploy [--database NAME]``

schemachange forwards every ``SNOWFLAKE_*`` environment variable to the
connector, so the subprocess gets a scrubbed environment that contains only the
connection this sample means to use. A developer's cloud credentials in the
shell can therefore never redirect an emulator deploy.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from phi_guardrails import settings as cfg
from phi_guardrails.settings import SAMPLE_ROOT, Settings

SCHEMACHANGE = Path(sys.executable).with_name("schemachange")


def change_history_table(database: str) -> str:
    return f"{database}.SCHEMACHANGE.CHANGE_HISTORY"


def schemachange_env(settings: Settings) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SNOWFLAKE_", "SNOWSQL_"))}
    env.update(
        SNOWFLAKE_ACCOUNT=settings.account,
        SNOWFLAKE_USER=settings.user,
        SNOWFLAKE_ROLE=settings.role,
        SNOWFLAKE_WAREHOUSE=settings.warehouse,
        SNOWFLAKE_DATABASE=settings.database,
        SNOWFLAKE_AUTHENTICATOR="SNOWFLAKE_JWT",
        SNOWFLAKE_PRIVATE_KEY_FILE=str(settings.private_key_path),
    )
    env[cfg.PLATFORM_DETECTION_ENV] = os.environ[cfg.PLATFORM_DETECTION_ENV]
    if settings.is_emulator:
        env.update(
            SNOWFLAKE_HOST=str(settings.host),
            SNOWFLAKE_PORT=str(settings.port),
            SNOWFLAKE_PROTOCOL=str(settings.protocol),
            SNOWFLAKE_DISABLE_OCSP_CHECKS="true",  # see Settings.connect_kwargs
        )
    return env


def deploy(settings: Settings, *, log_level: str = "WARNING") -> None:
    """Create the database if needed, then apply every pending migration in ``migrations/``."""
    with cfg.connect(settings) as conn:
        conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS IDENTIFIER('{settings.database}')")
    template_vars = {
        "database": settings.database,
        "warehouse": settings.warehouse,
        "persona_public_key": cfg.public_key_body(settings),
    }
    subprocess.run(
        [
            str(SCHEMACHANGE),
            "deploy",
            "--config-folder",
            str(SAMPLE_ROOT),
            "--change-history-table",
            change_history_table(settings.database),
            "--vars",
            json.dumps(template_vars),
            "--log-level",
            log_level,
        ],
        check=True,
        env=schemachange_env(settings),
        cwd=SAMPLE_ROOT,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", help="target database (default: $PHI_DATABASE or PHI)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    settings = cfg.load()
    if args.database:
        settings = settings.with_database(args.database.upper())
    deploy(settings, log_level=args.log_level)


if __name__ == "__main__":
    main()
