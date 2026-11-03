"""Run schemachange against one schema of the inventory database.

schemachange forwards every ``SNOWFLAKE_*`` environment variable to the
connector, so the subprocess gets a scrubbed environment that contains only the
connection this sample means to use. A developer's cloud credentials in the
shell can therefore never redirect an emulator deploy.

The change history table lives *inside* the schema it describes
(``<schema>.CHANGE_HISTORY``). A clone carries the history with it, schemachange
on the clone applies only what is new, and a swap moves schema and history
together, so ``status`` and ``rollback`` can always read the version of each
side from the side itself.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

from release import settings as cfg
from release.settings import SAMPLE_ROOT, Settings

SCHEMACHANGE = Path(sys.executable).with_name("schemachange")
MIGRATIONS = SAMPLE_ROOT / "migrations"
CHANGE_HISTORY = "CHANGE_HISTORY"


class DeployError(RuntimeError):
    """schemachange exited non-zero: a migration failed."""


def change_history_table(settings: Settings, schema: str) -> str:
    return f"{settings.database}.{schema}.{CHANGE_HISTORY}"


def schemachange_env(settings: Settings, schema: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SNOWFLAKE_", "SNOWSQL_"))}
    env.update(
        SNOWFLAKE_ACCOUNT=settings.account,
        SNOWFLAKE_USER=settings.user,
        SNOWFLAKE_ROLE=settings.role,
        SNOWFLAKE_WAREHOUSE=settings.warehouse,
        SNOWFLAKE_DATABASE=settings.database,
        SNOWFLAKE_SCHEMA=schema,
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


def deploy(settings: Settings, schema: str, *, root_folder: Path = MIGRATIONS, log_level: str = "WARNING") -> None:
    """Apply every pending script in ``root_folder`` to ``settings.database``.``schema``.

    Scripts address their objects as ``{{ database }}.{{ schema }}.<name>``, so the
    same migration files target the live schema on bootstrap and the green
    candidate on every release. Raises ``DeployError`` when a script fails.
    """
    template_vars = {"database": settings.database, "schema": schema}
    result = subprocess.run(
        [
            str(SCHEMACHANGE),
            "deploy",
            "--config-folder",
            str(SAMPLE_ROOT),
            "--root-folder",
            str(root_folder),
            "--change-history-table",
            change_history_table(settings, schema),
            "--vars",
            json.dumps(template_vars),
            "--log-level",
            log_level,
        ],
        env=schemachange_env(settings, schema),
        cwd=SAMPLE_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise DeployError(_error_summary(result.stderr + result.stdout))


_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_LOG_ERROR = re.compile(r"\[error\s*\]\s*(?P<message>.*\S)")


def _error_summary(output: str) -> str:
    """The Snowflake error schemachange logged, without colours and boilerplate."""
    messages = [m["message"] for m in _LOG_ERROR.finditer(_ANSI.sub("", output))]
    errors = [m for m in messages if re.match(r"\d{6} \(", m)]
    return (errors or messages or [output.strip().splitlines()[-1] if output.strip() else "no output"])[0]


@contextmanager
def staged(*scripts: Path | str, base: Iterable[Path] = ()) -> Iterator[Path]:
    """A temporary root folder holding exactly ``base`` plus ``scripts``.

    Used to deploy a subset of the migrations (``make seed`` bootstraps the
    baseline only) or to add a script to the set, the way a pull request
    would (the demo stacks ``demos/V1.3.0__bad_negative_stock.sql`` on top).
    """
    with tempfile.TemporaryDirectory(prefix="migrations-") as tmp:
        root = Path(tmp)
        for script in [*base, *scripts]:
            path = Path(script)
            shutil.copy(path if path.is_absolute() else SAMPLE_ROOT / path, root / path.name)
        yield root


def migration_files() -> list[Path]:
    return sorted(MIGRATIONS.glob("*.sql"))
