"""The release gate: parse ``validations.sql`` and run every check against green."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from jinja2 import Environment, StrictUndefined
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from release.deploy import CHANGE_HISTORY
from release.settings import Settings

VALIDATIONS = Path(__file__).with_name("validations.sql")
_CHECK = re.compile(r"^-- check: (?P<name>\S+)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class Check:
    name: str
    description: str
    sql: str


@dataclass(frozen=True)
class CheckResult:
    check: Check
    columns: tuple[str, ...] = ()
    violations: list[tuple] = field(default_factory=list)
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.error is None and not self.violations

    def summary(self) -> str:
        if self.error:
            return f"error: {self.error}"
        if not self.violations:
            return self.check.description.split(". ")[0].rstrip(".")
        example = ", ".join(f"{c}={v}" for c, v in zip(self.columns, self.violations[0], strict=True))
        return f"{len(self.violations)} violation(s), e.g. {example}"


def parse(text: str) -> list[Check]:
    """Split a rendered validations file into checks (anything before the first check is a header)."""
    marks = list(_CHECK.finditer(text))
    checks = []
    for mark, nxt in zip(marks, [*marks[1:], None], strict=True):
        body = text[mark.end() : nxt.start() if nxt else len(text)].strip().splitlines()
        description = " ".join(line[3:].strip() for line in body if line.startswith("-- "))
        sql = "\n".join(line for line in body if not line.startswith("--")).strip().rstrip(";").strip()
        if not sql:
            raise ValueError(f"check {mark['name']} in {VALIDATIONS.name} has no query")
        checks.append(Check(mark["name"], description, sql))
    return checks


def render(context: dict, template: Path = VALIDATIONS) -> list[Check]:
    env = Environment(undefined=StrictUndefined, keep_trailing_newline=True)
    return parse(env.from_string(template.read_text(encoding="utf-8")).render(**context))


def base_tables(conn: SnowflakeConnection, settings: Settings, schema: str) -> list[str]:
    """Base tables in ``schema``, without schemachange's change history table."""
    rows = (
        conn.cursor()
        .execute(
            f"SELECT TABLE_NAME FROM {settings.database}.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'",
            (schema,),
        )
        .fetchall()
    )
    return sorted(name for (name,) in rows if name != CHANGE_HISTORY)


def views(conn: SnowflakeConnection, settings: Settings, schema: str) -> list[str]:
    """Views in ``schema``."""
    sql = f"SELECT TABLE_NAME FROM {settings.database}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = %s"
    rows = conn.cursor().execute(sql, (schema,)).fetchall()
    return sorted(name for (name,) in rows)


def context(conn: SnowflakeConnection, settings: Settings) -> dict:
    return {
        "database": settings.database,
        "live_schema": settings.schema,
        "green_schema": settings.green_schema,
        "live": f"{settings.database}.{settings.schema}",
        "green": f"{settings.database}.{settings.green_schema}",
        "live_tables": base_tables(conn, settings, settings.schema),
        "green_views": views(conn, settings, settings.green_schema),
    }


def run_check(conn: SnowflakeConnection, check: Check) -> CheckResult:
    cur = conn.cursor()
    try:
        rows = cur.execute(check.sql).fetchall()
    except ProgrammingError as exc:
        message = str(exc.msg or exc).strip().splitlines()[-1]
        return CheckResult(check, error=f"{exc.errno or 0:06d} {message}")
    return CheckResult(check, tuple(d.name for d in cur.description), list(rows))


def run(conn: SnowflakeConnection, settings: Settings) -> list[CheckResult]:
    return [run_check(conn, check) for check in render(context(conn, settings))]
