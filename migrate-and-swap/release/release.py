"""Blue/green schema releases: clone live, migrate the clone, validate it, swap it in.

Usage: ``python -m release {release,rollback,status} [options]``

``release``
    1. ``CREATE OR REPLACE SCHEMA <live>_GREEN CLONE <live>`` (zero-copy, seconds)
    2. ``schemachange deploy`` against green (the change history travels with it)
    3. the gate in ``release/validations.sql`` against green
    4. ``ALTER SCHEMA <live> SWAP WITH <live>_GREEN`` (one atomic rename)

    Any failure in steps 2 or 3 exits non-zero *before* step 4, so live is never
    touched. After a swap, green holds the previous version.

``rollback``
    Swaps once more, if green holds the version live was released from.

``status``
    The version on each side and how the two sides relate.

Exit codes: 0 done, 1 validation failed (not swapped), 2 a migration failed
(not swapped), 3 precondition not met (nothing changed).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path

from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from release import gate
from release import settings as cfg
from release.deploy import CHANGE_HISTORY, MIGRATIONS, DeployError, deploy
from release.settings import Settings

Printer = Callable[[str], None]


class Exit(IntEnum):
    OK = 0
    VALIDATION_FAILED = 1
    MIGRATION_FAILED = 2
    PRECONDITION = 3


@dataclass(frozen=True)
class Side:
    """One schema and the schemachange history it carries."""

    schema: str
    exists: bool
    history: frozenset[tuple] = frozenset()  # (VERSION, SCRIPT, CHECKSUM, INSTALLED_ON) rows

    @property
    def version(self) -> str | None:
        versions = [row[0] for row in self.history if row[0]]
        return max(versions, key=_version_key) if versions else None


def _version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def describe(live: Side, green: Side) -> str:
    """How green relates to live, from their change histories."""
    if not live.exists:
        return "n/a (no live schema)"
    if not green.exists:
        return "absent"
    if green.history < live.history:
        return "previous version (rollback target)"
    if green.history > live.history:
        extra = len(green.history - live.history)
        return f"ahead of live by {extra} script run(s): a blocked, aborted or rolled-back candidate"
    if green.history == live.history:
        return "identical to live"
    return "diverged from live (neither history contains the other)"


class Releaser:
    def __init__(self, settings: Settings, conn: SnowflakeConnection, out: Printer = print) -> None:
        self.settings = settings
        self.conn = conn
        self.out = out
        self.live = f"{settings.database}.{settings.schema}"
        self.green = f"{settings.database}.{settings.green_schema}"

    # -- building blocks (public so a test can interleave its own statements) --

    def side(self, schema: str) -> Side:
        table = f"{self.settings.database}.{schema}.{CHANGE_HISTORY}"
        try:
            rows = self.conn.cursor().execute(f"SELECT VERSION, SCRIPT, CHECKSUM, INSTALLED_ON FROM {table}").fetchall()
        except ProgrammingError as exc:
            if exc.errno == 2003:  # the schema or its history table does not exist
                return Side(schema, exists=False)
            raise
        return Side(schema, exists=True, history=frozenset(rows))

    def sides(self) -> tuple[Side, Side]:
        return self.side(self.settings.schema), self.side(self.settings.green_schema)

    def row_counts(self, schema: str) -> dict[str, int]:
        cur = self.conn.cursor()
        return {
            table: cur.execute(f"SELECT COUNT(*) FROM {self.settings.database}.{schema}.{table}").fetchone()[0]
            for table in gate.base_tables(self.conn, self.settings, schema)
        }

    def clone(self) -> None:
        self.conn.cursor().execute(f"CREATE OR REPLACE SCHEMA {self.green} CLONE {self.live}")

    def migrate(self, root_folder: Path) -> None:
        deploy(self.settings, self.settings.green_schema, root_folder=root_folder)

    def validate(self) -> list[gate.CheckResult]:
        return gate.run(self.conn, self.settings)

    def swap(self) -> None:
        self.conn.cursor().execute(f"ALTER SCHEMA {self.live} SWAP WITH {self.green}")

    # -- commands --

    def release(self, root_folder: Path = MIGRATIONS, *, label: str | None = None) -> Exit:
        live, _ = self.sides()
        if not live.exists:
            self.out(f"{self.live} has no change history: bootstrap it first (make seed).")
            return Exit.PRECONDITION
        self.out(f"Releasing {label or root_folder.name + '/'} to {self.live} (live at version {live.version})")

        self.clone()
        self.out(f"  1. cloned {self.live} -> {self.green}")

        try:
            self.migrate(root_folder)
        except DeployError as exc:
            self.out(f"  2. migration FAILED on {self.green}: {exc}")
            self.out(f"RELEASE ABORTED: {self.live} untouched; inspect {self.green}.")
            return Exit.MIGRATION_FAILED
        candidate = self.side(self.settings.green_schema)
        ran = [row[1] for row in sorted(candidate.history - live.history, key=lambda row: row[3])]
        self.out(f"  2. schemachange on {self.green}: version {candidate.version}, ran {', '.join(ran) or 'nothing'}")

        results = self.validate()
        failed = [r for r in results if not r.passed]
        self.out(f"  3. validations: {len(results) - len(failed)} of {len(results)} passed")
        for result in results:
            self.out(f"     {'PASS' if result.passed else 'FAIL'} {result.check.name:<40} {result.summary()}")
        if failed:
            self.out(f"RELEASE BLOCKED: {len(failed)} check(s) failed; {self.live} untouched.")
            self.out(f"The candidate stays in {self.green} for inspection.")
            return Exit.VALIDATION_FAILED

        self.swap()
        self.out(f"  4. swapped: {self.live} is now version {candidate.version}; {self.green} holds {live.version}")
        self.out(f"RELEASED {candidate.version}. Roll back with: python -m release rollback")
        return Exit.OK

    def rollback(self) -> Exit:
        live, green = self.sides()
        if not (live.exists and green.exists and green.history < live.history):
            self.out(f"Nothing to roll back: {self.green} is {describe(live, green)}.")
            return Exit.PRECONDITION
        self.out(f"Rolling back {self.live} from {live.version} to {green.version}")
        live_rows, green_rows = self.row_counts(live.schema), self.row_counts(green.schema)
        drift = {t: (n, green_rows.get(t)) for t, n in live_rows.items() if green_rows.get(t) != n}
        for table, (now, then) in sorted(drift.items()):
            self.out(f"  note: {table} has {now} rows live, {then} in the previous version (writes since the release)")
        self.swap()
        self.out(f"ROLLED BACK: {self.live} is version {green.version} again; {self.green} holds {live.version}")
        return Exit.OK

    def status(self) -> Exit:
        live, green = self.sides()
        self.out(f"live   {self.live:<32} version {live.version if live.exists else '(missing)'}")
        green_version = green.version if green.exists else "-"
        self.out(f"green  {self.green:<32} version {green_version}: {describe(live, green)}")
        return Exit.OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m release", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    rel = sub.add_parser("release", help="clone, migrate, validate and swap")
    rel.add_argument("--root-folder", type=Path, default=MIGRATIONS, help="schemachange scripts (default: migrations/)")
    sub.add_parser("rollback", help="swap back to the previous version")
    sub.add_parser("status", help="show the version on each side")
    args = parser.parse_args(argv)

    settings = cfg.load()
    with cfg.connect(settings) as conn:
        releaser = Releaser(settings, conn)
        if args.command == "release":
            return releaser.release(args.root_folder.resolve())
        if args.command == "rollback":
            return releaser.rollback()
        return releaser.status()


if __name__ == "__main__":
    sys.exit(main())
