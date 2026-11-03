"""Evaluate access-contract cases against a deployed database.

The :class:`Oracle` derives every expected value from the loaded Synthea rows
and the contract alone: visible rows from the persona's facilities, clear
values from the source data, masked values from the masking rule stated in the
contract header. It never reads a policy, so a policy bug cannot leak into the
expectation.

The :class:`Observer` queries each object once per persona, connected as that
persona's user with its role, and caches the result for all column cases.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from phi_guardrails import settings as cfg
from phi_guardrails.contract import AccessObject, Case, Contract
from phi_guardrails.settings import Settings
from phi_guardrails.synthea import Dataset

MASK = "***MASKED***"
# Authorization errors that satisfy a "denied" cell: 002003 (object does not
# exist or not authorized) and 003001 (insufficient privileges). The contract
# pins that access is refused, not which of the two messages is returned.
NOT_AUTHORIZED = frozenset({2003, 3001})
SUPPRESSION_THRESHOLD = 11  # ENCOUNTER_SUMMARY hides cells with fewer patients
Row = dict[str, object]
Key = tuple


def masked(value: object, sample: object) -> object:
    """The contract's masking rule; ``sample`` is any non-null value of the column."""
    if isinstance(sample, datetime):
        return None if value is None else datetime(value.year, 1, 1)  # type: ignore[union-attr]
    if isinstance(sample, date):
        return None if value is None else date(value.year, 1, 1)  # type: ignore[union-attr]
    return MASK


# Python equivalents of the views in migrations/R__analyst_views.sql, computed
# over the source rows (not over the database).
def _encounter_summary(data: Dataset) -> list[Row]:
    names = {f["FACILITY_ID"]: f["NAME"] for f in data.facilities}
    groups: dict[Key, list[Row]] = defaultdict(list)
    for enc in data.encounters:
        groups[(enc["FACILITY_ID"], enc["START_TS"].year, enc["ENCOUNTER_CLASS"])].append(enc)  # type: ignore[union-attr]
    out = []
    for (facility, year, klass), encs in groups.items():
        patients = {e["PATIENT_ID"] for e in encs}
        if len(patients) >= SUPPRESSION_THRESHOLD:
            out.append(
                {
                    "FACILITY_ID": facility,
                    "FACILITY_NAME": names[facility],
                    "ENCOUNTER_YEAR": year,
                    "ENCOUNTER_CLASS": klass,
                    "ENCOUNTERS": len(encs),
                    "PATIENTS": len(patients),
                    "TOTAL_CLAIM_COST": sum((e["TOTAL_CLAIM_COST"] for e in encs), Decimal("0")),
                }
            )
    return out


def _patient_cohort(data: Dataset) -> list[Row]:
    cols = ("PATIENT_ID", "PRIMARY_FACILITY_ID", "BIRTH_DATE", "GENDER", "RACE", "ETHNICITY", "STATE", "ZIP", "EMAIL")
    return [{c: p[c] for c in cols} for p in data.patients]


VIEW_SOURCES = {
    "ANALYTICS.ENCOUNTER_SUMMARY": _encounter_summary,
    "ANALYTICS.PATIENT_COHORT": _patient_cohort,
}


class Oracle:
    def __init__(self, contract: Contract, data: Dataset) -> None:
        self.contract = contract
        self.data = data

    def source_rows(self, obj: AccessObject) -> list[Row]:
        view = VIEW_SOURCES.get(obj.name)
        return view(self.data) if view else self.data.table(obj.name)

    def visible_rows(self, persona: str, obj: AccessObject) -> dict[Key, Row]:
        rule = obj.rows[persona]
        rows = self.source_rows(obj)
        if rule == "facility":
            allowed = set(self.contract.personas[persona].facilities)
            rows = [r for r in rows if r[obj.facility_column] in allowed]  # type: ignore[index]
        elif rule in ("none", "denied"):
            rows = []
        return {tuple(r[k] for k in obj.key): r for r in rows}

    def expected_value(self, obj: AccessObject, column: str, outcome: str, row: Row) -> object:
        if outcome == "clear":
            return row[column]
        sample = next((r[column] for r in self.source_rows(obj) if r[column] is not None), "")
        return masked(row[column], sample)


@dataclass
class Observation:
    columns: list[str] = field(default_factory=list)
    rows: dict[Key, Row] = field(default_factory=dict)
    error: ProgrammingError | None = None


class Observer:
    """Reads each object once per persona, as the persona's user and role."""

    def __init__(self, settings: Settings, contract: Contract) -> None:
        self.settings = settings
        self.contract = contract
        self._conns: dict[str, SnowflakeConnection] = {}
        self._cache: dict[tuple[str, str], Observation] = {}

    def connection(self, persona: str) -> SnowflakeConnection:
        if persona not in self._conns:
            p = self.contract.personas[persona]
            self._conns[persona] = cfg.connect(self.settings, user=p.user, role=p.role)
        return self._conns[persona]

    def observe(self, persona: str, obj: AccessObject) -> Observation:
        cache_key = (persona, obj.name)
        if cache_key not in self._cache:
            self._cache[cache_key] = self._query(persona, obj)
        return self._cache[cache_key]

    def _query(self, persona: str, obj: AccessObject) -> Observation:
        cur = self.connection(persona).cursor()
        try:
            cur.execute(f"SELECT * FROM {self.settings.database}.{obj.name}")
        except ProgrammingError as exc:
            return Observation(error=exc)
        columns = [d[0] for d in cur.description]
        rows = {}
        for values in cur.fetchall():
            row = dict(zip(columns, values, strict=True))
            rows[tuple(row[k] for k in obj.key)] = row
        return Observation(columns=columns, rows=rows)

    def invalidate(self) -> None:
        """Forget cached results, e.g. after a policy change."""
        self._cache.clear()

    def close(self) -> None:
        for conn in self._conns.values():
            conn.close()
        self._conns.clear()


def _describe_error(exc: ProgrammingError) -> str:
    return f"{exc.errno:06d} {str(exc.msg).splitlines()[-1].strip()}"


def _sample(keys: set, limit: int = 2) -> str:
    return ", ".join(repr(k[0] if len(k) == 1 else k) for k in sorted(keys, key=repr)[:limit])


class Matrix:
    def __init__(self, settings: Settings, contract: Contract, data: Dataset) -> None:
        self.contract = contract
        self.oracle = Oracle(contract, data)
        self.observer = Observer(settings, contract)

    def check(self, case: Case) -> list[str]:
        """Return why ``case`` fails; an empty list means it passes."""
        obj = self.contract.objects[case.object]
        obs = self.observer.observe(case.persona, obj)
        if case.column is None:
            return self._check_rows(case, obj, obs)
        return self._check_column(case, obj, obs)

    def _check_rows(self, case: Case, obj: AccessObject, obs: Observation) -> list[str]:
        if case.expect == "denied":
            if obs.error is None:
                return [f"expected an authorization error, but the query returned {len(obs.rows)} row(s)"]
            if obs.error.errno not in NOT_AUTHORIZED:
                return [f"expected an authorization error, got {_describe_error(obs.error)}"]
            return []
        if obs.error is not None:
            return [f"query failed: {_describe_error(obs.error)}"]
        expected = set(self.oracle.visible_rows(case.persona, obj))
        actual = set(obs.rows)
        problems = []
        if missing := expected - actual:
            problems.append(f"{len(missing)} expected row(s) not visible, e.g. {_sample(missing)}")
        if extra := actual - expected:
            problems.append(f"{len(extra)} row(s) visible that should not be, e.g. {_sample(extra)}")
        return problems

    def _check_column(self, case: Case, obj: AccessObject, obs: Observation) -> list[str]:
        if obs.error is not None:
            return [f"query failed: {_describe_error(obs.error)}"]
        if case.column not in obs.columns:
            return [f"column {case.column} is not in the result ({', '.join(obs.columns)})"]
        expected_rows = self.oracle.visible_rows(case.persona, obj)
        keys = expected_rows.keys() & obs.rows.keys()
        if not keys:
            return ["no visible rows to compare (see the rows case)"]
        wrong = []
        for key in sorted(keys, key=repr):
            want = self.oracle.expected_value(obj, case.column, case.expect, expected_rows[key])
            got = obs.rows[key][case.column]
            if got != want:
                wrong.append((key, want, got))
        if not wrong:
            return []
        key, want, got = wrong[0]
        return [
            f"{len(wrong)} of {len(keys)} row(s) not {case.expect}, "
            f"e.g. {_sample({key}, 1)}: expected {want!r}, got {got!r}"
        ]

    def run(self, cases: list[Case] | None = None) -> list[tuple[Case, list[str]]]:
        return [(case, self.check(case)) for case in (cases or self.contract.cases())]

    def close(self) -> None:
        self.observer.close()
