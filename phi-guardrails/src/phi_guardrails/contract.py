"""Parse and validate ``access-contract.yml`` into test cases.

Each cell of the contract becomes one :class:`Case`:

* a rows case per (persona, object): ``all``, ``facility``, ``none`` or ``denied``;
* a column case per (persona, object, column): ``clear`` or ``masked``, for
  personas that can see rows of the object.

Validation is strict, so the matrix cannot silently shrink: a persona missing
from a ``rows`` rule, or a column missing a cell for a persona that can see the
rows, is an error rather than an untested cell.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from phi_guardrails.settings import SAMPLE_ROOT

CONTRACT_PATH = SAMPLE_ROOT / "access-contract.yml"
ROW_OUTCOMES = frozenset({"all", "facility", "none", "denied"})
COLUMN_OUTCOMES = frozenset({"clear", "masked"})
VISIBLE = frozenset({"all", "facility"})


class ContractError(ValueError):
    pass


@dataclass(frozen=True)
class Persona:
    role: str
    user: str
    facilities: tuple[int, ...]


@dataclass(frozen=True)
class Column:
    name: str
    access: dict[str, str]
    phi_type: str | None
    declares_phi_type: bool


@dataclass(frozen=True)
class AccessObject:
    name: str
    key: tuple[str, ...]
    rows: dict[str, str]
    columns: dict[str, Column]
    facility_column: str | None

    @property
    def is_table(self) -> bool:
        """Tables classify their columns (phi_type); views inherit classification."""
        return all(c.declares_phi_type for c in self.columns.values())


@dataclass(frozen=True)
class Case:
    persona: str
    object: str
    column: str | None
    expect: str

    @property
    def id(self) -> str:
        return f"{self.persona}-{self.object}-{self.column or 'rows'}-{self.expect}"


@dataclass(frozen=True)
class Contract:
    personas: dict[str, Persona]
    objects: dict[str, AccessObject]

    def cases(self) -> list[Case]:
        out: list[Case] = []
        for obj in self.objects.values():
            for role in self.personas:
                out.append(Case(role, obj.name, None, obj.rows[role]))
                if obj.rows[role] in VISIBLE:
                    out.extend(Case(role, obj.name, col.name, col.access[role]) for col in obj.columns.values())
        return out


def _persona(role: str, spec: dict) -> Persona:
    return Persona(role=role, user=spec["user"], facilities=tuple(spec.get("facilities", ())))


def _object(name: str, spec: dict, personas: dict[str, Persona]) -> AccessObject:
    rows = spec["rows"]
    if set(rows) != set(personas):
        raise ContractError(f"{name}: rows must list exactly {sorted(personas)}, got {sorted(rows)}")
    if bad := {r: o for r, o in rows.items() if o not in ROW_OUTCOMES}:
        raise ContractError(f"{name}: unknown row outcome(s) {bad}")
    facility_column = spec.get("facility_column")
    for role, outcome in rows.items():
        if outcome == "facility" and not (facility_column and personas[role].facilities):
            raise ContractError(f"{name}: {role} is facility-scoped but facilities/facility_column is missing")
    visible = {r for r, o in rows.items() if o in VISIBLE}
    columns = {}
    for col_name, cell in spec["columns"].items():
        access = {k: v for k, v in cell.items() if k != "phi_type"}
        if set(access) != visible:
            raise ContractError(
                f"{name}.{col_name}: needs one cell for each persona that sees rows {sorted(visible)}, "
                f"got {sorted(access)}"
            )
        if bad := {r: o for r, o in access.items() if o not in COLUMN_OUTCOMES}:
            raise ContractError(f"{name}.{col_name}: unknown column outcome(s) {bad}")
        columns[col_name] = Column(
            name=col_name,
            access=access,
            phi_type=cell.get("phi_type"),
            declares_phi_type="phi_type" in cell,
        )
    key = tuple(spec["key"])
    if missing := set(key) - set(columns):
        raise ContractError(f"{name}: key column(s) {sorted(missing)} are not listed in columns")
    return AccessObject(name, key, rows, columns, facility_column)


def load_contract(path: Path = CONTRACT_PATH) -> Contract:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw.get("version") != 1:
        raise ContractError(f"{path}: expected contract version 1, got {raw.get('version')!r}")
    personas = {role: _persona(role, spec) for role, spec in raw["personas"].items()}
    objects = {name: _object(name, spec, personas) for name, spec in raw["objects"].items()}
    return Contract(personas, objects)
