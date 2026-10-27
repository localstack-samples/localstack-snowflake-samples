"""Deployment and tenant provisioning, run with the admin identity.

Used by `make seed`, `make demo` and the test suite; the API never imports it.
Everything here is synchronous: it is batch tooling, not the request path.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas

from app.settings import SAMPLE_ROOT, Settings
from app.tenancy import admin_role, viewer_role

INIT_DIR = SAMPLE_ROOT / "init"
# Init files keep account-level setup above this marker and database-relative
# DDL below it; the test suite deploys the part below into its own databases.
DEPLOY_MARKER = "-- @deploy:"
SCHEMA = "APP"
GRANTS_FILE = "04_grants.sf.sql"
# Load order matters only for readability; there are no enforced foreign keys.
TENANT_TABLES = ("MERCHANTS", "CUSTOMERS", "ORDERS", "LINE_ITEMS")

IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{0,254}$")


def _identifier(name: str) -> str:
    """Database names come from code and fixtures, never from requests; check anyway."""
    if not IDENTIFIER_PATTERN.fullmatch(name):
        raise ValueError(f"unsafe identifier {name!r}")
    return name


def query(conn: SnowflakeConnection, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []


def run_script(conn: SnowflakeConnection, sql: str) -> None:
    for cur in conn.execute_string(sql, remove_comments=True):
        cur.close()


def deployable_sql(*, only: str | None = None) -> str:
    """Database-relative DDL from init/*.sf.sql (everything below the marker), in file order."""
    parts = []
    for path in sorted(INIT_DIR.glob("*.sf.sql")):
        if only is not None and path.name != only:
            continue
        text = path.read_text(encoding="utf-8")
        if DEPLOY_MARKER not in text:
            raise RuntimeError(f"{path.name} has no '{DEPLOY_MARKER}' marker")
        parts.append(text.split(DEPLOY_MARKER, 1)[1].split("\n", 1)[1])
    return "\n".join(parts)


def apply_grants(conn: SnowflakeConnection, database: str) -> None:
    """Database-level grants (a clone never copies them) plus the grants file's schema/table grants."""
    database = _identifier(database)
    query(conn, f"USE DATABASE {database}")
    for role in ("TENANT_READER", "PLATFORM_ADMIN"):
        query(conn, f"GRANT USAGE ON DATABASE {database} TO ROLE {role}")
    run_script(conn, deployable_sql(only=GRANTS_FILE))


def deploy_database(conn: SnowflakeConnection, database: str) -> None:
    """Create `database` from scratch with every table, policy and grant in it."""
    database = _identifier(database)
    query(conn, f"CREATE OR REPLACE DATABASE {database}")
    query(conn, f"USE DATABASE {database}")
    run_script(conn, deployable_sql())
    apply_grants(conn, database)


def drop_database(conn: SnowflakeConnection, database: str) -> None:
    query(conn, f"DROP DATABASE IF EXISTS {_identifier(database)}")


def roles_granted_to_user(conn: SnowflakeConnection, user: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(f"SHOW GRANTS TO USER {_identifier(user)}")
        columns = [d.name.lower() for d in cur.description]
        index = next(columns.index(c) for c in ("role", "name") if c in columns)
        return {row[index] for row in cur.fetchall()}


def provision_tenants(conn: SnowflakeConnection, settings: Settings, tenant_ids: Iterable[str]) -> int:
    """Create each tenant's viewer and admin roles and grant both to the app's service user.

    Idempotent and incremental: tenants whose admin role is already granted to
    the service user (the last step) are skipped. The rest are sent as
    multi-statement requests, a batch of tenants per round trip. Returns the
    number of tenants provisioned.
    """
    app_user = _identifier(settings.tb_app_user)
    granted = roles_granted_to_user(conn, app_user)
    pending = [t for t in tenant_ids if admin_role(t) not in granted or viewer_role(t) not in granted]
    statements: list[str] = []
    for tenant_id in pending:
        viewer, admin = viewer_role(tenant_id), admin_role(tenant_id)
        statements += [
            f"CREATE ROLE IF NOT EXISTS {viewer}",
            f"CREATE ROLE IF NOT EXISTS {admin}",
            f"GRANT ROLE TENANT_READER TO ROLE {viewer}",
            f"GRANT ROLE {viewer} TO ROLE {admin}",
            f"GRANT ROLE TENANT_ADMIN TO ROLE {admin}",
            f"GRANT ROLE {viewer} TO USER {app_user}",
            f"GRANT ROLE {admin} TO USER {app_user}",
        ]
    batch = 70
    with conn.cursor() as cur:
        for start in range(0, len(statements), batch):
            chunk = statements[start : start + batch]
            cur.execute(";\n".join(chunk), num_statements=len(chunk))
            while cur.nextset():
                pass
    return len(pending)


def load_tables(conn: SnowflakeConnection, database: str, frames: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Replace the contents of each table with its frame (TRUNCATE, then write_pandas)."""
    database = _identifier(database)
    loaded = {}
    for table, frame in frames.items():
        table = _identifier(table)
        query(conn, f"TRUNCATE TABLE {database}.{SCHEMA}.{table}")
        result = write_pandas(
            conn, frame, table, database=database, schema=SCHEMA, use_logical_type=True, quote_identifiers=True
        )
        if not result.success:
            raise RuntimeError(f"write_pandas into {table} failed: {result.copy_results}")
        loaded[table] = result.nrows
    return loaded


@dataclass(frozen=True)
class PolicyRef:
    kind: str  # ROW_ACCESS_POLICY | MASKING_POLICY
    database: str
    schema: str
    name: str
    column: str | None  # masking: the protected column
    arg_columns: tuple[str, ...]  # row access: the policy's argument columns


def policy_refs(conn: SnowflakeConnection, database: str, table: str) -> list[PolicyRef]:
    database, table = _identifier(database), _identifier(table)
    rows = query(
        conn,
        f"SELECT POLICY_KIND, POLICY_DB, POLICY_SCHEMA, POLICY_NAME, REF_COLUMN_NAME, REF_ARG_COLUMN_NAMES "
        f"FROM TABLE({database}.INFORMATION_SCHEMA.POLICY_REFERENCES(REF_ENTITY_NAME => ?, REF_ENTITY_DOMAIN => 'table'))",
        [f"{database}.{SCHEMA}.{table}"],
    )
    return [
        PolicyRef(kind, db, schema, name, column, tuple(json.loads(args)) if args else ())
        for kind, db, schema, name, column, args in rows
    ]


def clone_database(conn: SnowflakeConnection, source: str, target: str) -> None:
    """CREATE DATABASE target CLONE source, provisioned with the source's policies and grants.

    Every policy reference the source's tables carry (POLICY_REFERENCES) is
    ensured on the clone, pointing at the clone's own copy of the policy, and
    the grants are applied (a clone never copies grants on the database
    itself). Each test database therefore matches production exactly.

    The step is driven by the *source's* references, not by a list in code: a
    source table without a policy yields a clone without one, so the leak test
    fails when the policy is removed from init/.
    """
    source, target = _identifier(source), _identifier(target)
    query(conn, f"CREATE OR REPLACE DATABASE {target} CLONE {source}")
    query(conn, f"USE DATABASE {target}")
    # The clone's own copy of every policy (CREATE ... IF NOT EXISTS).
    run_script(conn, deployable_sql(only="02_policies.sf.sql"))
    statements: list[str] = []
    for table in TENANT_TABLES:
        have = policy_refs(conn, target, table)
        for ref in policy_refs(conn, source, table):
            own = PolicyRef(ref.kind, target, ref.schema, ref.name, ref.column, ref.arg_columns)
            if own in have:
                continue
            policy = f"{target}.{_identifier(ref.schema)}.{_identifier(ref.name)}"
            fqtn = f"{target}.{SCHEMA}.{table}"
            if ref.kind == "ROW_ACCESS_POLICY":
                cols = ", ".join(_identifier(c) for c in ref.arg_columns)
                statements.append(f"ALTER TABLE {fqtn} ADD ROW ACCESS POLICY {policy} ON ({cols})")
            elif ref.kind == "MASKING_POLICY" and ref.column:
                column = _identifier(ref.column)
                if any(h.kind == ref.kind and h.column == ref.column for h in have):
                    statements.append(f"ALTER TABLE {fqtn} MODIFY COLUMN {column} UNSET MASKING POLICY")
                statements.append(f"ALTER TABLE {fqtn} MODIFY COLUMN {column} SET MASKING POLICY {policy}")
            else:
                raise RuntimeError(f"unexpected policy reference {ref}")
    for statement in statements:
        query(conn, statement)
    apply_grants(conn, target)
